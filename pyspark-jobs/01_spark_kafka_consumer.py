# pyspark-jobs/01_spark_kafka_consumer.py
"""
Spark Streaming Consumer: Lee eventos de Kafka + escribe a Delta (local)
- Conecta a Kafka
- Ingesta eventos en streaming
- Validación de schema
- Escribe a tablas Delta (local) de forma append continua
Propósito: Ingesta rápida y confiable en tiempo real
Durabilidad: Delta + Checkpoints
"""
import os
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql.functions import (from_json, col, current_timestamp, decode)
from utils import (HealthCheck, SparkSessionFactory, Schemas, DataValidator)
from utils.encoding_utils import MessageEncoder
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    SPARK_APP_NAME,
    SPARK_LOG_LEVEL
)
from config.logging_config import setup_logging

logger = setup_logging("spark_kafka_consumer")

class SparkKafkaConsumer:
    def __init__(self, encoding="utf-8"):
        self.spark = None
        self.checkpoint_path_delta = "spark_checkpoints/delta_consumer"
        self.delta_table_path = "delta_tables/events_raw"
        self.health_check = HealthCheck(logger)
        self.spark_factory = SparkSessionFactory(logger)
        self.data_validator = DataValidator(logger)
        self.encoder = MessageEncoder(encoding, logger)

    def _init_spark(self):
        """Inicializar SparkSession usando factory"""
        try:
            self.spark = self.spark_factory.create_spark_session(app_name=SPARK_APP_NAME, enable_s3=False)
            self.spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
            logger.info(f"SparkSession inicializada (v{self.spark.version})")
            return True
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            return False
    
    def read_kafka_stream(self):
        """Leer stream de Kafka en tiempo real"""
        logger.info("Conectando a Kafka...")
        logger.info(f"  Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"  Topic: {KAFKA_TOPIC}")
        logger.info(f"  Group ID: {KAFKA_GROUP_ID}")
        host, port = KAFKA_BOOTSTRAP_SERVERS.split(':') # extraer host y puerto en string
        success, elapsed = self.health_check.wait_for_kafka(host, int(port), max_wait=30)
        if not success:
            raise Exception(f"Kafka no disponible después de {elapsed}s")
        try:
            df_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", KAFKA_AUTO_OFFSET_RESET) \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", 100) \
                .load()
            logger.info("Stream de Kafka conectado")

            # Extraer metadata de Kafka
            df_with_kafka_meta = df_stream.select(
                col("partition").alias("kafka_partition"),      # partición de Kafka
                col("offset").alias("kafka_offset"),            # offset dentro de la partición
                decode(col("value").cast("string"), self.encoder.encoding).alias("value_str")  # valor del mensaje como string
            )
            
            # Parsear JSON usando schema centralizado
            schema = Schemas.kafka_event_schema()
            df_parsed = df_with_kafka_meta.select(
                from_json(col("value_str"), schema).alias("data"),
                "kafka_partition",
                "kafka_offset"
            ).select("data.*", "kafka_partition", "kafka_offset")

            # Enriquecer con timestamp de ingesta
            df_enriched = df_parsed \
                .withColumn("ingested_at", current_timestamp())
            logger.info("Stream parseado y enriquecido")
            logger.info(f"  Columnas: {df_enriched.columns}")
            return df_enriched
        except Exception as e:
            logger.error(f"Error leyendo Kafka: {e}")
            raise
    
    def write_to_delta(self, df_stream):
        """
        Escribir stream a Delta LOCAL
        Características:
        - writeStream: Para streaming continuo
        - format("delta"): Formato Delta
        - checkpointLocation: Para exactitud "exactly-once"
        - mergeSchema: Permite schema evolution
        - append mode: Agrega datos continuamente
        """
        logger.info("Configurando escritura a Delta (local)...")
        logger.info(f"  Tabla: {self.delta_table_path}")
        logger.info(f"  Checkpoint: {self.checkpoint_path_delta}")
        logger.info(f"  Modo: APPEND (datos continuos)")
        try:
            os.makedirs(self.checkpoint_path_delta, exist_ok=True)
            # Configurar streaming write
            query = df_stream.writeStream \
                .format("delta") \
                .option("checkpointLocation", self.checkpoint_path_delta) \
                .option("mergeSchema", "true") \
                .outputMode("append") \
                .start(self.delta_table_path)
            logger.info("Escritura a Delta iniciada (streaming)")
            return query
        except Exception as e:
            logger.error(f"Error configurando Delta: {e}")
            raise
    
    def run(self, duration_seconds=120):
        """
        Ejecutar consumer por duración especificada
        Args:
            duration_seconds: Cuánto tiempo debe correr (default: 120 segundos)
        Propósito: Ingestar eventos en streaming, escribir a Delta
        """
        logger.info("SPARK KAFKA CONSUMER - INICIANDO")
        logger.info(f"Duración: {duration_seconds} segundos\n")
        try:
            # 1. Inicializar Spark
            if not self._init_spark():
                raise Exception("Spark initialization failed")
            # 2. Leer stream de Kafka
            df_stream = self.read_kafka_stream()
            # 3. Escribir a Delta LOCAL
            query_delta = self.write_to_delta(df_stream)
            # 4. Esperar duración configurada
            logger.info(f"  Esperando {duration_seconds} segundos para procesar eventos...")
            logger.info("   (El streaming está ingiriendo datos continuamente a Delta)\n")
            time.sleep(duration_seconds)
            # 5. Detener query
            logger.info("Deteniendo stream...")
            query_delta.stop()
            # 6. Validar usando DataValidator
            logger.info("Validando datos procesados...")
            df_delta = self.spark.read.format("delta").load(self.delta_table_path)
            logger.info("VALIDACIONES Y ESTADÍSTICAS")
            _ = self.data_validator.check_nulls(df_delta, Schemas.get_critical_columns())
            stats = self.data_validator.get_data_statistics(df_delta)
            if stats['total_rows'] > 0:
                logger.info("SPARK KAFKA CONSUMER - SUCCESS")
                return True
            else:
                logger.warning("No se procesaron datos (posible timeout)")
                return False
                
        except Exception as e:
            logger.error(f"SPARK KAFKA CONSUMER - FAILED: {e}")
            raise
            
        finally:
            if self.spark:
                logger.info("Limpiando Spark resources...")
                self.spark.stop()

def main():
    """Main entry point"""
    consumer = SparkKafkaConsumer()
    success = consumer.run(duration_seconds=120)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()