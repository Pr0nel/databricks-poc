# pyspark-jobs/01_spark_kafka_consumer.py
"""
Spark Streaming Consumer: Lee eventos de Kafka + escribe a Delta (local)
- Conecta a Kafka
- Ingesta eventos en streaming
- Validación de schema
- Escribe a tablas Delta (local) de forma append continua
- no escribe a S3 (Parquet)(eso lo hace 02_spark_delta_to_s3.py)
Propósito: Ingesta rápida y confiable en tiempo real
Durabilidad: Delta + Checkpoints
"""
import os
import time
import socket
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, current_timestamp)
from pyspark.sql.types import (StructType, StructField, StringType, LongType, IntegerType)
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BRONZE,
    SPARK_APP_NAME,
    SPARK_LOG_LEVEL
)
from config.logging_config import setup_logging

logger = setup_logging("spark_kafka_consumer")

class SparkKafkaConsumer:
    def __init__(self):
        self.spark = None
        self.checkpoint_path_delta = "spark_checkpoints/delta_consumer"
        self.checkpoint_path_s3 = "spark_checkpoints/s3_consumer"
        self.delta_table_path = "delta_tables/events_raw"

    def _is_kafka_available(self, timeout=2):
        """Health check: verificar que Kafka está disponible (socket check)"""
        try:
            host, port = KAFKA_BOOTSTRAP_SERVERS.split(':')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port))) == 0
            sock.close()
            return result
        except Exception as e:
            logger.warning(f"Health check error: {e}")
            return False
    
    def _wait_for_kafka(self, max_wait=30):
        """Esperar a que Kafka esté disponible"""
        logger.info(f"Esperando a que Kafka esté disponible (máx {max_wait}s)...")
        elapsed = 0
        while elapsed < max_wait:
            if self._is_kafka_available():
                logger.info("Kafka disponible (health check OK)")
                return True
            elapsed += 2
            logger.debug(f"    Kafka no disponible... ({elapsed}s/{max_wait}s)")
            time.sleep(2)
        
        logger.error(f"Kafka no disponible después de {max_wait}s")
        return False

    def _init_spark(self):
        """Inicializar SparkSession con configuración optimizada"""
        logger.info("Inicializando SparkSession...")
        try:
            self.spark = SparkSession.builder \
                .appName(SPARK_APP_NAME) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
                .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.endpoint.region", AWS_DEFAULT_REGION) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.sql.streaming.checkpointLocation.skipMissingFiles", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.debug.maxToStringFields", "15") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
            logger.info(f"SparkSession inicializada (v{self.spark.version})")
            return True
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            return False
    
    def _define_schema(self):
        """Definir schema esperado de eventos Kafka"""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("timestamp", LongType(), True)
        ])
        logger.info("Schema definido para eventos")
        return schema
    
    def read_kafka_stream(self):
        """Leer stream de Kafka"""
        logger.info("Conectando a Kafka...")
        logger.info(f"  Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"  Topic: {KAFKA_TOPIC}")
        logger.info(f"  Group ID: {KAFKA_GROUP_ID}")
        if not self._wait_for_kafka(max_wait=30): # Verifica conexión básica y es más rápido que AdminClient
            raise Exception("Kafka no disponible después del health check")
        try:
            # Internamente crea AdminClient y valida topic + particiones
            df_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", KAFKA_AUTO_OFFSET_RESET) \
                .option("maxOffsetsPerTrigger", 100) \
                .load()
            logger.info("Stream de Kafka conectado")
            
            # PASO 1: Extraer metadata de Kafka ANTES de parsear JSON
            # En este punto df_stream tiene estas columnas: key, value, topic, partition, offset, timestamp, timestampType
            df_with_kafka_meta = df_stream.select(
                col("partition").alias("kafka_partition"),      # EXTRAE partition
                col("offset").alias("kafka_offset"),            # EXTRAE offset
                col("value").cast("string").alias("value_str")  # Renombra value y aplica cast a string
            )

            # PASO 2: Parsear JSON
            # Resultado: id, value, type, timestamp, kafka_partition, kafka_offset
            schema = self._define_schema()
            df_parsed = df_with_kafka_meta.select(
                from_json(col("value_str"), schema).alias("data"),
                "kafka_partition",  # MANTIENE metadata
                "kafka_offset"      # MANTIENE metadata
            ).select("data.*", "kafka_partition", "kafka_offset")

            # PASO 3: Agregar metadatos timestamp de ingesta
            df_enriched = df_parsed \
                .withColumn("ingested_at", current_timestamp())
            
            logger.info("Stream parseado y con metadatos")
            return df_enriched
            
        except Exception as e:
            logger.error(f"Error leyendo Kafka: {e}")
            raise
    
    def write_to_delta(self, df_stream):
        """Escribir stream a Delta (local)"""
        logger.info("Configurando escritura a Delta...")
        logger.info(f"  Tabla: {self.delta_table_path}")
        logger.info(f"  Checkpoint: {self.checkpoint_path_delta}")
        try:
            # Crear directorio checkpoint si no existe
            os.makedirs(self.checkpoint_path_delta, exist_ok=True)
            query = df_stream.writeStream \
                .format("delta") \
                .option("checkpointLocation", self.checkpoint_path_delta) \
                .option("mergeSchema", "true") \
                .start(self.delta_table_path)
            logger.info("Escritura a Delta iniciada")
            return query
        except Exception as e:
            logger.error(f"Error configurando Delta: {e}")
            raise
    
    def write_to_s3(self, df_stream):
        """Escribir stream a S3 (Parquet particionado por fecha)"""
        logger.info("Configurando escritura a S3...")
        logger.info(f"  Path: {S3_BRONZE}")
        logger.info(f"  Formato: Parquet")
        logger.info(f"  Checkpoint: {self.checkpoint_path_s3}")
        try:
            # Crear directorio checkpoint si no existe
            os.makedirs(self.checkpoint_path_s3, exist_ok=True)
            # Particionar por fecha
            df_with_date = df_stream.withColumn(
                "ingestion_date",
                col("ingested_at").cast("date")
            )
            
            query = df_with_date.writeStream \
                .format("parquet") \
                .option("checkpointLocation", self.checkpoint_path_s3) \
                .option("path", S3_BRONZE) \
                .partitionBy("ingestion_date") \
                .start()
            
            logger.info("Escritura a S3 iniciada")
            return query
            
        except Exception as e:
            logger.error(f"Error configurando S3: {e}")
            raise
    
    def run(self, duration_seconds=60):
        """Ejecutar consumer por duración especificada"""
        logger.info("SPARK KAFKA CONSUMER - INICIANDO")
        try:
            # 1. Inicializar Spark
            if not self._init_spark():
                raise Exception("Spark initialization failed")
            # 2. Leer stream de Kafka
            df_stream = self.read_kafka_stream()
            # 3. Escribir a Delta
            query_delta = self.write_to_delta(df_stream)
            # 4. Escribir a S3
            query_s3 = self.write_to_s3(df_stream)
            
            logger.info(f"Esperando {duration_seconds} segundos para procesar eventos...")
            time.sleep(duration_seconds) # Esperar duración configurada
            
            # Detener queries
            logger.info("Deteniendo streams...")
            query_delta.stop()
            query_s3.stop()
            
            # Validar que se procesaron datos
            logger.info("Validando datos procesados...")
            df_delta = self.spark.read.format("delta").load(self.delta_table_path)
            row_count = df_delta.count()
            logger.info(f"  Filas en Delta: {row_count}")
            if row_count > 0:
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