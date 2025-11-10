# pyspark-jobs/02_spark_delta_to_s3.py
"""
Spark Batch Job: Delta → S3
- Lee Delta Table (batch, no streaming)
- Validaciones básicas
- Escribe a S3 Parquet (particionado por fecha)
- Propósito: Persistir datos de Delta a S3 para durabilidad y Auto Loader (ETAPA 4)
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BRONZE,
    SPARK_APP_NAME,
    SPARK_LOG_LEVEL
)
from config.logging_config import setup_logging

logger = setup_logging("spark_delta_to_s3")

class DeltaToS3Writer:
    def __init__(self):
        self.spark = None
        self.delta_path = "delta_tables/events_raw"
        self.s3_path = S3_BRONZE
    
    def _init_spark(self):
        """Inicializar SparkSession con acceso a S3"""
        logger.info("Inicializando SparkSession con acceso a S3...")
        try:
            self.spark = SparkSession.builder \
                .appName(f"{SPARK_APP_NAME}-delta-to-s3") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
                .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.endpoint.region", AWS_DEFAULT_REGION) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
            logger.info(f"SparkSession inicializada (v{self.spark.version})")
            return True
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            return False
    
    def read_delta(self):
        """Leer tabla Delta (batch read, no streaming)"""
        logger.info(f"Leyendo tabla Delta: {self.delta_path}")
        try:
            df = self.spark.read.format("delta").load(self.delta_path)
            row_count = df.count()
            logger.info(f"  ✅ Tabla Delta leída")
            logger.info(f"  Total de filas: {row_count}")
            
            if row_count == 0:
                logger.warning("  ⚠️  Delta table está vacía")
                return None
            
            return df
        except Exception as e:
            logger.error(f"Error leyendo Delta: {e}")
            logger.info("  💡 Asegúrate de ejecutar 01_spark_kafka_consumer.py primero")
            return None
    
    def validate_data(self, df):
        """Validaciones básicas de datos"""
        logger.info("Ejecutando validaciones...")
        
        try:
            total_rows = df.count()
            
            # Validación 1: Valores nulos en columnas críticas
            logger.info("  [1/3] Conteo de nulos en columnas críticas...")
            critical_cols = ["id", "value", "type", "timestamp"]
            null_check = df.select([
                count(when(col(c).isNull(), 1)).alias(f"null_{c}")
                for c in critical_cols if c in df.columns
            ]).collect()[0]
            
            nulls_found = False
            for col_name in critical_cols:
                if col_name in df.columns:
                    null_count = getattr(null_check, f"null_{col_name}")
                    if null_count > 0:
                        logger.warning(f"    ⚠️  Nulos en {col_name}: {null_count}")
                        nulls_found = True
            
            if not nulls_found:
                logger.info("    ✅ Sin nulos críticos detectados")
            
            # Validación 2: Rango de datos
            logger.info("  [2/3] Validación de rangos...")
            if "id" in df.columns:
                min_id = df.agg({"id": "min"}).collect()[0][0]
                max_id = df.agg({"id": "max"}).collect()[0][0]
                logger.info(f"    ID range: {min_id} - {max_id}")
            
            # Validación 3: Conteo de particiones
            logger.info("  [3/3] Información de particiones...")
            num_partitions = df.rdd.getNumPartitions()
            logger.info(f"    Total filas: {total_rows}")
            logger.info(f"    Particiones: {num_partitions}")
            
            logger.info("✅ Validaciones completadas")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️  Error en validaciones: {e}")
            return True  # No bloquear si hay error en validación
    
    def write_to_s3(self, df):
        """Escribir DataFrame a S3 (parquet, particionado por fecha)"""
        logger.info(f"Escribiendo a S3: {self.s3_path}")
        logger.info(f"  Formato: Parquet")
        logger.info(f"  Particionado por: ingestion_date")
        
        try:
            # Agregar columna de fecha si no existe
            if "ingestion_date" not in df.columns:
                if "ingested_at" in df.columns:
                    df = df.withColumn("ingestion_date", col("ingested_at").cast("date"))
                else:
                    logger.warning("  ⚠️  No hay columna de fecha, usando fecha actual")
                    from pyspark.sql.functions import current_date
                    df = df.withColumn("ingestion_date", current_date())
            
            # Escribir a S3
            df.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("ingestion_date") \
                .save(self.s3_path)
            
            logger.info(f"  ✅ Datos escritos a S3 exitosamente")
            
            # Verificar que se escribió
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(self.s3_path))
            
            logger.info(f"  Archivos en S3: {len(files) if files else 0}")
            return True
            
        except Exception as e:
            logger.error(f"Error escribiendo a S3: {e}")
            return False
    
    def run(self):
        """Ejecutar job: Delta → S3"""
        logger.info("=" * 80)
        logger.info("DELTA TO S3 BATCH JOB - INICIANDO")
        logger.info("=" * 80)
        
        try:
            # 1. Inicializar Spark
            if not self._init_spark():
                raise Exception("Spark initialization failed")
            
            # 2. Leer Delta
            df = self.read_delta()
            if df is None:
                logger.warning("⚠️  No se encontraron datos en Delta")
                return False
            
            # 3. Validar datos
            self.validate_data(df)
            
            # 4. Escribir a S3
            if not self.write_to_s3(df):
                raise Exception("Failed writing to S3")
            
            logger.info("=" * 80)
            logger.info("DELTA TO S3 BATCH JOB - SUCCESS")
            logger.info("=" * 80)
            return True
            
        except Exception as e:
            logger.error(f"DELTA TO S3 BATCH JOB - FAILED: {e}")
            logger.error("=" * 80)
            return False
            
        finally:
            if self.spark:
                logger.info("Limpiando Spark resources...")
                self.spark.stop()

def main():
    """Main entry point"""
    writer = DeltaToS3Writer()
    success = writer.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
