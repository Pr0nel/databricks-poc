# pyspark-jobs/02_spark_write_delta_s3.py
"""
Spark Batch Job: Delta → S3
- Lee Delta Table (batch, no streaming)
- Validaciones básicas
- Escribe a S3 (Parquet, particionado por fecha)
- Propósito: Persistir datos de Delta a S3 para durabilidad y Auto Loader (ETAPA 4)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql.functions import col, current_date
from utils import SparkSessionFactory, Schemas, DataValidator
from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BRONZE,
    SPARK_APP_NAME,
    SPARK_LOG_LEVEL
)
from config.logging_config import setup_logging

logger = setup_logging("spark_write_delta_s3")

class DeltaToS3Writer:
    def __init__(self):
        self.spark = None
        self.delta_path = "delta_tables/events_raw"
        self.s3_path = S3_BRONZE
        self.spark_factory = SparkSessionFactory(logger)
        self.data_validator = DataValidator(logger)
    
    def _init_spark(self):
        """Inicializar SparkSession con acceso a S3"""
        logger.info("Inicializando SparkSession con acceso a S3...")
        try:
            aws_creds = {
                "access_key": AWS_ACCESS_KEY_ID,
                "secret_key": AWS_SECRET_ACCESS_KEY,
                "region": AWS_DEFAULT_REGION
            }
            self.spark = self.spark_factory.create_spark_session(
                app_name=f"{SPARK_APP_NAME}-write-delta-s3",
                enable_s3=True,
                aws_credentials=aws_creds
            )
            self.spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
            logger.info(f"SparkSession inicializada con S3 access")
            return True
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            return False
    
    def read_delta(self):
        """Leer tabla Delta (batch read, no streaming)"""
        logger.info(f"Leyendo tabla Delta: {self.delta_path}")
        try:
            df = self.spark.read.format("delta").load(self.delta_path)
            stats = self.data_validator.get_data_statistics(df)
            row_count = stats['total_rows']
            logger.info(f"  Tabla Delta leída")
            logger.info(f"  Total de filas: {row_count}")
            if row_count == 0:
                logger.warning("  Delta table está vacía")
                return None
            return df
        except Exception as e:
            logger.error(f"Error leyendo Delta: {e}")
            return None
    
    def validate_data(self, df):
        """Validaciones básicas de datos"""
        logger.info("Ejecutando validaciones...")
        try:
            self.data_validator.check_nulls(df, Schemas.get_critical_columns())
            _ = self.data_validator.get_data_statistics(df)
            logger.info("Validaciones completadas")
            return True
        except Exception as e:
            logger.warning(f"Error en validaciones: {e}")
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
                    logger.warning("  No hay columna de fecha, usando fecha actual")
                    df = df.withColumn("ingestion_date", current_date())
            # Escribir a S3
            df.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("ingestion_date") \
                .save(self.s3_path)
            logger.info(f"  Datos escritos a S3 exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error escribiendo a S3: {e}")
            return False
    
    def run(self):
        """Ejecutar job: Delta → S3"""
        logger.info("DELTA TO S3 BATCH JOB - INICIANDO")
        try:
            # 1. Inicializar Spark
            if not self._init_spark():
                raise Exception("Spark initialization failed")
            # 2. Leer Delta
            df = self.read_delta()
            if df is None:
                logger.warning("No se encontraron datos en Delta")
                return False
            # 3. Validar datos
            self.validate_data(df)
            # 4. Escribir a S3
            if not self.write_to_s3(df):
                raise Exception("Failed writing to S3")
            logger.info("DELTA TO S3 BATCH JOB - SUCCESS")
            return True
        except Exception as e:
            logger.error(f"DELTA TO S3 BATCH JOB - FAILED: {e}")
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