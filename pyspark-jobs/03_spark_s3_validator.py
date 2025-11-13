# pyspark-jobs/03_spark_s3_validator.py
"""
Spark Batch Job: S3 Validation & Analytics
- Lee datos Parquet de S3 (bronze layer)
- Ejecuta quality checks
- Genera métricas de calidad
- Detecta anomalías
- Propósito: Validar datos antes de transformaciones Silver/Gold
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, count, min as spark_min, max as spark_max, avg, stddev)
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

logger = setup_logging("spark_s3_validator")

class SparkS3Validator:
    def __init__(self):
        self.spark = None
        self.s3_path = S3_BRONZE
        self.spark_factory = SparkSessionFactory(logger)
        self.data_validator = DataValidator(logger)
        self.validation_results = {
            'quality_checks': 'NOT_RUN',
            'schema_evolution': 'NOT_RUN'
        }
    
    def _init_spark(self):
        """Inicializar SparkSession con S3 credentials"""
        logger.info("Inicializando SparkSession con acceso a S3...")
        try:
            aws_creds = {
                "access_key": AWS_ACCESS_KEY_ID,
                "secret_key": AWS_SECRET_ACCESS_KEY,
                "region": AWS_DEFAULT_REGION
            }
            self.spark = self.spark_factory.create_spark_session(
                app_name=f"{SPARK_APP_NAME}-s3-validator",
                enable_s3=True,
                aws_credentials=aws_creds
            )
            self.spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
            logger.info(f"SparkSession inicializada (v{self.spark.version})")
            return True
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            return False
    
    def read_s3_data(self):
        """Leer datos de S3"""
        logger.info(f"Leyendo datos de S3: {self.s3_path}")
        try:
            df = self.spark.read.parquet(self.s3_path)
            count_rows = df.count()
            logger.info(f"  Archivos leídos exitosamente")
            logger.info(f"  Total de filas: {count_rows}")
            return df
        except Exception as e:
            logger.error(f"Error leyendo S3: {e}")
            return None
    
    def show_schema(self, df: DataFrame):
        """Mostrar schema de los datos"""
        logger.info("SCHEMA DE LOS DATOS")
        df.printSchema()
    
    def show_sample_data(self, df: DataFrame, num_rows: int = 10):
        """Mostrar datos de ejemplo"""
        logger.info(f"PRIMERAS {num_rows} FILAS")
        df.show(num_rows, truncate=False)
    
    def data_quality_checks(self, df: DataFrame):
        """Ejecutar quality checks completos"""
        logger.info("DATA QUALITY CHECKS")
        try:
            # CHECK 1: Nulos (usando DataValidator)
            logger.info("[1/5] Validando nulos...")
            self.data_validator.check_nulls(df, Schemas.get_critical_columns())
            # CHECK 2: Estadísticas numéricas
            logger.info("[2/5] Estadísticas de columnas numéricas...")
            numeric_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in ['integer', 'long', 'double', 'float']]
            if numeric_cols:
                for col_name in numeric_cols:
                    stats = df.agg(
                        spark_min(col(col_name)).alias("min"),
                        spark_max(col(col_name)).alias("max"),
                        avg(col(col_name)).alias("avg"),
                        stddev(col(col_name)).alias("stddev"),
                        count(col(col_name)).alias("count")
                    ).collect()[0]
                    logger.info(f"   {col_name}:")
                    logger.info(f"      min: {stats['min']}")
                    logger.info(f"      max: {stats['max']}")
                    logger.info(f"      avg: {stats['avg']:.2f}" if stats['avg'] else "      avg: N/A")
                    logger.info(f"      stddev: {stats['stddev']:.2f}" if stats['stddev'] else "      stddev: N/A")
            else:
                logger.info("   No hay columnas numéricas")
            # CHECK 3: Duplicados
            logger.info("[3/5] Detección de duplicados...")
            self.data_validator.check_duplicates_by_id(df)
            # CHECK 4: Distribución de datos
            logger.info("[4/5] Distribución de datos...")
            if "ingestion_date" in df.columns:
                date_dist = df.groupBy("ingestion_date").count().orderBy("ingestion_date")
                logger.info("   Registros por fecha de ingesta:")
                date_dist.show(10, truncate=False)
            else:
                logger.info("   Columna 'ingestion_date' no existe, skip")
            # CHECK 5: Resumen general
            logger.info("[5/5] Resumen general...")
            stats = self.data_validator.get_data_statistics(df)
            logger.info(f"   Total de filas: {stats['total_rows']}")
            logger.info(f"   Particiones: {stats['num_partitions']}")
            logger.info(f"   Columnas: {stats['schema_columns']}")
            logger.info("Data Quality Checks completados")
            self.validation_results['quality_checks'] = 'PASSED'
        except Exception as e:
            logger.error(f"Error en quality checks: {e}")
            self.validation_results['quality_checks'] = 'FAILED'
            raise
    
    def validate_schema_evolution(self, df: DataFrame):
        """Validar schema para detectar cambios"""
        logger.info("SCHEMA EVOLUTION VALIDATION")
        # Columnas esperadas
        expected_columns = {"id", "value", "type", "timestamp", "ingested_at"}
        actual_columns = set(df.columns) - {"ingestion_date"}  # Excluir columna de partición
        logger.info(f"  Columnas esperadas: {sorted(expected_columns)}")
        logger.info(f"  Columnas actuales:  {sorted(actual_columns)}")
        missing = expected_columns - actual_columns
        new_columns = actual_columns - expected_columns
        if missing:
            logger.warning(f"  Faltan columnas: {missing}")
            self.validation_results['schema_evolution'] = 'INCOMPLETE'
        if new_columns:
            logger.info(f"  Nuevas columnas (schema evolution): {new_columns}")
            self.validation_results['schema_evolution'] = 'EVOLVED'
        if not missing and not new_columns:
            logger.info(f"  Schema exactamente como esperado")
            self.validation_results['schema_evolution'] = 'PASSED'
    
    def generate_report(self):
        """Generar reporte final"""
        logger.info("VALIDATION REPORT")
        for check, result in self.validation_results.items():
            status_map = {
                "PASSED": "PASS",
                "EVOLVED": "WARN",
                "INCOMPLETE": "WARN",
                "FAILED": "FAIL",
                "NOT_RUN": "SKIP"
            }
            status = status_map.get(result, "UNKNOWN")
            logger.info(f"[{status:5}] {check:20} : {result}")
    
    def run(self):
        """Ejecutar validación completa"""
        logger.info("SPARK S3 VALIDATOR - INICIANDO")
        try:
            # 1. Inicializar Spark
            if not self._init_spark():
                raise Exception("Spark initialization failed")
            # 2. Leer datos de S3
            df = self.read_s3_data()
            if df is None:
                logger.warning("No se encontraron datos en S3")
                return False
            # 3. Ejecutar validaciones
            self.show_schema(df)
            self.show_sample_data(df, num_rows=10)
            self.data_quality_checks(df)
            self.validate_schema_evolution(df)
            self.generate_report()
            logger.info("SPARK S3 VALIDATOR - SUCCESS")
            return True
        except Exception as e:
            logger.error(f"SPARK S3 VALIDATOR - FAILED: {e}")
            return False
        finally:
            if self.spark:
                logger.info("Limpiando Spark resources...")
                self.spark.stop()

def main():
    """Main entry point"""
    validator = SparkS3Validator()
    success = validator.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()