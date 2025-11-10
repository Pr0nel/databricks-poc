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

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull,
    min as spark_min, max as spark_max, avg, stddev, current_timestamp
)
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
        self.validation_results = {}
    
    def _init_spark(self):
        """Inicializar SparkSession con S3 credentials"""
        logger.info("Inicializando SparkSession con acceso a S3...")
        
        try:
            self.spark = SparkSession.builder \
                .appName(f"{SPARK_APP_NAME}-s3-validator") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
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
    
    def read_s3_data(self):
        """Leer datos de S3"""
        logger.info(f"Leyendo datos de S3: {self.s3_path}")
        
        try:
            df = self.spark.read.parquet(self.s3_path)
            count_rows = df.count()
            logger.info(f"  ✅ Archivos leídos exitosamente")
            logger.info(f"  Total de filas: {count_rows}")
            return df
            
        except Exception as e:
            logger.error(f"Error leyendo S3: {e}")
            logger.info("  💡 Asegúrate de ejecutar 02_spark_delta_to_s3.py primero")
            return None
    
    def show_schema(self, df):
        """Mostrar schema de los datos"""
        logger.info("\n" + "=" * 60)
        logger.info("SCHEMA DE LOS DATOS")
        logger.info("=" * 60)
        df.printSchema()
    
    def show_sample_data(self, df, num_rows=10):
        """Mostrar datos de ejemplo"""
        logger.info("\n" + "=" * 60)
        logger.info(f"PRIMERAS {num_rows} FILAS")
        logger.info("=" * 60)
        df.show(num_rows, truncate=False)
    
    def data_quality_checks(self, df):
        """Ejecutar quality checks completos"""
        logger.info("\n" + "=" * 60)
        logger.info("DATA QUALITY CHECKS")
        logger.info("=" * 60)
        
        try:
            total_rows = df.count()
            
            # CHECK 1: Nulos
            logger.info("\n[1/5] Conteo de valores nulos por columna...")
            null_counts = df.select([
                count(when(col(c).isNull(), 1)).alias(f"null_{c}")
                for c in df.columns
            ]).collect()[0]
            
            for col_name in df.columns:
                null_count = getattr(null_counts, f"null_{col_name}")
                null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
                status = "⚠️ " if null_count > 0 else "✅"
                logger.info(f"   {status} {col_name}: {null_count} ({null_pct:.2f}%)")
            
            # CHECK 2: Estadísticas numéricas
            logger.info("\n[2/5] Estadísticas de columnas numéricas...")
            numeric_cols = [f.name for f in df.schema.fields 
                          if f.dataType.typeName() in ['integer', 'long', 'double', 'float']]
            
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
            logger.info("\n[3/5] Detección de duplicados...")
            if "id" in df.columns:
                duplicates = df.groupBy("id").count().filter(col("count") > 1)
                dup_count = duplicates.count()
                if dup_count > 0:
                    logger.warning(f"   ⚠️  {dup_count} IDs duplicados encontrados")
                    duplicates.show(5)
                else:
                    logger.info(f"   ✅ Sin duplicados detectados")
            else:
                logger.info("   Columna 'id' no existe, skip")
            
            # CHECK 4: Distribución de datos
            logger.info("\n[4/5] Distribución de datos...")
            if "ingestion_date" in df.columns:
                date_dist = df.groupBy("ingestion_date").count().orderBy("ingestion_date")
                logger.info("   Registros por fecha de ingesta:")
                date_dist.show(10, truncate=False)
            else:
                logger.info("   Columna 'ingestion_date' no existe, skip")
            
            # CHECK 5: Resumen
            logger.info("\n[5/5] Resumen general...")
            logger.info(f"   Total de filas: {total_rows}")
            logger.info(f"   Total de columnas: {len(df.columns)}")
            logger.info(f"   Particiones Spark: {df.rdd.getNumPartitions()}")
            
            logger.info("\n✅ Data Quality Checks completados")
            self.validation_results['quality_checks'] = 'PASSED'
            
        except Exception as e:
            logger.error(f"Error en quality checks: {e}")
            self.validation_results['quality_checks'] = 'FAILED'
    
    def validate_schema_evolution(self, df):
        """Validar schema para detectar cambios"""
        logger.info("\n" + "=" * 60)
        logger.info("SCHEMA EVOLUTION VALIDATION")
        logger.info("=" * 60)
        
        # Columnas esperadas
        expected_columns = {"id", "value", "type", "timestamp", "ingested_at"}
        actual_columns = set(df.columns) - {"ingestion_date"}  # Excluir columna de partición
        
        logger.info(f"  Columnas esperadas: {sorted(expected_columns)}")
        logger.info(f"  Columnas actuales:  {sorted(actual_columns)}")
        
        missing = expected_columns - actual_columns
        new_columns = actual_columns - expected_columns
        
        if missing:
            logger.warning(f"  ⚠️  Faltan columnas: {missing}")
            self.validation_results['schema_evolution'] = 'INCOMPLETE'
        
        if new_columns:
            logger.info(f"  ℹ️  Nuevas columnas (schema evolution): {new_columns}")
            self.validation_results['schema_evolution'] = 'EVOLVED'
        
        if not missing and not new_columns:
            logger.info(f"  ✅ Schema exactamente como esperado")
            self.validation_results['schema_evolution'] = 'PASSED'
    
    def generate_report(self):
        """Generar reporte final"""
        logger.info("\n" + "=" * 60)
        logger.info("VALIDATION REPORT")
        logger.info("=" * 60)
        
        for check, result in self.validation_results.items():
            status = "✅" if result == "PASSED" else "⚠️ " if result in ["EVOLVED", "INCOMPLETE"] else "❌"
            logger.info(f"{status} {check}: {result}")
        
        logger.info("=" * 60)
    
    def run(self):
        """Ejecutar validación completa"""
        logger.info("=" * 80)
        logger.info("SPARK S3 VALIDATOR - INICIANDO")
        logger.info("=" * 80)
        
        try:
            # 1. Inicializar Spark
            if not self._init_spark():
                raise Exception("Spark initialization failed")
            
            # 2. Leer datos de S3
            df = self.read_s3_data()
            if df is None:
                logger.warning("⚠️  No se encontraron datos en S3")
                logger.info("💡 Ejecuta 02_spark_delta_to_s3.py primero")
                return False
            
            # 3. Ejecutar validaciones
            self.show_schema(df)
            self.show_sample_data(df, num_rows=10)
            self.data_quality_checks(df)
            self.validate_schema_evolution(df)
            self.generate_report()
            
            logger.info("=" * 80)
            logger.info("SPARK S3 VALIDATOR - SUCCESS")
            logger.info("=" * 80)
            return True
            
        except Exception as e:
            logger.error(f"SPARK S3 VALIDATOR - FAILED: {e}")
            logger.error("=" * 80)
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