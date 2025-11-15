# utils/spark_utils.py
"""
Spark Utils - Factory para crear SparkSession con configuración centralizada
Evita duplicación de configuración Spark en cada job y facilita cambios globales en configuración.
"""
import logging
from pyspark.sql import SparkSession
from typing import Optional

class SparkSessionFactory:
    """
    Factory para crear SparkSession con configuración centralizada
    Ventajas:
    - Una única fuente de verdad para config Spark
    - Fácil cambiar configuración globalmente
    - Reduce duplicación en múltiples jobs
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def create_spark_session(self,
                             app_name: str,
                             enable_s3: bool = False,
                             aws_credentials: Optional[dict] = None) -> SparkSession:
        """
        Crear SparkSession con configuración estándar
        Args:
            app_name: Nombre de la aplicación
            enable_s3: Si habilitar acceso a S3
            aws_credentials: Dict con access_key, secret_key, region
        Returns:
            SparkSession configurada
        """
        self.logger.info(f"Creando SparkSession: {app_name}")
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"  # kafka
                   "io.delta:delta-spark_2.12:3.1.0,"                   # delta lake
                   "org.apache.hadoop:hadoop-aws:3.3.4,"                # hadoop aws
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.debug.maxToStringFields", "15")
        
        # Agregar configuración S3 si es necesario
        if enable_s3 and aws_credentials:
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", aws_credentials.get("access_key")) \
                .config("spark.hadoop.fs.s3a.secret.key", aws_credentials.get("secret_key")) \
                .config("spark.hadoop.fs.s3a.endpoint.region", aws_credentials.get("region")) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                       "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.sql.streaming.checkpointLocation.skipMissingFiles", "true")
        spark = builder.getOrCreate()
        self.logger.info(f"SparkSession creada (v{spark.version})")
        return spark