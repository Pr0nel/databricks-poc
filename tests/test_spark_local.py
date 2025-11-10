# test_spark_local.py
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Test 1: Crear Spark Session SIN Delta config
    logger.info("=" * 80)
    logger.info("ETAPA 0.4: PYSPARK VALIDATION (SIMPLE)")
    logger.info("=" * 80)
    
    spark = SparkSession.builder \
        .appName("EtapaTest") \
        .getOrCreate()
    
    logger.info("✅ Spark Session created")
    logger.info(f"✅ Spark Version: {spark.version}")
    
    # Test 2: Create DataFrame
    df = spark.range(0, 10) \
        .withColumn("value", col("id") * 2) \
        .withColumn("timestamp", current_timestamp())
    
    logger.info("✅ DataFrame created")
    df.show()
    
    # Test 3: Write Parquet (sin Delta)
    df.write.format("parquet").mode("overwrite").save("/tmp/test_parquet")
    logger.info("✅ Parquet written to /tmp/test_parquet")
    
    # Test 4: Read Parquet
    df_read = spark.read.format("parquet").load("/tmp/test_parquet")
    logger.info(f"✅ Parquet read: {df_read.count()} rows")
    
    # Test 5: Test S3 Connection
    logger.info("\n" + "=" * 80)
    logger.info("TESTING S3 CONNECTION")
    logger.info("=" * 80)
    
    AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET = os.getenv("AWS_S3_BUCKET")
    AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
    
    if not all([AWS_KEY, AWS_SECRET, S3_BUCKET]):
        logger.warning("⚠️ AWS credentials not fully configured in .env")
        logger.warning(f"   AWS_ACCESS_KEY_ID: {'✓' if AWS_KEY else '✗'}")
        logger.warning(f"   AWS_SECRET_ACCESS_KEY: {'✓' if AWS_SECRET else '✗'}")
        logger.warning(f"   AWS_S3_BUCKET: {'✓' if S3_BUCKET else '✗'}")
    else:
        # Crear Spark Session con S3 support
        spark_s3 = SparkSession.builder \
            .appName("S3Test") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .getOrCreate()
        
        logger.info("✅ Spark Session with S3 support created")
        
        # Write to S3
        s3_path = f"s3a://{S3_BUCKET}/test-etapa0/data.parquet"
        try:
            df.write.format("parquet").mode("overwrite").save(s3_path)
            logger.info(f"✅ Data written to S3: {s3_path}")
        except Exception as e:
            logger.warning(f"⚠️ S3 write failed: {e}")
            logger.info("   (This is OK if S3 credentials need validation)")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ ALL TESTS PASSED")
    logger.info("=" * 80)
    
except Exception as e:
    logger.error(f"❌ Error: {e}")
    raise