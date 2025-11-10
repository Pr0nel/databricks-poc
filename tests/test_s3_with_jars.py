# test_s3_with_jars.py
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

try:
    logger.info("=" * 80)
    logger.info("TESTING S3 CONNECTION WITH JARS")
    logger.info("=" * 80)
    
    if not all([AWS_KEY, AWS_SECRET, S3_BUCKET]):
        logger.error("❌ AWS credentials missing in .env")
        logger.error(f"   AWS_ACCESS_KEY_ID: {'✓' if AWS_KEY else '✗'}")
        logger.error(f"   AWS_SECRET_ACCESS_KEY: {'✓' if AWS_SECRET else '✗'}")
        logger.error(f"   AWS_S3_BUCKET: {'✓' if S3_BUCKET else '✗'}")
        raise ValueError("Missing AWS credentials")
    
    logger.info(f"✅ AWS credentials loaded")
    logger.info(f"   Bucket: {S3_BUCKET}")
    logger.info(f"   Region: {AWS_REGION}")
    
    # Crear Spark Session CON packages de maven
    spark = SparkSession.builder \
        .appName("S3Test") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.261") \
        .getOrCreate()
    
    logger.info("✅ Spark Session with S3 support created")
    
    # Test 1: Create test DataFrame
    df = spark.range(0, 5) \
        .withColumn("value", col("id") * 100) \
        .withColumn("test", lit("s3-test"))
    
    logger.info("✅ Test DataFrame created")
    
    # Test 2: Write to S3
    s3_path = f"s3a://{S3_BUCKET}/test-etapa0/spark-test.parquet"
    logger.info(f"📝 Writing to: {s3_path}")
    
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(s3_path)
    
    logger.info(f"✅ Data written to S3 successfully!")
    
    # Test 3: Read from S3
    logger.info(f"📖 Reading from: {s3_path}")
    df_read = spark.read.format("parquet").load(s3_path)
    count = df_read.count()
    
    logger.info(f"✅ Data read from S3 successfully! ({count} rows)")
    df_read.show()
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ S3 CONNECTION TEST PASSED")
    logger.info("=" * 80)
    
except Exception as e:
    logger.error(f"❌ Error: {type(e).__name__}: {e}")
    import traceback
    logger.error(traceback.format_exc())
    raise