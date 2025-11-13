# config/settings.py
import os
import re
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

config_path = Path(__file__).parent / "config.yaml"
with open(config_path, "r") as f:
    config_raw = yaml.safe_load(f)

def expand_env_vars(obj):
    """Reemplazar ${VAR} con valores de .env"""
    if isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [expand_env_vars(item) for item in obj]
    elif isinstance(obj, str):
        # Buscar variables como ${VAR_NAME}
        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, "")
        return re.sub(r'\$\{([^}]+)\}', replace_var, obj)
    return obj

config = expand_env_vars(config_raw)

AWS_ACCESS_KEY_ID = config["aws"]["access_key_id"]
AWS_SECRET_ACCESS_KEY = config["aws"]["secret_key"]
AWS_DEFAULT_REGION = config["aws"]["region"]
S3_BUCKET = config["aws"]["s3"]["bucket"]
S3_PATHS = config["aws"]["s3"]["paths"]

S3_BRONZE = f"s3a://{S3_BUCKET}/{S3_PATHS['bronze']}"
S3_SILVER = f"s3a://{S3_BUCKET}/{S3_PATHS['silver']}"
S3_GOLD = f"s3a://{S3_BUCKET}/{S3_PATHS['gold']}"
S3_SCHEMAS = S3_PATHS['schemas']

KAFKA_BOOTSTRAP_SERVERS = config["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_ENCODING = config["kafka"]["encoding"]
KAFKA_GROUP_ID = config["kafka"]["group_id"]
KAFKA_AUTO_OFFSET_RESET = config["kafka"]["auto_offset_reset"]

DATABRICKS_HOST = config["databricks"]["host"]
DATABRICKS_TOKEN = config["databricks"]["token"]

SPARK_APP_NAME = config["spark"]["app_name"]
SPARK_LOG_LEVEL = config["spark"]["log_level"]

PIPELINE_TIMEOUTS = {
    "setup_s3": int(os.getenv("PIPELINE_SETUP_TIMEOUT", config["pipeline"]["timeouts"]["setup_s3"])),
    "kafka_producer": int(os.getenv("PIPELINE_PRODUCER_TIMEOUT", config["pipeline"]["timeouts"]["kafka_producer"])),
    "spark_consumer": int(os.getenv("PIPELINE_CONSUMER_TIMEOUT", config["pipeline"]["timeouts"]["spark_consumer"])),
    "spark_delta": int(os.getenv("PIPELINE_DELTA_TIMEOUT", config["pipeline"]["timeouts"]["spark_delta"])),
    "spark_validator": int(os.getenv("PIPELINE_VALIDATOR_TIMEOUT", config["pipeline"]["timeouts"]["spark_validator"]))
}

PIPELINE_MAX_RETRIES = int(os.getenv("PIPELINE_MAX_RETRIES", config["pipeline"]["max_retries"]))
PIPELINE_RETRY_DELAY_BASE = int(os.getenv("PIPELINE_RETRY_DELAY_BASE", config["pipeline"]["retry_delay_base"]))

def validate_config():
    """Validar que todas las configuraciones necesarias estén presentes"""
    required = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "S3_BUCKET": S3_BUCKET,
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Algunos elementos de config no se encuentran: {missing}")
    return True

if __name__ == "__main__":
    validate_config()
    print("Configuration validated")
    print(f"   S3 Bucket: {S3_BUCKET}")
    print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Databricks: {DATABRICKS_HOST}")