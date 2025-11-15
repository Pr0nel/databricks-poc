# scripts/inspect_type_values.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.logging_config import setup_logging
from utils.spark_utils import SparkSessionFactory

logger = setup_logging("data_inspector")

TARGET_DELTA_TABLE_PATH = "delta_tables/events_raw"

def inspect_data():
    factory = SparkSessionFactory(logger)
    spark = factory.create_spark_session(app_name="DataInspector")

    try:
        logger.info(f"Leyendo datos de: {TARGET_DELTA_TABLE_PATH}")
        df = spark.read.format("delta").load(TARGET_DELTA_TABLE_PATH)
        # Encontrar los valores únicos en la columna 'type'
        type_counts = df.groupBy("type").count().orderBy("count", ascending=False)
        logger.info("\n--- CONTEO DE VALORES ÚNICOS EN LA COLUMNA 'type' ---")
        type_counts.show(n=20, truncate=False) 
    except Exception as e:
        logger.error(f"Error durante la inspección: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession detenida")

if __name__ == "__main__":
    inspect_data()