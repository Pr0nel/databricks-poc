# utils/schema_definitions.py
"""
Schema Definitions - Definiciones centralizadas de schemas
Schemas de Kafka, Delta, etc.
"""
from pyspark.sql.types import (StructType, StructField, StringType, LongType, IntegerType)

class Schemas:
    """Definiciones centralizadas de schemas para todo el proyecto"""
    @staticmethod
    def kafka_event_schema() -> StructType:
        """Schema de eventos crudos de Kafka"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("timestamp", LongType(), True)
        ])
    
    @staticmethod
    def enriched_event_schema() -> StructType:
        """Schema después de enriquecimiento en Delta"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True)
        ])
    
    @staticmethod
    def get_expected_columns() -> set:
        """Columnas esperadas para validaciones"""
        return {"id",
                "value",
                "type",
                "timestamp",
                "ingested_at",
                "kafka_partition",
                "kafka_offset"
        }
    
    @staticmethod
    def get_critical_columns() -> list:
        """Columnas que no pueden tener nulos"""
        return ["id", "value", "type", "timestamp"]