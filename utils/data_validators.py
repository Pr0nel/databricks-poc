# utils/data_validators.py
"""
Data Validators - Validaciones comunes de datos
Reutiliza lógica de validación en consumer, validatory futuras transformaciones.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

class DataValidator:
    """Validaciones comunes de datos para reutilizar en múltiples jobs"""
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def check_nulls(self, df: DataFrame, critical_cols: list) -> bool:
        """
        Validar nulos en columnas críticas
        Args:
            df: DataFrame a validar
            critical_cols: Columnas que NO pueden tener nulos
        Returns:
            True si está OK, False si hay nulos
        """
        self.logger.info("Validando nulos en columnas críticas...")
        null_check = df.select([
            count(when(col(c).isNull(), 1)).alias(f"null_{c}")
            for c in critical_cols if c in df.columns
        ]).collect()[0]
        nulls_found = False
        for col_name in critical_cols:
            if col_name in df.columns:
                null_count = getattr(null_check, f"null_{col_name}")
                if null_count > 0:
                    self.logger.warning(f"   {null_count} nulos en {col_name}")
                    nulls_found = True
        if not nulls_found:
            self.logger.info("   Sin nulos detectados")
        return not nulls_found
    
    def get_data_statistics(self, df: DataFrame) -> dict:
        """
        Obtener estadísticas del DataFrame
        Returns:
            Dict con métricas
        """
        total_rows = df.count()
        num_partitions = df.rdd.getNumPartitions()
        stats = {
            "total_rows": total_rows,
            "num_partitions": num_partitions,
            "schema_columns": len(df.columns),
        }
        self.logger.info(f"Estadísticas: {stats}")
        return stats
    
    def check_duplicates_by_id(self, df: DataFrame) -> int:
        """
        Detectar duplicados por columna 'id'
        Returns:
            Cantidad de IDs duplicados
        """
        if "id" not in df.columns:
            self.logger.warning("Columna 'id' no existe, skip")
            return 0
        duplicates = df.groupBy("id").count().filter(col("count") > 1)
        dup_count = duplicates.count()
        if dup_count > 0:
            self.logger.warning(f"   {dup_count} IDs duplicados encontrados")
        else:
            self.logger.info("   Sin duplicados detectados")
        return dup_count