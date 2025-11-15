# utils/data_validators.py
"""
Data Validators - Validaciones comunes de datos
Reutiliza lógica de validación en consumer, transformaciones y escritura a S3.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, when, min, avg, stddev, length, max as spark_max)

class DataValidator:
    """Validaciones comunes de datos para reutilizar en todo el pipeline"""
    def __init__(self, logger: logging.Logger, fail_on_error: bool = False):
        """
        Args:
            logger: Logger para salida
            fail_on_error: Si True, fallar en primer error; si False, continuar con warnings
        Returns:
            None
        """
        self.logger = logger
        self.fail_on_error = fail_on_error
        self.validation_results = {}
        self.quality_score = 100

    def check_row_count(self, df: DataFrame, min_rows: int = 1) -> bool:
        """
        Validar cantidad de filas. Previene escribir DataFrames vacíos.
        Args:
            df: DataFrame a validar
            min_rows: Mínimo de filas requerido
        Returns:
            True si cumple, False si no
        """
        self.logger.info("[1/10] Validando cantidad de filas...")
        try:
            row_count = df.count()
            if row_count < min_rows:
                msg = f"DataFrame vacío o insuficiente: {row_count} < {min_rows} filas"
                self.logger.error(msg)
                self.validation_results['row_count'] = f"FAIL: {row_count} rows"
                self._deduct_score(30)
                return False if self.fail_on_error else True
            self.logger.info(f"{row_count} filas OK")
            self.validation_results['row_count'] = f"PASS: {row_count} rows"
            return True
        except Exception as e:
            self.logger.error(f"Error contando filas: {e}")
            self.validation_results['row_count'] = "ERROR"
            self._deduct_score(20)
            return False if self.fail_on_error else True
        
    def check_schema_integrity(self, df: DataFrame, expected_columns: set = None) -> bool:
        """
        Validar integridad del schema. Previene cambios de schema no documentados.
        Args:
            df: DataFrame a validar
            expected_columns: Set de columnas esperadas
        Returns:
            True si schema OK, False si hay discrepancias
        """
        self.logger.info("[2/10] Validando integridad de schema...")
        try:
            actual_columns = set(df.columns)
            if expected_columns:
                missing = expected_columns - actual_columns
                extra = actual_columns - expected_columns
                if missing:
                    msg = f"Faltan columnas: {missing}"
                    self.logger.error(msg)
                    self.validation_results['schema'] = f"INCOMPLETE: missing {missing}"
                    self._deduct_score(25)
                    return False if self.fail_on_error else True
                if extra:
                    self.logger.warning(f"Columnas extras (schema evolution): {extra}")
            self.logger.info(f"Schema válido: {len(actual_columns)} columnas")
            self.validation_results['schema'] = f"PASS: {len(actual_columns)} columns"
            return True
        except Exception as e:
            self.logger.error(f"Error validando schema: {e}")
            self.validation_results['schema'] = "ERROR"
            self._deduct_score(20)
            return False if self.fail_on_error else True

    def check_nulls(self, df: DataFrame, critical_cols: list) -> bool:
        """
        Validar nulos en columnas críticas
        Args:
            df: DataFrame a validar
            critical_cols: Columnas que NO pueden tener nulos
        Returns:
            True si está OK, False si hay nulos críticos
        """
        self.logger.info("[3/10] Validando nulos en columnas críticas...")
        try:
            if not critical_cols:
                self.logger.info("Sin columnas críticas definidas")
                self.validation_results['nulls'] = "SKIP: no critical cols"
                return True
            total_rows = df.count()
            if total_rows == 0:
                return True  # Ya validado en row_count
            violations = []
            null_details = []
            for col_name in critical_cols:
                if col_name not in df.columns:
                    self.logger.warning(f"   Columna '{col_name}' no existe, skipping")
                    continue
                null_count = df.filter(col(col_name).isNull()).count()
                null_ratio = (null_count / total_rows * 100) if total_rows > 0 else 0
                if null_count > 0:
                    msg = f"   {col_name}: {null_count} nulos ({null_ratio:.1f}%)"
                    self.logger.error(msg)
                    violations.append(col_name)
                    null_details.append(f"{col_name}({null_count})")
                    self._deduct_score(10)  # -10% por cada columna con nulos
                else:
                    self.logger.info(f"   {col_name}: OK")
            if violations:
                self.validation_results['nulls'] = f"FAIL: {', '.join(null_details)}"
                return False if self.fail_on_error else True
            self.logger.info("Sin nulos en columnas críticas")
            self.validation_results['nulls'] = "PASS: no nulls"
            return True
        except Exception as e:
            self.logger.error(f"Error validando nulos: {e}")
            self.validation_results['nulls'] = "ERROR"
            self._deduct_score(15)
            return False if self.fail_on_error else True

    def check_duplicates_by_id(self, df: DataFrame, key_columns: list = None) -> int:
        """
        Detectar duplicados por columnas clave
        Args:
            df: DataFrame a validar
            key_columns: Columnas para detectar duplicados (default: ["id"])
        Returns:
            Cantidad de duplicados
        """
        self.logger.info("[4/10] Detectando duplicados...")
        try:
            if key_columns is None:
                key_columns = ["id"] if "id" in df.columns else []
            if not key_columns:
                self.logger.info("   Sin columnas clave para detectar duplicados")
                self.validation_results['duplicates'] = "SKIP: no key cols"
                return 0
            total_rows = df.count()
            # Duplicados por key
            dedup_count = df.dropDuplicates(key_columns).count()
            dup_count = total_rows - dedup_count
            if dup_count > 0:
                dup_ratio = float((dup_count / total_rows * 100)) if total_rows > 0 else 0.0
                msg = f"   {dup_count} duplicados por {key_columns} ({dup_ratio:.1f}%)"
                self.logger.warning(msg)
                self.validation_results['duplicates'] = f"WARN: {dup_count} duplicates"
                self._deduct_score(10)
            else:
                self.logger.info(f"   Sin duplicados")
                self.validation_results['duplicates'] = "PASS: 0 duplicates"
            return dup_count
        except Exception as e:
            self.logger.error(f"Error detectando duplicados: {e}")
            self.validation_results['duplicates'] = "ERROR"
            self._deduct_score(10)
            return 0

    def check_numeric_ranges(self, df: DataFrame, numeric_ranges: dict = None) -> bool:
        """
        Validar que valores numéricos están en rangos válidos
        Previene valores imposibles u outliers extremos
        Args:
            df: DataFrame a validar
            numeric_ranges: {col_name: (min_val, max_val), ...}
        Returns:
            True si OK, False si hay valores fuera de rango
        """
        self.logger.info("[5/10] Validando rangos numéricos...")
        try:
            if not numeric_ranges:
                self.logger.info("   Sin validación de rangos configurada")
                self.validation_results['ranges'] = "SKIP: no config"
                return True
            violations = []
            for col_name, (min_val, max_val) in numeric_ranges.items():
                if col_name not in df.columns:
                    continue
                out_of_range = df.filter(
                    (col(col_name) < min_val) | (col(col_name) > max_val)
                ).count()
                if out_of_range > 0:
                    msg = f"   {col_name}: {out_of_range} valores fuera de [{min_val}, {max_val}]"
                    self.logger.warning(msg)
                    violations.append(col_name)
                    self._deduct_score(10)
                else:
                    self.logger.info(f"   {col_name}: en rango [{min_val}, {max_val}]")
            if violations:
                self.validation_results['ranges'] = f"WARN: {len(violations)} columns out of range"
                return True  # Warning, not error
            self.logger.info("Todos los valores numéricos en rangos válidos")
            self.validation_results['ranges'] = "PASS: all in range"
            return True
        except Exception as e:
            self.logger.error(f"Error validando rangos: {e}")
            self.validation_results['ranges'] = "ERROR"
            self._deduct_score(10)
            return False if self.fail_on_error else True

    def check_data_types(self, df: DataFrame) -> bool:
        """
        Validar que tipos de datos son correctos
        Detecta strings gigantes, NaN en números, etc.
        Args:
            df: DataFrame a validar
        Returns:
            True si OK, False si hay problemas de tipos
        """
        self.logger.info("[6/10] Validando tipos de datos...")
        try:
            schema_issues = []
            for field in df.schema.fields:
                col_name = field.name
                data_type = field.dataType.typeName()
                # Detectar strings demasiado largos
                if data_type == 'string':
                    max_len_row = df.agg(
                        spark_max(
                            (when(col(col_name).isNull(), 0)
                            .otherwise(length(col(col_name))))
                        ).alias("max_len")
                    ).collect()[0]["max_len"]
                    if max_len_row and max_len_row > 1000000:  # > 1MB
                        msg = f"   {col_name}: string muy grande ({max_len_row} bytes)"
                        self.logger.warning(msg)
                        schema_issues.append(col_name)
            if schema_issues:
                self.validation_results['data_types'] = f"WARN: {len(schema_issues)} type issues"
            else:
                self.logger.info("Todos los tipos de datos válidos")
                self.validation_results['data_types'] = "PASS: all types valid"
            return True
        except Exception as e:
            self.logger.error(f"Error validando tipos: {e}")
            self.validation_results['data_types'] = "ERROR"
            self._deduct_score(5)
            return False if self.fail_on_error else True

    def check_statistical_anomalies(self, df: DataFrame) -> bool:
        """
        Detectar anomalías estadísticas (outliers)
        Valores > 3 desviaciones estándar
        Args:
            df: DataFrame a validar
        Returns:
            True si OK, False si hay anomalías severas
        """
        self.logger.info("[7/10] Detectando anomalías estadísticas...")
        LIST_DATATYPE=['integer', 'long', 'double', 'float']
        try:
            numeric_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in LIST_DATATYPE]
            if not numeric_cols:
                self.logger.info("   Sin columnas numéricas para analizar")
                self.validation_results['anomalies'] = "SKIP: no numeric cols"
                return True
            anomalies = []
            for col_name in numeric_cols:
                stats = df.agg(
                    avg(col(col_name)).alias("avg"),
                    stddev(col(col_name)).alias("stddev")
                ).collect()[0]
                avg_val = stats['avg']
                stddev_val = stats['stddev']
                if avg_val is None or stddev_val is None or stddev_val == 0:
                    continue
                # Detectar valores > 3 desviaciones estándar
                outliers = df.filter(
                    (col(col_name) > avg_val + 3*stddev_val) |
                    (col(col_name) < avg_val - 3*stddev_val)
                ).count()
                if outliers > 0:
                    msg = f"   {col_name}: {outliers} outliers (avg={avg_val:.2f}, σ={stddev_val:.2f})"
                    self.logger.warning(msg)
                    anomalies.append(col_name)
            if anomalies:
                self.validation_results['anomalies'] = f"WARN: outliers in {len(anomalies)} cols"
            else:
                self.logger.info("Sin anomalías detectadas")
                self.validation_results['anomalies'] = "PASS: no outliers"
            return True
        except Exception as e:
            self.logger.error(f"Error detectando anomalías: {e}")
            self.validation_results['anomalies'] = "ERROR"
            return False if self.fail_on_error else True

    def check_partition_distribution(self, df: DataFrame, partition_col: str = None) -> bool:
        """
        Validar distribución de datos (skew). Previene particiones desbalanceadas.
        Args:
            df: DataFrame a validar
            partition_col: Columna de partición
        Returns:
            True si distribución OK, False si hay skew severo
        """
        self.logger.info("[8/10] Validando distribución de datos...")
        try:
            if not partition_col or partition_col not in df.columns:
                self.logger.info("   Sin columna de partición especificada")
                self.validation_results['distribution'] = "SKIP: no partition col"
                return True
            dist = df.groupBy(partition_col).count().collect()
            if len(dist) > 0:
                counts = [row['count'] for row in dist]
                max_val = max(counts)
                min_val = min(counts)
                avg_val = sum(counts) / len(counts)
                # Detectar skew: si una partición es > 10x el promedio
                if max_val > avg_val * 10:
                    msg = f"   Skew detectado: min={min_val}," \
                        + f" max={max_val}," \
                        + f" avg={avg_val:.0f}," \
                        + f" ratio={max_val/avg_val:.1f}x"
                    self.logger.warning(msg)
                    self.validation_results['distribution'] = f"WARN: data skew detected"
                    self._deduct_score(5)
                else:
                    self.logger.info(f"   Distribución balanceada (min={min_val}, avg={avg_val:.0f}, max={max_val})")
                    self.validation_results['distribution'] = "PASS: balanced"
            return True
        except Exception as e:
            self.logger.error(f"Error validando distribución: {e}")
            self.validation_results['distribution'] = "ERROR"
            return False if self.fail_on_error else True

    def check_encoding_consistency(self, df: DataFrame, string_columns: list = None) -> bool:
        """
        Validar encoding consistente en strings. Detecta caracteres corruptos.
        Args:
            df: DataFrame a validar
            string_columns: Columnas string a validar (default: todas las string)
        Returns:
            True si está OK, False si hay problemas de encoding
        """
        self.logger.info("[9/10] Validando encoding de strings...")
        try:
            if not string_columns:
                string_columns = [f.name for f in df.schema.fields if f.dataType.typeName() == 'string']
            encoding_issues = 0
            for col_name in string_columns:
                if col_name not in df.columns:
                    continue
                # Buscar caracteres inválidos
                invalid_chars = df.filter(
                    col(col_name).contains('\ufffd') # Unicode replacement char
                ).count()
                if invalid_chars > 0:
                    msg = f"   {col_name}: {invalid_chars} strings con caracteres inválidos"
                    self.logger.warning(msg)
                    encoding_issues += 1
                    self._deduct_score(5)
            if encoding_issues == 0:
                self.logger.info("Encoding consistente")
                self.validation_results['encoding'] = "PASS: consistent"
            else:
                self.validation_results['encoding'] = f"WARN: {encoding_issues} cols with issues"
            return True
        except Exception as e:
            self.logger.error(f"Error validando encoding: {e}")
            self.validation_results['encoding'] = "ERROR"
            return False if self.fail_on_error else True

    def check_custom_rules(self, df: DataFrame, custom_rules: dict = None) -> bool:
        """
        Validar reglas de negocio custom
        Permite agregar validaciones específicas del dominio
        Args:
            custom_rules: {rule_name: lambda_function, ...}
        Returns:
            True si todas pasan, False si alguna falla
        """
        self.logger.info("[10/10] Validando reglas de negocio custom...")
        try:
            if not custom_rules:
                self.logger.info("   Sin reglas custom configuradas")
                self.validation_results['custom'] = "SKIP: no rules"
                return True
            rule_failures = []
            for rule_name, rule_func in custom_rules.items():
                try:
                    is_valid = rule_func(df)
                    if not is_valid:
                        msg = f"   Regla '{rule_name}' falló"
                        self.logger.error(msg)
                        rule_failures.append(rule_name)
                        self._deduct_score(10)
                    else:
                        self.logger.info(f"   Regla '{rule_name}' OK")
                except Exception as e:
                    self.logger.error(f"   Error evaluando '{rule_name}': {e}")
                    rule_failures.append(rule_name)
            if rule_failures:
                self.validation_results['custom'] = f"FAIL: {len(rule_failures)} rules failed"
                return False if self.fail_on_error else True
            self.validation_results['custom'] = "PASS: all rules passed"
            return True
        except Exception as e:
            self.logger.error(f"Error validando reglas custom: {e}")
            self.validation_results['custom'] = "ERROR"
            return False if self.fail_on_error else True

    def get_data_statistics(self, df: DataFrame) -> dict:
        """
        Obtener estadísticas del DataFrame
        Args:
            df: DataFrame a analizar
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
    
    def _deduct_score(self, points: int):
        """
        Deducir puntos del score de calidad
        Args:
            points: Puntos a deducir
        Returns:
            None
        """
        self.quality_score = max(0, self.quality_score - points)
    
    def generate_quality_report(self) -> dict:
        """
        Generar reporte final de calidad
        Args:
            None
        Returns:
            Dict con resumen de validaciones y score
        """
        self.logger.info("DATA QUALITY REPORT")
        passed = sum(1 for r in self.validation_results.values() if 'PASS' in r)
        warned = sum(1 for r in self.validation_results.values() if 'WARN' in r)
        failed = sum(1 for r in self.validation_results.values() if 'FAIL' in r)        
        self.logger.info(f"Resultados: {passed} PASS | {warned} WARN | {failed} FAIL")
        self.logger.info(f"Score de Calidad: {self.quality_score}/100")
        self.logger.info("Detalle por validación:")
        for check, result in self.validation_results.items():
            icon = "·" if "PASS" in result else ("⚠" if "WARN" in result else "X")
            self.logger.info(f"  [{icon}] {check:20} : {result}")
        return {
            'passed': passed,
            'warned': warned,
            'failed': failed,
            'quality_score': self.quality_score,
            'can_upload': self.quality_score >= 70 and failed == 0
        }
    
    def run_all_checks(self, df: DataFrame, 
                       expected_columns: set = None,
                       critical_columns: list = None,
                       key_columns: list = None,
                       numeric_ranges: dict = None,
                       partition_col: str = None,
                       custom_rules: dict = None) -> bool:
        """
        Ejecutar TODAS las 10 validaciones
        Args:
            df: DataFrame a validar
            expected_columns: Columnas esperadas para schema
            critical_columns: Columnas que no pueden tener nulos
            key_columns: Columnas para detectar duplicados
            numeric_ranges: Rangos válidos para columnas numéricas
            partition_col: Columna de partición para validar distribución
            custom_rules: Reglas de negocio custom
        Returns:
            bool: True si calidad >= 70%, False si < 70% o hay errores críticos
        """
        self.logger.info("INICIANDO VALIDACIONES COMPLETAS DE CALIDAD\n")
        self.check_row_count(df, min_rows=1) # score: -30
        self.check_schema_integrity(df, expected_columns) # score: -25
        self.check_nulls(df, critical_columns or []) # score: -10 por columna con nulos
        self.check_duplicates_by_id(df, key_columns) # score: -10 si hay duplicados
        self.check_numeric_ranges(df, numeric_ranges) # score: -10
        self.check_data_types(df) # score: -5
        self.check_statistical_anomalies(df) # score: -10
        self.check_partition_distribution(df, partition_col) # score: -5
        self.check_encoding_consistency(df) # score: -5
        self.check_custom_rules(df, custom_rules) # score: -10 por regla fallida
        report = self.generate_quality_report()
        if report['can_upload']:
            self.logger.info("DATOS LISTOS (Calidad OK)")
            return True
        else:
            self.logger.error("DATOS NO LISTOS (Calidad insuficiente)")
            return False