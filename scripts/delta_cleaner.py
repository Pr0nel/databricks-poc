# scripts/delta_cleaner.py
"""
Script para limpiar entorno de pruebas:
- Borra datos de una tabla Delta (local).
- Borra directorio de checkpoint de Spark Streaming.
Soporta borrado condicional y modo dry-run.
"""
import sys
import shutil
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.spark_utils import SparkSessionFactory
from delta.tables import DeltaTable
from config.logging_config import setup_logging

logger = setup_logging("delta_cleaner")

TARGET_DELTA_TABLE_PATH = "delta_tables/events_raw"
CHECKPOINT_PATH = "spark_checkpoints/delta_consumer"

class DeltaCleaner:
    def __init__(self):
        self.spark_factory = SparkSessionFactory(logger)
        self.spark = self.spark_factory.create_spark_session(app_name="DeltaCleaner", enable_s3=False)
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"SparkSession v{self.spark.version}")

    def cleanup_delta_table(self, table_path, dry_run=False, condition=None):
        """
        Limpiar tabla Delta. Si se proporciona 'condition', realiza un borrado condicional.
        Si condition=None, borra todas las filas.
        Args:
            table_path: Path a la tabla Delta
            dry_run: Si True, simula la operación sin borrar datos
            condition: Condición SQL para borrado condicional (ej. 'ingestion_date < "2024-01-01"')
        Returns:
            None
        """
        log_prefix = f"[DRY RUN] " if dry_run else ""
        action = f"Borrado condicional (condition='{condition}')" if condition else "Borrado total"
        logger.info(f"{log_prefix}Limpiando Delta ({action}): {table_path}")
        try:
            path = Path(table_path)
            if not path.exists():
                logger.info(f"Path no existe, creando vacío")
                path.mkdir(parents=True, exist_ok=True)
                return
            # Esta es la única operación de lectura/conteo costosa.
            df = self.spark.read.format("delta").load(table_path)
            row_count = df.count()
            logger.info(f"Encontradas {row_count} filas en total en la tabla Delta")
            if dry_run:
                rows_to_delete = row_count
                if condition:
                    rows_to_delete = df.filter(condition).count()
                logger.info(f"[DRY RUN] Se ejecutaría {action}. Filas a BORRAR: {rows_to_delete}")
                return
            if row_count > 0:
                # Inicializar DeltaTable API
                delta_table = DeltaTable.forPath(self.spark, table_path)
                # Ejecutar DELETE. Si condition es None, borra todo
                # La condición se pasa directamente como una cadena SQL
                delta_table.delete(condition=condition)
                # Contar filas restantes para confirmar el borrado
                rows_after = delta_table.toDF().count()
                rows_deleted = row_count - rows_after
                logger.info(f"{action} completado. Filas borradas: {rows_deleted}, restantes: {rows_after}")
            else:
                logger.info("Tabla ya vacía. No se requiere DELETE.")
        except Exception as e:
            logger.error(f"Error en cleanup_delta: {e}")
            # Fallback: limpiar físicamente (se mantiene como última opción)
            logger.warning("Limpiando físicamente...")
            shutil.rmtree(Path(table_path), ignore_errors=True)
            raise

    def cleanup_checkpoint_dir(self, checkpoint_path, dry_run=False):
        """Eliminar checkpoint."""
        log_prefix = f"[DRY RUN] " if dry_run else ""
        logger.info(f"{log_prefix}Limpiando checkpoint: {checkpoint_path}")
        try:
            path = Path(checkpoint_path)
            if path.exists():
                if dry_run:
                    logger.info(f"[DRY RUN] Se SIMULARÍA la eliminación del Checkpoint.")
                    return
                shutil.rmtree(path)
                logger.info(f"Checkpoint eliminado")
            else:
                logger.info(f"Checkpoint no existe")
        except Exception as e:
            logger.error(f"Error: {e}")
            raise

    def close(self):
        self.spark.stop()
        logger.info("Spark cerrado")

def main():
    """Punto de entrada principal para la limpieza."""
    parser = argparse.ArgumentParser(
        description="Limpia tabla Delta y directorio de checkpoint."
    )
    parser.add_argument(
        "--table-path",
        default=TARGET_DELTA_TABLE_PATH,
        help=f"Path a la tabla Delta (por defecto: {TARGET_DELTA_TABLE_PATH})"
    )
    parser.add_argument(
        "--checkpoint-path",
        default=CHECKPOINT_PATH,
        help=f"Path al checkpoint (por defecto: {CHECKPOINT_PATH})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular la operación sin borrar datos"
    )
    parser.add_argument(
        "--condition",
        type=str,
        default=None,
        help="Condición SQL para borrado condicional (ej. 'ingestion_date < \"2024-01-01\"')"
    )
    args = parser.parse_args()
    cleaner = None
    try:
        cleaner = DeltaCleaner()
        cleaner.cleanup_delta_table(args.table_path, dry_run=args.dry_run, condition=args.condition)
        cleaner.cleanup_checkpoint_dir(args.checkpoint_path, dry_run=args.dry_run)
        logger.info("LIMPIEZA DE ENTORNO - COMPLETADA")
    except Exception as e:
        logger.error(f"El proceso de limpieza falló: {e}")
        sys.exit(1)
    finally:
        if cleaner:
            cleaner.close()

if __name__ == "__main__":
    main()