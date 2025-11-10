import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(name, level=logging.INFO, max_bytes=52428800, backup_count=10):
    """
    Logging con rotación automática de archivos.
    Args:
        name: Nombre del logger
        level: Nivel de logging (INFO, DEBUG, WARNING, ERROR)
        max_bytes: Tamaño máximo del archivo antes de rotar (default: 50 MB - 52428800 bytes)
        backup_count: Número de archivos backup a mantener (default: 10)
    """
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{name}.log"
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        return logger
    formatter = logging.Formatter(
        '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    rotating_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,  # 50 MB
        backupCount=backup_count  # Mantener 10 backups
    )
    rotating_handler.setFormatter(formatter)
    logger.addHandler(rotating_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

if __name__ == "__main__":
    logger = setup_logging("test")
    logger.info("Logging configured with rotation")
    logger.info(f"   Log file: logs/test.log")
    logger.info(f"   Max size: 50 MB")
    logger.info(f"   Backup files: 10")