#!/usr/bin/env python3
"""Orquestador de Pipeline"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import subprocess
import time
import signal
from config.settings import (
    PIPELINE_TIMEOUTS,
    PIPELINE_MAX_RETRIES,
    PIPELINE_RETRY_DELAY_BASE
)
from config.logging_config import setup_logging

logger = setup_logging("orchestrator")
TIMEOUTS = PIPELINE_TIMEOUTS                    # Configuración de timeouts
# Configuración de reintentos
MAX_RETRIES = PIPELINE_MAX_RETRIES
RETRY_DELAY_BASE = PIPELINE_RETRY_DELAY_BASE    # Configuración de reintentos en segundos

class PipelineOrchestrator:
    def __init__(self):
        self.processes = []
        self.success = True
        signal.signal(signal.SIGINT, self.handle_interrupt) # Signal handlers para Ctrl+C

    def handle_interrupt(self, _signum, _frame):
        """Manejar Ctrl+C y limpiar procesos"""
        logger.warning("\nPipeline interrumpido por el usuario")
        self.cleanup()
        sys.exit(1)
    
    def run_command(self, cmd, description, timeout=None, background=False):
        """
        Ejecutar comando con manejo de timeout
        Args:
            cmd: Comando a ejecutar
            description: Descripción del paso
            timeout: Timeout en segundos
            background: Si True, ejecutar en background
        Returns:
            True si exitoso, False si falló
        """
        logger.info(f"STEP: {description}")
        if timeout:
            logger.info(f" Timeout: {timeout} segundos")
        logger.info(f"Command: {cmd}")
        try:
            if background:
                process = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                self.processes.append(process)
                logger.info(f"{description} - INICIADO (PID: {process.pid})\n")
                return True
            else:
                subprocess.run(
                    cmd,
                    shell=True,
                    check=True,
                    timeout=timeout
                )
                logger.info(f"{description} - COMPLETADO\n")
                return True
        except subprocess.TimeoutExpired:
            logger.error(f"{description} - TIMEOUT ({timeout}s)")
            logger.error("El comando tardó más de lo permitido\n")
            return False
        except subprocess.CalledProcessError as e:
            logger.error(f"{description} - FALLÓ")
            logger.error(f"Error code: {e.returncode}\n")
            return False
        except Exception as e:
            logger.error(f"{description} - ERROR: {e}\n")
            return False
    
    def run_command_with_retry(self, cmd, description, timeout=None, retries=MAX_RETRIES, background=False):
        """
        Ejecutar comando con reintentos automáticos
        Args:
            cmd: Comando a ejecutar
            description: Descripción del paso
            timeout: Timeout en segundos
            retries: Número de reintentos
            background: Si True, ejecutar en background
        Returns:
            True si exitoso, False si falló después de todos los reintentos
        """
        for attempt in range(1, retries + 1):
            logger.info(f"Intento {attempt}/{retries}...")
            if self.run_command(cmd, description, timeout=timeout, background=background):
                return True
            if attempt < retries:
                wait_time = RETRY_DELAY_BASE * attempt
                logger.warning(f"Reintentando en {wait_time}s...")
                time.sleep(wait_time)
        logger.error(f"{description} - FALLÓ después de {retries} intentos\n")
        return False
    
    def cleanup(self):
        """Terminar procesos en background"""
        if not self.processes:
            return
        logger.info("\nLimpiando procesos en background...")
        for process in self.processes:
            try:
                logger.info(f"  Terminando proceso PID: {process.pid}")
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning(f"   Forzando kill del proceso PID: {process.pid}")
                try:
                    process.kill()
                except:
                    pass
            except Exception as e:
                logger.warning(f"Error al terminar proceso: {e}")
        logger.info("Limpieza completada.\n")
    
    def run_pipeline(self):
        """Ejecutar pipeline: Setup S3 -> Producer -> Consumer"""
        logger.info("PIPELINE - INICIANDO")
        # PASO 1: Setup S3
        logger.info("PASO 1: Setup S3 structure")
        if not self.run_command_with_retry(
            "python3 scripts/setup_s3.py",
            "Setup S3 structure",
            timeout=TIMEOUTS["setup_s3"],
            retries=MAX_RETRIES
        ):
            self.success = False
            return False
        
        time.sleep(2)
        
        # PASO 2: Kafka Producer
        logger.info("PASO 2: Kafka Producer")
        if not self.run_command_with_retry(
            "python3 scripts/kafka_producer.py",
            "Kafka Producer (genera 50 eventos)",
            timeout=TIMEOUTS["kafka_producer"],
            retries=MAX_RETRIES,
            background=False
        ):
            self.success = False
            return False
        
        time.sleep(3)
        
        # PASO 3: Spark Consumer
        logger.info("PASO 3: Spark Consumer")
        if not self.run_command_with_retry(
            "python3 pyspark-jobs/01_spark_kafka_consumer.py",
            "Spark Consumer (consume Kafka -> S3)",
            timeout=TIMEOUTS["spark_consumer"],
            retries=MAX_RETRIES,
            background=False
        ):
            self.success = False
            return False
        
        time.sleep(2)

        # PASO 4: Spark S3 Validator
        logger.info("PASO 4: Spark write to Delta")
        if not self.run_command_with_retry(
            "python3 pyspark-jobs/02_spark_write_delta_s3.py",
            "Spark write to Delta (S3 -> Delta)",
            timeout=TIMEOUTS["spark_delta"],
            retries=MAX_RETRIES,
            background=False
        ):
            self.success = False
            return False
        
        time.sleep(2)

        # PASO 5: Spark S3 Validator
        logger.info("PASO 5: Spark S3 Validator")
        if not self.run_command_with_retry(
            "python3 pyspark-jobs/03_spark_s3_validator.py",
            "Spark S3 Validator",
            timeout=TIMEOUTS["spark_validator"],
            retries=MAX_RETRIES,
            background=False
        ):
            self.success = False
            return False

        self.cleanup()
        if self.success:
            logger.info("PIPELINE - COMPLETADO CON ÉXITO")
        else:
            logger.info("PIPELINE - COMPLETADO CON ERRORES")
        return self.success

def main():
    """Main entry point"""
    try:
        orchestrator = PipelineOrchestrator()
        success = orchestrator.run_pipeline()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Error no manejado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()