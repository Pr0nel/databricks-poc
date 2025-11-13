# utils/health_check.py
"""
Health Check Module - Validaciones reutilizables para Kafka y otros servicios
Centraliza lógica de health checks para evitar duplicación en producer, consumer y jobs de Spark.
"""
import time
import socket
import logging
from typing import Tuple

class HealthCheck:
    """
    Módulo reutilizable de health checks para Kafka y otros servicios
    Centraliza la lógica de validación para producers, consumers, y jobs sin duplicación.
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def is_kafka_available(self, host: str, port: int, timeout: int = 2) -> bool:
        """
        Health check: Verificar que Kafka está disponible (socket check)
        Args:
            host: Hostname de Kafka
            port: Puerto de Kafka
            timeout: Segundos para esperar respuesta
        Returns:
            True si Kafka responde, False si no
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port))) == 0
            sock.close()
            return result
        except Exception as e:
            self.logger.warning(f"Health check error: {e}")
            return False
    
    def wait_for_kafka(self, host: str, port: int, max_wait: int = 30) -> Tuple[bool, int]:
        """
        Esperar a que Kafka esté disponible (health check loop)
        Args:
            host: Hostname de Kafka
            port: Puerto de Kafka
            max_wait: Segundos máximos a esperar
        Returns:
            Tupla: (success: bool, elapsed_time: int)
        """
        self.logger.info(f"Esperando a que Kafka esté disponible (máx {max_wait}s)...")
        elapsed = 0
        while elapsed < max_wait:
            if self.is_kafka_available(host, port):
                self.logger.info(f"Kafka disponible (health check OK después de {elapsed}s)")
                return True, elapsed
            elapsed += 2
            self.logger.debug(f"    Kafka no disponible... ({elapsed}s/{max_wait}s)")
            time.sleep(2)
        self.logger.error(f"Kafka no disponible después de {max_wait}s")
        return False, elapsed