# scripts/kafka_producer.py
import json
import time
import socket
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from kafka import KafkaProducer
from config.settings import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
from config.logging_config import setup_logging

logger = setup_logging("kafka_producer")

class EventProducer:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        
    def _is_kafka_available(self, timeout=2):
        """
        Health check: verificar que Kafka está disponible (socket check)
        Args:
            timeout: Segundos para esperar respuesta
        Returns:
            True si Kafka responde, False si no
        """
        try:
            host, port = self.bootstrap_servers.split(':')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port))) == 0
            sock.close()
            return result
        except Exception as e:
            logger.warning(f"Health check error: {e}")
            return False
    
    def _wait_for_kafka(self, max_wait=30):
        """
        Esperar a que Kafka esté disponible (health check loop)
        Args:
            max_wait: Segundos máximos a esperar
        Raises:
            Exception si Kafka no está disponible después de max_wait segundos
        """
        logger.info(f"Esperando a que Kafka esté disponible (máx {max_wait}s)...")
        elapsed = 0
        while elapsed < max_wait:
            if self._is_kafka_available():
                logger.info("Kafka disponible (health check OK)")
                return
            elapsed += 2
            logger.debug(f"    Kafka no disponible... ({elapsed}s/{max_wait}s)")
            time.sleep(2)
        raise Exception(f"Kafka no disponible después de {max_wait}s")

    def connect(self):
        """Conexión a Kafka con 3 capas de robustez:
            1. Health check (socket check)
            2. Retry loop con backoff exponencial
            3. Timeouts configurados en KafkaProducer
        """
        # Layer 1: Health check
        try:
            self._wait_for_kafka(max_wait=30)
        except Exception as e:
            logger.error(f"{e}")
            raise
        
        # Layer 2: Retry con backoff exponencial
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Intento {attempt}/{max_attempts} de conexión a Kafka...")
                logger.info(f"   Bootstrap servers: {self.bootstrap_servers}")
                logger.info(f"   Topic: {self.topic}")
                
                # Layer 3: Configurar timeouts y reintentos internos
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    # Timeouts
                    request_timeout_ms=10000,        # 10 segundos timeout
                    connections_max_idle_ms=60000,   # 60 segundos idle
                    # Reintentos internos
                    retries=3,
                    reconnect_backoff_ms=100,        # 100ms entre reintentos
                    reconnect_backoff_max_ms=5000,   # Máximo 5 segundos
                )
                
                logger.info(f"Conectado a Kafka exitosamente (intento {attempt})\n")
                return
                
            except Exception as e:
                logger.warning(f"Intento {attempt} falló: {type(e).__name__}: {e}")
                if attempt < max_attempts:
                    # Backoff exponencial: 2s, 4s, 8s, 16s, 32s (capped a 30s)
                    wait_time = min(2 ** attempt, 30)
                    logger.info(f"Esperando {wait_time}s antes de reintentar...\n")
                    time.sleep(wait_time)
                else:
                    logger.error(f"\nNo se pudo conectar a Kafka después de {max_attempts} intentos")
                    raise
    
    def send_event(self, event):
        """Enviar un evento a Kafka"""
        try:
            # 1. Envía evento (asíncrono, retorna inmediatamente)
            future = self.producer.send(self.topic, value=event) # se crea promesa de resultado
            # 2. Espera confirmación (máximo 30 segundos)
            record_metadata = future.get(timeout=30) # Espera confirmación de Kafka (max. 30 segundos), con retorno de metadata sobre el envío
            # 3. Si llegamos aquí, fue exitoso
            logger.debug(f"Evento enviado a la partición {record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Error al enviar evento: {e}")
            return False
    
    def send_batch(self, num_events=100, delay=0.1):
        """
        Enviar lote de eventos a Kafka
        Args:
            num_events: Cantidad de eventos a producir
            delay: Segundos entre eventos
        """
        logger.info(f"Comenzando a producir {num_events} eventos...")
        logger.info(f"   Tópico: {self.topic}")
        logger.info(f"   Delay entre eventos: {delay}s\n")
        success_count = 0
        failed_count = 0
        for i in range(num_events):
            event = {
                "id": i,
                "value": i * 10,
                "type": "test_event",
                "timestamp": int(time.time() * 1000)
            }
            if self.send_event(event):
                success_count += 1
            else:
                failed_count += 1            
            if (i + 1) % 10 == 0:
                logger.info(f"   Producidos: {i + 1}/{num_events}") # Log cada 10 eventos
            time.sleep(delay)
        logger.info(f"Producción completada")
        logger.info(f"   Exitosos: {success_count}/{num_events}")
        logger.info(f"   Fallidos: {failed_count}/{num_events}\n")

    def close(self):
        """Cerrar conexión a Kafka"""
        if self.producer:
            self.producer.close()
            logger.info("Conexión del productor de Kafka cerrada")

def main():
    """Main entry point"""
    try:
        producer = EventProducer()
        producer.connect()
        producer.send_batch(num_events=50, delay=0.5)
        producer.close()
        logger.info("KAFKA PRODUCER - SUCCESS")
        
    except Exception as e:
        logger.error(f"KAFKA PRODUCER - FAILED: {e}")
        raise

if __name__ == "__main__":
    main()