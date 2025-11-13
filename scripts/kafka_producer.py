# scripts/kafka_producer.py
import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from kafka import KafkaProducer
from utils import (HealthCheck, RetryPolicy, MessageEncoder)
from config.settings import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_ENCODING, PIPELINE_MAX_RETRIES)
from config.logging_config import setup_logging

logger = setup_logging("kafka_producer")

class EventProducer:
    def __init__(self,
                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                 topic=KAFKA_TOPIC,
                 encoding=KAFKA_ENCODING,
                 max_retries=PIPELINE_MAX_RETRIES):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.sent_count = 0
        self.failed_count = 0
        self.health_check = HealthCheck(logger)
        self.encoder = MessageEncoder(encoding, logger)
        self.retry_policy = RetryPolicy(max_retries, logger=logger)

    def connect(self):
        """Conexión a Kafka con salud y reintentos"""
        host, port = self.bootstrap_servers.split(':')
        # Health check
        success, elapsed = self.health_check.wait_for_kafka(host, int(port), max_wait=30)
        if not success:
            raise Exception(f"Kafka no disponible después de {elapsed}s")
        # Conectar con reintentos
        self.retry_policy.execute_with_retry(self._create_producer)
    
    def _create_producer(self):
        """Crear KafkaProducer (Configurar timeouts, acks y ejecutado con reintentos)"""
        logger.info(f"Intentando conectar a Kafka...")
        logger.info(f"   Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"   Topic: {self.topic}\n")
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.encoder.serialize,
            request_timeout_ms=10000,       # 10 segundos timeout
            connections_max_idle_ms=60000,  # 60 segundos idle
            retries=3,                      # 3 reintentos internos
            reconnect_backoff_ms=100,       # 100ms entre reintentos
            reconnect_backoff_max_ms=5000,  # Máximo 5 segundos
            acks="all",                     # Mayor durabilidad: espera confirmación de todos los réplicas (solo leader: acks="1")
            linger_ms=5,                    # Espera 5ms para acumular más mensajes (mejora throughput)
            batch_size=16384                # Tamaño de batch (16KB) para agrupar mensajes
        )

    def _on_send_success(self, metadata):
        """Callback ejecutado si el envío es exitoso"""
        self.sent_count += 1
        logger.debug(f"Evento enviado exitosamente a la partición {metadata.partition}")

    def _on_send_error(self, exc):
        """Callback ejecutado si el envío falla"""
        self.failed_count += 1
        logger.error(f"Error al enviar evento: {exc}")

    def send_event_async(self, event):
        """
        Enviar un evento a Kafka de forma ASÍNCRONA con callbacks
        Args:
            event: Diccionario con datos del evento
        Returns:
            FutureRecordMetadata: Promesa del resultado
        """
        try:
            # Envía evento (asíncrono)
            future = self.producer.send(self.topic, value=event)
            # Añadir callbacks para manejo de éxito/fallo
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            return future
        except Exception as e:
            logger.error(f"Error al enviar evento: {e}")
            return None
        
    def send_event_sync(self, event, timeout=30):
        """Enviar evento de forma SÍNCRONA (bloquea)"""
        try:
            future = self.producer.send(self.topic, value=event)
            record_metadata = future.get(timeout=timeout)  # Espera respuesta
            logger.debug(f"Evento enviado a partición {record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Error al enviar evento: {e}")
            return False
    
    def send_batch(self, num_events=100, delay=0.1, validate_first=True):
        """
        Enviar lote de eventos a Kafka (primer evento valida conexión, resto asincronos)
        Args:
            num_events: Cantidad de eventos a producir
            delay: Segundos entre eventos
            validate_first: Si True, primer evento es síncrono para validar
        """
        logger.info(f"Comenzando a producir {num_events} eventos...")
        logger.info(f"   Tópico: {self.topic}")
        logger.info(f"   Modo: {'VALIDACIÓN (sync+async)' if validate_first else 'ASYNC'}")
        logger.info(f"   Delay entre eventos: {delay}s\n")
        for i in range(num_events):
            event = {
                "id": i,
                "value": i * 10,
                "type": "test_event",
                "timestamp": int(time.time() * 1000)
            }
            # Primer evento: validar conexión
            if i == 0 and validate_first:
                logger.info(f"Evento 0: Validando conexión (SÍNCRONO)...")
                if not self.send_event_sync(event, timeout=30):
                    logger.error("Primera validación falló. Abortando batch.")
                    self.failed_count += 1
                    return False  # Abortamos todo
                self.sent_count += 1
                logger.info("Conexión validada. Continuando con eventos asincronos...\n")
            else:
                self.send_event_async(event) # Resto: asincronos
                # Contadores en callbacks
            if (i + 1) % 10 == 0:
                logger.info(f"   Producidos: {i + 1}/{num_events}") # Log cada 10 eventos
            time.sleep(delay)
        logger.info("Flushing mensajes pendientes...")
        self.producer.flush()  # Espera a que todos los mensajes se envíen
        logger.info(f"Producción completada: {self.sent_count} exitosos, {self.failed_count} fallidos")

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
    except ConnectionError as e:
        logger.error(f"KAFKA PRODUCER - FAILED (ConnectionError): {e}")
        raise
    except ValueError as e:
        logger.error(f"KAFKA PRODUCER - FAILED (ValueError): {e}")
        raise
    except Exception as e:
        logger.error(f"KAFKA PRODUCER - FAILED (Unexpected): {e}")
        raise

if __name__ == "__main__":
    main()