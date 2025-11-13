# utils/encoding_utils.py
import json
import logging
from typing import Any

class MessageEncoder:
    """Encoder centralizado para mensajes Kafka"""
    # Constante técnica - Qué encodings soporta el sistema
    SUPPORTED_ENCODINGS = ['utf-8', 'utf-8-sig', 'latin-1', 'ascii']    
    def __init__(self, encoding: str = 'utf-8', logger: logging.Logger = None):
        if encoding not in self.SUPPORTED_ENCODINGS:
            raise ValueError(
                f"Encoding '{encoding}' no soportado. "
                f"Opciones: {', '.join(self.SUPPORTED_ENCODINGS)}"
            )
        self.encoding = encoding
        self.logger = logger or logging.getLogger(__name__)
    
    def serialize(self, data: Any) -> bytes:
        """Serializar datos a bytes"""
        try:
            json_str = json.dumps(data, ensure_ascii=True)
            return json_str.encode(self.encoding)
        except Exception as e:
            self.logger.error(f"Error serializando: {e}")
            raise
    
    def deserialize(self, data: bytes) -> Any:
        """Deserializar bytes a datos"""
        try:
            json_str = data.decode(self.encoding)
            return json.loads(json_str)
        except Exception as e:
            self.logger.error(f"Error deserializando: {e}")
            raise