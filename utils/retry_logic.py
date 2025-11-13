# utils/retry_logic.py
"""
Retry Logic - Políticas de reintentos centralizadas
Reutiliza lógica de reintentos en producer, consumer y jobs sin duplicación de código.
"""
import time
import logging
from typing import Callable, Any, Optional

class RetryPolicy:
    """
    Política de reintentos centralizada con exponential backoff
    Evita duplicar lógica de reintentos en múltiples lugares
    """
    def __init__(self, 
                 max_retries: int = 3,
                 base_delay: int = 2,
                 max_delay: int = 60,
                 logger: Optional[logging.Logger] = None):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.logger = logger or logging.getLogger(__name__)
    
    def execute_with_retry(self,
                          func: Callable,
                          *args,
                          **kwargs) -> Any:
        """
        Ejecutar función con reintentos automáticos
        Args:
            func: Función a ejecutar
            *args, **kwargs: Argumentos para la función
        Returns:
            Resultado de la función
        Raises:
            Exception si falla después de max_retries
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Intento {attempt}/{self.max_retries}")
                result = func(*args, **kwargs)
                self.logger.info(f"Exitoso en intento {attempt}")
                return result
            except Exception as e:
                self.logger.warning(f"Intento {attempt} falló: {e}")
                if attempt < self.max_retries:
                    # Exponential backoff: 2s, 4s, 8s, 16s, 32s (capped a 60s)
                    wait_time = min(self.base_delay ** attempt, self.max_delay)
                    self.logger.info(f"Reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Falló después de {self.max_retries} intentos")
                    raise