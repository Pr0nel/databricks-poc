# scripts/validate_infrastructure.py
"""
Validador integral de infraestructura
Verifica: Databricks, S3, Docker, Spark, Delta
"""
import boto3
import requests
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BUCKET,
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    KAFKA_BOOTSTRAP_SERVERS,
    validate_config
)
from config.logging_config import setup_logging
from utils import HealthCheck
from utils import SparkSessionFactory

logger = setup_logging("validate_infrastructure")

class InfrastructureValidator:
    def __init__(self):
        self.results = {}
        self.health_check = HealthCheck(logger)
    
    def validate_env_vars(self):
        """Validar que todas las variables de entorno estén configuradas"""
        logger.info("[1/6] VALIDANDO VARIABLES DE ENTORNO")
        try:
            validate_config()
            logger.info("Todas las variables de entorno están configuradas")
            self.results['env_vars'] = 'PASS'
            return True
        except ValueError as e:
            logger.error(f"Error en variables de entorno: {e}")
            self.results['env_vars'] = 'FAIL'
            return False
    
    def validate_aws_credentials(self):
        """Validar credenciales AWS"""
        logger.info("[2/6] VALIDANDO CREDENCIALES AWS")
        try:            
            # Verificar que las credenciales existen
            if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
                logger.error("AWS_ACCESS_KEY_ID o AWS_SECRET_ACCESS_KEY no configurados")
                self.results['aws_credentials'] = 'FAIL'
                return False
            # Crear cliente S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_DEFAULT_REGION
            )
            # Intentar listar buckets (test de credenciales)
            response = s3_client.list_buckets()
            buckets = [b['Name'] for b in response['Buckets']]
            logger.info("Credenciales AWS válidas")
            logger.info(f"   Region: {AWS_DEFAULT_REGION}")
            logger.info(f"   Buckets disponibles: {len(buckets)}")
            # Verificar que nuestro bucket existe
            if S3_BUCKET in buckets:
                logger.info(f"Bucket '{S3_BUCKET}' existe")
            else:
                logger.warning(f"Bucket '{S3_BUCKET}' NO existe (hay que crearlo)")
                logger.info(f"   Buckets encontrados: {', '.join(buckets[:5])}")
            self.results['aws_credentials'] = 'PASS'
            return True
        except Exception as e:
            logger.error(f"Error con credenciales AWS: {e}")
            self.results['aws_credentials'] = 'FAIL'
            return False
    
    def validate_s3_structure(self):
        """Validar estructura medallion en S3"""
        logger.info("[3/6] VALIDANDO ESTRUCTURA S3")
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_DEFAULT_REGION
            )
            # Intentar listar contenido del bucket
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=10)
            if 'Contents' in response:
                logger.info(f"Bucket '{S3_BUCKET}' contiene archivos:")
                for obj in response['Contents'][:10]:
                    size_mb = obj['Size'] / (1024 * 1024)
                    logger.info(f"   - {obj['Key']} ({size_mb:.2f} MB)")
                    # Detectar capas Medallion
                    if 'bronze' in obj['Key']:
                        logger.info("      └─ Capa: BRONZE")
                    elif 'silver' in obj['Key']:
                        logger.info("      └─ Capa: SILVER")
                    elif 'gold' in obj['Key']:
                        logger.info("      └─ Capa: GOLD")
            else:
                logger.warning(f"Bucket '{S3_BUCKET}' está vacío")
                logger.info("   (Es normal si es primera ejecución)")
            self.results['s3_structure'] = 'PASS'
            return True
        except Exception as e:
            logger.error(f"Error validando S3: {e}")
            self.results['s3_structure'] = 'FAIL'
            return False
    
    def validate_databricks_connection(self):
        """Validar conexión a Databricks"""
        logger.info("[4/6] VALIDANDO CONEXIÓN A DATABRICKS")
        try:
            if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
                logger.error("DATABRICKS_HOST o DATABRICKS_TOKEN no configurados")
                self.results['databricks'] = 'FAIL'
                return False
            # Hacer request a Databricks API
            headers = {
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            }
            # Endpoint simple para verificar token
            url = f"{DATABRICKS_HOST}/api/2.0/workspace/get-status"
            payload = {"path": "/"}
            response = requests.get(url, headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Conexión a Databricks exitosa")
                logger.info(f"   Host: {DATABRICKS_HOST}")
                logger.info(f"   Token: válido")
                self.results['databricks'] = 'PASS'
                return True
            elif response.status_code == 401:
                logger.error("Token de Databricks inválido o expirado")
                self.results['databricks'] = 'FAIL'
                return False
            else:
                logger.error(f"Error Databricks: {response.status_code}")
                self.results['databricks'] = 'FAIL'
                return False
        except requests.exceptions.ConnectionError:
            logger.error(f"No se puede conectar a {DATABRICKS_HOST}")
            logger.info("   Verifica que el URL sea correcto: https://workspace-name.cloud.databricks.com")
            self.results['databricks'] = 'FAIL'
            return False
        except Exception as e:
            logger.error(f"Error validando Databricks: {e}")
            self.results['databricks'] = 'FAIL'
            return False
    
    def validate_kafka(self):
        """Validar que Kafka está corriendo"""
        logger.info("[5/6] VALIDANDO KAFKA")
        try:
            host, port = KAFKA_BOOTSTRAP_SERVERS.split(':')
            success, elapsed = self.health_check.wait_for_kafka(host, int(port), max_wait=5)
            if success:
                logger.info("Kafka disponible")
                logger.info(f"   Host: {host}")
                logger.info(f"   Puerto: {port}")
                logger.info(f"   Respuesta en: {elapsed}s")
                self.results['kafka'] = 'PASS'
                return True
            else:
                logger.error("Kafka no está disponible")
                logger.info(f"   Ejecuta: ./scripts/docker-helpers.sh dev-up")
                self.results['kafka'] = 'FAIL'
                return False
        except Exception as e:
            logger.error(f"Error validando Kafka: {e}")
            self.results['kafka'] = 'FAIL'
            return False
    
    def validate_spark(self):
        """Validar que Spark + Delta pueden iniciar"""
        logger.info("[6/6] VALIDANDO SPARK & DELTA LAKE")
        try:
            factory = SparkSessionFactory(logger)
            logger.info("Intentando crear SparkSession...")
            spark = factory.create_spark_session(
                app_name="validation-check",
                enable_s3=False
            )
            logger.info(f"SparkSession creada exitosamente")
            logger.info(f"   Versión Spark: {spark.version}")
            logger.info(f"   Python: {spark.sparkContext.pythonVer}")
            # Verificar que Delta está disponible
            df_test = spark.createDataFrame([(1, 'test')], ['id', 'value'])
            df_test.write.format("delta").mode("overwrite").save("delta_tables/validation_test")
            logger.info("Delta Lake funciona correctamente")
            spark.stop()
            self.results['spark'] = 'PASS'
            return True
        except Exception as e:
            logger.error(f"Error con Spark/Delta: {e}")
            logger.info("   Verifica que tengas Java 11+ instalado")
            self.results['spark'] = 'FAIL'
            return False
    
    def print_report(self):
        """Imprimir reporte final"""
        logger.info("REPORTE FINAL DE VALIDACIÓN")
        status_icons = {
            'PASS': '[·]',
            'FAIL': '[X]'
        }
        all_pass = True
        for check, result in self.results.items():
            icon = status_icons.get(result, '?')
            logger.info(f"{icon} {check.upper():25} : {result}")
            if result != 'PASS':
                all_pass = False
        if all_pass:
            logger.info("TODA LA INFRAESTRUCTURA VALIDADA EXITOSAMENTE")
            logger.info("   Listo para ejecutar run_pipeline.py")
        else:
            logger.info("ALGUNOS COMPONENTES NECESITAN ATENCIÓN")
            logger.info("   Ver logs arriba para detalles")
        return all_pass
    
    def run_all_validations(self):
        """Ejecutar todas las validaciones"""
        logger.info("INICIANDO VALIDACIÓN DE INFRAESTRUCTURA\n")
        checks = [
            ('env_vars', self.validate_env_vars),
            ('aws_credentials', self.validate_aws_credentials),
            ('s3_structure', self.validate_s3_structure),
            ('databricks', self.validate_databricks_connection),
            ('kafka', self.validate_kafka),
            ('spark', self.validate_spark),
        ]
        for check_name, check_func in checks:
            try:
                check_func()
            except Exception as e:
                logger.error(f"Excepción no capturada en {check_name}: {e}")
                self.results[check_name] = 'FAIL'
        return self.print_report()

def main():
    """Main entry point"""
    validator = InfrastructureValidator()
    success = validator.run_all_validations()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()