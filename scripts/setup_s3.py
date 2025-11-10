# scripts/setup_s3.py
import boto3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_DEFAULT_REGION,
    S3_BUCKET,
    S3_BRONZE,
    S3_SILVER,
    S3_GOLD,
    S3_SCHEMAS
)
from config.logging_config import setup_logging

logger = setup_logging("setup_s3")

class SetupS3:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION
        )
        self.bucket = S3_BUCKET
    
    def create_folder(self, folder_name):
        """Crear carpeta en S3"""
        try:
            key = f"{folder_name}/{S3_SCHEMAS}/.gitkeep"
            self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=b'')
            logger.info(f"Folder creado: {folder_name}/{S3_SCHEMAS}/")
        except Exception as e:
            logger.error(f"Fallo al crear folder {folder_name}: {e}")
    
    def setup_structure(self):
        """Crear estructura Medallion en S3"""
        logger.info(f"Configurando estructura en s3://{self.bucket}/...")
        layers = [
            ("Bronze", S3_BRONZE),
            ("Silver", S3_SILVER),
            ("Gold", S3_GOLD),
        ]
        for layer_name, layer_path in layers:
            try:
                self.s3_client.put_object(Bucket=self.bucket, Key=f"{layer_path.replace('s3a://' + self.bucket + '/', '')}/.gitkeep", Body=b'')
                logger.info(f"Capa creada: {layer_name} ({layer_path})")
                schemas_key = f"{layer_path.replace('s3a://' + self.bucket + '/', '')}/{S3_SCHEMAS}/.gitkeep"
                self.s3_client.put_object(Bucket=self.bucket, Key=schemas_key, Body=b'')
                logger.info(f"Carpeta _schemas creada: {layer_path}/{S3_SCHEMAS}/")
            except Exception as e:
                logger.error(f"Fallo al configurar {layer_name}: {e}")
        logger.info("Configuración de la estructura en S3 completada")
    
    def list_contents(self):
        """Listar contenido del bucket"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket)
            if 'Contents' in response:
                logger.info(f"Bucket s3://{self.bucket}/ contenido:")
                for obj in response['Contents']:
                    logger.info(f"   - {obj['Key']}")
            else:
                logger.info(f"Bucket s3://{self.bucket}/ está vacío")
        except Exception as e:
            logger.error(f"Fallo al listar el bucket: {e}")

if __name__ == "__main__":
    setup = SetupS3()
    setup.setup_structure()
    setup.list_contents()