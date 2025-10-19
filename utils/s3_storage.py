"""
S3 Storage Manager - Manejo de almacenamiento en AWS S3
Proporciona funcionalidad para subir, descargar y gestionar archivos en S3
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path
from typing import Union, List, Optional
import io
import json
import time


class S3StorageManager:
    """
    Gestor de almacenamiento en AWS S3

    Proporciona métodos para:
    - Subir archivos y DataFrames
    - Descargar archivos
    - Listar objetos
    - Validar conexión
    """

    def __init__(self, bucket_name: str, region: str, access_key: str, secret_key: str):
        """
        Inicializa el gestor de S3

        Args:
            bucket_name: Nombre del bucket S3
            region: Región de AWS (ej: 'sa-east-1')
            access_key: AWS Access Key ID
            secret_key: AWS Secret Access Key
        """
        self.bucket_name = bucket_name
        self.region = region

        # Crear cliente S3
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )

            # Verificar credenciales
            self._validate_credentials()
            print(f"[S3] Conectado exitosamente al bucket: {bucket_name}")

        except NoCredentialsError:
            raise Exception("Credenciales de AWS no encontradas o inválidas")
        except Exception as e:
            raise Exception(f"Error al inicializar S3 client: {e}")

    def _validate_credentials(self):
        """Valida que las credenciales sean correctas y el bucket exista"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise Exception(f"El bucket '{self.bucket_name}' no existe")
            elif error_code == '403':
                raise Exception(f"No tienes permisos para acceder al bucket '{self.bucket_name}'")
            else:
                raise Exception(f"Error al validar bucket: {e}")

    def upload_file(self, file_path: Union[str, Path], s3_key: str, max_retries: int = 3) -> bool:
        """
        Sube un archivo local a S3

        Args:
            file_path: Ruta del archivo local
            s3_key: Clave (path) del archivo en S3
            max_retries: Número máximo de reintentos

        Returns:
            True si se subió exitosamente, False en caso contrario
        """
        file_path = Path(file_path)

        if not file_path.exists():
            print(f"[S3] ERROR: Archivo no existe: {file_path}")
            return False

        for attempt in range(max_retries):
            try:
                self.s3_client.upload_file(
                    str(file_path),
                    self.bucket_name,
                    s3_key
                )

                file_size = file_path.stat().st_size / 1024  # KB
                print(f"[S3] Subido: {s3_key} ({file_size:.1f} KB)")
                return True

            except ClientError as e:
                print(f"[S3] Error en intento {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                else:
                    print(f"[S3] FALLO: No se pudo subir {s3_key} después de {max_retries} intentos")
                    return False

    def upload_bytes(self, data: bytes, s3_key: str, max_retries: int = 3) -> bool:
        """
        Sube datos en bytes directamente a S3

        Args:
            data: Datos en bytes
            s3_key: Clave (path) del archivo en S3
            max_retries: Número máximo de reintentos

        Returns:
            True si se subió exitosamente
        """
        for attempt in range(max_retries):
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=data
                )

                data_size = len(data) / 1024  # KB
                print(f"[S3] Subido: {s3_key} ({data_size:.1f} KB)")
                return True

            except ClientError as e:
                print(f"[S3] Error en intento {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"[S3] FALLO: No se pudo subir {s3_key}")
                    return False

    def upload_dataframe(self, df, s3_key: str, max_retries: int = 3) -> bool:
        """
        Sube un DataFrame de pandas como CSV a S3

        Args:
            df: DataFrame de pandas
            s3_key: Clave (path) del archivo en S3 (debe terminar en .csv)
            max_retries: Número máximo de reintentos

        Returns:
            True si se subió exitosamente
        """
        try:
            # Convertir DataFrame a CSV en memoria
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue().encode('utf-8')

            return self.upload_bytes(csv_data, s3_key, max_retries)

        except Exception as e:
            print(f"[S3] Error al convertir DataFrame a CSV: {e}")
            return False

    def upload_json(self, data: dict, s3_key: str, max_retries: int = 3) -> bool:
        """
        Sube un diccionario como JSON a S3

        Args:
            data: Diccionario de Python
            s3_key: Clave (path) del archivo en S3 (debe terminar en .json)
            max_retries: Número máximo de reintentos

        Returns:
            True si se subió exitosamente
        """
        try:
            json_data = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            return self.upload_bytes(json_data, s3_key, max_retries)
        except Exception as e:
            print(f"[S3] Error al convertir dict a JSON: {e}")
            return False

    def download_file(self, s3_key: str, local_path: Union[str, Path]) -> bool:
        """
        Descarga un archivo de S3 a local

        Args:
            s3_key: Clave (path) del archivo en S3
            local_path: Ruta local donde guardar el archivo

        Returns:
            True si se descargó exitosamente
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_path)
            )
            print(f"[S3] Descargado: {s3_key} -> {local_path}")
            return True

        except ClientError as e:
            print(f"[S3] Error al descargar {s3_key}: {e}")
            return False

    def list_objects(self, prefix: str = '') -> List[str]:
        """
        Lista objetos en el bucket con un prefijo específico

        Args:
            prefix: Prefijo para filtrar objetos (ej: 'executions/2025-10-18/')

        Returns:
            Lista de claves (paths) de los objetos
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []

        except ClientError as e:
            print(f"[S3] Error al listar objetos: {e}")
            return []

    def delete_object(self, s3_key: str, silent: bool = False) -> bool:
        """
        Elimina un objeto de S3

        Args:
            s3_key: Clave (path) del archivo en S3
            silent: Si es True, no imprime mensaje de confirmación

        Returns:
            True si se eliminó exitosamente
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            if not silent:
                print(f"[S3] Eliminado: {s3_key}")
            return True

        except ClientError as e:
            print(f"[S3] Error al eliminar {s3_key}: {e}")
            return False

    def object_exists(self, s3_key: str) -> bool:
        """
        Verifica si un objeto existe en S3

        Args:
            s3_key: Clave (path) del archivo en S3

        Returns:
            True si el objeto existe
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False
