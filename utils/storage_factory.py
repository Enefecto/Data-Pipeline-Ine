"""
Storage Factory - Factory Pattern para Almacenamiento
Proporciona una interfaz unificada para almacenamiento local o S3
"""

from pathlib import Path
from typing import Union, Optional
import pandas as pd
import json

from config import Config
from utils.s3_storage import S3StorageManager


class LocalStorage:
    """
    Almacenamiento local en sistema de archivos
    """

    def __init__(self, base_dir: Union[str, Path]):
        self.base_dir = Path(base_dir)
        print(f"[LOCAL] Modo de almacenamiento: LOCAL")
        print(f"[LOCAL] Directorio base: {self.base_dir}")

    def save_file(self, data: bytes, filename: str, subfolder: str = "") -> bool:
        """
        Guarda bytes en un archivo local

        Args:
            data: Datos en bytes
            filename: Nombre del archivo
            subfolder: Subcarpeta (ej: '18-10-2025/raw')

        Returns:
            True si se guardó exitosamente
        """
        try:
            file_path = self.base_dir / subfolder / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'wb') as f:
                f.write(data)

            size_kb = len(data) / 1024
            print(f"[LOCAL] Guardado: {file_path} ({size_kb:.1f} KB)")
            return True

        except Exception as e:
            print(f"[LOCAL] Error al guardar {filename}: {e}")
            return False

    def save_dataframe(self, df: pd.DataFrame, filename: str, subfolder: str = "") -> bool:
        """
        Guarda un DataFrame como CSV

        Args:
            df: DataFrame de pandas
            filename: Nombre del archivo (debe terminar en .csv)
            subfolder: Subcarpeta

        Returns:
            True si se guardó exitosamente
        """
        try:
            file_path = self.base_dir / subfolder / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_csv(file_path, index=False)

            size_kb = file_path.stat().st_size / 1024
            print(f"[LOCAL] Guardado CSV: {file_path} ({size_kb:.1f} KB)")
            return True

        except Exception as e:
            print(f"[LOCAL] Error al guardar DataFrame {filename}: {e}")
            return False

    def save_json(self, data: dict, filename: str, subfolder: str = "") -> bool:
        """
        Guarda un diccionario como JSON

        Args:
            data: Diccionario de Python
            filename: Nombre del archivo (debe terminar en .json)
            subfolder: Subcarpeta

        Returns:
            True si se guardó exitosamente
        """
        try:
            file_path = self.base_dir / subfolder / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            size_kb = file_path.stat().st_size / 1024
            print(f"[LOCAL] Guardado JSON: {file_path} ({size_kb:.1f} KB)")
            return True

        except Exception as e:
            print(f"[LOCAL] Error al guardar JSON {filename}: {e}")
            return False

    def get_path(self, filename: str, subfolder: str = "") -> Path:
        """
        Retorna la ruta completa de un archivo

        Args:
            filename: Nombre del archivo
            subfolder: Subcarpeta

        Returns:
            Path completo del archivo
        """
        return self.base_dir / subfolder / filename

    def list_files(self, subfolder: str = "", pattern: str = "*") -> list:
        """
        Lista archivos en una subcarpeta

        Args:
            subfolder: Subcarpeta
            pattern: Patrón de búsqueda (ej: '*.csv')

        Returns:
            Lista de Paths de archivos
        """
        folder_path = self.base_dir / subfolder
        if folder_path.exists():
            return list(folder_path.glob(pattern))
        return []

    def load_file(self, filename: str, subfolder: str = "") -> bytes:
        """
        Carga un archivo y retorna sus bytes

        Args:
            filename: Nombre del archivo
            subfolder: Subcarpeta

        Returns:
            Contenido del archivo en bytes
        """
        file_path = self.base_dir / subfolder / filename
        with open(file_path, 'rb') as f:
            return f.read()

    def load_json(self, filename: str, subfolder: str = "") -> dict:
        """
        Carga un archivo JSON

        Args:
            filename: Nombre del archivo
            subfolder: Subcarpeta

        Returns:
            Diccionario con el contenido del JSON
        """
        file_path = self.base_dir / subfolder / filename
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def rename_file(self, old_name: str, new_name: str, subfolder: str = "") -> int:
        """
        Renombra un archivo

        Args:
            old_name: Nombre actual del archivo
            new_name: Nuevo nombre del archivo
            subfolder: Subcarpeta donde está el archivo

        Returns:
            Tamaño del archivo renombrado en bytes
        """
        old_path = self.base_dir / subfolder / old_name
        new_path = self.base_dir / subfolder / new_name
        old_path.rename(new_path)
        return new_path.stat().st_size

    def delete_folder(self, subfolder: str) -> bool:
        """
        Elimina una carpeta completa y todo su contenido

        Args:
            subfolder: Ruta de la carpeta a eliminar (relativa al base_dir)

        Returns:
            True si se eliminó exitosamente
        """
        import shutil
        try:
            folder_path = self.base_dir / subfolder
            print(f"[LOCAL] Intentando eliminar carpeta: {folder_path}")

            if folder_path.exists():
                # Contar archivos antes de eliminar
                archivos = list(folder_path.rglob("*"))
                num_archivos = len([f for f in archivos if f.is_file()])

                shutil.rmtree(folder_path)
                print(f"[LOCAL] ✓ Carpeta eliminada: {folder_path} ({num_archivos} archivos)")
                return True
            else:
                print(f"[LOCAL] ℹ Carpeta no existe: {folder_path}")
                return False
        except Exception as e:
            print(f"[LOCAL] ✗ Error al eliminar carpeta {subfolder}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def folder_exists(self, subfolder: str) -> bool:
        """
        Verifica si una carpeta existe

        Args:
            subfolder: Ruta de la carpeta (relativa al base_dir)

        Returns:
            True si la carpeta existe
        """
        folder_path = self.base_dir / subfolder
        print(f"[LOCAL] Verificando si existe carpeta: {folder_path}")
        existe = folder_path.exists()
        if existe:
            archivos = list(folder_path.rglob("*"))
            num_archivos = len([f for f in archivos if f.is_file()])
            print(f"[LOCAL] ✓ Carpeta encontrada: {folder_path} ({num_archivos} archivos)")
        else:
            print(f"[LOCAL] ℹ Carpeta no existe: {folder_path}")
        return existe


class S3Storage:
    """
    Almacenamiento en AWS S3
    """

    def __init__(self, bucket_name: str, region: str, access_key: str, secret_key: str):
        self.s3_manager = S3StorageManager(bucket_name, region, access_key, secret_key)
        self.bucket_name = bucket_name
        print(f"[S3] Modo de almacenamiento: S3")
        print(f"[S3] Bucket: {bucket_name}")
        print(f"[S3] Region: {region}")

    def save_file(self, data: bytes, filename: str, subfolder: str = "") -> bool:
        """
        Guarda bytes en S3

        Args:
            data: Datos en bytes
            filename: Nombre del archivo
            subfolder: Subfolder en S3 (ej: 'executions/18-10-2025/raw')

        Returns:
            True si se guardó exitosamente
        """
        s3_key = f"executions/{subfolder}/{filename}" if subfolder else f"executions/{filename}"
        return self.s3_manager.upload_bytes(data, s3_key)

    def save_dataframe(self, df: pd.DataFrame, filename: str, subfolder: str = "") -> bool:
        """
        Guarda un DataFrame como CSV en S3

        Args:
            df: DataFrame de pandas
            filename: Nombre del archivo (debe terminar en .csv)
            subfolder: Subfolder en S3

        Returns:
            True si se guardó exitosamente
        """
        s3_key = f"executions/{subfolder}/{filename}" if subfolder else f"executions/{filename}"
        return self.s3_manager.upload_dataframe(df, s3_key)

    def save_json(self, data: dict, filename: str, subfolder: str = "") -> bool:
        """
        Guarda un diccionario como JSON en S3

        Args:
            data: Diccionario de Python
            filename: Nombre del archivo (debe terminar en .json)
            subfolder: Subfolder en S3

        Returns:
            True si se guardó exitosamente
        """
        s3_key = f"executions/{subfolder}/{filename}" if subfolder else f"executions/{filename}"
        return self.s3_manager.upload_json(data, s3_key)

    def get_path(self, filename: str, subfolder: str = "") -> str:
        """
        Retorna la clave S3 de un archivo

        Args:
            filename: Nombre del archivo
            subfolder: Subfolder

        Returns:
            S3 key (path en S3)
        """
        return f"executions/{subfolder}/{filename}" if subfolder else f"executions/{filename}"

    def list_files(self, subfolder: str = "", pattern: str = "*") -> list:
        """
        Lista archivos en S3

        Args:
            subfolder: Subfolder
            pattern: Patrón de búsqueda (simplificado, solo extensión)

        Returns:
            Lista de claves S3
        """
        prefix = f"executions/{subfolder}/" if subfolder else "executions/"
        all_objects = self.s3_manager.list_objects(prefix)

        # Filtrar por patrón si se especifica
        if pattern != "*":
            extension = pattern.replace("*", "")
            return [obj for obj in all_objects if obj.endswith(extension)]

        return all_objects

    def load_file(self, filename: str, subfolder: str = "") -> bytes:
        """
        Carga un archivo desde S3 y retorna sus bytes

        Args:
            filename: Nombre del archivo
            subfolder: Subfolder en S3

        Returns:
            Contenido del archivo en bytes
        """
        s3_key = f"executions/{subfolder}/{filename}" if subfolder else f"executions/{filename}"

        # Descargar a un archivo temporal y leerlo
        import tempfile
        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            if self.s3_manager.download_file(s3_key, tmp_file.name):
                with open(tmp_file.name, 'rb') as f:
                    return f.read()

        raise Exception(f"No se pudo cargar el archivo {s3_key} desde S3")

    def load_json(self, filename: str, subfolder: str = "") -> dict:
        """
        Carga un archivo JSON desde S3

        Args:
            filename: Nombre del archivo
            subfolder: Subfolder en S3

        Returns:
            Diccionario con el contenido del JSON
        """
        file_bytes = self.load_file(filename, subfolder)
        return json.loads(file_bytes.decode('utf-8'))

    def rename_file(self, old_name: str, new_name: str, subfolder: str = "") -> int:
        """
        Renombra un archivo en S3 (copy + delete)

        Args:
            old_name: Nombre actual del archivo
            new_name: Nuevo nombre del archivo
            subfolder: Subfolder donde está el archivo

        Returns:
            Tamaño del archivo renombrado en bytes
        """
        old_key = f"executions/{subfolder}/{old_name}" if subfolder else f"executions/{old_name}"
        new_key = f"executions/{subfolder}/{new_name}" if subfolder else f"executions/{new_name}"

        # Leer archivo original
        file_data = self.load_file(old_name, subfolder)

        # Subir con nuevo nombre
        self.s3_manager.upload_bytes(file_data, new_key)

        # Eliminar archivo original
        self.s3_manager.delete_object(old_key)

        return len(file_data)

    def delete_folder(self, subfolder: str) -> bool:
        """
        Elimina una carpeta completa en S3 (todos los objetos con ese prefijo)

        Args:
            subfolder: Ruta de la carpeta a eliminar

        Returns:
            True si se eliminó exitosamente
        """
        try:
            prefix = f"executions/{subfolder}/"
            print(f"[S3] Buscando objetos con prefijo: {prefix}")

            objects = self.s3_manager.list_objects(prefix)

            if not objects:
                print(f"[S3] No se encontraron objetos con el prefijo: {prefix}")
                return False

            print(f"[S3] Encontrados {len(objects)} objetos para eliminar")
            print(f"[S3] Eliminando archivos...")

            # Eliminar todos los objetos con ese prefijo (modo silencioso)
            eliminados = 0
            fallidos = 0
            for obj_key in objects:
                if self.s3_manager.delete_object(obj_key, silent=True):
                    eliminados += 1
                else:
                    fallidos += 1

            if fallidos > 0:
                print(f"[S3] Advertencia: {fallidos} archivos no pudieron ser eliminados")

            print(f"[S3] ✓ Carpeta eliminada: {prefix} ({eliminados} archivos eliminados)")
            return eliminados > 0
        except Exception as e:
            print(f"[S3] ✗ Error al eliminar carpeta {subfolder}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def folder_exists(self, subfolder: str) -> bool:
        """
        Verifica si una carpeta existe en S3 (si hay objetos con ese prefijo)

        Args:
            subfolder: Ruta de la carpeta

        Returns:
            True si existen objetos con ese prefijo
        """
        prefix = f"executions/{subfolder}/"
        print(f"[S3] Verificando si existe carpeta: {prefix}")
        objects = self.s3_manager.list_objects(prefix)
        existe = len(objects) > 0
        if existe:
            print(f"[S3] ✓ Carpeta encontrada: {prefix} ({len(objects)} objetos)")
        else:
            print(f"[S3] ℹ Carpeta no existe: {prefix}")
        return existe


class StorageFactory:
    """
    Factory para crear instancias de almacenamiento según configuración
    """

    _instance: Optional[Union[LocalStorage, S3Storage]] = None

    @classmethod
    def get_storage(cls) -> Union[LocalStorage, S3Storage]:
        """
        Retorna una instancia de almacenamiento (Local o S3) según Config.PRODUCTION

        Returns:
            LocalStorage si PRODUCTION=false, S3Storage si PRODUCTION=true
        """
        if cls._instance is None:
            if Config.PRODUCTION:
                # Modo producción: usar S3
                if not Config.S3_BUCKET_NAME:
                    raise Exception("S3_BUCKET_NAME no configurado en .env")
                if not Config.AWS_ACCESS_KEY_ID or not Config.AWS_SECRET_ACCESS_KEY:
                    raise Exception("Credenciales de AWS no configuradas en .env")

                cls._instance = S3Storage(
                    bucket_name=Config.S3_BUCKET_NAME,
                    region=Config.AWS_REGION,
                    access_key=Config.AWS_ACCESS_KEY_ID,
                    secret_key=Config.AWS_SECRET_ACCESS_KEY
                )
            else:
                # Modo desarrollo: usar almacenamiento local
                cls._instance = LocalStorage(base_dir=Config.OUTPUT_DIR)

        return cls._instance

    @classmethod
    def reset(cls):
        """Resetea la instancia (útil para testing)"""
        cls._instance = None
