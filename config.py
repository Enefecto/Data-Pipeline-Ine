"""
Configuración del INE Scraper
Optimizado para AWS Lambda y ejecución concurrente
Incluye configuración para pipeline de datos multi-etapa
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

class Config:
    # ===== CONFIGURACIÓN DE PIPELINE =====
    # Etapas del pipeline de procesamiento
    PIPELINE_STAGES = ['raw', 'cleaned']

    # Etapa actual (puede sobreescribirse con variable de entorno)
    CURRENT_STAGE = os.getenv('PIPELINE_STAGE', 'raw')

    # ===== CONFIGURACIÓN DE CONCURRENCIA =====
    # Número de navegadores/workers concurrentes
    # Para AWS Lambda: 2-4 navegadores es óptimo (balance costo/velocidad)
    # Para local/servidor: puede aumentarse según CPU disponible
    MAX_CONCURRENT_BROWSERS = int(os.getenv('MAX_CONCURRENT_BROWSERS', '4'))

    # Timeout para cada descarga individual (segundos)
    DOWNLOAD_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', '60'))

    # Pausa entre descargas por worker (segundos)
    DELAY_BETWEEN_DOWNLOADS = float(os.getenv('DELAY_BETWEEN_DOWNLOADS', '1.0'))

    # ===== CONFIGURACIÓN DE AWS LAMBDA =====
    # Detectar si está corriendo en Lambda
    IS_LAMBDA = os.getenv('AWS_LAMBDA_FUNCTION_NAME') is not None

    # Memoria Lambda recomendada: 2048-3008 MB para 3 navegadores
    # Timeout Lambda recomendado: 15 minutos (900 segundos)
    LAMBDA_MEMORY = int(os.getenv('LAMBDA_MEMORY', '2048'))

    # ===== CONFIGURACIÓN DE BASE DE DATOS =====
    # URL de conexión a Neon (PostgreSQL)
    # Formato: postgresql://user:password@host/dbname?sslmode=require
    DATABASE_URL = os.getenv('DATABASE_URL', '')

    # Pool de conexiones para concurrencia
    DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '5'))
    DB_MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '10'))

    # ===== CONFIGURACIÓN DE ARCHIVOS =====
    # Catálogo de datasets
    CATALOG_PATH = os.getenv('CATALOG_PATH', '/app/ine_catalog.json')

    # Directorio de salida
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/app/outputs')

    # Guardar archivos localmente (False para Lambda, solo DB)
    SAVE_LOCAL_FILES = os.getenv('SAVE_LOCAL_FILES', 'true').lower() == 'true'

    # ===== CONFIGURACIÓN DEL NAVEGADOR =====
    # Ejecutar en modo headless
    HEADLESS = os.getenv('HEADLESS', 'true').lower() == 'true'

    # User agent
    USER_AGENT = os.getenv('USER_AGENT',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    # Viewport
    VIEWPORT_WIDTH = int(os.getenv('VIEWPORT_WIDTH', '1920'))
    VIEWPORT_HEIGHT = int(os.getenv('VIEWPORT_HEIGHT', '1080'))

    # ===== CONFIGURACIÓN DE MODO DE EJECUCIÓN =====
    # Máximo de datasets a procesar (None = todos)
    # Útil para testing
    MAX_DATASETS = int(os.getenv('MAX_DATASETS')) if os.getenv('MAX_DATASETS') else None

    # ===== CONFIGURACIÓN DE LOGS =====
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # ===== CONFIGURACIÓN DE REINTENTOS =====
    # Número de reintentos por dataset fallido
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))

    # ===== COSTOS ESTIMADOS AWS LAMBDA =====
    # Estas son estimaciones para planificación
    @staticmethod
    def estimar_costo_lambda(num_datasets=87, num_ejecuciones=1):
        """
        Estima el costo de ejecutar el scraper en AWS Lambda

        Supuestos:
        - Memoria: 2048 MB
        - Duración por ejecución: ~5-8 minutos con 3 navegadores concurrentes
        - Región: us-east-1
        - Precio Lambda: $0.0000166667 por GB-segundo
        """
        memory_gb = 2.048  # 2048 MB
        duracion_segundos = 420  # ~7 minutos promedio
        precio_por_gb_segundo = 0.0000166667

        costo_por_ejecucion = memory_gb * duracion_segundos * precio_por_gb_segundo
        costo_total = costo_por_ejecucion * num_ejecuciones

        return {
            'costo_por_ejecucion_usd': round(costo_por_ejecucion, 4),
            'costo_total_usd': round(costo_total, 4),
            'duracion_estimada_minutos': duracion_segundos / 60,
            'num_ejecuciones': num_ejecuciones
        }

    @staticmethod
    def print_config():
        """Imprime la configuración actual"""
        print("=" * 60)
        print("CONFIGURACIÓN DEL SCRAPER")
        print("=" * 60)
        print(f"Navegadores concurrentes: {Config.MAX_CONCURRENT_BROWSERS}")
        print(f"Timeout por descarga: {Config.DOWNLOAD_TIMEOUT}s")
        print(f"Modo headless: {Config.HEADLESS}")
        print(f"Max datasets: {Config.MAX_DATASETS or 'Todos (87)'}")
        print(f"Guardar archivos local: {Config.SAVE_LOCAL_FILES}")
        print(f"Ejecutando en Lambda: {Config.IS_LAMBDA}")
        if Config.DATABASE_URL:
            print(f"Base de datos: Configurada ✓")
        else:
            print(f"Base de datos: No configurada")
        print("=" * 60)

        # Mostrar estimación de costos
        if Config.IS_LAMBDA:
            estimacion = Config.estimar_costo_lambda()
            print(f"\nESTIMACIÓN DE COSTOS AWS LAMBDA:")
            print(f"Costo por ejecución: ${estimacion['costo_por_ejecucion_usd']}")
            print(f"Duración estimada: {estimacion['duracion_estimada_minutos']:.1f} min")
            print("=" * 60)