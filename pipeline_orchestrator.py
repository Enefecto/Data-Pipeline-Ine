"""
Pipeline Orchestrator - Orquestador del Pipeline INE
Ejecuta los 6 pasos del pipeline en secuencia y genera un reporte consolidado
"""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from config import Config
from utils.storage_factory import StorageFactory

# Importar cada paso del pipeline
import sys
sys.path.insert(0, str(Path(__file__).parent / "steps"))

from steps.step1_scraper import INEScraperConcurrent
from steps.step2_standardize_names import NameStandardizer
from steps.step3_remove_columns import ColumnRemover
from steps.step4_filter_stations import StationFilter
from steps.step5_create_views import ViewCreator
from steps.step6_upload_to_db import DatabaseUploader
from steps.step7_generate_report import ReportGenerator


class PipelineOrchestrator:
    def __init__(self):
        self.output_base = Path(Config.OUTPUT_DIR)
        self.inicio_pipeline = time.time()
        self.pasos_completados = []
        self.pasos_fallidos = []
        self.storage = StorageFactory.get_storage()
        self.fecha_hoy = datetime.now().strftime("%d-%m-%Y")

    def limpiar_ejecucion_previa(self):
        """
        Elimina la ejecución previa del mismo día si existe.
        Útil para desarrollo donde se ejecutan múltiples pipelines en el mismo día.
        En producción solo se ejecuta una vez a la semana.
        """
        print("\n" + "="*80)
        print("VERIFICACION DE EJECUCION PREVIA".center(80))
        print("="*80)
        print(f"\n🔍 Verificando si existe una ejecución previa para: {self.fecha_hoy}")
        print("")

        # Verificar si ya existe una ejecución para hoy
        if self.storage.folder_exists(self.fecha_hoy):
            print(f"\n⚠️  ATENCION: Ya existe una ejecución para la fecha: {self.fecha_hoy}")
            print("   Esta ejecución será eliminada para comenzar desde cero...")
            print("")

            resultado = self.storage.delete_folder(self.fecha_hoy)

            if resultado:
                print(f"\n   ✅ Ejecución previa eliminada exitosamente")
                print("   El pipeline comenzará con datos limpios")
            else:
                print(f"\n   ⚠️  No se pudo eliminar la ejecución previa")
                print("   Esto puede causar conflictos. Verifica los permisos de la carpeta.")

            print("\n" + "="*80 + "\n")
        else:
            print(f"\n✅ No hay ejecución previa para {self.fecha_hoy}")
            print("   Comenzando pipeline limpio desde cero")
            print("\n" + "="*80 + "\n")

    async def ejecutar_pipeline_completo(self):
        """Ejecuta los 7 pasos del pipeline en secuencia"""
        print("""
╔═══════════════════════════════════════════════════════════════════╗
║                                                                    ║
║           PIPELINE COMPLETO INE - OBSERVATORIO AMBIENTAL          ║
║                                                                    ║
║  Paso 1: Scraping de datos del INE                               ║
║  Paso 2: Estandarización de nombres                              ║
║  Paso 3: Eliminación de columnas (Flags)                         ║
║  Paso 4: Filtrado de estaciones con datos insuficientes          ║
║  Paso 5: Creación de vistas consolidadas                         ║
║  Paso 6: Carga a base de datos (Neon PostgreSQL)                 ║
║  Paso 7: Generación de reporte consolidado                       ║
║                                                                    ║
╚═══════════════════════════════════════════════════════════════════╝
        """)

        # Mostrar configuración de almacenamiento
        print("\n" + "="*80)
        print("CONFIGURACION DE ALMACENAMIENTO".center(80))
        print("="*80)
        print(f"Modo de almacenamiento:  {Config.STORAGE_MODE}")
        if Config.PRODUCTION:
            print(f"Bucket S3:               {Config.S3_BUCKET_NAME}")
            print(f"Region AWS:              {Config.AWS_REGION}")
        else:
            print(f"Directorio local:        {Config.OUTPUT_DIR}")
        print("="*80 + "\n")

        # Limpiar ejecución previa del mismo día (útil para desarrollo)
        self.limpiar_ejecucion_previa()

        # Usar try-finally para GARANTIZAR que el reporte consolidado se genere SIEMPRE
        try:
            # PASO 1: Scraping
            try:
                print("\n" + "="*80)
                print("PASO 1: SCRAPING DE DATOS DEL INE")
                print("="*80 + "\n")

                inicio = time.time()
                scraper = INEScraperConcurrent()
                scraper.cargar_catalogo()
                resultados, tiempo_scraping = await scraper.scrape_all_concurrent()

                # Reintentar fallidos si existen
                exitosos_reintento = 0
                if len(scraper.resultados['fallidos']) > 0:
                    exitosos_reintento, _ = await scraper.retry_failed_datasets()

                scraper.generar_reporte(tiempo_scraping, exitosos_reintento)

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 1,
                    "nombre": "Scraping",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 1: {e}")
                self.pasos_fallidos.append({
                    "paso": 1,
                    "nombre": "Scraping",
                    "error": str(e)
                })
                raise

            # PASO 2: Estandarización de nombres
            try:
                print("\n" + "="*80)
                print("PASO 2: ESTANDARIZACION DE NOMBRES")
                print("="*80 + "\n")

                inicio = time.time()
                standardizer = NameStandardizer()
                standardizer.cargar_mapeo()
                tiempo_total = standardizer.estandarizar_archivos()
                standardizer.generar_reporte(tiempo_total)

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 2,
                    "nombre": "Standardize Names",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 2: {e}")
                self.pasos_fallidos.append({
                    "paso": 2,
                    "nombre": "Standardize Names",
                    "error": str(e)
                })
                raise

            # PASO 3: Eliminación de columnas
            try:
                print("\n" + "="*80)
                print("PASO 3: ELIMINACION DE COLUMNAS")
                print("="*80 + "\n")

                inicio = time.time()
                remover = ColumnRemover()
                tiempo_total = remover.procesar_archivos()
                remover.generar_reporte(tiempo_total)

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 3,
                    "nombre": "Remove Columns",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 3: {e}")
                self.pasos_fallidos.append({
                    "paso": 3,
                    "nombre": "Remove Columns",
                    "error": str(e)
                })
                raise

            # PASO 4: Filtrado de estaciones
            try:
                print("\n" + "="*80)
                print("PASO 4: FILTRADO DE ESTACIONES")
                print("="*80 + "\n")

                inicio = time.time()
                filterer = StationFilter()
                tiempo_total = filterer.procesar_archivos()
                filterer.generar_reporte(tiempo_total)

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 4,
                    "nombre": "Filter Stations",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 4: {e}")
                self.pasos_fallidos.append({
                    "paso": 4,
                    "nombre": "Filter Stations",
                    "error": str(e)
                })
                raise

            # PASO 5: Creación de vistas
            try:
                print("\n" + "="*80)
                print("PASO 5: CREACION DE VISTAS CONSOLIDADAS")
                print("="*80 + "\n")

                inicio = time.time()
                creator = ViewCreator()
                tiempo_total = creator.procesar_vistas()
                creator.generar_reporte(tiempo_total)

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 5,
                    "nombre": "Create Views",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 5: {e}")
                self.pasos_fallidos.append({
                    "paso": 5,
                    "nombre": "Create Views",
                    "error": str(e)
                })
                raise

            # PASO 6: Carga a base de datos
            try:
                print("\n" + "="*80)
                print("PASO 6: CARGA A BASE DE DATOS")
                print("="*80 + "\n")

                inicio = time.time()
                uploader = DatabaseUploader()
                tiempo_total = uploader.subir_todas_las_vistas()
                uploader.generar_reporte(tiempo_total)
                uploader.engine.dispose()

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 6,
                    "nombre": "Upload to DB",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 6: {e}")
                self.pasos_fallidos.append({
                    "paso": 6,
                    "nombre": "Upload to DB",
                    "error": str(e)
                })
                # No hacemos raise aquí porque queremos generar el reporte final

            # PASO 7: Generación de reporte consolidado
            try:
                print("\n" + "="*80)
                print("PASO 7: GENERACION DE REPORTE CONSOLIDADO")
                print("="*80 + "\n")

                inicio = time.time()
                generator = ReportGenerator()
                tiempo_total = time.time() - inicio
                generator.generar_reporte(tiempo_total)

                elapsed = time.time() - inicio
                self.pasos_completados.append({
                    "paso": 7,
                    "nombre": "Generate Report",
                    "duracion_segundos": elapsed,
                    "exitoso": True
                })

            except Exception as e:
                print(f"\n[ERROR] ERROR EN PASO 7: {e}")
                self.pasos_fallidos.append({
                    "paso": 7,
                    "nombre": "Generate Report",
                    "error": str(e)
                })
                # No hacemos raise para permitir que el pipeline termine

        except Exception as e:
            # Capturar cualquier excepción general que no fue manejada
            print(f"\n[ERROR] ERROR NO MANEJADO EN PIPELINE: {e}")
            import traceback
            traceback.print_exc()


async def main():
    """Función principal que ejecuta el pipeline completo"""
    try:
        orchestrator = PipelineOrchestrator()
        await orchestrator.ejecutar_pipeline_completo()

        print("\n[OK] PIPELINE COMPLETADO EXITOSAMENTE!")

    except Exception as e:
        print(f"\n[ERROR] ERROR FATAL EN EL PIPELINE: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
