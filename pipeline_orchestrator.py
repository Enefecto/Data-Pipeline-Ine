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

# Importar cada paso del pipeline
import sys
sys.path.insert(0, str(Path(__file__).parent / "steps"))

from steps.step1_scraper import INEScraperConcurrent
from steps.step2_standardize_names import NameStandardizer
from steps.step3_remove_columns import ColumnRemover
from steps.step4_filter_stations import StationFilter
from steps.step5_create_views import ViewCreator
from steps.step6_upload_to_db import DatabaseUploader


class PipelineOrchestrator:
    def __init__(self):
        self.output_base = Path(Config.OUTPUT_DIR)
        self.inicio_pipeline = time.time()
        self.pasos_completados = []
        self.pasos_fallidos = []

    async def ejecutar_pipeline_completo(self):
        """Ejecuta los 6 pasos del pipeline en secuencia"""
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
║                                                                    ║
╚═══════════════════════════════════════════════════════════════════╝
        """)

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
                tiempo_total = creator.generar_vistas()
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

        finally:
            # SIEMPRE generar reporte consolidado, sin importar si hubo errores
            print("\n" + "="*80)
            print("GENERANDO REPORTE CONSOLIDADO DEL PIPELINE")
            print("="*80 + "\n")
            self.generar_reporte_consolidado()

    def generar_reporte_consolidado(self):
        """Genera un reporte consolidado de todo el pipeline"""
        try:
            tiempo_total = time.time() - self.inicio_pipeline

            # Buscar la carpeta de fecha más reciente
            fecha_folders = sorted([f for f in self.output_base.iterdir() if f.is_dir()], reverse=True)
            if not fecha_folders:
                print("[WARN] No se encontro carpeta de salida para generar reporte consolidado")
                return

            fecha_folder = fecha_folders[0]
            reporte_dir = fecha_folder / "reportes"

            if not reporte_dir.exists():
                print(f"[WARN] No se encontro carpeta de reportes: {reporte_dir}")
                return

            # Leer reportes individuales de cada paso
            reportes_individuales = {}
            for paso_num in range(1, 7):
                reporte_files = {
                    1: "paso1_scraper.json",
                    2: "paso2_standardize.json",
                    3: "paso3_remove_columns.json",
                    4: "paso4_filter_stations.json",
                    5: "paso5_create_views.json",
                    6: "paso6_upload_to_db.json"
                }

                reporte_path = reporte_dir / reporte_files[paso_num]
                if reporte_path.exists():
                    with open(reporte_path, 'r', encoding='utf-8') as f:
                        reportes_individuales[f"paso_{paso_num}"] = json.load(f)

            # Crear reporte consolidado
            reporte_consolidado = {
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "pipeline": "Pipeline Completo INE - Observatorio Ambiental",
                    "version": "2.0",
                    "fecha_ejecucion": fecha_folder.name
                },
                "resumen_pipeline": {
                    "pasos_totales": 6,
                    "pasos_completados": len(self.pasos_completados),
                    "pasos_fallidos": len(self.pasos_fallidos),
                    "tiempo_total_segundos": round(tiempo_total, 2),
                    "tiempo_total_minutos": round(tiempo_total / 60, 2),
                    "tiempo_total_horas": round(tiempo_total / 3600, 2)
                },
                "pasos_ejecutados": self.pasos_completados,
                "pasos_fallidos": self.pasos_fallidos,
                "reportes_individuales": reportes_individuales,
                "estructura_final": {
                    "raw": "Datos raw procesados (estandarizados, sin flags, filtrados)",
                    "views": "Vistas consolidadas generadas",
                    "reportes": "Reportes JSON de cada paso + reporte consolidado"
                }
            }

            # Guardar reporte consolidado
            reporte_path = reporte_dir / "pipeline_completo.json"
            with open(reporte_path, 'w', encoding='utf-8') as f:
                json.dump(reporte_consolidado, f, indent=2, ensure_ascii=False)

            # Imprimir resumen en consola
            print("\n" + "="*80)
            print("REPORTE CONSOLIDADO DEL PIPELINE".center(80))
            print("="*80)
            print(f"\nRESUMEN GENERAL:")
            print(f"   Fecha de ejecucion:       {fecha_folder.name}")
            print(f"   Pasos completados:        {len(self.pasos_completados)}/6")
            print(f"   Pasos fallidos:           {len(self.pasos_fallidos)}")
            print(f"   Tiempo total:             {tiempo_total/60:.1f} minutos ({tiempo_total:.1f}s)")

            print(f"\nDESGLOSE DE TIEMPOS:")
            for paso in self.pasos_completados:
                print(f"   Paso {paso['paso']} ({paso['nombre']}): {paso['duracion_segundos']:.1f}s")

            if self.pasos_fallidos:
                print(f"\nPASOS FALLIDOS:")
                for paso in self.pasos_fallidos:
                    print(f"   Paso {paso['paso']} ({paso['nombre']}): {paso['error'][:80]}")

            print(f"\nESTRUCTURA FINAL:")
            print(f"   {fecha_folder}/")
            print(f"   |-- raw/              (Datos procesados)")
            print(f"   |-- views/            (Vistas consolidadas)")
            print(f"   `-- reportes/         (Reportes JSON)")

            print(f"\nReporte consolidado guardado: {reporte_path}")
            print("="*80 + "\n")

        except Exception as e:
            print(f"[ERROR] Error al generar reporte consolidado: {e}")
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
