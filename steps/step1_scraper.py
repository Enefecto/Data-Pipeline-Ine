"""
INE Scraper - Versión Concurrente
Optimizado para AWS Lambda con múltiples navegadores en paralelo
"""

import asyncio
import json
import re
import time
import os
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout

from config import Config

# Agregar imports para storage
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.storage_factory import StorageFactory


class INEScraperConcurrent:
    def __init__(self):
        self.catalog_path = Config.CATALOG_PATH

        # Inicializar storage (Local o S3 según PRODUCTION)
        self.storage = StorageFactory.get_storage()
        self.fecha_hoy = datetime.now().strftime("%d-%m-%Y")

        # Para compatibilidad con código legacy (usado en rutas locales temporales)
        self.fecha_folder = Path(Config.OUTPUT_DIR) / self.fecha_hoy
        self.data_dir = self.fecha_folder / "raw"
        self.reporte_dir = self.fecha_folder / "reportes"

        # Crear directorios solo si es modo LOCAL
        if not Config.PRODUCTION:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.reporte_dir.mkdir(parents=True, exist_ok=True)

        self.datasets = []
        self.resultados = {
            "exitosos": [],
            "fallidos": []
        }
        self.lock = asyncio.Lock()  # Para acceso thread-safe a resultados

    def limpiar_nombre_archivo(self, nombre: str) -> str:
        """Convierte el nombre del dataset en un nombre de archivo válido"""
        nombre_limpio = re.sub(r'[^\w\s-]', '', nombre)
        nombre_limpio = re.sub(r'\s+', '_', nombre_limpio)
        return nombre_limpio[:100]

    def cargar_catalogo(self) -> List[Dict]:
        """Carga el catálogo de datasets"""
        print("📖 Cargando catálogo...")

        with open(self.catalog_path, 'r', encoding='utf-8') as f:
            catalogo = json.load(f)

        self.datasets = catalogo['datasets']
        print(f"   ✅ {len(self.datasets)} datasets cargados\n")

        return self.datasets

    async def forzar_idioma_espanol(self, page: Page) -> bool:
        """Asegura que la página esté en español"""
        try:
            # Verificar si ya está en español
            if '?lang=es' in page.url or await page.locator('a:has-text("Exportar")').count() > 0:
                return True

            # Buscar link de español
            for selector in ['a:has-text("Español")', 'a[href*="lang=es"]']:
                try:
                    link = page.locator(selector).first
                    if await link.count() > 0:
                        await link.click()
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        await page.wait_for_timeout(2000)
                        return True
                except:
                    continue

            return False
        except Exception as e:
            print(f"   ⚠️  Error cambiando idioma: {e}")
            return False

    async def descargar_dataset(self, page: Page, dataset_info: Dict, idx: int, total: int, worker_id: int) -> Dict:
        """Descarga un dataset individual"""
        dataset_id = dataset_info['id']
        url = dataset_info['url']
        nombre = dataset_info['nombre']
        categoria = dataset_info.get('categoria', 'general')

        start_time = time.time()
        paso_actual = ""

        try:
            # PASO 1: Navegar
            paso_actual = "navegación"
            url_espanol = url if 'lang=es' in url else f"{url}&lang=es"
            await page.goto(url_espanol, wait_until="domcontentloaded", timeout=Config.DOWNLOAD_TIMEOUT * 1000)
            await page.wait_for_timeout(3000)
            await self.forzar_idioma_espanol(page)

            # PASO 2: Buscar menú Exportar
            paso_actual = "búsqueda de menú Exportar"
            menu_export = None
            selectores_menu = [
                'li#menubar-export',
                'li#menubar-export a',
                'a:has-text("Exportar")',
                'div.menubar a:has-text("Exportar")',
                '#menubar-export'
            ]

            for selector in selectores_menu:
                try:
                    locator = page.locator(selector).first
                    if await locator.count() > 0:
                        menu_export = locator
                        break
                except:
                    continue

            if not menu_export:
                raise Exception("No se encontró el menú Exportar")

            await menu_export.hover()
            await page.wait_for_timeout(2000)

            # PASO 3: Buscar opción CSV
            paso_actual = "búsqueda de opción CSV"
            opcion_csv = None
            selectores_csv = [
                'li#menuitemExportCSV a',
                'a:has-text("Archivo de texto (CSV)")',
                'a:has-text("Text file (CSV)")',
                '[id*="ExportCSV"]'
            ]

            for selector in selectores_csv:
                try:
                    locator = page.locator(selector).first
                    if await locator.count() > 0 and await locator.is_visible():
                        opcion_csv = locator
                        break
                except:
                    continue

            if not opcion_csv:
                raise Exception("No se encontró opción CSV en el menú")

            await opcion_csv.click()
            await page.wait_for_timeout(3000)

            # PASO 4: Buscar iframe del modal
            paso_actual = "acceso a modal (iframe)"
            iframe_locator = page.frame_locator('iframe#DialogFrame')
            await iframe_locator.locator('body').wait_for(timeout=30000, state='attached')

            # PASO 5: Buscar botón de descarga
            paso_actual = "búsqueda de botón Descargar"
            boton_descargar = None

            try:
                inputs = await iframe_locator.locator('input[type="button"], input[type="submit"]').all()
                for inp in inputs:
                    try:
                        value = await inp.get_attribute('value')
                        if value and ('Descargar' in value or 'Download' in value or 'escargar' in value):
                            boton_descargar = inp
                            break
                    except:
                        continue
            except:
                pass

            if not boton_descargar:
                selectores_boton = [
                    'input[value="Descargar"]',
                    'input[value="Download"]',
                    '[id*="btnExport"]'
                ]
                for selector in selectores_boton:
                    try:
                        locator = iframe_locator.locator(selector).first
                        if await locator.count() > 0:
                            boton_descargar = locator
                            break
                    except:
                        continue

            if not boton_descargar:
                raise Exception("No se encontró botón de descarga")

            # PASO 6: Descargar archivo
            paso_actual = "descarga de archivo"
            async with page.expect_download(timeout=45000) as download_info:
                await boton_descargar.click()

            download = await download_info.value

            # Guardar archivo
            nombre_archivo = self.limpiar_nombre_archivo(nombre)
            filename = f"{nombre_archivo}.csv"

            # Descargar a archivo temporal primero (compatible con Playwright v1.40.0)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
                tmp_path = tmp_file.name

            await download.save_as(tmp_path)

            # Leer el archivo temporal como bytes
            with open(tmp_path, 'rb') as f:
                download_bytes = f.read()

            file_size = len(download_bytes)

            # Guardar usando StorageFactory (local o S3 según configuración)
            subfolder = f"{self.fecha_hoy}/raw"
            self.storage.save_file(download_bytes, filename, subfolder)
            filepath_str = f"{subfolder}/{filename}"

            # Eliminar archivo temporal
            os.unlink(tmp_path)

            elapsed = time.time() - start_time
            size_kb = file_size / 1024

            # Mensaje de éxito consolidado
            print(f"[{idx}/{total}] ✓ {nombre} ({size_kb:.0f} KB)", flush=True)

            return {
                "id": dataset_id,
                "status": "exitoso",
                "filepath": filepath_str,
                "nombre": nombre,
                "nombre_archivo": filename,
                "size": file_size,
                "size_kb": round(size_kb, 2),
                "categoria": categoria,
                "duracion_segundos": round(elapsed, 2),
                "worker_id": worker_id
            }

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)

            # Mensaje de error consolidado con paso donde falló
            print(f"[{idx}/{total}] ✗ {nombre} - Error en {paso_actual}: {error_msg[:50]}", flush=True)

            return {
                "id": dataset_id,
                "status": "fallido",
                "error": error_msg,
                "paso_fallo": paso_actual,
                "nombre": nombre,
                "url": url,
                "duracion_segundos": round(elapsed, 2),
                "worker_id": worker_id
            }

    async def worker(self, worker_id: int, queue: asyncio.Queue, browser: Browser, total_datasets: int):
        """Worker que procesa datasets de la cola"""
        try:
            context = await browser.new_context(
                viewport={'width': Config.VIEWPORT_WIDTH, 'height': Config.VIEWPORT_HEIGHT},
                user_agent=Config.USER_AGENT,
                accept_downloads=True
            )
            page = await context.new_page()
            page.set_default_timeout(Config.DOWNLOAD_TIMEOUT * 1000)

            while True:
                try:
                    task = await asyncio.wait_for(queue.get(), timeout=2.0)

                    if task is None:  # Señal de terminación
                        queue.task_done()
                        break

                    idx, dataset = task

                    # Procesar descarga
                    resultado = await self.descargar_dataset(page, dataset, idx, total_datasets, worker_id)

                    # Guardar resultado de forma thread-safe
                    async with self.lock:
                        if resultado['status'] == 'exitoso':
                            self.resultados['exitosos'].append(resultado)
                        else:
                            self.resultados['fallidos'].append(resultado)

                    # Pausa entre descargas
                    await asyncio.sleep(Config.DELAY_BETWEEN_DOWNLOADS)

                    queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"\n[W{worker_id}] ERROR CRÍTICO: {e}", flush=True)
                    try:
                        queue.task_done()
                    except:
                        pass

            await context.close()

        except Exception as e:
            print(f"\n[W{worker_id}] ERROR AL INICIALIZAR: {e}", flush=True)
            import traceback
            traceback.print_exc()

    async def scrape_all_concurrent(self):
        """Descarga todos los datasets usando múltiples navegadores concurrentes"""
        print("🚀 Iniciando descarga CONCURRENTE...")

        Config.print_config()

        # Determinar datasets a procesar
        if Config.MAX_DATASETS:
            datasets_a_procesar = self.datasets[:Config.MAX_DATASETS]
            print(f"\n📦 Datasets: {Config.MAX_DATASETS} (TEST)")
        else:
            datasets_a_procesar = self.datasets
            print(f"\n📦 Datasets: {len(datasets_a_procesar)} (COMPLETO)")

        total_datasets = len(datasets_a_procesar)
        num_workers = Config.MAX_CONCURRENT_BROWSERS

        # Estimación de tiempo (aprox. 12 segundos por dataset / número de workers)
        tiempo_estimado = (total_datasets * 12) / num_workers / 60
        print(f"⏱️  Estimado: {tiempo_estimado:.1f} minutos con {num_workers} navegadores")
        print(f"📁 Carpeta de salida: {self.data_dir}\n")

        start_time = time.time()

        # Crear cola de tareas
        queue = asyncio.Queue()
        for idx, dataset in enumerate(datasets_a_procesar, 1):
            await queue.put((idx, dataset))

        print("\n" + "=" * 80)
        print("INICIANDO DESCARGA")
        print("=" * 80 + "\n")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=Config.HEADLESS)

            # Crear workers
            workers = []
            for worker_id in range(num_workers):
                worker_task = asyncio.create_task(
                    self.worker(worker_id + 1, queue, browser, total_datasets)
                )
                workers.append(worker_task)

            await asyncio.sleep(1)

            # Esperar a que se procesen todas las tareas
            await queue.join()

            # Enviar señal de terminación a workers
            for _ in range(num_workers):
                await queue.put(None)

            await asyncio.gather(*workers, return_exceptions=True)
            await browser.close()

        elapsed = time.time() - start_time

        return self.resultados, elapsed

    async def retry_failed_datasets(self):
        """Reintenta descargar los datasets que fallaron en el primer intento"""
        fallidos = self.resultados['fallidos'].copy()

        if not fallidos:
            return 0, 0  # No hay nada que reintentar

        num_fallidos = len(fallidos)
        print("\n" + "=" * 80)
        print(f"🔄 REINTENTANDO DATASETS FALLIDOS ({num_fallidos})")
        print("=" * 80 + "\n")

        # Limpiar la lista de fallidos actual (se rellenará con los que sigan fallando)
        self.resultados['fallidos'] = []

        start_time = time.time()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=Config.HEADLESS)
            context = await browser.new_context(
                viewport={'width': Config.VIEWPORT_WIDTH, 'height': Config.VIEWPORT_HEIGHT},
                user_agent=Config.USER_AGENT,
                accept_downloads=True
            )
            page = await context.new_page()
            page.set_default_timeout(Config.DOWNLOAD_TIMEOUT * 1000)

            for idx, dataset_fallido in enumerate(fallidos, 1):
                # Reconstruir la información del dataset desde el resultado fallido
                dataset_info = {
                    'id': dataset_fallido['id'],
                    'url': dataset_fallido['url'],
                    'nombre': dataset_fallido['nombre'],
                    'categoria': dataset_fallido.get('categoria', 'general')
                }

                # Intentar descargar nuevamente
                resultado = await self.descargar_dataset(page, dataset_info, idx, num_fallidos, worker_id=0)

                # Actualizar resultados
                if resultado['status'] == 'exitoso':
                    # Marcar que fue exitoso después de reintento
                    resultado['fue_reintentado'] = True
                    resultado['intento_previo_fallo'] = dataset_fallido.get('error', 'Unknown')
                    self.resultados['exitosos'].append(resultado)
                else:
                    # Sigue fallando, agregar a la lista de fallidos
                    resultado['fue_reintentado'] = True
                    self.resultados['fallidos'].append(resultado)

                # Pausa entre reintentos
                await asyncio.sleep(Config.DELAY_BETWEEN_DOWNLOADS)

            await context.close()
            await browser.close()

        elapsed = time.time() - start_time

        # Calcular cuántos tuvieron éxito en el reintento
        exitosos_reintento = sum(1 for r in self.resultados['exitosos'] if r.get('fue_reintentado', False))
        fallidos_reintento = len(self.resultados['fallidos'])

        print("\n" + "=" * 80)
        print(f"✅ REINTENTO COMPLETADO: {exitosos_reintento} exitosos, {fallidos_reintento} fallidos")
        print("=" * 80 + "\n")

        return exitosos_reintento, elapsed

    def generar_reporte(self, tiempo_total_segundos: float, exitosos_reintento: int = 0):
        """Genera reporte de resultados en consola y JSON"""
        exitosos = len(self.resultados['exitosos'])
        fallidos = len(self.resultados['fallidos'])
        total = exitosos + fallidos
        tasa_exito = (exitosos/total*100 if total > 0 else 0)

        # Calcular estadísticas
        total_size = sum(r['size'] for r in self.resultados['exitosos']) if exitosos > 0 else 0
        total_size_mb = total_size / (1024*1024)

        duracion_promedio = sum(r['duracion_segundos'] for r in self.resultados['exitosos']) / exitosos if exitosos > 0 else 0
        duracion_min = min((r['duracion_segundos'] for r in self.resultados['exitosos']), default=0)
        duracion_max = max((r['duracion_segundos'] for r in self.resultados['exitosos']), default=0)

        # REPORTE EN CONSOLA
        print("\n" + "=" * 80)
        print("REPORTE FINAL DE DESCARGA".center(80))
        print("=" * 80)
        print(f"\n📊 RESUMEN:")
        print(f"   Total de datasets:        {total}")
        if exitosos_reintento > 0:
            print(f"   ✓ Exitosos:               {exitosos} ({exitosos_reintento} reintentado(s))")
        else:
            print(f"   ✓ Exitosos:               {exitosos}")
        print(f"   ✗ Fallidos:               {fallidos}")
        print(f"   Tasa de éxito:            {tasa_exito:.1f}%")
        print(f"\n⏱️  TIEMPOS:")
        print(f"   Tiempo total:             {tiempo_total_segundos/60:.1f} minutos ({tiempo_total_segundos:.1f}s)")
        print(f"   Tiempo promedio/dataset:  {duracion_promedio:.1f}s")
        print(f"   Tiempo más rápido:        {duracion_min:.1f}s")
        print(f"   Tiempo más lento:         {duracion_max:.1f}s")
        print(f"   Velocidad:                {tiempo_total_segundos/total:.1f}s/dataset (con {Config.MAX_CONCURRENT_BROWSERS} workers)")
        print(f"\n💾 DATOS:")
        print(f"   Total descargado:         {total_size_mb:.2f} MB")
        print(f"   Tamaño promedio:          {total_size/exitosos/1024:.1f} KB" if exitosos > 0 else "   Tamaño promedio:          N/A")

        # Mostrar fallidos si existen
        if fallidos > 0:
            print(f"\n❌ DATASETS FALLIDOS ({fallidos}):")
            for r in self.resultados['fallidos']:
                print(f"   [{r['id']}] {r['nombre']}")
                print(f"      └─ Error en {r['paso_fallo']}: {r['error'][:60]}")

        print("\n" + "=" * 80 + "\n")

        # GUARDAR REPORTE JSON
        reporte = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "version_scraper": "2.0 - Concurrente"
            },
            "configuracion": {
                "workers_concurrentes": Config.MAX_CONCURRENT_BROWSERS,
                "timeout_descarga": Config.DOWNLOAD_TIMEOUT,
                "delay_entre_descargas": Config.DELAY_BETWEEN_DOWNLOADS,
                "modo_headless": Config.HEADLESS
            },
            "resumen": {
                "total_datasets": total,
                "exitosos": exitosos,
                "exitosos_reintentados": exitosos_reintento,
                "fallidos": fallidos,
                "tasa_exito_porcentaje": round(tasa_exito, 2)
            },
            "tiempos": {
                "total_segundos": round(tiempo_total_segundos, 2),
                "total_minutos": round(tiempo_total_segundos/60, 2),
                "promedio_por_dataset_segundos": round(duracion_promedio, 2),
                "minimo_segundos": round(duracion_min, 2),
                "maximo_segundos": round(duracion_max, 2),
                "velocidad_segundos_por_dataset": round(tiempo_total_segundos/total, 2) if total > 0 else 0
            },
            "datos": {
                "total_bytes": total_size,
                "total_mb": round(total_size_mb, 2),
                "promedio_kb_por_dataset": round(total_size/exitosos/1024, 2) if exitosos > 0 else 0
            },
            "datasets_exitosos": self.resultados['exitosos'],
            "datasets_fallidos": self.resultados['fallidos']
        }

        # Guardar reporte usando StorageFactory
        self.storage.save_json(reporte, "paso1_scraper.json", f"{self.fecha_hoy}/reportes")
        print(f"[OK] Reporte JSON guardado: {self.fecha_hoy}/reportes/paso1_scraper.json\n")

        return reporte


def limpiar_ejecucion_previa_si_existe():
    """
    Limpia la ejecución previa del mismo día si existe.
    Útil para desarrollo donde se ejecutan múltiples pipelines en el mismo día.
    """
    storage = StorageFactory.get_storage()
    fecha_hoy = datetime.now().strftime("%d-%m-%Y")

    print("\n" + "="*80)
    print("VERIFICACION DE EJECUCION PREVIA".center(80))
    print("="*80)
    print(f"\n🔍 Verificando si existe una ejecución previa para: {fecha_hoy}")
    print("")

    # Verificar si ya existe una ejecución para hoy
    if storage.folder_exists(fecha_hoy):
        print(f"\n⚠️  ATENCION: Ya existe una ejecución para la fecha: {fecha_hoy}")
        print("   Esta ejecución será eliminada para comenzar desde cero...")
        print("")

        resultado = storage.delete_folder(fecha_hoy)

        if resultado:
            print(f"\n   ✅ Ejecución previa eliminada exitosamente")
            print("   El pipeline comenzará con datos limpios")
        else:
            print(f"\n   ⚠️  No se pudo eliminar la ejecución previa")
            print("   Esto puede causar conflictos. Verifica los permisos.")

        print("\n" + "="*80 + "\n")
    else:
        print(f"\n✅ No hay ejecución previa para {fecha_hoy}")
        print("   Comenzando pipeline limpio desde cero")
        print("\n" + "="*80 + "\n")


async def main():
    print("""
╔══════════════════════════════════════════════╗
║     INE SCRAPER - VERSIÓN CONCURRENTE        ║
║      Optimizado para AWS Lambda              ║
╚══════════════════════════════════════════════╝
    """)

    # IMPORTANTE: Limpiar ejecución previa ANTES de inicializar el scraper
    limpiar_ejecucion_previa_si_existe()

    scraper = INEScraperConcurrent()
    scraper.cargar_catalogo()

    # Ejecutar descarga inicial
    resultados, tiempo_total = await scraper.scrape_all_concurrent()

    exitosos_reintento = 0
    tiempo_reintento = 0

    # Si hay fallidos, intentar reintentar automáticamente
    if len(scraper.resultados['fallidos']) > 0:
        exitosos_reintento, tiempo_reintento = await scraper.retry_failed_datasets()
        tiempo_total += tiempo_reintento

    # Generar reporte final con información de reintentos
    scraper.generar_reporte(tiempo_total, exitosos_reintento)

    print("✅ Proceso completado!")
    if Config.SAVE_LOCAL_FILES:
        fecha_hoy = datetime.now().strftime("%d-%m-%Y")
        print(f"📁 Archivos CSV: outputs/{fecha_hoy}/raw/")
        print(f"📄 Reporte JSON: outputs/{fecha_hoy}/reportes/")


if __name__ == "__main__":
    asyncio.run(main())
