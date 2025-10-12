"""
INE Scraper - Versión Concurrente
Optimizado para AWS Lambda con múltiples navegadores en paralelo
"""

import asyncio
import json
import re
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout

from config import Config


class INEScraperConcurrent:
    def __init__(self):
        self.catalog_path = Config.CATALOG_PATH

        # Crear estructura de carpetas con fecha actual
        fecha_hoy = datetime.now().strftime("%d-%m-%Y")
        self.base_output_dir = Path(Config.OUTPUT_DIR) / fecha_hoy
        self.data_dir = self.base_output_dir / "data"
        self.reporte_dir = self.base_output_dir / "reporte"

        # Crear directorios solo si se guardan archivos localmente
        if Config.SAVE_LOCAL_FILES:
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
            await iframe_locator.locator('body').wait_for(timeout=5000, state='attached')

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

            file_size = 0
            filepath_str = ""

            if Config.SAVE_LOCAL_FILES:
                filepath = self.data_dir / filename
                await download.save_as(filepath)
                file_size = filepath.stat().st_size
                filepath_str = str(filepath)
            else:
                download_bytes = await download.read_all_bytes()
                file_size = len(download_bytes)

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
        print(f"📁 Carpeta de salida: {self.base_output_dir}\n")

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

    def generar_reporte(self, tiempo_total_segundos: float):
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

        if Config.SAVE_LOCAL_FILES:
            reporte_path = self.reporte_dir / "reporte_descarga.json"
            with open(reporte_path, 'w', encoding='utf-8') as f:
                json.dump(reporte, f, indent=2, ensure_ascii=False)
            print(f"📄 Reporte JSON guardado: {reporte_path}\n")

        return reporte


async def main():
    print("""
╔══════════════════════════════════════════════╗
║     INE SCRAPER - VERSIÓN CONCURRENTE        ║
║      Optimizado para AWS Lambda              ║
╚══════════════════════════════════════════════╝
    """)

    scraper = INEScraperConcurrent()
    scraper.cargar_catalogo()

    resultados, tiempo_total = await scraper.scrape_all_concurrent()

    scraper.generar_reporte(tiempo_total)

    print("✅ Proceso completado!")
    if Config.SAVE_LOCAL_FILES:
        fecha_hoy = datetime.now().strftime("%d-%m-%Y")
        print(f"📁 Archivos CSV: outputs/{fecha_hoy}/data/")
        print(f"📄 Reporte JSON: outputs/{fecha_hoy}/reporte/")


if __name__ == "__main__":
    asyncio.run(main())
