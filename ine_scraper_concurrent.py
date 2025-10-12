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

        print(f"\n[Worker-{worker_id}] [{idx}/{total}] {dataset_id}")
        print(f"   📄 {nombre[:70]}...")

        try:
            # PASO 1: Navegar con parámetro de idioma español
            url_espanol = url if 'lang=es' in url else f"{url}&lang=es"
            print(f"   🌐 Navegando...")

            await page.goto(url_espanol, wait_until="domcontentloaded", timeout=Config.DOWNLOAD_TIMEOUT * 1000)
            await page.wait_for_timeout(3000)

            # Verificar idioma
            await self.forzar_idioma_espanol(page)

            # PASO 2: Buscar y hacer hover en el menú Exportar
            print(f"   🔍 Buscando menú Exportar...")

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
                        print(f"   ✅ Menú encontrado")
                        break
                except:
                    continue

            if not menu_export:
                raise Exception("No se encontró el menú Exportar")

            # Hover para abrir submenú
            print(f"   🖱️  Abriendo menú...")
            await menu_export.hover()
            await page.wait_for_timeout(2000)

            # PASO 3: Buscar opción CSV
            print(f"   🔍 Buscando opción CSV...")

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
                        print(f"   ✅ Opción CSV encontrada")
                        break
                except:
                    continue

            if not opcion_csv:
                raise Exception("No se encontró opción CSV en el menú")

            # Click en CSV para abrir modal
            print(f"   🖱️  Haciendo click en CSV...")
            await opcion_csv.click()

            # Esperar modal
            print(f"   ⏳ Esperando modal...")
            await page.wait_for_timeout(3000)

            # PASO 4: Buscar iframe del modal
            print(f"   🔍 Buscando iframe del modal...")

            iframe_locator = page.frame_locator('iframe#DialogFrame')
            await iframe_locator.locator('body').wait_for(timeout=5000, state='attached')
            print(f"   ✅ Iframe encontrado")

            # PASO 5: Buscar botón de descarga DENTRO DEL IFRAME
            print(f"   🔍 Buscando botón Descargar...")

            boton_descargar = None

            try:
                inputs = await iframe_locator.locator('input[type="button"], input[type="submit"]').all()

                for inp in inputs:
                    try:
                        value = await inp.get_attribute('value')
                        if value and ('Descargar' in value or 'Download' in value or 'escargar' in value):
                            boton_descargar = inp
                            print(f"   ✅ Botón encontrado: '{value}'")
                            break
                    except:
                        continue
            except Exception as e:
                print(f"   ⚠️  Error buscando botón: {e}")

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
                            print(f"   ✅ Botón encontrado con selector")
                            break
                    except:
                        continue

            if not boton_descargar:
                raise Exception("No se encontró botón de descarga en el iframe")

            # PASO 6: Click en descargar y esperar descarga
            print(f"   📥 Descargando...")

            async with page.expect_download(timeout=45000) as download_info:
                await boton_descargar.click()

            download = await download_info.value

            # Guardar archivo con el NOMBRE del dataset
            nombre_archivo = self.limpiar_nombre_archivo(nombre)
            filename = f"{nombre_archivo}.csv"

            file_size = 0
            filepath_str = ""

            if Config.SAVE_LOCAL_FILES:
                filepath = self.data_dir / filename
                await download.save_as(filepath)
                file_size = filepath.stat().st_size
                filepath_str = str(filepath)
                size_kb = file_size / 1024
                print(f"   ✅ Descargado ({size_kb:.1f} KB)")
            else:
                # En Lambda, solo obtenemos los bytes para cargar a DB
                download_bytes = await download.read_all_bytes()
                file_size = len(download_bytes)
                print(f"   ✅ Descargado ({file_size / 1024:.1f} KB)")

            return {
                "id": dataset_id,
                "status": "exitoso",
                "filepath": filepath_str,
                "nombre": nombre,
                "nombre_archivo": filename,
                "size": file_size,
                "categoria": categoria,
                "worker_id": worker_id
            }

        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Error: {error_msg[:100]}")

            return {
                "id": dataset_id,
                "status": "fallido",
                "error": error_msg,
                "nombre": nombre,
                "url": url,
                "worker_id": worker_id
            }

    async def worker(self, worker_id: int, queue: asyncio.Queue, browser: Browser, total_datasets: int):
        """Worker que procesa datasets de la cola"""
        print(f"[Worker-{worker_id}] Iniciando...")

        try:
            context = await browser.new_context(
                viewport={'width': Config.VIEWPORT_WIDTH, 'height': Config.VIEWPORT_HEIGHT},
                user_agent=Config.USER_AGENT,
                accept_downloads=True
            )
            page = await context.new_page()
            page.set_default_timeout(Config.DOWNLOAD_TIMEOUT * 1000)

            print(f"[Worker-{worker_id}] Listo para procesar")

            while True:
                try:
                    # Obtener tarea de la cola
                    task = await asyncio.wait_for(queue.get(), timeout=2.0)

                    if task is None:  # Señal de terminación
                        print(f"[Worker-{worker_id}] Recibida señal de terminación")
                        queue.task_done()
                        break

                    idx, dataset = task
                    print(f"[Worker-{worker_id}] Procesando tarea {idx}/{total_datasets}")

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
                    # No hay tareas, seguir esperando
                    continue
                except Exception as e:
                    print(f"[Worker-{worker_id}] Error procesando tarea: {e}")
                    try:
                        queue.task_done()
                    except:
                        pass

            await context.close()
            print(f"[Worker-{worker_id}] Finalizado")

        except Exception as e:
            print(f"[Worker-{worker_id}] Error crítico al inicializar: {e}")
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
        print(f"📋 Agregando {total_datasets} tareas a la cola...")
        for idx, dataset in enumerate(datasets_a_procesar, 1):
            await queue.put((idx, dataset))
        print(f"✅ Cola creada con {queue.qsize()} tareas\n")

        async with async_playwright() as p:
            print("🌐 Lanzando navegador Chromium...")
            # Lanzar navegador
            browser = await p.chromium.launch(headless=Config.HEADLESS)
            print(f"✅ Navegador lanzado\n")

            # Crear workers
            print(f"👷 Creando {num_workers} workers...")
            workers = []
            for worker_id in range(num_workers):
                worker_task = asyncio.create_task(
                    self.worker(worker_id + 1, queue, browser, total_datasets)
                )
                workers.append(worker_task)

            # Dar tiempo a que los workers se inicialicen
            await asyncio.sleep(2)
            print(f"✅ Workers creados y listos\n")

            # Esperar a que se procesen todas las tareas
            print("⏳ Procesando descargas...\n")
            await queue.join()

            # Enviar señal de terminación a workers
            print("\n📡 Enviando señales de terminación a workers...")
            for _ in range(num_workers):
                await queue.put(None)

            # Esperar a que terminen todos los workers
            print("⏳ Esperando finalización de workers...")
            await asyncio.gather(*workers, return_exceptions=True)

            print("🔒 Cerrando navegador...")
            await browser.close()

        elapsed = time.time() - start_time

        print(f"\n{'='*60}")
        print(f"✨ Completado en {elapsed/60:.1f} minutos")
        print(f"   Velocidad: {elapsed/total_datasets:.1f} segundos/dataset")
        print(f"{'='*60}\n")

        return self.resultados

    def generar_reporte(self):
        """Genera reporte de resultados"""
        exitosos = len(self.resultados['exitosos'])
        fallidos = len(self.resultados['fallidos'])
        total = exitosos + fallidos

        print(f"""
╔══════════════════════════════════════════════╗
║           REPORTE DE DESCARGA                ║
╠══════════════════════════════════════════════╣
║ Total:     {total:<34}║
║ Exitosos:  {exitosos:<34}║
║ Fallidos:  {fallidos:<34}║
║ Tasa:      {(exitosos/total*100 if total > 0 else 0):.1f}% exitoso                      ║
╚══════════════════════════════════════════════╝
""")

        if exitosos > 0:
            total_size = sum(r['size'] for r in self.resultados['exitosos'])
            print(f"💾 Total descargado: {total_size / (1024*1024):.2f} MB\n")

        # Guardar reporte
        reporte = {
            "timestamp": datetime.now().isoformat(),
            "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "total": total,
            "exitosos": exitosos,
            "fallidos": fallidos,
            "tasa_exito": round(exitosos/total*100, 2) if total > 0 else 0,
            "workers_usados": Config.MAX_CONCURRENT_BROWSERS,
            "detalles": self.resultados
        }

        if Config.SAVE_LOCAL_FILES:
            reporte_path = self.reporte_dir / "reporte_descarga.json"
            with open(reporte_path, 'w', encoding='utf-8') as f:
                json.dump(reporte, f, indent=2, ensure_ascii=False)
            print(f"📄 Reporte: {reporte_path}")

        if fallidos > 0:
            print(f"\n❌ Primeros fallidos ({min(fallidos, 5)}):")
            for r in self.resultados['fallidos'][:5]:
                print(f"   - {r['id']}: {r.get('error', '')[:80]}")

        # Guardar también listado de archivos descargados
        if exitosos > 0 and Config.SAVE_LOCAL_FILES:
            listado_path = self.reporte_dir / "archivos_descargados.txt"
            with open(listado_path, 'w', encoding='utf-8') as f:
                f.write(f"ARCHIVOS DESCARGADOS - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
                f.write(f"{'='*80}\n\n")
                for r in self.resultados['exitosos']:
                    f.write(f"{r['nombre_archivo']}\n")
                    f.write(f"  ID: {r['id']}\n")
                    f.write(f"  Nombre: {r['nombre']}\n")
                    f.write(f"  Tamaño: {r['size'] / 1024:.1f} KB\n")
                    f.write(f"  Worker: {r['worker_id']}\n\n")

            print(f"📋 Listado: {listado_path}")

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

    await scraper.scrape_all_concurrent()

    scraper.generar_reporte()

    print("\n✅ Proceso completado!")
    if Config.SAVE_LOCAL_FILES:
        fecha_hoy = datetime.now().strftime("%d-%m-%Y")
        print(f"📁 Datos en: outputs/{fecha_hoy}/data/")
        print(f"📄 Reporte en: outputs/{fecha_hoy}/reporte/")


if __name__ == "__main__":
    asyncio.run(main())
