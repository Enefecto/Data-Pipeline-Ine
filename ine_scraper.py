"""
INE Scraper - Descargador automatizado de datasets del INE Chile
Versión Final - 100% Funcional
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import json
from pathlib import Path
from datetime import datetime
import time
import re

class INEScraper:
    def __init__(self, catalog_path="/app/ine_catalog.json"):
        self.catalog_path = catalog_path

        # Crear estructura de carpetas con fecha actual
        fecha_hoy = datetime.now().strftime("%d-%m-%Y")
        self.base_output_dir = Path(f"/app/outputs/{fecha_hoy}")
        self.data_dir = self.base_output_dir / "data"
        self.reporte_dir = self.base_output_dir / "reporte"

        # Crear directorios
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.reporte_dir.mkdir(parents=True, exist_ok=True)

        self.datasets = []
        self.resultados = {
            "exitosos": [],
            "fallidos": []
        }

    def limpiar_nombre_archivo(self, nombre):
        """Convierte el nombre del dataset en un nombre de archivo válido"""
        # Eliminar caracteres especiales y reemplazar espacios
        nombre_limpio = re.sub(r'[^\w\s-]', '', nombre)
        nombre_limpio = re.sub(r'\s+', '_', nombre_limpio)
        # Limitar longitud
        return nombre_limpio[:100]

    def cargar_catalogo(self):
        """Carga el catálogo de datasets"""
        print("📖 Cargando catálogo...")

        with open(self.catalog_path, 'r', encoding='utf-8') as f:
            catalogo = json.load(f)

        self.datasets = catalogo['datasets']
        print(f"   ✅ {len(self.datasets)} datasets cargados\n")

        return self.datasets

    def forzar_idioma_espanol(self, page):
        """Asegura que la página esté en español"""
        try:
            # Verificar si ya está en español
            if '?lang=es' in page.url or page.locator('a:has-text("Exportar")').count() > 0:
                return True

            # Buscar link de español
            for selector in ['a:has-text("Español")', 'a[href*="lang=es"]']:
                try:
                    link = page.locator(selector).first
                    if link.count() > 0:
                        link.click()
                        page.wait_for_load_state("networkidle", timeout=10000)
                        page.wait_for_timeout(2000)
                        return True
                except:
                    continue

            return False
        except Exception as e:
            print(f"   ⚠️  Error cambiando idioma: {e}")
            return False

    def descargar_dataset(self, page, dataset_info, idx, total):
        """Descarga un dataset individual"""
        dataset_id = dataset_info['id']
        url = dataset_info['url']
        nombre = dataset_info['nombre']
        categoria = dataset_info.get('categoria', 'general')

        print(f"\n[{idx}/{total}] {dataset_id}")
        print(f"   📄 {nombre[:70]}...")

        try:
            # PASO 1: Navegar con parámetro de idioma español
            url_espanol = url if 'lang=es' in url else f"{url}&lang=es"
            print(f"   🌐 Navegando...")

            page.goto(url_espanol, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(5000)

            # Verificar idioma
            self.forzar_idioma_espanol(page)

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
                    if locator.count() > 0:
                        menu_export = locator
                        print(f"   ✅ Menú encontrado")
                        break
                except:
                    continue

            if not menu_export:
                raise Exception("No se encontró el menú Exportar")

            # Hover para abrir submenú
            print(f"   🖱️  Abriendo menú...")
            menu_export.hover()
            page.wait_for_timeout(2000)

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
                    if locator.count() > 0 and locator.is_visible():
                        opcion_csv = locator
                        print(f"   ✅ Opción CSV encontrada")
                        break
                except:
                    continue

            if not opcion_csv:
                raise Exception("No se encontró opción CSV en el menú")

            # Click en CSV para abrir modal
            print(f"   🖱️  Haciendo click en CSV...")
            opcion_csv.click()

            # Esperar modal
            print(f"   ⏳ Esperando modal...")
            page.wait_for_timeout(5000)

            # Intentar detectar modal
            try:
                page.wait_for_selector('.ui-dialog, [role="dialog"], .modal, .popup, input[type="submit"]', timeout=5000, state='attached')
                print(f"   ✅ Modal detectado")
            except:
                print(f"   ⚠️  Modal no detectado estándar, continuando...")

            page.wait_for_timeout(2000)

            # PASO 4: Buscar iframe del modal (¡LA CLAVE!)
            print(f"   🔍 Buscando iframe del modal...")
            iframe_found = False
            modal_frame = None

            try:
                # Buscar iframe con id DialogFrame
                iframe_locator = page.frame_locator('iframe#DialogFrame')
                # Verificar que el iframe está cargado
                iframe_locator.locator('body').wait_for(timeout=5000, state='attached')
                modal_frame = iframe_locator
                iframe_found = True
                print(f"   ✅ Iframe encontrado")
            except Exception as e:
                print(f"   ⚠️  Error buscando iframe: {e}")
                raise Exception("No se encontró iframe del modal")

            # PASO 5: Buscar botón de descarga DENTRO DEL IFRAME
            print(f"   🔍 Buscando botón Descargar...")

            boton_descargar = None

            try:
                # Buscar inputs dentro del iframe
                inputs = modal_frame.locator('input[type="button"], input[type="submit"]').all()

                for i, inp in enumerate(inputs):
                    try:
                        value = inp.get_attribute('value')
                        visible = inp.is_visible()

                        # Buscar específicamente "Descargar" o "Download"
                        if value and ('Descargar' in value or 'Download' in value or 'escargar' in value):
                            boton_descargar = inp
                            print(f"   ✅ Botón encontrado: '{value}'")
                            break
                    except:
                        continue
            except Exception as e:
                print(f"   ⚠️  Error buscando botón: {e}")

            # Si no encontró, intentar selectores específicos
            if not boton_descargar:
                selectores_boton = [
                    'input[value="Descargar"]',
                    'input[value="Download"]',
                    '[id*="btnExport"]'
                ]

                for selector in selectores_boton:
                    try:
                        locator = modal_frame.locator(selector).first
                        if locator.count() > 0:
                            boton_descargar = locator
                            print(f"   ✅ Botón encontrado con selector")
                            break
                    except:
                        continue

            if not boton_descargar:
                raise Exception("No se encontró botón de descarga en el iframe")

            # PASO 6: Click en descargar y esperar descarga
            print(f"   📥 Descargando...")

            with page.expect_download(timeout=45000) as download_info:
                boton_descargar.click()

            download = download_info.value

            # Guardar archivo con el NOMBRE del dataset
            nombre_archivo = self.limpiar_nombre_archivo(nombre)
            filename = f"{nombre_archivo}.csv"
            filepath = self.data_dir / filename
            download.save_as(filepath)

            size_kb = filepath.stat().st_size / 1024
            print(f"   ✅ Descargado ({size_kb:.1f} KB)")

            return {
                "id": dataset_id,
                "status": "exitoso",
                "filepath": str(filepath),
                "nombre": nombre,
                "nombre_archivo": filename,
                "size": filepath.stat().st_size,
                "categoria": categoria
            }

        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Error: {error_msg[:100]}")

            return {
                "id": dataset_id,
                "status": "fallido",
                "error": error_msg,
                "nombre": nombre,
                "url": url
            }

    def scrape_all(self, max_datasets=None, headless=True):
        """Descarga todos los datasets"""
        print("🚀 Iniciando descarga...")

        if max_datasets:
            datasets_a_procesar = self.datasets[:max_datasets]
            print(f"📦 Datasets: {max_datasets} (TEST)")
        else:
            datasets_a_procesar = self.datasets
            print(f"📦 Datasets: {len(datasets_a_procesar)} (COMPLETO)")

        print(f"⏱️  Estimado: {len(datasets_a_procesar) * 12 / 60:.1f} minutos")
        print(f"📁 Carpeta de salida: {self.base_output_dir}\n")

        start_time = time.time()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                accept_downloads=True
            )
            page = context.new_page()
            page.set_default_timeout(60000)

            total = len(datasets_a_procesar)

            for idx, dataset in enumerate(datasets_a_procesar, 1):
                resultado = self.descargar_dataset(page, dataset, idx, total)

                if resultado['status'] == 'exitoso':
                    self.resultados['exitosos'].append(resultado)
                else:
                    self.resultados['fallidos'].append(resultado)

                # Pausa entre descargas
                time.sleep(2)

            browser.close()

        elapsed = time.time() - start_time

        print(f"\n{'='*60}")
        print(f"✨ Completado en {elapsed/60:.1f} minutos")
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
            "detalles": self.resultados
        }

        reporte_path = self.reporte_dir / "reporte_descarga.json"
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"📄 Reporte: {reporte_path}")

        if fallidos > 0:
            print(f"\n❌ Primeros fallidos ({min(fallidos, 5)}):")
            for r in self.resultados['fallidos'][:5]:
                print(f"   - {r['id']}: {r.get('error', '')[:80]}")

        # Guardar también listado de archivos descargados
        if exitosos > 0:
            listado_path = self.reporte_dir / "archivos_descargados.txt"
            with open(listado_path, 'w', encoding='utf-8') as f:
                f.write(f"ARCHIVOS DESCARGADOS - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
                f.write(f"{'='*80}\n\n")
                for r in self.resultados['exitosos']:
                    f.write(f"{r['nombre_archivo']}\n")
                    f.write(f"  ID: {r['id']}\n")
                    f.write(f"  Nombre: {r['nombre']}\n")
                    f.write(f"  Tamaño: {r['size'] / 1024:.1f} KB\n\n")

            print(f"📋 Listado: {listado_path}")


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════╗
║       INE SCRAPER - VERSIÓN FINAL            ║
║          100% FUNCIONAL                      ║
╚══════════════════════════════════════════════╝
    """)

    scraper = INEScraper()
    scraper.cargar_catalogo()

    # DESCARGA COMPLETA DE TODOS LOS DATASETS
    # Para test, cambiar a max_datasets=3
    scraper.scrape_all(max_datasets=None, headless=True)

    scraper.generar_reporte()

    print("\n✅ Proceso completado!")
    fecha_hoy = datetime.now().strftime("%d-%m-%Y")
    print(f"📁 Datos en: outputs/{fecha_hoy}/data/")
    print(f"📄 Reporte en: outputs/{fecha_hoy}/reporte/")
