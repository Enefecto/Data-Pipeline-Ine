"""
Generate Consolidated Report - Generador de Reporte Consolidado
Lee los reportes individuales de cada paso y genera un reporte consolidado final
Etapa 7 del pipeline (final)
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Agregar el directorio padre al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config
from utils.storage_factory import StorageFactory


class ReportGenerator:
    def __init__(self):
        # Inicializar storage (S3 o Local según configuración)
        self.storage = StorageFactory.get_storage()
        self.fecha_hoy = datetime.now().strftime("%d-%m-%Y")

        # Verificar que exista la carpeta de fecha
        if not self.storage.folder_exists(self.fecha_hoy):
            raise Exception(f"No se encontró la carpeta de ejecución para la fecha: {self.fecha_hoy}")

        print(f"[INFO] Generando reporte consolidado para: {self.fecha_hoy}")

        self.pasos_completados = []
        self.pasos_fallidos = []
        self.reportes_individuales = {}
        self.tiempo_total_pipeline = 0

    def leer_reportes_individuales(self):
        """Lee todos los reportes individuales de cada paso"""
        reportes_subfolder = f"{self.fecha_hoy}/reportes"

        print(f"[INFO] Leyendo reportes individuales desde: {reportes_subfolder}")

        reporte_files = {
            1: "paso1_scraper.json",
            2: "paso2_standardize.json",
            3: "paso3_remove_columns.json",
            4: "paso4_filter_stations.json",
            5: "paso5_create_views.json",
            6: "paso6_upload_to_db.json"
        }

        for paso_num in range(1, 7):
            try:
                filename = reporte_files[paso_num]
                reporte_data = self.storage.load_json(filename, reportes_subfolder)
                self.reportes_individuales[f"paso_{paso_num}"] = reporte_data

                # Extraer tiempo de ejecución (buscar en diferentes ubicaciones según el paso)
                tiempo_paso = None

                # Opción 1: tiempo_total en raíz (algunos pasos)
                if "tiempo_total" in reporte_data:
                    tiempo_paso = reporte_data["tiempo_total"]
                # Opción 2: tiempos.total_segundos (otros pasos)
                elif "tiempos" in reporte_data and "total_segundos" in reporte_data["tiempos"]:
                    tiempo_paso = reporte_data["tiempos"]["total_segundos"]

                if tiempo_paso is not None:
                    self.tiempo_total_pipeline += tiempo_paso

                    # Determinar el nombre del paso
                    nombre_paso = filename.replace("paso", "Paso ").replace(".json", "").replace("_", " ").title()

                    self.pasos_completados.append({
                        "paso": paso_num,
                        "nombre": nombre_paso,
                        "duracion_segundos": round(tiempo_paso, 2),
                        "exitoso": True
                    })

                print(f"   ✓ Reporte paso {paso_num} cargado")

            except Exception as e:
                nombre_paso = reporte_files[paso_num].replace("paso", "Paso ").replace(".json", "").replace("_", " ").title()
                print(f"   ⚠️  Reporte paso {paso_num} no encontrado: {reporte_files[paso_num]}")

                self.pasos_fallidos.append({
                    "paso": paso_num,
                    "nombre": nombre_paso,
                    "error": f"Reporte no encontrado: {str(e)}"
                })

    def crear_reporte_consolidado(self) -> Dict:
        """Crea el diccionario con el reporte consolidado"""
        return {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "pipeline": "Pipeline Completo INE - Observatorio Ambiental",
                "version": "2.0",
                "fecha_ejecucion": self.fecha_hoy,
                "storage_mode": Config.STORAGE_MODE,
                "generado_por": "step7_generate_report.py"
            },
            "resumen_pipeline": {
                "pasos_totales": 6,
                "pasos_completados": len(self.pasos_completados),
                "pasos_fallidos": len(self.pasos_fallidos),
                "tiempo_total_segundos": round(self.tiempo_total_pipeline, 2),
                "tiempo_total_minutos": round(self.tiempo_total_pipeline / 60, 2),
                "tiempo_total_horas": round(self.tiempo_total_pipeline / 3600, 2)
            },
            "pasos_ejecutados": self.pasos_completados,
            "pasos_fallidos": self.pasos_fallidos,
            "reportes_individuales": self.reportes_individuales,
            "estructura_final": {
                "raw": "Datos raw procesados (estandarizados, sin flags, filtrados)",
                "views": "Vistas consolidadas generadas",
                "reportes": "Reportes JSON de cada paso + reporte consolidado"
            }
        }

    def guardar_reporte(self, reporte_consolidado: Dict):
        """Guarda el reporte consolidado en storage"""
        reportes_subfolder = f"{self.fecha_hoy}/reportes"

        try:
            self.storage.save_json(reporte_consolidado, "pipeline_completo.json", reportes_subfolder)
            print(f"\n[OK] Reporte consolidado guardado: {reportes_subfolder}/pipeline_completo.json")
        except Exception as e:
            print(f"\n[ERROR] No se pudo guardar el reporte consolidado: {e}")
            raise

    def imprimir_resumen(self):
        """Imprime el resumen del pipeline en consola"""
        print("\n" + "="*80)
        print("REPORTE CONSOLIDADO DEL PIPELINE".center(80))
        print("="*80)
        print(f"\nRESUMEN GENERAL:")
        print(f"   Fecha de ejecucion:       {self.fecha_hoy}")
        print(f"   Storage mode:             {Config.STORAGE_MODE}")
        print(f"   Pasos completados:        {len(self.pasos_completados)}/6")
        print(f"   Pasos fallidos:           {len(self.pasos_fallidos)}")
        print(f"   Tiempo total:             {self.tiempo_total_pipeline/60:.1f} minutos ({self.tiempo_total_pipeline:.1f}s)")

        if self.pasos_completados:
            print(f"\nDESGLOSE DE TIEMPOS:")
            for paso in self.pasos_completados:
                print(f"   Paso {paso['paso']} ({paso['nombre']}): {paso['duracion_segundos']:.1f}s")

        if self.pasos_fallidos:
            print(f"\nPASOS FALLIDOS:")
            for paso in self.pasos_fallidos:
                error_msg = paso['error'][:80] if len(paso['error']) > 80 else paso['error']
                print(f"   Paso {paso['paso']} ({paso['nombre']}): {error_msg}")

        print(f"\nESTRUCTURA FINAL:")
        print(f"   {self.fecha_hoy}/")
        print(f"   |-- raw/              (Datos procesados)")
        print(f"   |-- views/            (Vistas consolidadas)")
        print(f"   `-- reportes/         (Reportes JSON + consolidado)")

        print("\n" + "="*80)
        print("\n[OK] REPORTE CONSOLIDADO GENERADO EXITOSAMENTE!\n")

    def generar_reporte(self, tiempo_ejecucion: float):
        """
        Método principal para generar el reporte consolidado

        Args:
            tiempo_ejecucion: Tiempo de ejecución de este paso (para consistencia con otros steps)
        """
        # 1. Leer reportes individuales
        self.leer_reportes_individuales()

        # 2. Crear reporte consolidado
        reporte_consolidado = self.crear_reporte_consolidado()

        # 3. Guardar reporte
        self.guardar_reporte(reporte_consolidado)

        # 4. Imprimir resumen en consola
        self.imprimir_resumen()

        # 5. Generar reporte JSON de este paso (para consistencia)
        reporte_paso7 = {
            "paso": 7,
            "nombre": "Generate Consolidated Report",
            "timestamp": datetime.now().isoformat(),
            "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "tiempo_total": tiempo_ejecucion,
            "reportes_leidos": len(self.reportes_individuales),
            "pasos_completados": len(self.pasos_completados),
            "pasos_fallidos": len(self.pasos_fallidos),
            "tiempo_total_pipeline_segundos": round(self.tiempo_total_pipeline, 2),
            "tiempo_total_pipeline_minutos": round(self.tiempo_total_pipeline / 60, 2)
        }

        reportes_subfolder = f"{self.fecha_hoy}/reportes"
        self.storage.save_json(reporte_paso7, "paso7_generate_report.json", reportes_subfolder)
        print(f"[OK] Reporte JSON guardado: {reportes_subfolder}/paso7_generate_report.json\n")


def main():
    """Función principal para ejecutar el paso 7 de forma independiente"""
    print("""
╔═══════════════════════════════════════════════════════════════════╗
║                                                                    ║
║           PASO 7: GENERACION DE REPORTE CONSOLIDADO              ║
║                                                                    ║
╚═══════════════════════════════════════════════════════════════════╝
    """)

    try:
        inicio = time.time()

        generator = ReportGenerator()
        tiempo_total = time.time() - inicio
        generator.generar_reporte(tiempo_total)

        print("\n[OK] Paso 7 completado exitosamente!")

    except Exception as e:
        print(f"\n[ERROR] Error en Paso 7: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
