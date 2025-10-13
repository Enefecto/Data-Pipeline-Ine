"""
Standardize Names - Estandarización de Nombres de Archivos
Transforma los archivos CSV raw con nombres originales a nombres estandarizados
"""

import json
import shutil
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from config import Config


class NameStandardizer:
    def __init__(self):
        self.mapping_path = "/app/dictionary/dataset_name_mapping.json"
        self.output_base = Path(Config.OUTPUT_DIR)

        # Buscar la carpeta de fecha más reciente
        fecha_folders = sorted([f for f in self.output_base.iterdir() if f.is_dir()], reverse=True)

        if not fecha_folders:
            raise Exception("No se encontraron carpetas de salida para procesar")

        self.fecha_folder = fecha_folders[0]  # La más reciente
        self.raw_data_dir = self.fecha_folder / "raw" / "data"
        self.raw_reporte_dir = self.fecha_folder / "raw" / "reporte"

        self.standardized_data_dir = self.fecha_folder / "standardized" / "data"
        self.standardized_reporte_dir = self.fecha_folder / "standardized" / "reporte"

        # Crear directorios de salida
        self.standardized_data_dir.mkdir(parents=True, exist_ok=True)
        self.standardized_reporte_dir.mkdir(parents=True, exist_ok=True)

        self.mapping = {}
        self.resultados = {
            "exitosos": [],
            "fallidos": [],
            "no_mapeados": []
        }

    def cargar_mapeo(self):
        """Carga el archivo de mapeo de nombres"""
        print("📖 Cargando mapeo de nombres...")

        with open(self.mapping_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.mapping = data['mappings']

        print(f"   ✅ {len(self.mapping)} mapeos cargados\n")

    def obtener_dataset_id_desde_archivo(self, filename: str) -> str:
        """
        Obtiene el dataset ID del archivo original basándose en el nombre
        Lee el reporte raw para obtener el mapping correcto
        """
        # Leer reporte raw para obtener el mapping de archivo a dataset_id
        reporte_path = self.raw_reporte_dir / "reporte_descarga.json"

        if not reporte_path.exists():
            return None

        with open(reporte_path, 'r', encoding='utf-8') as f:
            reporte = json.load(f)

        # Buscar en datasets exitosos
        for dataset in reporte['datasets_exitosos']:
            if dataset['nombre_archivo'] == filename:
                return dataset['id']

        return None

    def estandarizar_archivos(self):
        """Estandariza los nombres de todos los archivos CSV"""
        print("🔄 Iniciando estandarización de nombres...")
        print(f"📁 Carpeta raw: {self.raw_data_dir}")
        print(f"📁 Carpeta standardized: {self.standardized_data_dir}\n")

        start_time = time.time()

        # Obtener todos los archivos CSV
        csv_files = list(self.raw_data_dir.glob("*.csv"))
        total_archivos = len(csv_files)

        print(f"📊 Total de archivos a procesar: {total_archivos}\n")
        print("=" * 80)

        for idx, archivo_path in enumerate(csv_files, 1):
            filename = archivo_path.name

            try:
                # Obtener dataset_id del archivo
                dataset_id = self.obtener_dataset_id_desde_archivo(filename)

                if not dataset_id:
                    print(f"[{idx}/{total_archivos}] ⚠️  {filename}")
                    print(f"      └─ No se pudo obtener dataset_id")
                    self.resultados['fallidos'].append({
                        "archivo_original": filename,
                        "error": "No se encontró dataset_id en reporte"
                    })
                    continue

                # Obtener mapeo
                if dataset_id not in self.mapping:
                    print(f"[{idx}/{total_archivos}] ⚠️  {filename}")
                    print(f"      └─ Dataset ID {dataset_id} no tiene mapeo")
                    self.resultados['no_mapeados'].append({
                        "archivo_original": filename,
                        "dataset_id": dataset_id
                    })
                    continue

                mapeo = self.mapping[dataset_id]
                nombre_estandarizado = mapeo['nombre_estandarizado']
                nuevo_filename = f"{nombre_estandarizado}.csv"

                # Copiar archivo con nuevo nombre
                destino_path = self.standardized_data_dir / nuevo_filename
                shutil.copy2(archivo_path, destino_path)

                print(f"[{idx}/{total_archivos}] ✓ {mapeo['nombre_original'][:50]}...")
                print(f"      └─ {filename} → {nuevo_filename}")

                self.resultados['exitosos'].append({
                    "dataset_id": dataset_id,
                    "nombre_original": mapeo['nombre_original'],
                    "archivo_original": filename,
                    "nombre_estandarizado": nombre_estandarizado,
                    "archivo_nuevo": nuevo_filename,
                    "categoria": mapeo['categoria'],
                    "size": archivo_path.stat().st_size
                })

            except Exception as e:
                print(f"[{idx}/{total_archivos}] ✗ {filename}")
                print(f"      └─ Error: {str(e)[:60]}")
                self.resultados['fallidos'].append({
                    "archivo_original": filename,
                    "error": str(e)
                })

        elapsed = time.time() - start_time
        return elapsed

    def generar_reporte(self, tiempo_total_segundos: float):
        """Genera reporte de la estandarización"""
        exitosos = len(self.resultados['exitosos'])
        fallidos = len(self.resultados['fallidos'])
        no_mapeados = len(self.resultados['no_mapeados'])
        total = exitosos + fallidos + no_mapeados

        tasa_exito = (exitosos / total * 100) if total > 0 else 0

        # REPORTE EN CONSOLA
        print("\n" + "=" * 80)
        print("REPORTE DE ESTANDARIZACIÓN DE NOMBRES".center(80))
        print("=" * 80)
        print(f"\n📊 RESUMEN:")
        print(f"   Total de archivos:        {total}")
        print(f"   ✓ Estandarizados:         {exitosos}")
        print(f"   ⚠️  No mapeados:           {no_mapeados}")
        print(f"   ✗ Fallidos:               {fallidos}")
        print(f"   Tasa de éxito:            {tasa_exito:.1f}%")
        print(f"\n⏱️  TIEMPO:")
        print(f"   Tiempo total:             {tiempo_total_segundos:.2f}s")
        print(f"   Tiempo promedio/archivo:  {tiempo_total_segundos/total:.2f}s" if total > 0 else "   Tiempo promedio/archivo:  N/A")

        if no_mapeados > 0:
            print(f"\n⚠️  ARCHIVOS NO MAPEADOS ({no_mapeados}):")
            for r in self.resultados['no_mapeados']:
                print(f"   [{r['dataset_id']}] {r['archivo_original']}")

        if fallidos > 0:
            print(f"\n✗ ARCHIVOS FALLIDOS ({fallidos}):")
            for r in self.resultados['fallidos']:
                print(f"   {r['archivo_original']}")
                print(f"      └─ Error: {r['error'][:60]}")

        print("\n" + "=" * 80 + "\n")

        # GUARDAR REPORTE JSON
        reporte = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "etapa": "standardized",
                "carpeta_origen": str(self.raw_data_dir),
                "carpeta_destino": str(self.standardized_data_dir)
            },
            "resumen": {
                "total_archivos": total,
                "estandarizados": exitosos,
                "no_mapeados": no_mapeados,
                "fallidos": fallidos,
                "tasa_exito_porcentaje": round(tasa_exito, 2)
            },
            "tiempos": {
                "total_segundos": round(tiempo_total_segundos, 2),
                "promedio_por_archivo_segundos": round(tiempo_total_segundos/total, 2) if total > 0 else 0
            },
            "archivos_estandarizados": self.resultados['exitosos'],
            "archivos_no_mapeados": self.resultados['no_mapeados'],
            "archivos_fallidos": self.resultados['fallidos']
        }

        reporte_path = self.standardized_reporte_dir / "reporte_estandarizacion.json"
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"📄 Reporte JSON guardado: {reporte_path}\n")

        return reporte


def main():
    print("""
╔══════════════════════════════════════════════╗
║   ESTANDARIZACIÓN DE NOMBRES - PIPELINE      ║
║          Etapa 2: Standardized               ║
╚══════════════════════════════════════════════╝
    """)

    try:
        standardizer = NameStandardizer()
        standardizer.cargar_mapeo()
        tiempo_total = standardizer.estandarizar_archivos()
        standardizer.generar_reporte(tiempo_total)

        print("✅ Estandarización completada!")
        print(f"📁 Archivos estandarizados: {standardizer.standardized_data_dir}")
        print(f"📄 Reporte: {standardizer.standardized_reporte_dir}")

    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
