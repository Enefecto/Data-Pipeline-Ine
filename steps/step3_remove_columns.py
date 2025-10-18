"""
Remove Columns - Eliminación de Columnas No Deseadas
Elimina las columnas 'Flag Codes' y 'Flags' de todos los archivos CSV
(Procesamiento in-place en carpeta raw)
"""

import json
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict

from config import Config


class ColumnRemover:
    def __init__(self):
        self.output_base = Path(Config.OUTPUT_DIR)

        # Columnas a eliminar (varias variaciones posibles)
        self.columns_to_remove = ['Flag Codes', 'Flags', 'flag_codes', 'flags', 'FLAG CODES', 'FLAGS']

        # Buscar la carpeta de fecha más reciente
        fecha_folders = sorted([f for f in self.output_base.iterdir() if f.is_dir()], reverse=True)

        if not fecha_folders:
            raise Exception("No se encontraron carpetas de salida para procesar")

        self.fecha_folder = fecha_folders[0]  # La más reciente
        self.raw_data_dir = self.fecha_folder / "raw"
        self.reporte_dir = self.fecha_folder / "reportes"

        # Crear directorio de reportes si no existe
        self.reporte_dir.mkdir(parents=True, exist_ok=True)

        self.resultados = {
            "exitosos": [],
            "fallidos": [],
            "sin_columnas": []
        }

    def eliminar_columnas_archivo(self, archivo_path: Path) -> Dict:
        """
        Elimina las columnas flag_codes y flags de un archivo CSV (in-place)

        Returns:
            Dict con información del procesamiento
        """
        filename = archivo_path.name

        try:
            size_original = archivo_path.stat().st_size

            # Leer CSV
            df = pd.read_csv(archivo_path)

            columnas_originales = df.columns.tolist()
            columnas_eliminadas = []

            # Buscar y eliminar columnas
            for col_to_remove in self.columns_to_remove:
                if col_to_remove in df.columns:
                    df = df.drop(columns=[col_to_remove])
                    columnas_eliminadas.append(col_to_remove)

            columnas_finales = df.columns.tolist()

            # Sobrescribir archivo in-place
            df.to_csv(archivo_path, index=False)
            size_final = archivo_path.stat().st_size

            return {
                "status": "success",
                "filename": filename,
                "columnas_originales": columnas_originales,
                "columnas_eliminadas": columnas_eliminadas,
                "columnas_finales": columnas_finales,
                "num_filas": len(df),
                "num_columnas_original": len(columnas_originales),
                "num_columnas_final": len(columnas_finales),
                "size_original": size_original,
                "size_final": size_final
            }

        except Exception as e:
            return {
                "status": "error",
                "filename": filename,
                "error": str(e)
            }

    def procesar_archivos(self):
        """Procesa todos los archivos CSV eliminando las columnas especificadas (in-place)"""
        print("🔄 Iniciando eliminación de columnas (in-place)...")
        print(f"📁 Carpeta raw: {self.raw_data_dir}")
        print(f"🗑️  Columnas a eliminar: {', '.join(set([c for c in self.columns_to_remove]))}\n")

        start_time = time.time()

        # Obtener todos los archivos CSV
        csv_files = list(self.raw_data_dir.glob("*.csv"))
        total_archivos = len(csv_files)

        print(f"📊 Total de archivos a procesar: {total_archivos}\n")
        print("=" * 80)

        for idx, archivo_path in enumerate(csv_files, 1):
            resultado = self.eliminar_columnas_archivo(archivo_path)

            if resultado["status"] == "success":
                if len(resultado["columnas_eliminadas"]) > 0:
                    print(f"[{idx}/{total_archivos}] ✓ {resultado['filename']}")
                    print(f"      └─ Eliminadas: {', '.join(resultado['columnas_eliminadas'])}")
                    print(f"      └─ Columnas: {resultado['num_columnas_original']} → {resultado['num_columnas_final']}")
                    self.resultados['exitosos'].append(resultado)
                else:
                    print(f"[{idx}/{total_archivos}] ℹ️  {resultado['filename']}")
                    print(f"      └─ No se encontraron columnas a eliminar")
                    self.resultados['sin_columnas'].append(resultado)
            else:
                print(f"[{idx}/{total_archivos}] ✗ {resultado['filename']}")
                print(f"      └─ Error: {resultado['error'][:60]}")
                self.resultados['fallidos'].append(resultado)

        elapsed = time.time() - start_time
        return elapsed

    def generar_reporte(self, tiempo_total_segundos: float):
        """Genera reporte de la eliminación de columnas"""
        exitosos = len(self.resultados['exitosos'])
        sin_columnas = len(self.resultados['sin_columnas'])
        fallidos = len(self.resultados['fallidos'])
        total = exitosos + sin_columnas + fallidos

        tasa_exito = ((exitosos + sin_columnas) / total * 100) if total > 0 else 0

        # Calcular estadísticas
        total_columnas_eliminadas = sum(
            len(r['columnas_eliminadas']) for r in self.resultados['exitosos']
        )

        size_original_total = sum(
            r['size_original'] for r in self.resultados['exitosos']
        )
        size_final_total = sum(
            r['size_final'] for r in self.resultados['exitosos']
        )
        reduccion_size = size_original_total - size_final_total
        reduccion_porcentaje = (reduccion_size / size_original_total * 100) if size_original_total > 0 else 0

        # REPORTE EN CONSOLA
        print("\n" + "=" * 80)
        print("REPORTE DE ELIMINACIÓN DE COLUMNAS".center(80))
        print("=" * 80)
        print(f"\n📊 RESUMEN:")
        print(f"   Total de archivos:            {total}")
        print(f"   ✓ Procesados con éxito:       {exitosos}")
        print(f"   ℹ️  Sin columnas a eliminar:   {sin_columnas}")
        print(f"   ✗ Fallidos:                   {fallidos}")
        print(f"   Tasa de éxito:                {tasa_exito:.1f}%")
        print(f"\n📉 ESTADÍSTICAS:")
        print(f"   Total columnas eliminadas:    {total_columnas_eliminadas}")
        print(f"   Reducción de tamaño:          {reduccion_size:,} bytes ({reduccion_porcentaje:.2f}%)")
        print(f"\n⏱️  TIEMPO:")
        print(f"   Tiempo total:                 {tiempo_total_segundos:.2f}s")
        print(f"   Tiempo promedio/archivo:      {tiempo_total_segundos/total:.2f}s" if total > 0 else "   Tiempo promedio/archivo:      N/A")

        if fallidos > 0:
            print(f"\n✗ ARCHIVOS FALLIDOS ({fallidos}):")
            for r in self.resultados['fallidos']:
                print(f"   {r['filename']}")
                print(f"      └─ Error: {r['error'][:60]}")

        print("\n" + "=" * 80 + "\n")

        # GUARDAR REPORTE JSON
        reporte = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "etapa": "remove_columns",
                "carpeta_raw": str(self.raw_data_dir),
                "columnas_objetivo": list(set(self.columns_to_remove))
            },
            "resumen": {
                "total_archivos": total,
                "procesados_exitosos": exitosos,
                "sin_columnas_a_eliminar": sin_columnas,
                "fallidos": fallidos,
                "tasa_exito_porcentaje": round(tasa_exito, 2),
                "total_columnas_eliminadas": total_columnas_eliminadas
            },
            "estadisticas_tamanio": {
                "size_original_bytes": size_original_total,
                "size_final_bytes": size_final_total,
                "reduccion_bytes": reduccion_size,
                "reduccion_porcentaje": round(reduccion_porcentaje, 2)
            },
            "tiempos": {
                "total_segundos": round(tiempo_total_segundos, 2),
                "promedio_por_archivo_segundos": round(tiempo_total_segundos/total, 2) if total > 0 else 0
            },
            "archivos_procesados": self.resultados['exitosos'],
            "archivos_sin_columnas": self.resultados['sin_columnas'],
            "archivos_fallidos": self.resultados['fallidos']
        }

        reporte_path = self.reporte_dir / "paso3_remove_columns.json"
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"📄 Reporte JSON guardado: {reporte_path}\n")

        return reporte


def main():
    print("""
╔══════════════════════════════════════════════╗
║   ELIMINACIÓN DE COLUMNAS - PIPELINE         ║
║        Paso 3: Remove Columns                ║
╚══════════════════════════════════════════════╝
    """)

    try:
        remover = ColumnRemover()
        tiempo_total = remover.procesar_archivos()
        remover.generar_reporte(tiempo_total)

        print("✅ Eliminación de columnas completada!")
        print(f"📁 Archivos procesados in-place en: {remover.raw_data_dir}")
        print(f"📄 Reporte: {remover.reporte_dir}")

    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
