"""
Generador de Reporte Consolidado
Genera el reporte pipeline_completo.json a partir de los reportes individuales existentes
"""

import json
from pathlib import Path
from datetime import datetime

def generar_reporte_consolidado():
    """Genera un reporte consolidado del pipeline a partir de reportes individuales"""

    output_base = Path("outputs")

    # Buscar la carpeta de fecha más reciente
    fecha_folders = sorted([f for f in output_base.iterdir() if f.is_dir()], reverse=True)

    if not fecha_folders:
        print("ERROR: No se encontro carpeta de salida")
        return

    fecha_folder = fecha_folders[0]
    reporte_dir = fecha_folder / "reportes"

    if not reporte_dir.exists():
        print(f"ERROR: No se encontro carpeta de reportes en: {fecha_folder}")
        return

    print(f"Procesando reportes de: {fecha_folder.name}")
    print(f"Carpeta de reportes: {reporte_dir}\n")

    # Leer reportes individuales de cada paso
    reportes_individuales = {}
    pasos_info = []
    tiempo_total = 0

    reporte_files = {
        1: ("paso1_scraper.json", "Scraping"),
        2: ("paso2_standardize.json", "Standardize Names"),
        3: ("paso3_remove_columns.json", "Remove Columns"),
        4: ("paso4_filter_stations.json", "Filter Stations"),
        5: ("paso5_create_views.json", "Create Views"),
        6: ("paso6_upload_to_db.json", "Upload to DB")
    }

    for paso_num, (filename, nombre_paso) in reporte_files.items():
        reporte_path = reporte_dir / filename

        if reporte_path.exists():
            print(f"[OK] Paso {paso_num}: {nombre_paso} - {filename}")
            with open(reporte_path, 'r', encoding='utf-8') as f:
                reporte_data = json.load(f)
                reportes_individuales[f"paso_{paso_num}"] = reporte_data

                # Extraer tiempo del reporte
                if "tiempos" in reporte_data and "total_segundos" in reporte_data["tiempos"]:
                    duracion = reporte_data["tiempos"]["total_segundos"]
                    tiempo_total += duracion

                    pasos_info.append({
                        "paso": paso_num,
                        "nombre": nombre_paso,
                        "duracion_segundos": duracion,
                        "exitoso": True
                    })
        else:
            print(f"[WARN] Paso {paso_num}: {nombre_paso} - Reporte no encontrado")

    if not reportes_individuales:
        print("\nERROR: No se encontraron reportes para consolidar")
        return

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
            "pasos_completados": len(pasos_info),
            "pasos_fallidos": 6 - len(pasos_info),
            "tiempo_total_segundos": round(tiempo_total, 2),
            "tiempo_total_minutos": round(tiempo_total / 60, 2),
            "tiempo_total_horas": round(tiempo_total / 3600, 2)
        },
        "pasos_ejecutados": pasos_info,
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
    print(f"   Pasos completados:        {len(pasos_info)}/6")
    print(f"   Pasos sin reporte:        {6 - len(pasos_info)}")
    print(f"   Tiempo total:             {tiempo_total/60:.1f} minutos ({tiempo_total:.1f}s)")

    print(f"\nDESGLOSE DE TIEMPOS:")
    for paso in pasos_info:
        print(f"   Paso {paso['paso']} ({paso['nombre']}): {paso['duracion_segundos']:.1f}s")

    print(f"\nESTRUCTURA FINAL:")
    print(f"   {fecha_folder}/")
    print(f"   |-- raw/              ({len(list((fecha_folder / 'raw').glob('*.csv')))} archivos CSV)")
    if (fecha_folder / "views").exists():
        print(f"   |-- views/            ({len(list((fecha_folder / 'views').glob('*.csv')))} vistas CSV)")
    print(f"   `-- reportes/         ({len(list(reporte_dir.glob('*.json')))} reportes JSON)")

    print(f"\nReporte consolidado guardado: {reporte_path}")
    print("="*80 + "\n")

    print("[OK] Reporte consolidado generado exitosamente!")


if __name__ == "__main__":
    generar_reporte_consolidado()
