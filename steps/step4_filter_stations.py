"""
Filter Stations - Filtrado de Estaciones con Datos Insuficientes
Elimina estaciones con 2 o menos registros de cada archivo CSV
Etapa 4 del pipeline
"""

import json
import time
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# Agregar el directorio padre al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config


class StationFilter:
    def __init__(self):
        self.output_base = Path(Config.OUTPUT_DIR)

        # Buscar la carpeta de fecha más reciente
        fecha_folders = sorted([f for f in self.output_base.iterdir() if f.is_dir()], reverse=True)

        if not fecha_folders:
            raise Exception("No se encontraron carpetas de salida para procesar")

        self.fecha_folder = fecha_folders[0]  # La más reciente

        # Buscar la carpeta más reciente con datos procesados
        if (self.fecha_folder / "columns_removed" / "data").exists():
            self.input_data_dir = self.fecha_folder / "columns_removed" / "data"
        elif (self.fecha_folder / "standardized" / "data").exists():
            self.input_data_dir = self.fecha_folder / "standardized" / "data"
        else:
            self.input_data_dir = self.fecha_folder / "raw" / "data"

        self.output_data_dir = self.fecha_folder / "filtered_stations" / "data"
        self.output_reporte_dir = self.fecha_folder / "filtered_stations" / "reporte"

        # Crear directorios de salida
        self.output_data_dir.mkdir(parents=True, exist_ok=True)
        self.output_reporte_dir.mkdir(parents=True, exist_ok=True)

        # Cargar mapeo de columnas de estaciones
        mapping_path = Path(__file__).parent.parent / "dictionary" / "station_columns_mapping.json"
        with open(mapping_path, 'r', encoding='utf-8') as f:
            self.station_mapping = json.load(f)

        self.resultados = {
            "exitosos": [],
            "fallidos": [],
            "sin_filtrado": []
        }

        # Umbral mínimo de registros (inclusive)
        self.MIN_REGISTROS = 3

    def detectar_columna_estacion(self, df: pd.DataFrame, filename: str) -> str:
        """
        Detecta la columna de estación para un archivo específico

        Args:
            df: DataFrame del archivo
            filename: Nombre del archivo (sin extensión)

        Returns:
            Nombre de la columna de estación
        """
        # Buscar en el mapeo
        archivo_sin_ext = filename.replace('.csv', '')

        if archivo_sin_ext in self.station_mapping['mappings']:
            station_col = self.station_mapping['mappings'][archivo_sin_ext]['station_column']
            if station_col in df.columns:
                return station_col

        # Fallback: buscar en todas las columnas conocidas
        for col_type in self.station_mapping['station_column_types'].keys():
            if col_type in df.columns:
                return col_type

        raise ValueError(f"No se pudo detectar columna de estación para {filename}")

    def filtrar_estaciones_archivo(self, archivo_path: Path) -> Dict:
        """
        Filtra estaciones con datos insuficientes de un archivo CSV

        Args:
            archivo_path: Ruta al archivo CSV

        Returns:
            Dict con información del procesamiento
        """
        filename = archivo_path.name

        try:
            # Leer CSV
            df = pd.read_csv(archivo_path)

            if len(df) == 0:
                return {
                    "status": "warning",
                    "filename": filename,
                    "mensaje": "Archivo vacío, se copia sin cambios",
                    "registros_originales": 0,
                    "registros_finales": 0
                }

            # Detectar columna de estación
            try:
                station_col = self.detectar_columna_estacion(df, filename)
            except ValueError as e:
                return {
                    "status": "error",
                    "filename": filename,
                    "error": str(e)
                }

            # Contar registros por estación
            station_counts = df.groupby(station_col)['Value'].count()

            # Identificar estaciones a eliminar (con 2 o menos registros)
            estaciones_a_eliminar = station_counts[station_counts < self.MIN_REGISTROS].index.tolist()

            registros_originales = len(df)
            estaciones_originales = df[station_col].nunique()

            if len(estaciones_a_eliminar) == 0:
                # No hay nada que filtrar
                df_filtrado = df
                resultado = {
                    "status": "sin_cambios",
                    "filename": filename,
                    "station_column": station_col,
                    "registros_originales": registros_originales,
                    "registros_finales": len(df_filtrado),
                    "estaciones_originales": estaciones_originales,
                    "estaciones_finales": estaciones_originales,
                    "estaciones_eliminadas": [],
                    "num_estaciones_eliminadas": 0,
                    "registros_eliminados": 0
                }
            else:
                # Filtrar DataFrame
                df_filtrado = df[~df[station_col].isin(estaciones_a_eliminar)]

                registros_finales = len(df_filtrado)
                estaciones_finales = df_filtrado[station_col].nunique()
                registros_eliminados = registros_originales - registros_finales

                # Crear detalle de estaciones eliminadas con sus conteos
                estaciones_eliminadas_detalle = [
                    {
                        "estacion_id": est,
                        "num_registros": int(station_counts[est])
                    }
                    for est in estaciones_a_eliminar
                ]

                resultado = {
                    "status": "success",
                    "filename": filename,
                    "station_column": station_col,
                    "registros_originales": registros_originales,
                    "registros_finales": registros_finales,
                    "estaciones_originales": estaciones_originales,
                    "estaciones_finales": estaciones_finales,
                    "estaciones_eliminadas": estaciones_eliminadas_detalle,
                    "num_estaciones_eliminadas": len(estaciones_a_eliminar),
                    "registros_eliminados": registros_eliminados,
                    "porcentaje_registros_eliminados": round((registros_eliminados / registros_originales) * 100, 2)
                }

            # Guardar archivo procesado
            output_path = self.output_data_dir / filename
            df_filtrado.to_csv(output_path, index=False)

            resultado["size_original"] = archivo_path.stat().st_size
            resultado["size_final"] = output_path.stat().st_size

            return resultado

        except Exception as e:
            import traceback
            return {
                "status": "error",
                "filename": filename,
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def procesar_archivos(self):
        """Procesa todos los archivos CSV filtrando estaciones con datos insuficientes"""
        print("Iniciando filtrado de estaciones...")
        print(f"Carpeta entrada: {self.input_data_dir}")
        print(f"Carpeta salida: {self.output_data_dir}")
        print(f"Umbral minimo: {self.MIN_REGISTROS} registros por estacion\n")

        start_time = time.time()

        # Obtener todos los archivos CSV
        csv_files = list(self.input_data_dir.glob("*.csv"))
        total_archivos = len(csv_files)

        print(f"Total de archivos a procesar: {total_archivos}\n")
        print("=" * 80)

        for idx, archivo_path in enumerate(csv_files, 1):
            resultado = self.filtrar_estaciones_archivo(archivo_path)

            if resultado["status"] == "success":
                print(f"[{idx}/{total_archivos}] [OK] {resultado['filename']}")
                print(f"      Columna estacion: {resultado['station_column']}")
                print(f"      Estaciones: {resultado['estaciones_originales']} -> {resultado['estaciones_finales']} " +
                      f"(-{resultado['num_estaciones_eliminadas']})")
                print(f"      Registros: {resultado['registros_originales']} -> {resultado['registros_finales']} " +
                      f"(-{resultado['registros_eliminados']}, {resultado['porcentaje_registros_eliminados']}%)")
                self.resultados['exitosos'].append(resultado)
            elif resultado["status"] == "sin_cambios":
                print(f"[{idx}/{total_archivos}] [INFO] {resultado['filename']}")
                print(f"      Sin estaciones a eliminar (todas tienen >={self.MIN_REGISTROS} registros)")
                self.resultados['sin_filtrado'].append(resultado)
            elif resultado["status"] == "warning":
                print(f"[{idx}/{total_archivos}] [WARN] {resultado['filename']}")
                print(f"      {resultado['mensaje']}")
                self.resultados['sin_filtrado'].append(resultado)
            else:
                print(f"[{idx}/{total_archivos}] [ERROR] {resultado['filename']}")
                print(f"      Error: {resultado['error'][:80]}")
                self.resultados['fallidos'].append(resultado)

        elapsed = time.time() - start_time
        return elapsed

    def generar_reporte(self, tiempo_total_segundos: float):
        """Genera reporte del filtrado de estaciones"""
        exitosos = len(self.resultados['exitosos'])
        sin_filtrado = len(self.resultados['sin_filtrado'])
        fallidos = len(self.resultados['fallidos'])
        total = exitosos + sin_filtrado + fallidos

        tasa_exito = ((exitosos + sin_filtrado) / total * 100) if total > 0 else 0

        # Calcular estadísticas
        total_estaciones_eliminadas = sum(
            r['num_estaciones_eliminadas'] for r in self.resultados['exitosos']
        )

        total_registros_eliminados = sum(
            r['registros_eliminados'] for r in self.resultados['exitosos']
        )

        total_registros_originales = sum(
            r['registros_originales'] for r in self.resultados['exitosos']
        )

        porcentaje_registros_eliminados = (
            (total_registros_eliminados / total_registros_originales * 100)
            if total_registros_originales > 0 else 0
        )

        # REPORTE EN CONSOLA
        print("\n" + "=" * 80)
        print("REPORTE DE FILTRADO DE ESTACIONES".center(80))
        print("=" * 80)
        print(f"\nRESUMEN:")
        print(f"   Total de archivos:                    {total}")
        print(f"   [OK] Procesados con filtrado:         {exitosos}")
        print(f"   [INFO] Sin estaciones a filtrar:      {sin_filtrado}")
        print(f"   [ERROR] Fallidos:                     {fallidos}")
        print(f"   Tasa de exito:                        {tasa_exito:.1f}%")
        print(f"\nESTADISTICAS DE FILTRADO:")
        print(f"   Umbral mínimo:                        {self.MIN_REGISTROS} registros por estación")
        print(f"   Total estaciones eliminadas:          {total_estaciones_eliminadas}")
        print(f"   Total registros eliminados:           {total_registros_eliminados:,}")
        print(f"   Porcentaje de datos eliminados:       {porcentaje_registros_eliminados:.2f}%")
        print(f"\nTIEMPO:")
        print(f"   Tiempo total:                         {tiempo_total_segundos:.2f}s")
        print(f"   Tiempo promedio/archivo:              {tiempo_total_segundos/total:.2f}s" if total > 0 else "   Tiempo promedio/archivo:      N/A")

        if exitosos > 0:
            print(f"\n[OK] TOP 5 ARCHIVOS CON MAS ESTACIONES ELIMINADAS:")
            top_eliminadas = sorted(
                self.resultados['exitosos'],
                key=lambda x: x['num_estaciones_eliminadas'],
                reverse=True
            )[:5]
            for r in top_eliminadas:
                print(f"   {r['filename']}: {r['num_estaciones_eliminadas']} estaciones " +
                      f"({r['registros_eliminados']} registros)")

        if fallidos > 0:
            print(f"\n[ERROR] ARCHIVOS FALLIDOS ({fallidos}):")
            for r in self.resultados['fallidos']:
                print(f"   {r['filename']}")
                print(f"      Error: {r['error'][:80]}")

        print("\n" + "=" * 80 + "\n")

        # GUARDAR REPORTE JSON
        reporte = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "etapa": "filtered_stations",
                "carpeta_origen": str(self.input_data_dir),
                "carpeta_destino": str(self.output_data_dir),
                "umbral_minimo_registros": self.MIN_REGISTROS
            },
            "resumen": {
                "total_archivos": total,
                "procesados_con_filtrado": exitosos,
                "sin_filtrado": sin_filtrado,
                "fallidos": fallidos,
                "tasa_exito_porcentaje": round(tasa_exito, 2)
            },
            "estadisticas_filtrado": {
                "total_estaciones_eliminadas": total_estaciones_eliminadas,
                "total_registros_eliminados": total_registros_eliminados,
                "total_registros_originales": total_registros_originales,
                "porcentaje_registros_eliminados": round(porcentaje_registros_eliminados, 2)
            },
            "tiempos": {
                "total_segundos": round(tiempo_total_segundos, 2),
                "promedio_por_archivo_segundos": round(tiempo_total_segundos/total, 2) if total > 0 else 0
            },
            "archivos_con_filtrado": self.resultados['exitosos'],
            "archivos_sin_filtrado": self.resultados['sin_filtrado'],
            "archivos_fallidos": self.resultados['fallidos']
        }

        reporte_path = self.output_reporte_dir / "reporte_filtrado_estaciones.json"
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"Reporte JSON guardado: {reporte_path}\n")

        return reporte


def main():
    print("""
=================================================
   FILTRADO DE ESTACIONES - PIPELINE
     Etapa 4: Filter Stations
=================================================
    """)

    try:
        filterer = StationFilter()
        tiempo_total = filterer.procesar_archivos()
        filterer.generar_reporte(tiempo_total)

        print("\n[OK] Filtrado de estaciones completado!")
        print(f"Archivos procesados: {filterer.output_data_dir}")
        print(f"Reporte: {filterer.output_reporte_dir}")

    except Exception as e:
        print(f"\n[ERROR] Error fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
