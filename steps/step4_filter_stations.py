"""
Filter Stations - Filtrado de Estaciones con Datos Insuficientes
Elimina estaciones con 2 o menos registros de cada archivo CSV
(Procesamiento in-place en carpeta raw)
"""

import json
import time
import sys
import io
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# Agregar el directorio padre al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config
from utils.storage_factory import StorageFactory


class StationFilter:
    def __init__(self):
        self.storage = StorageFactory.get_storage()
        self.fecha_hoy = datetime.now().strftime("%d-%m-%Y")

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

    def filtrar_estaciones_archivo(self, filename: str, subfolder: str) -> Dict:
        """
        Filtra estaciones con datos insuficientes de un archivo CSV

        Args:
            filename: Nombre del archivo CSV
            subfolder: Subfolder donde está el archivo

        Returns:
            Dict con información del procesamiento
        """
        try:
            # Cargar archivo desde storage
            file_data = self.storage.load_file(filename, subfolder)
            size_original = len(file_data)

            # Leer CSV desde bytes
            df = pd.read_csv(io.BytesIO(file_data))

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

            # VALIDACIÓN: Eliminar registros con estación vacía o null
            registros_antes_null_check = len(df)
            if station_col in df.columns:
                df = df[df[station_col].notna() & (df[station_col] != '')]
            registros_null_eliminados = registros_antes_null_check - len(df)

            if registros_null_eliminados > 0:
                print(f"      [INFO] Eliminados {registros_null_eliminados} registros con estación NULL/vacía")

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
                    "registros_eliminados": 0,
                    "registros_null_eliminados": registros_null_eliminados
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
                    "porcentaje_registros_eliminados": round((registros_eliminados / registros_originales) * 100, 2),
                    "registros_null_eliminados": registros_null_eliminados
                }

            # Guardar archivo procesado in-place usando storage
            csv_buffer = io.StringIO()
            df_filtrado.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            size_final = len(csv_bytes)

            self.storage.save_file(csv_bytes, filename, subfolder)

            resultado["size_original"] = size_original
            resultado["size_final"] = size_final

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
        """Procesa todos los archivos CSV filtrando estaciones con datos insuficientes (in-place)"""
        print("Iniciando filtrado de estaciones (in-place)...")
        print(f"Umbral minimo: {self.MIN_REGISTROS} registros por estacion\n")

        start_time = time.time()

        # Obtener todos los archivos CSV usando storage
        subfolder = f"{self.fecha_hoy}/raw"
        csv_files = self.storage.list_files(subfolder, "*.csv")
        total_archivos = len(csv_files)

        print(f"Total de archivos a procesar: {total_archivos}\n")
        print("=" * 80)

        for idx, filepath in enumerate(csv_files, 1):
            filename = Path(filepath).name
            resultado = self.filtrar_estaciones_archivo(filename, subfolder)

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

        total_registros_null = sum(
            r.get('registros_null_eliminados', 0)
            for r in (self.resultados['exitosos'] + self.resultados['sin_filtrado'])
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
        print(f"   Registros con estación NULL:          {total_registros_null:,}")
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
                "etapa": "filter_stations",
                "carpeta_raw": f"{self.fecha_hoy}/raw",
                "storage_mode": Config.STORAGE_MODE,
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
                "total_registros_null_eliminados": total_registros_null,
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

        # Guardar reporte usando StorageFactory
        self.storage.save_json(reporte, "paso4_filter_stations.json", f"{self.fecha_hoy}/reportes")
        print(f"[OK] Reporte JSON guardado: {self.fecha_hoy}/reportes/paso4_filter_stations.json\n")

        return reporte


def main():
    print("""
=================================================
   FILTRADO DE ESTACIONES - PIPELINE
     Paso 4: Filter Stations
=================================================
    """)

    try:
        filterer = StationFilter()
        tiempo_total = filterer.procesar_archivos()
        filterer.generar_reporte(tiempo_total)

        print("\n[OK] Filtrado de estaciones completado!")
        print(f"Archivos procesados in-place en: {filterer.fecha_hoy}/raw")
        print(f"Reporte: {filterer.fecha_hoy}/reportes/paso4_filter_stations.json")

    except Exception as e:
        print(f"\n[ERROR] Error fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
