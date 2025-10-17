"""
Create Views - Generación de Vistas Consolidadas en CSV
Consolida las 87 tablas en vistas agregadas para reducir el número de tablas
Etapa 5 del pipeline
"""

import json
import time
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Set

# Agregar el directorio padre al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config


class ViewCreator:
    def __init__(self):
        self.output_base = Path(Config.OUTPUT_DIR)

        # Buscar la carpeta de fecha más reciente
        fecha_folders = sorted([f for f in self.output_base.iterdir() if f.is_dir()], reverse=True)

        if not fecha_folders:
            raise Exception("No se encontraron carpetas de salida para procesar")

        self.fecha_folder = fecha_folders[0]  # La más reciente

        # Input: datos filtrados del paso 4
        if (self.fecha_folder / "filtered_stations" / "data").exists():
            self.input_data_dir = self.fecha_folder / "filtered_stations" / "data"
        elif (self.fecha_folder / "columns_removed" / "data").exists():
            self.input_data_dir = self.fecha_folder / "columns_removed" / "data"
        else:
            self.input_data_dir = self.fecha_folder / "standardized" / "data"

        # Output: vistas consolidadas
        self.output_data_dir = self.fecha_folder / "views" / "data"
        self.output_reporte_dir = self.fecha_folder / "views" / "reporte"

        # Crear directorios de salida
        self.output_data_dir.mkdir(parents=True, exist_ok=True)
        self.output_reporte_dir.mkdir(parents=True, exist_ok=True)

        # Cargar mapeo de columnas de estaciones
        mapping_path = Path(__file__).parent.parent / "dictionary" / "station_columns_mapping.json"
        with open(mapping_path, 'r', encoding='utf-8') as f:
            self.station_mapping = json.load(f)

        self.resultados = {
            "vistas_aire": [],
            "vistas_agua": [],
            "catalogos": [],
            "fallidos": []
        }

        # Definición de vistas de AIRE
        self.air_views = {
            "v_temperatura": {
                "tables": ["temp_max_absoluta", "temp_min_absoluta", "temp_max_med", "temp_min_med", "temp_med"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_ESTACIONES_METEO",
                "station_name": "Estaciones meteorológicas DMC",
                "granularity": "mensual"
            },
            "v_humedad_radiacion_uv": {
                "tables": ["humedad_rel_med_mens", "rad_global_med", "uvb_prom"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_ESTACIONES_METEO",
                "station_name": "Estaciones meteorológicas DMC",
                "granularity": "mensual"
            },
            "v_mp25_anual": {
                "tables": ["mp25_max_hor_anual", "mp25_min_hor_anual", "mp25_perc50", "mp25_perc90", "mp25_perc95", "mp25_perc98"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_mp25_mensual": {
                "tables": ["mp25_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_mp10_anual": {
                "tables": ["mp10_max_hor_anual", "mp10_min_hor_anual", "mp10_perc50", "mp10_perc90", "mp10_perc95", "mp10_perc98"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_mp10_mensual": {
                "tables": ["mp10_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_o3_anual": {
                "tables": ["o3_max_hor_anual", "o3_min_hor_anual", "o3_perc50", "o3_perc90", "o3_perc95", "o3_perc98", "o3_perc99"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_o3_mensual": {
                "tables": ["o3_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_so2_anual": {
                "tables": ["so2_max_hor_anual", "so2_min_anual", "so2_perc50", "so2_perc90", "so2_perc95", "so2_perc98", "so2_perc99"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_so2_mensual": {
                "tables": ["so2_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_no2_anual": {
                "tables": ["no2_max_hor_anual", "no2_min_hor_anual", "no2_perc50", "no2_perc90", "no2_perc95", "no2_perc98", "no2_perc99"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_no2_mensual": {
                "tables": ["no2_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_co_anual": {
                "tables": ["co_max_hor_anual", "co_min_hor_anual", "co_perc50", "co_perc90", "co_perc95", "co_perc98", "co_perc99"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_co_mensual": {
                "tables": ["co_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_no_anual": {
                "tables": ["no_max_hor_anual", "no_min_hor_anual", "no_perc50", "no_perc90", "no_perc95", "no_perc98", "no_perc99"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_no_mensual": {
                "tables": ["no_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_nox_anual": {
                "tables": ["nox_max_hor_anual", "nox_min_hor_anual", "nox_perc50", "nox_perc90", "nox_perc95", "nox_perc98", "nox_perc99"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "anual"
            },
            "v_nox_mensual": {
                "tables": ["nox_med_mens"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_EST_MONITOREO_AIRE",
                "station_name": "Estaciones de monitoreo del aire",
                "granularity": "mensual"
            },
            "v_num_eventos_de_olas_de_calor": {
                "tables": ["num_eventos_de_olas_de_calor"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "DTI_CL_ESTACIONES_METEO",
                "station_name": "Estaciones meteorológicas DMC",
                "granularity": "mensual"
            }
        }

        # Definición de vistas consolidadas de AGUA
        self.water_consolidated_views = {
            "v_mar_mensual": {
                "tables": ["temp_superficial_del_mar", "nivel_medio_del_mar"],
                "period_col": "DTI_CL_MES",
                "period_name": "mes",
                "station_col": "CL_T017ESTACION_SHOA",
                "station_name": "Estación ambiental SHOA",
                "granularity": "mensual"
            },
            "v_glaciares_anual_cuenca": {
                "tables": ["num_glaciares_por_cuenca", "superficie_de_glaciares_por_cuenca",
                          "volumen_de_hielo_glaciar_estimado_por_cuenca", "volumen_de_agua_de_glaciares_estimada_por_cuenca"],
                "period_col": "DTI_CL_ANO",
                "period_name": "anio",
                "station_col": "DTI_CL_CUENCAS",
                "station_name": "Cuencas",
                "granularity": "anual",
                "station_rename": "cuenca"  # Renombrar 'estacion' a 'cuenca'
            }
        }

        # Tablas de agua que se convierten 1:1 a vistas con prefijo v_
        self.water_simple_tables = [
            "coliformes_fecales_en_matriz_biologica",
            "coliformes_fecales_en_matriz_acuosa",
            "metales_totales_en_la_matriz_sedimentaria",
            "metales_disueltos_en_la_matriz_acuosa",
            "caudal_medio_de_aguas_corrientes",
            "cantidad_de_agua_caida",
            "evaporacion_real_por_estacion",
            "volumen_del_embalse_por_embalse",
            "altura_nieve_equivalente_en_agua",
            "nivel_estatico_de_aguas_subterraneas"
        ]

    def crear_vista_consolidada_aire(self, view_name: str, view_config: Dict, es_agua: bool = False) -> Dict:
        """
        Crea una vista consolidada de AIRE juntando múltiples tablas
        Similar al SQL con UNION + LEFT JOIN
        """
        try:
            print(f"  Procesando vista: {view_name}")

            tables = view_config["tables"]
            period_col = view_config["period_col"]
            period_name = view_config["period_name"]
            station_col = view_config["station_col"]

            # Mapeo de columnas legibles según el tipo de dato
            # Basado en los datos reales de los CSVs
            if "DTI_CL_MES" in view_config.get("period_col", ""):
                period_name_col = "Mes"
            elif "DTI_CL_ANO" in view_config.get("period_col", ""):
                period_name_col = "Año"  # Puede ser "Año" o "anio"
            elif "DTI_CL_DIA" in view_config.get("period_col", ""):
                period_name_col = "Día"
            else:
                period_name_col = None

            # Mapeo de columnas de estación
            station_name_col = view_config.get("station_name", "")

            # Cargar todas las tablas
            dataframes = {}
            for table in tables:
                csv_path = self.input_data_dir / f"{table}.csv"
                if not csv_path.exists():
                    print(f"    [WARN] Tabla no encontrada: {table}.csv")
                    continue

                df = pd.read_csv(csv_path)

                # Eliminar columnas flag_codes y flags si existen
                df = df.drop(columns=['Flag Codes', 'Flags', 'flag_codes', 'flags', 'FLAG CODES', 'FLAGS'], errors='ignore')

                # Verificar si la columna de nombre de período existe (puede ser "Año" o "anio")
                if period_name_col and period_name_col not in df.columns:
                    # Buscar variantes
                    if period_name_col == "Año" and "anio" in df.columns:
                        period_name_col = "anio"
                    elif period_name_col == "anio" and "Año" in df.columns:
                        period_name_col = "Año"

                dataframes[table] = df

            if not dataframes:
                return {
                    "status": "error",
                    "view_name": view_name,
                    "error": "No se encontraron tablas para consolidar"
                }

            # Paso 1: Detectar columna de período real (puede variar)
            # Buscar entre las columnas disponibles
            period_col_variants = [period_col, "DTI_CL_MES", "DTI_CL_ANO", "DTI_CL_DIA"]
            period_col_real = None

            for df in dataframes.values():
                for variant in period_col_variants:
                    if variant in df.columns:
                        period_col_real = variant
                        break
                if period_col_real:
                    break

            if not period_col_real:
                return {
                    "status": "error",
                    "view_name": view_name,
                    "error": f"No se encontró columna de período válida. Esperado: {period_col}"
                }

            # Obtener todos los pares únicos (período, estación)
            all_pairs = set()
            for df in dataframes.values():
                # Usar la columna de período que realmente exista en este df
                actual_period_col = None
                for variant in period_col_variants:
                    if variant in df.columns:
                        actual_period_col = variant
                        break

                if actual_period_col and station_col in df.columns:
                    pairs = df[[actual_period_col, station_col]].drop_duplicates()
                    all_pairs.update(zip(pairs[actual_period_col], pairs[station_col]))

            # Crear DataFrame base con todos los pares
            pares_df = pd.DataFrame(list(all_pairs), columns=[period_col_real, station_col])
            pares_df = pares_df.sort_values([period_col_real, station_col])

            # Paso 2: Agregar nombres legibles
            # Crear mapeo de períodos desde el primer dataframe
            first_df = list(dataframes.values())[0]
            if period_name_col and period_name_col in first_df.columns:
                period_mapping = first_df[[period_col_real, period_name_col]].drop_duplicates()
                pares_df = pares_df.merge(period_mapping, on=period_col_real, how='left')

            # Crear mapeo de estaciones combinando TODOS los dataframes (no solo el primero)
            # Esto asegura que todas las estaciones tengan su nombre legible
            if station_name_col:
                all_station_mappings = []
                for df in dataframes.values():
                    if station_name_col in df.columns and station_col in df.columns:
                        mapping = df[[station_col, station_name_col]].drop_duplicates()
                        all_station_mappings.append(mapping)

                if all_station_mappings:
                    # Combinar todos los mapeos y eliminar duplicados
                    combined_station_mapping = pd.concat(all_station_mappings).drop_duplicates()
                    pares_df = pares_df.merge(combined_station_mapping, on=station_col, how='left')

            # Paso 3: JOIN con cada tabla
            result_df = pares_df.copy()

            for table, df in dataframes.items():
                if 'Value' in df.columns:
                    # Detectar columna de período en esta tabla específica
                    actual_period_col = None
                    for variant in period_col_variants:
                        if variant in df.columns:
                            actual_period_col = variant
                            break

                    if actual_period_col:
                        # Seleccionar solo las columnas necesarias
                        df_values = df[[actual_period_col, station_col, 'Value']].copy()

                        # Si la columna de período difiere, renombrar temporalmente
                        if actual_period_col != period_col_real:
                            df_values = df_values.rename(columns={actual_period_col: period_col_real})

                        df_values = df_values.rename(columns={'Value': table})

                        # Merge con los pares
                        result_df = result_df.merge(df_values, on=[period_col_real, station_col], how='left')

            # Paso 4: Crear estructura final de la vista
            # Las columnas finales deben ser: [period_name (legible), estacion (legible), table1, table2, ...]

            # Renombrar columnas para la salida final
            rename_dict = {}

            # Para el período: usar el nombre legible si existe, sino el ID
            if period_name_col and period_name_col in result_df.columns:
                rename_dict[period_name_col] = period_name
            elif period_col_real in result_df.columns:
                rename_dict[period_col_real] = period_name

            # Para la estación: usar el nombre legible
            if station_name_col and station_name_col in result_df.columns:
                rename_dict[station_name_col] = 'estacion'
            elif station_col in result_df.columns:
                rename_dict[station_col] = 'estacion'

            result_df = result_df.rename(columns=rename_dict)

            # Construir lista de columnas finales
            final_columns = [period_name, 'estacion'] + tables

            # Verificar qué columnas realmente existen antes de seleccionar
            existing_cols = [col for col in final_columns if col in result_df.columns]

            if len(existing_cols) < len(final_columns):
                missing = set(final_columns) - set(existing_cols)
                print(f"    [WARN] Columnas faltantes: {missing}")

            # Seleccionar solo las columnas que existen
            result_df = result_df[existing_cols]

            # Si es una vista de agua, eliminar todas las columnas DTI restantes
            if es_agua:
                dti_columns = [col for col in result_df.columns if col.startswith('DTI_')]
                if dti_columns:
                    result_df = result_df.drop(columns=dti_columns)

            # Guardar CSV
            output_path = self.output_data_dir / f"{view_name}.csv"
            result_df.to_csv(output_path, index=False)

            return {
                "status": "success",
                "view_name": view_name,
                "tipo": "aire_consolidada",
                "num_tablas_consolidadas": len(dataframes),
                "tablas_consolidadas": list(dataframes.keys()),
                "num_registros": len(result_df),
                "num_columnas": len(result_df.columns),
                "columnas": list(result_df.columns),
                "size_bytes": output_path.stat().st_size
            }

        except Exception as e:
            import traceback
            return {
                "status": "error",
                "view_name": view_name,
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def crear_vista_simple_agua(self, table_name: str) -> Dict:
        """
        Crea una vista simple de AGUA (1:1) con prefijo v_
        Solo elimina flag_codes y flags
        """
        try:
            csv_path = self.input_data_dir / f"{table_name}.csv"

            if not csv_path.exists():
                return {
                    "status": "error",
                    "view_name": f"v_{table_name}",
                    "error": f"Archivo no encontrado: {table_name}.csv"
                }

            # Leer CSV
            df = pd.read_csv(csv_path)

            # Eliminar columnas flag_codes y flags
            df = df.drop(columns=['Flag Codes', 'Flags', 'flag_codes', 'flags', 'FLAG CODES', 'FLAGS'], errors='ignore')

            # Eliminar todas las columnas DTI (columnas que empiezan con "DTI_")
            dti_columns = [col for col in df.columns if col.startswith('DTI_')]
            if dti_columns:
                df = df.drop(columns=dti_columns)

            # Guardar con prefijo v_
            view_name = f"v_{table_name}"
            output_path = self.output_data_dir / f"{view_name}.csv"
            df.to_csv(output_path, index=False)

            return {
                "status": "success",
                "view_name": view_name,
                "tipo": "agua_simple",
                "tabla_origen": table_name,
                "num_registros": len(df),
                "num_columnas": len(df.columns),
                "columnas": list(df.columns),
                "size_bytes": output_path.stat().st_size
            }

        except Exception as e:
            import traceback
            return {
                "status": "error",
                "view_name": f"v_{table_name}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def crear_catalogo_estaciones(self) -> Dict:
        """
        Crea vista v_estaciones con datos completos de las 98 estaciones de aire
        Sin created_at
        """
        try:
            print("  Procesando catalogo: v_estaciones")

            # Cargar datos hardcodeados de estaciones
            estaciones_path = Path(__file__).parent.parent / "dictionary" / "estaciones_aire_data.json"
            with open(estaciones_path, 'r', encoding='utf-8') as f:
                estaciones_data = json.load(f)

            # Crear DataFrame
            estaciones_df = pd.DataFrame(estaciones_data)

            # Reordenar columnas: nombre, latitud, longitud, numero_region, nombre_region, descripcion
            estaciones_df = estaciones_df[['nombre', 'latitud', 'longitud', 'numero_region', 'nombre_region', 'descripcion']]

            # Guardar CSV
            view_name = "v_estaciones"
            output_path = self.output_data_dir / f"{view_name}.csv"
            estaciones_df.to_csv(output_path, index=False)

            return {
                "status": "success",
                "view_name": view_name,
                "tipo": "catalogo",
                "num_registros": len(estaciones_df),
                "num_columnas": len(estaciones_df.columns),
                "columnas": list(estaciones_df.columns),
                "size_bytes": output_path.stat().st_size
            }

        except Exception as e:
            import traceback
            return {
                "status": "error",
                "view_name": "v_estaciones",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def crear_catalogo_entidades_agua(self) -> Dict:
        """
        Crea vista v_entidades_agua extrayendo entidades únicas de las tablas de agua
        Sin created_at, updated_at
        """
        try:
            print("  Procesando catalogo: v_entidades_agua")

            entidades_list = []

            # Mapeo de tabla -> (columna_entidad, tipo, descripcion)
            entidades_mapping = {
                "coliformes_fecales_en_matriz_biologica": ("Estaciones POAL", "Estación Costera - Coliformes Biológicos", "Estación de monitoreo costero - Análisis de coliformes fecales en organismos marinos"),
                "coliformes_fecales_en_matriz_acuosa": ("Estaciones POAL", "Estación Costera - Coliformes Acuosos", "Estación de monitoreo costero - Análisis de coliformes fecales en agua de mar"),
                "metales_totales_en_la_matriz_sedimentaria": ("Estaciones POAL", "Estación Costera - Metales Sedimentos", "Estación de monitoreo costero - Análisis de metales pesados en sedimentos marinos"),
                "metales_disueltos_en_la_matriz_acuosa": ("Estaciones POAL", "Estación Costera - Metales Disueltos", "Estación de monitoreo costero - Análisis de metales disueltos en agua de mar"),
                "caudal_medio_de_aguas_corrientes": ("Estaciones Fluviométricas", "Estación Fluviométrica", "Estación de medición de ríos - Monitoreo de caudal y flujo de agua"),
                "cantidad_de_agua_caida": ("Estaciones meteorológicas DMC", "Estación Meteorológica", "Estación meteorológica - Medición de lluvias y precipitaciones"),
                "evaporacion_real_por_estacion": ("Estación", "Estación de Evaporación", "Estación de evaporación - Medición de pérdida de agua por evaporación"),
                "volumen_del_embalse_por_embalse": ("Embalse", "Embalse", "Embalse o represa - Monitoreo de almacenamiento de agua"),
                "altura_nieve_equivalente_en_agua": ("Estaciones nivométricas", "Estación Nivométrica", "Estación de medición de nieve - Monitoreo de acumulación de nieve en cordillera"),
                "nivel_estatico_de_aguas_subterraneas": ("Estaciones Pozo", "Pozo de Monitoreo", "Pozo de monitoreo - Medición de nivel de aguas subterráneas (napas)"),
                "temp_superficial_del_mar": ("Estación ambiental SHOA", "Estación Oceanográfica", "Estación oceanográfica - Medición de temperatura del mar"),
                "nivel_medio_del_mar": ("Estación ambiental SHOA", "Estación Oceanográfica", "Estación oceanográfica - Medición de nivel del mar"),
            }

            # Para glaciares (cuencas)
            csv_path = self.input_data_dir / "num_glaciares_por_cuenca.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                if 'Cuencas' in df.columns:
                    for cuenca in df['Cuencas'].dropna().unique():
                        entidades_list.append({
                            'nombre': cuenca,
                            'tipo': 'Cuenca Hidrográfica',
                            'descripcion': 'Cuenca hidrográfica - Monitoreo de glaciares y balance hídrico regional'
                        })

            # Extraer entidades de cada tabla
            for table, (col_name, tipo, descripcion) in entidades_mapping.items():
                csv_path = self.input_data_dir / f"{table}.csv"
                if csv_path.exists():
                    df = pd.read_csv(csv_path)
                    if col_name in df.columns:
                        for entidad in df[col_name].dropna().unique():
                            entidades_list.append({
                                'nombre': entidad,
                                'tipo': tipo,
                                'descripcion': descripcion
                            })

            # Crear DataFrame y eliminar duplicados
            entidades_df = pd.DataFrame(entidades_list)
            entidades_df = entidades_df.drop_duplicates(subset=['nombre', 'tipo'])
            entidades_df = entidades_df.sort_values(['tipo', 'nombre'])

            # Agregar id
            entidades_df.insert(0, 'id', range(1, len(entidades_df) + 1))

            # Guardar CSV
            view_name = "v_entidades_agua"
            output_path = self.output_data_dir / f"{view_name}.csv"
            entidades_df.to_csv(output_path, index=False)

            return {
                "status": "success",
                "view_name": view_name,
                "tipo": "catalogo",
                "num_registros": len(entidades_df),
                "num_columnas": len(entidades_df.columns),
                "columnas": list(entidades_df.columns),
                "size_bytes": output_path.stat().st_size
            }

        except Exception as e:
            import traceback
            return {
                "status": "error",
                "view_name": "v_entidades_agua",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def procesar_vistas(self):
        """Procesa todas las vistas"""
        print("Iniciando generacion de vistas consolidadas...")
        print(f"Carpeta entrada: {self.input_data_dir}")
        print(f"Carpeta salida: {self.output_data_dir}\n")

        start_time = time.time()

        # PASO 1: Vistas consolidadas de AIRE
        print("=" * 80)
        print("VISTAS CONSOLIDADAS DE AIRE")
        print("=" * 80)

        for view_name, view_config in self.air_views.items():
            resultado = self.crear_vista_consolidada_aire(view_name, view_config)

            if resultado["status"] == "success":
                print(f"    [OK] {view_name}: {resultado['num_registros']} registros, {resultado['num_tablas_consolidadas']} tablas")
                self.resultados['vistas_aire'].append(resultado)
            else:
                print(f"    [ERROR] {view_name}: {resultado['error'][:60]}")
                self.resultados['fallidos'].append(resultado)

        # PASO 2: Vistas consolidadas de AGUA
        print("\n" + "=" * 80)
        print("VISTAS CONSOLIDADAS DE AGUA")
        print("=" * 80)

        for view_name, view_config in self.water_consolidated_views.items():
            resultado = self.crear_vista_consolidada_aire(view_name, view_config, es_agua=True)  # Usa la misma lógica

            if resultado["status"] == "success":
                print(f"    [OK] {view_name}: {resultado['num_registros']} registros, {resultado['num_tablas_consolidadas']} tablas")
                self.resultados['vistas_agua'].append(resultado)
            else:
                print(f"    [ERROR] {view_name}: {resultado['error'][:60]}")
                self.resultados['fallidos'].append(resultado)

        # PASO 3: Vistas simples de AGUA (1:1)
        print("\n" + "=" * 80)
        print("VISTAS SIMPLES DE AGUA (1:1)")
        print("=" * 80)

        for table_name in self.water_simple_tables:
            resultado = self.crear_vista_simple_agua(table_name)

            if resultado["status"] == "success":
                print(f"    [OK] {resultado['view_name']}: {resultado['num_registros']} registros")
                self.resultados['vistas_agua'].append(resultado)
            else:
                print(f"    [ERROR] {resultado['view_name']}: {resultado['error'][:60]}")
                self.resultados['fallidos'].append(resultado)

        # PASO 4: Catálogos
        print("\n" + "=" * 80)
        print("CATALOGOS")
        print("=" * 80)

        # Estaciones
        resultado_estaciones = self.crear_catalogo_estaciones()
        if resultado_estaciones["status"] == "success":
            print(f"    [OK] {resultado_estaciones['view_name']}: {resultado_estaciones['num_registros']} estaciones")
            self.resultados['catalogos'].append(resultado_estaciones)
        else:
            print(f"    [ERROR] {resultado_estaciones['view_name']}: {resultado_estaciones['error'][:60]}")
            self.resultados['fallidos'].append(resultado_estaciones)

        # Entidades de agua
        resultado_entidades = self.crear_catalogo_entidades_agua()
        if resultado_entidades["status"] == "success":
            print(f"    [OK] {resultado_entidades['view_name']}: {resultado_entidades['num_registros']} entidades")
            self.resultados['catalogos'].append(resultado_entidades)
        else:
            print(f"    [ERROR] {resultado_entidades['view_name']}: {resultado_entidades['error'][:60]}")
            self.resultados['fallidos'].append(resultado_entidades)

        elapsed = time.time() - start_time
        return elapsed

    def generar_reporte(self, tiempo_total_segundos: float):
        """Genera reporte de la creación de vistas"""
        total_vistas_aire = len(self.resultados['vistas_aire'])
        total_vistas_agua = len(self.resultados['vistas_agua'])
        total_catalogos = len(self.resultados['catalogos'])
        total_fallidos = len(self.resultados['fallidos'])
        total_exitosos = total_vistas_aire + total_vistas_agua + total_catalogos
        total = total_exitosos + total_fallidos

        tasa_exito = (total_exitosos / total * 100) if total > 0 else 0

        # REPORTE EN CONSOLA
        print("\n" + "=" * 80)
        print("REPORTE DE GENERACION DE VISTAS".center(80))
        print("=" * 80)
        print(f"\nRESUMEN:")
        print(f"   Total de vistas generadas:          {total_exitosos}")
        print(f"   [OK] Vistas de aire:                {total_vistas_aire}")
        print(f"   [OK] Vistas de agua:                {total_vistas_agua}")
        print(f"   [OK] Catalogos:                     {total_catalogos}")
        print(f"   [ERROR] Fallidos:                   {total_fallidos}")
        print(f"   Tasa de exito:                      {tasa_exito:.1f}%")
        print(f"\nREDUCCION:")
        print(f"   Tablas originales:                  87")
        print(f"   Vistas generadas:                   {total_exitosos}")
        print(f"   Reduccion:                          {((87 - total_exitosos) / 87 * 100):.1f}%")
        print(f"\nTIEMPO:")
        print(f"   Tiempo total:                       {tiempo_total_segundos:.2f}s")
        print(f"   Tiempo promedio/vista:              {tiempo_total_segundos/total:.2f}s" if total > 0 else "   Tiempo promedio/vista:      N/A")

        if total_fallidos > 0:
            print(f"\n[ERROR] VISTAS FALLIDAS ({total_fallidos}):")
            for r in self.resultados['fallidos']:
                print(f"   {r['view_name']}")
                print(f"      Error: {r['error'][:80]}")

        print("\n" + "=" * 80 + "\n")

        # GUARDAR REPORTE JSON
        reporte = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "etapa": "views",
                "carpeta_origen": str(self.input_data_dir),
                "carpeta_destino": str(self.output_data_dir)
            },
            "resumen": {
                "total_vistas": total_exitosos,
                "vistas_aire": total_vistas_aire,
                "vistas_agua": total_vistas_agua,
                "catalogos": total_catalogos,
                "fallidos": total_fallidos,
                "tasa_exito_porcentaje": round(tasa_exito, 2)
            },
            "reduccion": {
                "tablas_originales": 87,
                "vistas_generadas": total_exitosos,
                "reduccion_porcentaje": round((87 - total_exitosos) / 87 * 100, 2)
            },
            "tiempos": {
                "total_segundos": round(tiempo_total_segundos, 2),
                "promedio_por_vista_segundos": round(tiempo_total_segundos/total, 2) if total > 0 else 0
            },
            "vistas_aire": self.resultados['vistas_aire'],
            "vistas_agua": self.resultados['vistas_agua'],
            "catalogos": self.resultados['catalogos'],
            "fallidos": self.resultados['fallidos']
        }

        reporte_path = self.output_reporte_dir / "reporte_vistas.json"
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"Reporte JSON guardado: {reporte_path}\n")

        return reporte


def main():
    print("""
=================================================
   GENERACION DE VISTAS - PIPELINE
     Etapa 5: Create Views
=================================================
    """)

    try:
        creator = ViewCreator()
        tiempo_total = creator.procesar_vistas()
        creator.generar_reporte(tiempo_total)

        print("\n[OK] Generacion de vistas completada!")
        print(f"Archivos generados: {creator.output_data_dir}")
        print(f"Reporte: {creator.output_reporte_dir}")

    except Exception as e:
        print(f"\n[ERROR] Error fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
