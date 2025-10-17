"""
Upload to Database - Carga de Vistas a PostgreSQL (Neon)
Sube todas las vistas generadas a la base de datos Neon
Etapa 6 del pipeline
"""

import json
import time
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

# Agregar el directorio padre al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config


class DatabaseUploader:
    def __init__(self):
        self.output_base = Path(Config.OUTPUT_DIR)

        # Buscar la carpeta de fecha más reciente
        fecha_folders = sorted([f for f in self.output_base.iterdir() if f.is_dir()], reverse=True)

        if not fecha_folders:
            raise Exception("No se encontraron carpetas de salida para procesar")

        self.fecha_folder = fecha_folders[0]  # La más reciente

        # Input: vistas del paso 5
        self.input_data_dir = self.fecha_folder / "views" / "data"

        if not self.input_data_dir.exists():
            raise Exception(f"No se encontró la carpeta de vistas: {self.input_data_dir}")

        # Output: reporte de carga
        self.output_reporte_dir = self.fecha_folder / "uploaded_to_db" / "reporte"
        self.output_reporte_dir.mkdir(parents=True, exist_ok=True)

        # Verificar que DATABASE_URL esté configurada
        if not Config.DATABASE_URL:
            raise Exception("DATABASE_URL no está configurada en las variables de entorno")

        # Crear engine de SQLAlchemy
        self.engine = create_engine(
            Config.DATABASE_URL,
            poolclass=NullPool,  # No usar pool de conexiones para evitar problemas
            echo=False
        )

        self.resultados = {
            "exitosos": [],
            "fallidos": []
        }

    def limpiar_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el DataFrame antes de insertarlo en la base de datos
        """
        # Reemplazar NaN con None (NULL en SQL)
        df = df.where(pd.notna(df), None)

        # Convertir columnas numéricas que tengan valores None
        for col in df.columns:
            if df[col].dtype == 'object':
                # Intentar convertir a numérico si es posible
                try:
                    numeric_series = pd.to_numeric(df[col], errors='coerce')
                    # Si al menos 50% se convirtió, usar la versión numérica
                    if numeric_series.notna().sum() / len(df) > 0.5:
                        df[col] = numeric_series
                except:
                    pass

        return df

    def subir_vista(self, csv_path: Path) -> Dict:
        """
        Sube una vista CSV a la base de datos PostgreSQL
        """
        nombre_archivo = csv_path.name
        nombre_tabla = csv_path.stem  # Nombre sin extensión

        start_time = time.time()

        try:
            print(f"  Procesando: {nombre_tabla}")

            # Leer CSV
            df = pd.read_csv(csv_path)
            num_registros = len(df)

            if num_registros == 0:
                return {
                    "status": "warning",
                    "tabla": nombre_tabla,
                    "archivo": nombre_archivo,
                    "mensaje": "CSV vacío, se omite",
                    "registros": 0
                }

            # Limpiar DataFrame
            df = self.limpiar_dataframe(df)

            print(f"      Creando/verificando tabla...")

            # Verificar si la tabla existe y eliminar datos si es necesario
            with self.engine.connect() as conn:
                # Verificar si la tabla existe
                inspector = inspect(self.engine)
                if nombre_tabla in inspector.get_table_names():
                    # La tabla existe, verificar si tiene datos
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{nombre_tabla}"'))
                    count_actual = result.scalar()

                    if count_actual > 0:
                        print(f"      [INFO] La tabla ya tiene {count_actual} registros, se eliminan antes de insertar...")
                        conn.execute(text(f'DELETE FROM "{nombre_tabla}"'))
                        conn.commit()

                    # Usar replace para recrear la tabla
                    if_exists_mode = 'replace'
                else:
                    # La tabla no existe, crear nueva
                    if_exists_mode = 'replace'

            # Insertar datos usando pandas to_sql
            print(f"      Insertando {num_registros} registros...")
            df.to_sql(
                nombre_tabla,
                self.engine,
                if_exists=if_exists_mode,
                index=False,
                method='multi',
                chunksize=1000
            )

            elapsed = time.time() - start_time

            print(f"      [OK] {num_registros} registros insertados en {elapsed:.2f}s")

            return {
                "status": "success",
                "tabla": nombre_tabla,
                "archivo": nombre_archivo,
                "registros": num_registros,
                "columnas": list(df.columns),
                "duracion_segundos": round(elapsed, 2)
            }

        except Exception as e:
            elapsed = time.time() - start_time
            import traceback
            return {
                "status": "error",
                "tabla": nombre_tabla,
                "archivo": nombre_archivo,
                "error": str(e),
                "traceback": traceback.format_exc(),
                "duracion_segundos": round(elapsed, 2)
            }

    def subir_todas_las_vistas(self):
        """Sube todas las vistas CSV a la base de datos"""
        print("Iniciando carga de vistas a la base de datos...")
        print(f"Base de datos: {Config.DATABASE_URL.split('@')[1].split('/')[0]}")  # Mostrar solo el host
        print(f"Carpeta entrada: {self.input_data_dir}\n")

        # Verificar conexión
        try:
            print("Verificando conexión a la base de datos...")
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                print(f"✅ Conexión exitosa: {version[:50]}...\n")
        except Exception as e:
            print(f"❌ Error de conexión: {e}")
            raise

        start_time = time.time()

        # Obtener todos los archivos CSV
        csv_files = list(self.input_data_dir.glob("*.csv"))
        total_archivos = len(csv_files)

        print(f"Total de vistas a subir: {total_archivos}\n")
        print("=" * 80)

        for idx, csv_path in enumerate(csv_files, 1):
            resultado = self.subir_vista(csv_path)

            if resultado["status"] == "success":
                print(f"[{idx}/{total_archivos}] ✓ {resultado['tabla']}: {resultado['registros']} registros\n")
                self.resultados['exitosos'].append(resultado)
            elif resultado["status"] == "warning":
                print(f"[{idx}/{total_archivos}] ⚠ {resultado['tabla']}: {resultado['mensaje']}\n")
                self.resultados['exitosos'].append(resultado)
            else:
                print(f"[{idx}/{total_archivos}] ✗ {resultado['tabla']}: {resultado['error'][:80]}\n")
                self.resultados['fallidos'].append(resultado)

        elapsed = time.time() - start_time
        return elapsed

    def generar_reporte(self, tiempo_total_segundos: float):
        """Genera reporte de la carga a la base de datos"""
        exitosos = len(self.resultados['exitosos'])
        fallidos = len(self.resultados['fallidos'])
        total = exitosos + fallidos

        tasa_exito = (exitosos / total * 100) if total > 0 else 0

        # Calcular estadísticas
        total_registros = sum(r.get('registros', 0) for r in self.resultados['exitosos'])

        # REPORTE EN CONSOLA
        print("\n" + "=" * 80)
        print("REPORTE DE CARGA A BASE DE DATOS".center(80))
        print("=" * 80)
        print(f"\nRESUMEN:")
        print(f"   Total de vistas:                  {total}")
        print(f"   [OK] Exitosas:                    {exitosos}")
        print(f"   [ERROR] Fallidas:                 {fallidos}")
        print(f"   Tasa de éxito:                    {tasa_exito:.1f}%")
        print(f"\nDATOS:")
        print(f"   Total de registros insertados:    {total_registros:,}")
        print(f"\nTIEMPO:")
        print(f"   Tiempo total:                     {tiempo_total_segundos:.2f}s")
        print(f"   Tiempo promedio/vista:            {tiempo_total_segundos/total:.2f}s" if total > 0 else "   Tiempo promedio/vista:      N/A")

        if fallidos > 0:
            print(f"\n[ERROR] VISTAS FALLIDAS ({fallidos}):")
            for r in self.resultados['fallidos']:
                print(f"   {r['tabla']}")
                print(f"      Error: {r['error'][:80]}")

        print("\n" + "=" * 80 + "\n")

        # GUARDAR REPORTE JSON
        reporte = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "fecha": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "etapa": "upload_to_db",
                "base_de_datos": Config.DATABASE_URL.split('@')[1].split('?')[0],  # host/database sin credenciales
                "carpeta_origen": str(self.input_data_dir)
            },
            "resumen": {
                "total_vistas": total,
                "exitosas": exitosos,
                "fallidas": fallidos,
                "tasa_exito_porcentaje": round(tasa_exito, 2)
            },
            "datos": {
                "total_registros_insertados": total_registros
            },
            "tiempos": {
                "total_segundos": round(tiempo_total_segundos, 2),
                "promedio_por_vista_segundos": round(tiempo_total_segundos/total, 2) if total > 0 else 0
            },
            "vistas_exitosas": self.resultados['exitosos'],
            "vistas_fallidas": self.resultados['fallidos']
        }

        reporte_path = self.output_reporte_dir / "reporte_upload_db.json"
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)

        print(f"Reporte JSON guardado: {reporte_path}\n")

        return reporte


def main():
    print("""
=================================================
   CARGA A BASE DE DATOS - PIPELINE
     Etapa 6: Upload to Database
=================================================
    """)

    try:
        uploader = DatabaseUploader()
        tiempo_total = uploader.subir_todas_las_vistas()
        uploader.generar_reporte(tiempo_total)

        print("\n[OK] Carga a base de datos completada!")
        print(f"Reporte: {uploader.output_reporte_dir}")

        # Cerrar conexión
        uploader.engine.dispose()

    except Exception as e:
        print(f"\n[ERROR] Error fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
