# INE Scraper - Pipeline de Datos del INE Chile

## Descripción

Sistema de **pipeline multi-etapa** para extracción y procesamiento de datos del Instituto Nacional de Estadísticas (INE) de Chile. Incluye scraping concurrente, estandarización de nombres y preparación para base de datos.

**Optimizado para AWS Lambda** con configuración flexible de concurrencia. Pipeline diseñado para ser extensible y modular.

**Velocidad:** ~4-5 minutos para descargar 87 datasets con 4 navegadores concurrentes.

## Pipeline de Datos

El sistema procesa los datos en **etapas**:

```
1. raw          →  Descarga datos originales del INE
2. standardized →  Estandariza nombres de archivos
3. cleaned      →  (Futuro) Limpieza y validación de datos
4. transformed  →  (Futuro) Transformaciones y tablas derivadas
```

### Estructura de Carpetas

```
outputs/
  DD-MM-YYYY/
    raw/
      data/       # 87 CSVs originales con nombres del INE
      reporte/    # reporte_descarga.json
    standardized/
      data/       # 87 CSVs con nombres estandarizados para DB
      reporte/    # reporte_estandarizacion.json
    cleaned/      # (Futuro)
      data/
      reporte/
    transformed/  # (Futuro)
      data/
      reporte/
```

## Cómo Ejecutarlo

### Requisitos

- Docker
- Docker Compose

### Ejecución del Pipeline Completo

```bash
# Ejecuta ambas etapas: scraping + estandarización
docker-compose up --build
```

### Ejecución por Etapas

```bash
# Solo scraping (etapa 1: raw)
docker-compose up scraper --build

# Solo estandarización (etapa 2: standardized)
docker-compose up standardizer
```

### Modo Testing

Para probar con solo 5 datasets, descomenta en [docker-compose.yml](docker-compose.yml):

```yaml
- MAX_DATASETS=5
```

## Mapeo de Nombres

El archivo [dataset_name_mapping.json](dataset_name_mapping.json) contiene el mapeo de nombres originales a nombres estandarizados:

```json
{
  "E10000001": {
    "nombre_original": "Temperatura máxima absoluta",
    "nombre_estandarizado": "temp_max_absoluta",
    "categoria": "Aire"
  },
  ...
}
```

**Total de mappings:** 87 datasets

## Estructura del Proyecto

```
ine-scrapper-data/
├── ine_scraper.py              # Scraper concurrente (etapa 1)
├── standardize_names.py        # Estandarización de nombres (etapa 2)
├── dataset_name_mapping.json   # Mapeo de nombres
├── config.py                   # Configuración centralizada del pipeline
├── ine_catalog.json            # Catálogo con 87 datasets del INE
├── Dockerfile                  # Configuración del contenedor Docker
├── docker-compose.yml          # Orquestación del pipeline
├── requirements.txt            # Dependencias Python
└── outputs/
    └── DD-MM-YYYY/             # Salidas organizadas por fecha
        ├── raw/                # Etapa 1: Datos originales
        └── standardized/       # Etapa 2: Datos estandarizados
```

## Configuración

Variables en [config.py](config.py) (sobreescribibles en [docker-compose.yml](docker-compose.yml)):

### Pipeline
- `PIPELINE_STAGE`: Etapa actual del pipeline (default: raw)
- `PIPELINE_STAGES`: Lista de etapas disponibles

### Scraping
- `MAX_CONCURRENT_BROWSERS`: Navegadores concurrentes (default: 4)
- `DOWNLOAD_TIMEOUT`: Timeout por descarga en segundos (default: 60)
- `DELAY_BETWEEN_DOWNLOADS`: Pausa entre descargas (default: 1.0s)
- `HEADLESS`: Modo headless del navegador (default: true)

### General
- `OUTPUT_DIR`: Directorio base de salida (default: /app/outputs)
- `SAVE_LOCAL_FILES`: Guardar archivos localmente (default: true)
- `MAX_DATASETS`: Límite para testing (default: None = todos)

### Base de Datos (Futuro)
- `DATABASE_URL`: URL de conexión a Neon PostgreSQL

## Optimización para AWS Lambda

El sistema está diseñado para AWS Lambda:

- **Memoria recomendada:** 2048-3008 MB
- **Timeout recomendado:** 15 minutos (900 segundos)
- **Navegadores concurrentes:** 2-4 (ajustable según memoria)
- **Costo estimado:** ~$0.01-0.02 USD por ejecución completa
- **Frecuencia:** 5 ejecuciones en desarrollo, 1/mes en producción

## Reportes Generados

### Etapa 1: Raw
- **reporte_descarga.json**: Estadísticas de descarga, tiempos, datasets exitosos/fallidos

### Etapa 2: Standardized
- **reporte_estandarizacion.json**: Mapeos aplicados, archivos procesados, errores

Ambos reportes incluyen:
- Metadata (timestamp, etapa, versión)
- Resumen de resultados
- Tiempos de ejecución
- Detalles por dataset
- Información de errores (si existen)

## Próximas Etapas

### Etapa 3: Cleaned
- Validación de esquemas
- Limpieza de datos faltantes
- Normalización de valores
- Detección de outliers

### Etapa 4: Transformed
- Creación de tablas `entidades_agua` y `estaciones`
- Joins y agregaciones
- Tablas dimensionales
- Carga a base de datos Neon
