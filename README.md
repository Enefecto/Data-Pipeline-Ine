# INE Scraper - Pipeline de Datos del INE Chile

## Descripción

Sistema de **pipeline de 3 pasos** para extracción y procesamiento de datos del Instituto Nacional de Estadísticas (INE) de Chile.

**Velocidad:** ~7 minutos para procesar 87 datasets completos.

## Pipeline de Datos

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   PASO 1    │    │    PASO 2    │    │   PASO 3    │
│   SCRAPER   │───▶│ STANDARDIZER │───▶│   COLUMN    │
│             │    │              │    │   REMOVER   │
└─────────────┘    └──────────────┘    └─────────────┘
   Descarga         Estandariza         Elimina
   datos raw        nombres             columnas flags
```

### Paso 1: Scraper
- **Script:** `steps/step1_scraper.py`
- **Salida:** `outputs/[FECHA]/raw/data/*.csv`
- **Función:** Descarga 87 datasets del INE (~6 min con 4 navegadores)

### Paso 2: Standardizer
- **Script:** `steps/step2_standardize_names.py`
- **Salida:** `outputs/[FECHA]/standardized/data/*.csv`
- **Función:** Renombra archivos con nombres estandarizados
- **Ejemplo:** `Altura de nieve...csv` → `altura_nieve_equivalente_en_agua.csv`

### Paso 3: Column Remover
- **Script:** `steps/step3_remove_columns.py`
- **Salida:** `outputs/[FECHA]/columns_removed/data/*.csv`
- **Función:** Elimina columnas `Flag Codes` y `Flags`

## Ejecución

### Pipeline Completo
```bash
docker-compose up --build
```

### Pasos Individuales
```bash
# Solo scraper
docker-compose up scraper --build

# Solo standardizer
docker-compose up standardizer

# Solo column remover
docker-compose up column_remover

# Desde paso 2 en adelante
docker-compose up standardizer column_remover
```

### Modo Testing
Para probar con solo 5 datasets, descomenta en `docker-compose.yml`:
```yaml
- MAX_DATASETS=5
```

## Estructura del Proyecto

```
ine-scrapper-data/
├── dictionary/                  # Archivos de datos y mapeos
│   ├── data_columns.txt         # Listado de columnas por dataset
│   ├── dataset_name_mapping.json    # Mapeo de nombres
│   └── ine_catalog.json         # Catálogo con 87 datasets
├── steps/                       # Scripts del pipeline
│   ├── step1_scraper.py         # Paso 1: Descarga
│   ├── step2_standardize_names.py   # Paso 2: Estandarización
│   └── step3_remove_columns.py  # Paso 3: Limpieza columnas
├── config.py                    # Configuración centralizada
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── outputs/
    └── DD-MM-YYYY/
        ├── raw/
        │   ├── data/
        │   └── reporte/
        ├── standardized/
        │   ├── data/
        │   └── reporte/
        └── columns_removed/
            ├── data/
            └── reporte/
```

## Configuración

Variables en `docker-compose.yml`:

### Scraping
- `MAX_CONCURRENT_BROWSERS=4` - Navegadores concurrentes (2-6)
- `DOWNLOAD_TIMEOUT=60` - Timeout por descarga en segundos
- `DELAY_BETWEEN_DOWNLOADS=1.0` - Pausa entre descargas
- `HEADLESS=true` - Modo headless del navegador
- `MAX_DATASETS=5` - Limitar datasets (testing)

## Archivos en dictionary/

### data_columns.txt
Listado completo de columnas por cada dataset. Útil para planear siguientes pasos del pipeline.

### dataset_name_mapping.json
Mapeo de nombres originales a estandarizados:
```json
{
  "E10000001": {
    "nombre_original": "Temperatura máxima absoluta",
    "nombre_estandarizado": "temp_max_absoluta",
    "categoria": "Aire"
  }
}
```

### ine_catalog.json
Catálogo completo con 87 datasets del INE y sus URLs.

## Performance

| Paso | Tiempo | Archivos |
|------|--------|----------|
| Scraper | ~6 min | 87 datasets |
| Standardizer | <10 seg | 87 archivos |
| Column Remover | <30 seg | 87 archivos |
| **Total** | **~7 min** | **87 datasets** |

## Optimización AWS Lambda

- **Memoria recomendada:** 2048-3008 MB
- **Timeout recomendado:** 15 minutos
- **Navegadores concurrentes:** 2-4
- **Costo estimado:** ~$0.01-0.02 USD por ejecución
