# INE Scraper - Descargador de Datasets del INE Chile

## Descripción

Scraper automatizado **concurrente** que descarga 87 datasets del Instituto Nacional de Estadísticas (INE) de Chile en formato CSV. Utiliza múltiples navegadores en paralelo con Playwright async para optimizar el tiempo de descarga.

**Optimizado para AWS Lambda** con configuración flexible de concurrencia según recursos disponibles. Los archivos se organizan automáticamente por fecha de ejecución y se generan reportes completos en JSON y TXT.

**Velocidad:** ~5-8 minutos para descargar los 87 datasets con 3 navegadores concurrentes (vs ~15-20 minutos con versión secuencial).

## Cómo Ejecutarlo

### Requisitos

- Docker
- Docker Compose

### Comando

```bash
docker-compose up --build
```

Los archivos descargados se guardarán en `outputs/DD-MM-YYYY/data/`

### Configuración

Las variables de configuración se encuentran en [config.py](config.py) y pueden sobreescribirse mediante variables de entorno en [docker-compose.yml](docker-compose.yml):

- `MAX_CONCURRENT_BROWSERS`: Número de navegadores concurrentes (default: 3)
- `DOWNLOAD_TIMEOUT`: Timeout por descarga en segundos (default: 60)
- `DELAY_BETWEEN_DOWNLOADS`: Pausa entre descargas por worker en segundos (default: 1.0)
- `MAX_DATASETS`: Límite de datasets a procesar para testing (default: None = todos)
- `HEADLESS`: Ejecutar navegadores en modo headless (default: true)
- `SAVE_LOCAL_FILES`: Guardar archivos localmente (default: true)
- `DATABASE_URL`: URL de conexión a base de datos Neon (para uso futuro)

#### Modo Testing

Para probar con solo 5 datasets, descomenta en [docker-compose.yml](docker-compose.yml):

```yaml
- MAX_DATASETS=5
```

## Estructura del Proyecto

```
ine-scrapper-data/
├── ine_scraper_concurrent.py   # Script principal concurrente
├── config.py                    # Configuración centralizada
├── ine_catalog.json             # Catálogo con 87 datasets del INE
├── Dockerfile                   # Configuración del contenedor Docker
├── docker-compose.yml           # Orquestación y variables de entorno
├── requirements.txt             # Dependencias Python
└── outputs/
    └── DD-MM-YYYY/              # Salidas organizadas por fecha
        ├── data/                # 87 archivos CSV descargados
        └── reporte/             # Reportes de descarga (JSON y TXT)
```

## Optimización para AWS Lambda

El scraper está diseñado para ejecutarse eficientemente en AWS Lambda:

- **Memoria recomendada:** 2048-3008 MB
- **Timeout recomendado:** 15 minutos (900 segundos)
- **Navegadores concurrentes recomendados:** 2-4 (ajustable según memoria)
- **Costo estimado por ejecución:** ~$0.01-0.02 USD
- **Ejecuciones esperadas:** 5 veces durante desarrollo/pruebas, luego 1 vez al mes en producción
