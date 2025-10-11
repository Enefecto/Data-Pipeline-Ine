# INE Scraper - Descargador de Datasets del INE Chile

## Descripción

Scraper automatizado que descarga 87 datasets del Instituto Nacional de Estadísticas (INE) de Chile en formato CSV. Utiliza Playwright para navegar y descargar los archivos directamente desde el sitio web del INE, manejando iframes y modales de descarga. Los archivos se organizan automáticamente por fecha de ejecución y se generan reportes completos en JSON y TXT.

## Cómo Ejecutarlo

### Requisitos

- Docker
- Docker Compose

### Comando

```bash
docker-compose up --build
```

Los archivos descargados se guardarán en `outputs/DD-MM-YYYY/data/`
Los archivos de reportes se guardarán en `outputs/DD-MM-YYYY/reporte/`

## Estructura del Proyecto

```
ine-scrapper-data/
├── ine_scraper.py          # Script principal del scraper
├── ine_catalog.json        # Catálogo con 87 datasets del INE
├── Dockerfile              # Configuración del contenedor Docker
├── docker-compose.yml      # Orquestación de servicios
├── requirements.txt        # Dependencias Python
└── outputs/
    └── DD-MM-YYYY/         # Salidas organizadas por fecha
        ├── data/           # 87 archivos CSV descargados
        └── reporte/        # Reportes de descarga (JSON y TXT)
```
