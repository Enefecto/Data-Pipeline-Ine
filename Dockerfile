FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

# Instalar dependencias
RUN apt-get update && apt-get install -y \
    libxkbcommon0 \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar archivos del proyecto
COPY ine_catalog.json .
COPY dataset_name_mapping.json .
COPY config.py .
COPY ine_scraper.py .
COPY clean_data.py .

# Crear directorio de salida
RUN mkdir -p /app/outputs

# Comando por defecto (puede ser sobreescrito en docker-compose)
CMD ["python", "ine_scraper.py"]