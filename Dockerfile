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
COPY config.py .
COPY ine_scraper_concurrent.py .

# Crear directorio de salida
RUN mkdir -p /app/outputs

CMD ["python", "ine_scraper_concurrent.py"]