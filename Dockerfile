FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

# Configurar PYTHONPATH para que Python encuentre los módulos en /app
ENV PYTHONPATH=/app

# Instalar dependencias
RUN apt-get update && apt-get install -y \
    libxkbcommon0 \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar archivos del proyecto
COPY dictionary/ ./dictionary/
COPY config.py .
COPY steps/ ./steps/

# Crear directorio de salida
RUN mkdir -p /app/outputs

# Comando por defecto: Paso 1 del pipeline
CMD ["python", "steps/step1_scraper.py"]