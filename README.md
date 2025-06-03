# Análisis de Datos Meteorológicos con Mapreduce
## Universidad EAFIT - ST0263: Tópicos Especiales en Telemática, 2025-1

[![GitHub](https://img.shields.io/badge/GitHub-Repository-blue)](https://github.com/santig005/MapReduce---Data-enginering)
### Integrantes
- Santiago de Jesus Gomez Alzate
- Victor Jesus Villadiego
- Jacobo Zuluaga
### Descripción
Este proyecto implementa una solución de arquitectura batch basada en Hadoop para procesar datos meteorológicos históricos de Medellín. El sistema utiliza MapReduce para calcular estadísticas mensuales de temperatura máxima y precipitación, y presenta los resultados a través de una API web.

### Objetivo General
Implementar un flujo completo de procesamiento distribuido usando HDFS y MapReduce para analizar datos meteorológicos históricos y visualizar los resultados.

### Estructura del Proyecto
```
.
├── scripts/
│   ├── mrjob_script.sh          # Script principal de ejecución MapReduce
│   └── install_mrjob.sh         # Script de bootstrap para instalar MRJob
├── src/
│   └── weather_api_server.py    # API de visualización de resultados
├── input_output_example/
│   ├── open_meteo_processed_2023.jsonl    # Ejemplo de datos de entrada
│   └── output_example_open_meteo_2023.txt # Ejemplo de resultados
└── requirements.txt             # Dependencias del proyecto
```

### Requisitos
- Java JDK 8 o superior
- Python 3.8+
- Hadoop 3.x
- AWS EMR Cluster configurado
- Dependencias de Python:
  - mrjob
  - flask
  - boto3

### Configuración del Entorno

1. Configurar el cluster EMR:
   - Usar el script `install_mrjob.sh` como script de bootstrap
   - Configurar los pasos (steps) con `mrjob_script.sh`

### Flujo de Procesamiento

1. **Obtención de Datos**
   - Fuente: Open-Meteo API
   - Formato: JSONL
   - Datos: Temperatura máxima y precipitación diaria para Medellín (2023)

2. **Procesamiento MapReduce**
   - Script: `mrjob_script.sh`
   - Funcionalidad:
     - Agrupa datos por mes
     - Calcula temperatura máxima promedio
     - Suma precipitación total
   - Salida: Estadísticas mensuales en formato JSON

3. **Visualización**
   - API: `weather_api_server.py`
   - Endpoints:
     - `/api/weather`: Datos en formato JSON
     - `/view/weather`: Visualización en HTML

### Ejecución

1. **Configuración del Cluster EMR:**
```bash
# Crear cluster EMR con configuración básica
aws emr create-cluster \
    --name "Weather Analysis Cluster" \
    --release-label emr-6.x.0 \
    --applications Name=Hadoop Name=Hive \
    --ec2-attributes KeyName=my-key-pair \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --bootstrap-actions Path=s3://mapreduce-emr-project/install_mrjob.sh \
    --steps Type=CUSTOM_JAR,Name="Weather Analysis",Jar=s3://mapreduce-emr-project/mrjob_script.sh
```

2. **Procesamiento MapReduce en EMR:**
El procesamiento se realizó como un paso (step) en el cluster EMR, donde:
- Los datos de entrada se cargaron en S3: `s3://mapreduce-emr-project/input/`
- El script de MapReduce se ejecutó como un paso en el cluster
- Los resultados se almacenaron en: `s3://mapreduce-emr-project/output/`

3. **Iniciar la API de Visualización:**
```bash
# Iniciar el servidor Flask
python src/weather_api_server.py
```

### Ejemplo de Resultados
Los resultados incluyen:
- Temperatura máxima promedio mensual
- Precipitación total mensual
- Visualización tabular de datos

### Notas Técnicas
- El proyecto utiliza AWS EMR para el procesamiento distribuido
- Los datos se almacenan en S3
- La API utiliza Flask para servir los resultados
- Se implementa manejo de errores y validación de datos
