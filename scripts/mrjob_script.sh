#!/bin/bash
set -e    # Salir si algún comando falla
set -x    # Imprimir cada comando antes de ejecutarlo

# ----------------------------
# CONFIGURACIÓN
# ----------------------------
BUCKET="mapreduce-emr-project"
INPUT_JSONL_S3_PATH="s3://${BUCKET}/input/open_meteo_processed_2023.jsonl"
SCRIPT_S3_PATH="s3://${BUCKET}/scripts/monthly_weather_stats_mrjob.py"

LOCAL_SCRIPT_NAME="monthly_weather_stats_mrjob_local.py"

HDFS_INPUT_DIR="/user/hadoop/mr_input_data"
HDFS_INPUT_FILE_PATH="${HDFS_INPUT_DIR}/open_meteo_processed_2023.jsonl"
HDFS_OUTPUT_DIR="/user/hadoop/mr_output_data"

# En lugar de un destino “archivo” directo para s3-dist-cp,
# definiremos una ruta local temporal donde haremos getmerge,
# y luego subiremos con aws s3 cp.
MERGED_LOCAL_PATH="/tmp/final_weather_stats.txt"
S3_FINAL_OUTPUT_PATH="s3://${BUCKET}/output/final_weather_stats.txt"

# ----------------------------
# 1. Crear directorio de entrada en HDFS
# ----------------------------
echo "🔹 1. Creando directorio HDFS de entrada..."
hadoop fs -mkdir -p "${HDFS_INPUT_DIR}"

# ----------------------------
# 2. Descargar el script MRJob desde S3 al nodo maestro
# ----------------------------
echo "🔹 2. Descargando script MRJob desde S3 al nodo maestro..."
aws s3 cp "${SCRIPT_S3_PATH}" "./${LOCAL_SCRIPT_NAME}"
chmod +x "./${LOCAL_SCRIPT_NAME}"

# ----------------------------
# 3. Copiar datos de entrada (JSONL) de S3 a HDFS
# ----------------------------
echo "🔹 3. Copiando datos de entrada de S3 a HDFS..."
hadoop fs -cp -f "${INPUT_JSONL_S3_PATH}" "${HDFS_INPUT_FILE_PATH}"

echo "Verificando la existencia del archivo en HDFS: ${HDFS_INPUT_FILE_PATH}"
if ! hadoop fs -test -e "${HDFS_INPUT_FILE_PATH}"; then
    echo "❌ Error: El archivo ${HDFS_INPUT_FILE_PATH} NO existe en HDFS después de la copia."
    hadoop fs -ls "$(dirname ${HDFS_INPUT_FILE_PATH})" || echo "No se pudo listar el directorio padre."
    exit 1
else
    echo "✅ Archivo ${HDFS_INPUT_FILE_PATH} confirmado en HDFS."
fi

# ----------------------------
# 4. Limpiar directorio de salida HDFS anterior (si existe)
# ----------------------------
echo "🔹 4. Limpiando directorio de salida HDFS anterior (si existe)..."
hadoop fs -rm -r -skipTrash "${HDFS_OUTPUT_DIR}" || true

# ----------------------------
# 5. Ejecutar MapReduce via MRJob
# ----------------------------
echo "🔹 5. Ejecutando MapReduce vía MRJob..."
python3 "./${LOCAL_SCRIPT_NAME}" \
    -r hadoop \
    "hdfs://${HDFS_INPUT_FILE_PATH}" \
    --output-dir "hdfs://${HDFS_OUTPUT_DIR}" \
    --jobconf mapreduce.job.reduces=2

# Verificar que el directorio de salida exista
if ! hadoop fs -test -e "${HDFS_OUTPUT_DIR}"; then
    echo "❌ Error: El directorio de salida ${HDFS_OUTPUT_DIR} NO existe después de MapReduce."
    hadoop fs -ls /user/hadoop || true
    exit 1
else
    echo "✅ Directorio de salida HDFS existe: ${HDFS_OUTPUT_DIR}"
fi

# ----------------------------
# 6. Fusionar resultados de HDFS en un solo archivo local
# ----------------------------
echo "🔹 6. Fusionando todos los part-* en un archivo local temporal..."
# Comprueba si existen archivos part- en el output
PART_FILES_COUNT=$(hadoop fs -ls "${HDFS_OUTPUT_DIR}" | grep -E "part-[0-9]+" | wc -l)
if [ "${PART_FILES_COUNT}" -eq 0 ]; then
    echo "❌ Error: No se encontraron archivos part- en ${HDFS_OUTPUT_DIR}"
    exit 1
fi

# El comando getmerge toma todos los archivos textuales de HDFS en HDFS_OUTPUT_DIR
# y los concatena en el archivo local MERGED_LOCAL_PATH
hadoop fs -getmerge "${HDFS_OUTPUT_DIR}" "${MERGED_LOCAL_PATH}"

# Verificar que el archivo local exista y no esté vacío
if [ ! -f "${MERGED_LOCAL_PATH}" ] || [ ! -s "${MERGED_LOCAL_PATH}" ]; then
    echo "❌ Error: No se creó el archivo local fusionado (o está vacío): ${MERGED_LOCAL_PATH}"
    exit 1
else
    echo "✅ Archivo fusionado creado localmente: ${MERGED_LOCAL_PATH}"
fi

# ----------------------------
# 7. Subir archivo fusionado a S3
# ----------------------------
echo "🔹 7. Subiendo archivo fusionado a S3: ${S3_FINAL_OUTPUT_PATH}"
aws s3 cp "${MERGED_LOCAL_PATH}" "${S3_FINAL_OUTPUT_PATH}"

# Verificar subida
aws s3 ls "${S3_FINAL_OUTPUT_PATH}" | grep "${BUCKET}" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ Error: Falló la subida del archivo a S3."
    exit 1
else
    echo "✅ Archivo subido correctamente a S3: ${S3_FINAL_OUTPUT_PATH}"
fi

echo "✅ Trabajo completado exitosamente. Resultado final en: ${S3_FINAL_OUTPUT_PATH}"
exit 0
