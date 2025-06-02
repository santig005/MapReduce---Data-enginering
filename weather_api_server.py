# weather_api_server.py
from flask import Flask, jsonify, render_template_string
import io      # Para leer el archivo de S3 en memoria
import boto3   # AWS SDK para Python
import ast     # Para convertir la representación string de la lista a lista real

app = Flask(__name__)

# En lugar de definir bucket y key por separado, usamos la URI completa:
#   s3://mapreduce-emr-project/output/local_emr_node_output.txt
S3_URI = 's3://mapreduce-emr-project/output/local_emr_node_output.txt'

# HTML para una tabla simple de datos meteorológicos
HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Datos Meteorológicos Mensuales</title>
</head>
<body>
    <h1>Resumen Mensual de Temperatura y Precipitación</h1>
    <table border="1" cellpadding="8" cellspacing="0">
        <thead>
            <tr>
                <th>Mes</th>
                <th>Temperatura Promedio (°C)</th>
                <th>Precipitación (mm)</th>
            </tr>
        </thead>
        <tbody>
            {% for item in data %}
            <tr>
                <td>{{ item.month }}</td>
                <td>{{ "%.2f"|format(item.temperature) }}</td>
                <td>{{ "%.1f"|format(item.precipitation) }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

def parse_s3_uri(uri: str):
    """
    Toma una URI con formato "s3://bucket-name/path/to/object"
    y retorna (bucket_name, object_key).
    """
    if not uri.startswith('s3://'):
        raise ValueError(f"URI inválida: {uri}")
    path = uri[5:]  # quitar "s3://"
    parts = path.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"URI inválida o incompleta: {uri}")
    bucket_name, object_key = parts[0], parts[1]
    return bucket_name, object_key

def get_weather_from_s3():
    """
    Obtiene el archivo .txt de S3 usando la URI definida en S3_URI,
    lo lee línea a línea y convierte cada línea en un dict con claves
    'month', 'temperature' y 'precipitation'.
    Cada línea del .txt debe tener formato:
        "2023-01"    [24.75, 106.5]
    Separador entre mes y lista: espacio(s) o tab. Se usa split(maxsplit=1).
    """
    try:
        bucket, key = parse_s3_uri(S3_URI)
    except ValueError as e:
        print(f"Error al parsear S3_URI: {e}")
        return None

    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')

        results = []
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue  # saltar líneas vacías

            # Separar "mes" y "[temperatura, precipitación]"
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                print(f"Línea malformada, se omite: {line}")
                continue

            month = parts[0].strip('"')  # quitar comillas del mes
            try:
                value_list = ast.literal_eval(parts[1])
                if isinstance(value_list, list) and len(value_list) == 2:
                    temp   = float(value_list[0])
                    precip = float(value_list[1])
                    results.append({
                        'month': month,
                        'temperature': temp,
                        'precipitation': precip
                    })
                else:
                    print(f"Valor no es lista de dos elementos: {parts[1]}")
            except (ValueError, SyntaxError, TypeError):
                print(f"Error parseando valores: {parts[1]}")
                continue

        return results

    except Exception as e:
        print(f"Error al obtener datos de S3: {e}")
        return None


@app.route('/api/weather')
def weather_json():
    data = get_weather_from_s3()
    if data is not None:
        return jsonify(data)
    return jsonify({"error": "No se pudieron obtener los datos"}), 500


@app.route('/view/weather')
def weather_html():
    data = get_weather_from_s3()
    if data is not None:
        return render_template_string(HTML_TEMPLATE, data=data)
    return "Error al cargar datos meteorológicos.", 500


if __name__ == '__main__':
    # Asegúrense de tener configuradas las credenciales de AWS:
    # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (o un rol de IAM si ejecutan en EC2).
    app.run(debug=True, host='0.0.0.0', port=5001)