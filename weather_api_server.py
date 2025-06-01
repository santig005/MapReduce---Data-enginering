# weather_api_server.py
from flask import Flask, jsonify, render_template_string
import csv
import io  # Para leer el archivo de S3 en memoria
import boto3  # AWS SDK para Python
import ast  # Para convertir la representación string de la lista a lista real

app = Flask(__name__)

# Configuración de S3 (ajusten según su bucket y archivo)
S3_BUCKET_NAME = 'su-bucket-nombre'  # ¡CAMBIAR ESTO!
S3_WEATHER_KEY = 'proyecto-mr/output/weather_results.csv'  # ¡CAMBIAR ESTO!

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

def get_weather_from_s3():
    """
    Obtiene el archivo CSV de S3, lo lee línea a línea y convierte cada fila
    en un dict con claves 'month', 'temperature' y 'precipitation'.
    El CSV debe tener formato:
        "2023-01"    [24.75, 106.5]
        "2023-02"    [25.99, 56.7]
        ...
    Separador de columnas: TAB
    """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_WEATHER_KEY)
        content = obj['Body'].read().decode('utf-8')

        results = []
        csv_file = io.StringIO(content)
        reader = csv.reader(csv_file, delimiter='\t')

        for row in reader:
            if len(row) == 2:
                month = row[0].strip('"')
                try:
                    # Se espera que row[1] sea algo como "[24.75, 106.5]"
                    value_list = ast.literal_eval(row[1])
                    if (
                        isinstance(value_list, list)
                        and len(value_list) == 2
                    ):
                        temp = float(value_list[0])
                        precip = float(value_list[1])
                        results.append({
                            'month': month,
                            'temperature': temp,
                            'precipitation': precip
                        })
                except (ValueError, SyntaxError, TypeError):
                    # Si falla el parseo, se omite esa fila
                    print(f"Fila malformada, se omite: {row}")
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
