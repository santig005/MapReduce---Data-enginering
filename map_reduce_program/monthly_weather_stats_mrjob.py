# monthly_weather_stats_mrjob.py
from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from datetime import datetime

class MRMonthlyWeatherStats(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_monthly_data,
                   combiner=self.combiner_aggregate_partial_stats, # Opcional, pero bueno para rendimiento
                   reducer=self.reducer_calculate_final_stats)
        ]

    def mapper_extract_monthly_data(self, _, line):
        """
        Mapper: Lee cada registro diario (JSON por línea).
        Extrae el mes (YYYY-MM), la temperatura máxima y la precipitación.
        Emite: (YYYY-MM, (temp_max, 1, precip_sum))
        El '1' es para ayudar a contar el número de días para el promedio de temperatura.
        """
        try:
            record = json.loads(line)
            date_str = record.get('date')
            temp_max = record.get('temperature_2m_max')
            precip_sum = record.get('precipitation_sum')

            # Asegurarse de que los datos necesarios estén presentes y sean válidos
            if date_str and temp_max is not None and precip_sum is not None:
                # Intentar convertir a float para asegurar que son números
                temp_max_float = float(temp_max)
                precip_sum_float = float(precip_sum)
                
                # Extraer el mes en formato YYYY-MM
                month_year = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m')
                
                yield month_year, (temp_max_float, 1, precip_sum_float)
        except (ValueError, TypeError, AttributeError) as e:
            # Contar errores o líneas malformadas si se desea
            self.increment_counter('mapper_errors', str(e.__class__.__name__), 1)
            # Ignorar líneas que no se pueden procesar
            pass

    def combiner_aggregate_partial_stats(self, month_year, values):
        """
        Combiner (Opcional): Realiza una agregación parcial en el nodo map.
        Suma las temperaturas, cuenta los registros y suma las precipitaciones.
        Emite: (YYYY-MM, (sum_temp_max, num_records, sum_precip))
        """
        sum_temp = 0.0
        num_records = 0
        sum_precip = 0.0
        
        for temp, count, precip in values:
            sum_temp += temp
            num_records += count
            sum_precip += precip
            
        yield month_year, (sum_temp, num_records, sum_precip)

    def reducer_calculate_final_stats(self, month_year, values):
        """
        Reducer: Recibe todos los valores (o valores pre-agregados por el combiner) para un mes.
        Calcula la temperatura máxima promedio y la precipitación total.
        Emite: (YYYY-MM, {"avg_max_temp": X, "total_precip": Y})
        """
        total_temp_sum = 0.0
        total_num_records = 0
        total_precip_sum = 0.0
        
        for temp_sum_or_val, num_records_or_one, precip_sum_or_val in values:
            total_temp_sum += temp_sum_or_val
            total_num_records += num_records_or_one
            total_precip_sum += precip_sum_or_val
            
        avg_temp_max = 0
        if total_num_records > 0:
            avg_temp_max = total_temp_sum / total_num_records
        
        # Usar un diccionario para la salida para que sea un JSON más legible
        # MRJob por defecto emitirá: "YYYY-MM" <TAB> '{"avg_max_temp": X, "total_precip": Y}'
        output_value = {
            "avg_max_temp": round(avg_temp_max, 2),
            "total_precip": round(total_precip_sum, 2)
        }
        yield month_year, output_value

if __name__ == '__main__':
    MRMonthlyWeatherStats.run()