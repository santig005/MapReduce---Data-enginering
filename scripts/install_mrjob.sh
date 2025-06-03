#!/bin/bash
set -e # Salir inmediatamente si un comando falla
set -x # Imprimir cada comando antes de ejecutarlo (para depuración del bootstrap)

echo "Iniciando script de bootstrap para instalar MRJob"

# Intentar instalar mrjob usando python3 y pip
# Usar -q para menos verbosidad una vez que funcione
sudo python3 -m pip install -q mrjob

# Verificar la instalación (opcional, pero útil para logs)
echo "MRJob post-instalación:"
python3 -m pip show mrjob || echo "MRJob no encontrado después de la instalación"

echo "Script de bootstrap finalizado"