#!/bin/bash

# Скрипт для настройки окружения и запуска сборщика данных TRON

# Создаем виртуальное окружение, если его нет
if [ ! -d "venv" ]; then
    echo "Создание виртуального окружения Python..."
    python3 -m venv venv
fi

# Активируем виртуальное окружение
source venv/bin/activate

# Устанавливаем зависимости
echo "Установка зависимостей..."
pip install -r requirements.txt

# Создаем директорию для логов, если её нет
mkdir -p logs

# Запускаем сборщик данных для TRON
echo "Запуск сборщика данных TRON..."
python tron_historical_data.py --days 1 > logs/tron_collector_$(date +%Y%m%d_%H%M%S).log 2>&1

# Получаем текущие курсы валют
echo "Получение текущих курсов валют..."
python tron_historical_data.py --currency-rates > logs/currency_rates_$(date +%Y%m%d_%H%M%S).log 2>&1

echo "Готово!"