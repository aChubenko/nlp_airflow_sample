#!/bin/bash
set -e
# Инициализация базы данных
airflow db migrate

# Создание пользователя (один раз, не при каждом старте)
airflow users create \
  --username admin \
  --firstname Air \
  --lastname Flow \
  --role Admin \
  --email admin@example.com \
  --password admin

# Запуск webserver и scheduler
airflow scheduler & airflow webserver
