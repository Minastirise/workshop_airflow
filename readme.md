# Скачать и уcтановить Airflow (Docker Image)
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

# Скачать репозиторий и из терминала перейти в папку workshop_airflow
https://github.com/Minastirise/workshop_airflow

# Создать папки logs, dags и plugins в папке workshop_airflow/airflow
mkdir ./airflow
mkdir ./airflow/logs
mkdir ./airflow/dags
mkdir ./airflow/plugins

# Запустить команду
docker-compose up

# Проверить подлючение к Airflow
http://localhost:8080
airflow / airflow 

# Проверить запуск python скрипта - запускаем из отдельного окошка терминала как python файл
apt update -y
sudo apt install -y python3-dev python3-pip libpq-dev gcc
sudo pip install psycopg2

./load_currency_to_s3.py

# Проверить подключение к DBeaver
hostname = localhost
port = 5430
username = postgres
password = password
database = test

# Завершить работу Airflow
В другом окне в этой же директории выполнить команду:
docker-compose down
