"""
### Test example DAG
This is a simple ETL data pipeline example which extract rates data from API and load it into postgresql.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.dates import days_ago

import requests
import decimal
import psycopg2
from time import localtime, strftime
from datetime import datetime

# Можно так оставить переменные
# url_base = 'https://api.exchangerate.host/'

# table_name = 'rates'
# rate_base = 'BTC'
# rate_target = 'USD'

# pg_hostname = 'host.docker.internal'
# pg_port = '5430'
# pg_username = 'postgres'
# pg_pass = 'password'
# pg_db = 'test'

# connection_name = 'postgre_test_db'

# Можно воспользоваться сущностью Variable в Airflow
variables = Variable.set(key="currency_load_variables",
                         value={"table_name": "rates", "rate_base": "BTC", "rate_target": "USD", "connection_name":"pg_conn", "url_base":"https://api.exchangerate.host/",
                         "connection_name": "postgre_test_db"},
                         serialize_json=True)
dag_variables = Variable.get("currency_load_variables", deserialize_json=True)


default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

"""
Save rates in pustgresql
"""

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """
    Function returns dictionary with connection credentials

    :param conn_id: str with airflow connection id
    :return: Connection
    """
    conn = BaseHook.get_connection(conn_id)
    return conn
    
    
# def insert_data(hist_date, data, rate_date, value_):
def insert_data(**kwargs):
    task_instance = kwargs['task_instance']
    results = task_instance.xcom_pull(key='results', task_ids='import_rates')
    
    print("rate_date: ", results["rate_date"])
    print("value_: ", results["value_"])
    
    ingest_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
    
    pg_conn = get_conn_credentials(dag_variables.get('connection_name'))
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {dag_variables.get('table_name')} (ingest_datetime, rate_date, rate_base, rate_target, value_ ) valueS('{ingest_datetime}','{results['rate_date']}', '{dag_variables.get('rate_base')}', '{dag_variables.get('rate_target')}', '{results['value_']}');")
    conn.commit() 
    cursor.close()
    conn.close()	
	
"""
Run uploading code from exchangerate.host API
"""
def import_codes(**kwargs):
# Parameters
    hist_date = "latest"
    url = dag_variables.get('url_base') + hist_date
    ingest_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
    
    try:
        response = requests.get(url,
            params={'base': dag_variables.get('rate_base')})
    except Exception as err:
        print(f'Error occured: {err}')
        return
    data = response.json()
    rate_date = data['date']
    value_ = str(decimal.Decimal(data['rates']['USD']))[:20]
    
    ti = kwargs['ti']
    ti.xcom_push(key='results', value={"rate_date":rate_date, "value_":value_, "ingest_datetime":ingest_datetime })

    
"""
Identify Airflow DAG
"""
with DAG(dag_id="yandex-rates-test", schedule_interval="0 */3 * * *",
    default_args=default_args, tags=["yandex","test"], catchup=False
) as dag:
    dag.doc_md = __doc__
    import_rates_from_api = PythonOperator(
                task_id="import_rates",
                python_callable=import_codes)
    
    insert_rates_to_pg = PythonOperator(
                task_id="insert_data",
                python_callable=insert_data)                
    
    """    
    insert_rates_to_pg = PostgresOperator(
        task_id="insert_rates_to_pg",
        postgres_conn_id=dag_variables.get('connection_name'),
        sql = "INSERT INTO {{ params.table_name }} (ingest_datetime, rate_date, rate_base, rate_target, value_ ) values( '{{ params.ingest_datetime }}', '{{ params.rate_date }}', '{{ params.rate_base }}', '{{ params.rate_target }}', '{{ params.value_ }}' );",
        params ={
            "table_name":dag_variables.get('table_name'),
            "ingest_datetime": {{ ti.xcom_pull(task_ids="import_rates", key="ingest_datetime") }}, 
            "rate_date": {{ ti.xcom_pull(task_ids="import_rates", key="rate_date") }}, 
            "rate_base":dag_variables.get('rate_base'), 
            "rate_target":dag_variables.get('rate_target'), 
            "value_": {{ ti.xcom_pull(task_ids="import_rates", key="value_") }}
            }
    )    
    """
        
    import_rates_from_api >> insert_rates_to_pg
