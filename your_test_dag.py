"""
### DAG documntation
This is a simple ETL data pipeline example which extract rates data from API and load it into postgresql.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

"""
# Variable в Airflow
variables = Variable.set(key="key...",
                         value={"...": "...", "...": "..."},
                         serialize_json=True)
dag_variables = Variable.get("key...", deserialize_json=True)

variable1 = dag_variables.get('value1')

"""

# Put your code here
#
#
#
#
#
#



    
"""
Identify Airflow DAG
"""
with DAG(dag_id="...", schedule_interval="...",
    default_args=default_args, catchup=False
) as dag:
    dag.doc_md = __doc__
    
    ... = PythonOperator(
                task_id="...",
                python_callable=...)
    

