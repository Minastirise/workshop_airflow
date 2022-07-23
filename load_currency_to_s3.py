#!/usr/bin/python3

import requests
import decimal
import psycopg2
from time import localtime, strftime
from datetime import datetime

url_base = 'https://api.exchangerate.host/'

table_name = 'rates'
rate_base = 'BTC'
rate_target = 'USD'

pg_hostname = 'host.docker.internal'
pg_port = '5430'
pg_username = 'postgres'
pg_pass = 'password'
pg_db = 'test'


"""
Save rates in pustgresql
"""
def insert_data(hist_date, data, rate_date, value_):
    ingest_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
    
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = conn.cursor()
    if hist_date != "latest":
        cursor.execute(f"DELETE FROM {table_name} WHERE rate_date = '{rate_date}';")
        conn.commit()    
    cursor.execute(f"INSERT INTO {table_name} (ingest_datetime, rate_date, rate_base, rate_target, value_ ) VALUES('{ingest_datetime}','{rate_date}', '{rate_base}', '{rate_target}', '{value_}');")
    conn.commit() 
    cursor.close()
    conn.close()	
	
"""
Run uploading code from exchangerate.host API
"""
def import_codes():
# Parameters
    hist_date = "latest"
    url = url_base + hist_date
    try:
        response = requests.get(url,
            params={'base': rate_base})
    except Exception as err:
        print(f'Error occured: {err}')
        return
    data = response.json()
    rate_date = data['date']
    value_ = str(decimal.Decimal(data['rates']['USD']))[:20]
    
    insert_data(hist_date, data, rate_date, value_)
    
import_codes()
