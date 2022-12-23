#!/usr/bin/python3

"""
This is a simple ETL data pipeline example which extract rates data from API
 and load it into postgresql.
"""

import decimal
from time import localtime, strftime
import psycopg2
import requests

URL_BASE = 'https://api.exchangerate.host/'

TABLE_NAME = 'rates'
RATE_BASE = 'BTC'
RATE_TARGET = 'USD'

PG_HOSTNAME = 'host.docker.internal'
PG_PORT = '5430'
PG_USERNAME = 'postgres'
PG_PASS = 'password'
PG_DB = 'test'


def import_codes():
    """
    Run uploading code from exchangerate.host API
    """
    # Parameters
    hist_date = "latest"
    url = URL_BASE + hist_date

    response = requests.get(url,
                            params={'base': RATE_BASE},
                            timeout=120)
    if not response:
        print(f'Response Failed: {response.status_code}')
        return

    data = response.json()
    rate_date = data['date']
    value_ = str(decimal.Decimal(data['rates']['USD']))[:20]

    insert_data(hist_date, rate_date, value_)


def insert_data(hist_date, rate_date, value_):
    """
    Save rates in pustgresql
    """
    ingest_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())

    conn = psycopg2.connect(host=PG_HOSTNAME,
                            port=PG_PORT,
                            user=PG_USERNAME,
                            password=PG_PASS,
                            database=PG_DB)
    cursor = conn.cursor()
    if hist_date != "latest":
        cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rate_date = '{rate_date}';")
        conn.commit()
    cursor.execute(f"""INSERT INTO {TABLE_NAME}
		(ingest_datetime, rate_date, rate_base, rate_target, value_ )
		VALUES('{ingest_datetime}','{rate_date}', '{RATE_BASE}',
		'{RATE_TARGET}', '{value_}');
		""")
    conn.commit()
    cursor.close()
    conn.close()

import_codes()
