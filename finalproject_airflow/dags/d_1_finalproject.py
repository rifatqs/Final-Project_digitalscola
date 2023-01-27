from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.python import PythonOperator

import logging
import requests
import pandas as pd
from datetime import datetime

from modules.transformer import Transformer
from modules.connector import Connector


def fun_get_data_from_api(**kwargs):
    #pass
    #ambil data
    data = requests.get(Variable.get('url'))
    dataCovid = data.json()['data']['content']
    df = pd.json_normalize(dataCovid)
    print(df.info(verbose=True))
    
    #create connector
    get_conn = Connection.get_connection_from_secrets("Mysql")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn.host,
        user=get_conn.login,
        password=get_conn.password,
        db=get_conn.schema,
        port=get_conn.port
    )

    #drop table if exists
    try:
        p = "DROP table IF EXISTS covid_jabar"
        engine_sql.execute(p)
    except Exception as e:
        logging.error(e)

    #insert to mysql
    df.to_sql(con=engine_sql, name='covid_jabar', index=False)
    logging.info("data succesfully inserted to MYSQL")


def fun_generate_dim(**kwargs):
    #pass
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    #insert data
    transformer = Transformer(engine_sql, engine_postgres)
    transformer.create_dimension_case()
    transformer.create_dimension_district()
    transformer.create_dimension_province()

def fun_insert_province_daily(**kwargs):
    #pass
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    transformer = Transformer(engine_sql, engine_postgres)
    transformer.create_province_daily()

def fun_insert_district_daily(**kwargs):
    #pass
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    transformer = Transformer(engine_sql, engine_postgres)
    transformer.create_district_daily()


with DAG(
    dag_id='final_project',
    start_date=datetime(2022, 5, 28),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    op_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = fun_get_data_from_api
    )

    op_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable = fun_generate_dim
    )

    op_insert_province_daily = PythonOperator(
        task_id = 'insert_province_daily',
        python_callable = fun_insert_province_daily
    )

    op_insert_district_daily = PythonOperator(
        task_id = 'insert_district_daily',
        python_callable = fun_insert_district_daily
    )

op_get_data_from_api >> op_generate_dim
op_generate_dim >> op_insert_province_daily
op_generate_dim >> op_insert_district_daily