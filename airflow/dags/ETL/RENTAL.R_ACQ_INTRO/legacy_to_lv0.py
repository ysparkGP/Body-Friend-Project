import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Connection
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import boto3
from botocore.client import Config
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import io
import logging
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'goldenplanet',
    'email': ['yspark@goldenplanet.co.kr','dhlee@goldenplanet.co.kr','jwjang@goldenplanet.co.kr','ejshin@goldenplanet.co.kr'],
	'email_on_failure': True,
	'email_on_retry':False,
	'retries': 3,
	'retry_delay': timedelta(minutes=30)
}
@dag(
    dag_id = "legacy_to_lv0_RENTAL.R_ACQ_INTRO",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['RENTAL'],
    catchup=False
)
def etl_dag():
    job_info = {
        'schema' : 'RENTAL',
        'table' : 'R_ACQ_INTRO'
    }

    @task
    def etl_mssql(**context):
        schema_name = job_info['schema']
        table_name = job_info['table']
        sql = f"SELECT * FROM {schema_name}.DBO.{table_name}";
        mssql_hook = MsSqlHook(mssql_conn_id='UNI-ERP')
        ms_engine = mssql_hook.get_conn()
        df = pd.read_sql(sql, ms_engine)

        # print(context['dag_run'].dag_id)

        # postgres_hook = PostgresHook(postgres_conn_id='DATAHUB').get_conn()
        connection = Connection.get_connection_from_secrets(conn_id='DATAHUB')
        post_host = connection.host
        post_user = connection.login
        post_pass = connection.password
        post_db = connection.schema
        post_port = connection.port
        post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)
        
        # truncate_query = f'TRUNCATE TABLE lv0.{table_name.lower()}'
        # post_engine.execute(truncate_query)
        df.to_sql(name=table_name.lower(), con=post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False, method='multi')
        
        # 다음 스텝 조건을 위한 로그 생성
        now_timestamp = datetime.now() + timedelta(hours=9)
        now_date = now_timestamp.date()
        insert_log_query = f"insert into public.dag_log values('{context['dag_run'].dag_id}', '{now_date}', '{now_timestamp}')\
                                on conflict (dag_id, completion_date) DO\
                                UPDATE\
                                set dag_id = EXCLUDED.dag_id,\
                                completion_date = EXCLUDED.completion_date,\
                                completion_datetime = EXCLUDED.completion_datetime;\
                            "
        post_engine.execute(insert_log_query)
        # post_engine.commit()

    etl_mssql()

etl_dag()