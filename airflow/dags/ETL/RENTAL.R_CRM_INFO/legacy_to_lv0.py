import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
import re

local_tz = pendulum.timezone("Asia/Seoul")


default_args = {
    'owner': 'goldenplanet',
    'email': ['yspark@goldenplanet.co.kr','dhlee@goldenplanet.co.kr'],
	'email_on_failure': True,
	'email_on_retry':False,
	'retries': 3,
	'retry_delay': timedelta(minutes=30)
}
@dag(
    dag_id = "legacy_to_lv0_RENTAL.R_CRM_INFO",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval="0 0 1,8,15,22,29 * *", 
    tags=['RENTAL'],
    catchup=False
)
def etl_dag():
    job_info = {
        'schema' : 'RENTAL',
        'table' : 'R_CRM_INFO'
    }

    @task
    def etl_mssql(**context):
        mssql_hook = MsSqlHook(mssql_conn_id='UNI-ERP')
        ms_engine = mssql_hook.get_conn()

        # postgres_hook = PostgresHook(postgres_conn_id='DATAHUB').get_conn()
        connection = Connection.get_connection_from_secrets(conn_id='DATAHUB')
        post_host = connection.host
        post_user = connection.login
        post_pass = connection.password
        post_db = connection.schema
        post_port = connection.port
        post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)

        schema_name = job_info['schema']
        table_name = job_info['table']

        start_page = 1
        per_page = 100000
        now_row = 0

        count_sql = f"SELECT COUNT(*) as cnt FROM {schema_name}.DBO.{table_name};"
        count_record = mssql_hook.get_records(sql = count_sql)
        total_cnt = count_record[0][0]

        truncate_query = f'TRUNCATE TABLE lv0.{table_name.lower()}'
        post_engine.execute(truncate_query)
        
        while start_page <= total_cnt:
            sql = f"SELECT A.* FROM( SELECT *, ROW_NUMBER() OVER(ORDER BY ZIP_SEQ) AS row_num FROM {schema_name}.DBO.{table_name} ) AS A WHERE 1=1 AND A.row_num BETWEEN {start_page} AND {start_page+per_page}-1 ";
                    
            start_page += per_page
            df = pd.read_sql(sql, ms_engine)

            # NULL Byte 처리
            re_null = re.compile(pattern='\x00')
            df.replace(regex=re_null, value='', inplace=True)
            df.to_sql(name=table_name.lower(), con=post_engine, schema='lv0', if_exists='append', chunksize=1000, index=False, method='multi')

        # sql = f"SELECT * FROM {schema_name}.DBO.{table_name}";
        # df = pd.read_sql(sql, ms_engine)
        # context['task_instance'].xcom_push(key='xcom_push_value', value=df.to_string())
        # df.to_sql(name=table_name.lower(), con=post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False, method='multi')

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

    trigger_dag_task = TriggerDagRunOperator(
        task_id = f'source_to_lv0_call_trigger_{job_info["schema"]}.{job_info["table"]}',
        trigger_dag_id = f'lv0_dag_{job_info["schema"]}.{job_info["table"]}',
        trigger_run_id = None,
        execution_date = None,
        reset_dag_run = True,
        wait_for_completion = False,
        poke_interval = 60,
        allowed_states = ['success'],
        failed_states=None
    )

    etl_mssql() >> trigger_dag_task

etl_dag()