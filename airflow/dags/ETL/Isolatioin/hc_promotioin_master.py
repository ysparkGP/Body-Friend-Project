from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
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
    dag_id = "single_dag.PROMOTION",
    default_args=default_args,
    schedule_interval='0 22 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['SINGLE'],
    catchup=False
)
def lv0_job():
    job_info = {
        'schema' : 'SINGLE',
        'table' : 'PROMOTION'
    }

    def lv0_job_func(**context):
        postgres_hook = PostgresHook(postgres_conn_id='DATAHUB')
        postgres_conn = postgres_hook.get_conn()
        with postgres_conn.cursor() as postgres_cursor:
            sql = f"select hc.func_hc_to_lv1_product();"
            result = postgres_hook.get_records(sql)

            if not result[0][0]: raise AirflowException("hc.func_hc_to_lv1_promotion(): Failed.")

            now_timestamp = datetime.now() + timedelta(hours=9)
            now_date = now_timestamp.date()
            insert_log_query = f"insert into public.dag_log values('{context['dag_run'].dag_id}', '{now_date}', '{now_timestamp}')\
                                    on conflict (dag_id, completion_date) DO\
                                    UPDATE\
                                    set dag_id = EXCLUDED.dag_id,\
                                    completion_date = EXCLUDED.completion_date,\
                                    completion_datetime = EXCLUDED.completion_datetime;\
                                "
            postgres_cursor.execute(insert_log_query)
            postgres_conn.commit()


    lv0_job = PythonOperator(
        task_id=f'lv0_task_{job_info["schema"]}.{job_info["table"]}',
        python_callable=lv0_job_func
    )


    # branch >> [not_condition_task, lv0_job]
    # lv0_job >> trigger_dag_task
    lv0_job

lv0_job()