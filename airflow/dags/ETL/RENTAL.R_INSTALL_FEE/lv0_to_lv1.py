from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'goldenplanet',
    'email': ['yspark@goldenplanet.co.kr','dhlee@goldenplanet.co.kr','jwjang@goldenplanet.co.kr','ejshin@goldenplanet.co.kr'],
	'email_on_failure': True,
	'email_on_retry':False,
	'retries': 3,
	'retry_delay': timedelta(minutes=30)
}
@dag(
    dag_id = "lv0_dag_RENTAL.R_INSTALL_FEE",
    default_args=default_args,
    schedule_interval=None, # 혹은 "0 12 * * *" 와 같이 cron 표현식 사용
    start_date=datetime(2024,1,4),
    tags=['RENTAL']
)
def lv0_job():
    job_info = {
        'schema' : 'RENTAL',
        'table' : 'R_INSTALL_FEE'
    }

    def check_condition(**context):
        now = datetime.now() + timedelta(hours=9)
        now_date = now.date()
        postgres_hook = PostgresHook(postgres_conn_id='DATAHUB')
        target_cnt_query = f"select count(*)\
                            from public.dependency_manager\
                            where 1=1\
                            and tobe_dag = '{context['dag_run'].dag_id}';"
        check_cnt_query = f"select count(*)\
                    from public.dependency_manager  a \
                    join public.dag_log b \
                    on(a.asis_dag = b.dag_id)\
                    where 1=1\
                    and a.tobe_dag = '{context['dag_run'].dag_id}'\
                    and b.completion_date = '{now_date}';"
        target_cnt = postgres_hook.get_records(target_cnt_query)[0][0]
        check_cnt = postgres_hook.get_records(check_cnt_query)[0][0]
        
        if target_cnt != 0 and target_cnt != 0 and target_cnt == check_cnt : return f'lv0_task_{job_info["schema"]}.{job_info["table"]}'
        else: raise AirflowException("check_condition")
        
    branch = BranchPythonOperator(
        task_id='check_condition',
        python_callable=check_condition
    )

    not_condition_task = DummyOperator(task_id="no_task")

    # lv0_job = PostgresOperator(
    #     task_id = f'lv0_task_{job_info["schema"]}.{job_info["table"]}',
    #     postgres_conn_id = "DATAHUB",
    #     sql = f"select lv0.test();",
    #     runtime_parameters = {"search_path": "lv0"}
    # )

    def lv0_job_func(**context):
        postgres_hook = PostgresHook(postgres_conn_id='DATAHUB')
        postgres_conn = postgres_hook.get_conn()
        with postgres_conn.cursor() as postgres_cursor:
            sql = f"select lv1.func_daily_r_install_fee();"
            result = postgres_hook.get_records(sql)

            if not result[0][0]: raise AirflowException("lv1.func_daily_r_install_fee: Failed.")

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

    branch >> [not_condition_task, lv0_job]

lv0_job()