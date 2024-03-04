import requests
import json
import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.models import Connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow.exceptions import AirflowException
import time

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
    dag_id = "legacy_to_lv0_DATAAPI.SCHOOL",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['DATAAPI'],
    catchup=False
)
def school_dag():
    job_info = {
        'schema' : 'DATAAPI',
        'table' : 'SCHOOL'
    }

    def make_record(result_df, row):
        record = {
            'seq' : row.get('seq'),
            'adres' : row.get('adres'),
            'schoolName' : row.get('schoolName'),
            'region' : row.get('region'),
            'estType' : row.get('estType'),
            'schoolGubun' : row.get('schoolGubun'),
            'link' : row.get('link'),
            'campusName' : row.get('campusName')
        }
        df = pd.DataFrame.from_dict([record])
        return pd.concat([result_df, df], ignore_index=True)

    @task
    def request_school(**context):
        connection = Connection.get_connection_from_secrets(conn_id='DATAHUB')
        post_host = connection.host
        post_user = connection.login
        post_pass = connection.password
        post_db = connection.schema
        post_port = connection.port
        post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)
        result_df = pd.DataFrame([])

        gubuns = ['elem_list', 'midd_list','high_list', 'univ_list', 'seet_list']
        for gubun in gubuns:
            # 1 페이지부터
            pageNo = 1 
            temp = None
            error_cnt = 0

            while 1:
                api_url = 'https://www.career.go.kr/cnet/openapi/getOpenApi'
                params = {
                    'apiKey': '4585ed8375efbb9cc108641ae156e6f8',
                    'svcType': 'api',
                    'svcCode': 'SCHOOL',
                    'contentType': 'json',
                    'gubun': gubun,
                    'thisPage' : pageNo,
                    'perPage' : '1000'
                }
                response = requests.get(api_url, params=params)
                if response.status_code == 200:
                    temp_json = response.content.decode('utf8')
                    data = json.loads(temp_json)
                    dataSearch = data.get('dataSearch')
                    rows = dataSearch['content']
                    if len(rows) == 0:
                        print(f'{gubun} complete')
                        break

                    for row in rows:
                        result_df = make_record(result_df, row)
                    pageNo += 1

                else:
                    error_cnt += 1
                    if error_cnt >= 3:
                        raise AirflowException('API Call Error')

        result_df.to_sql('school',post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False, method='multi') 

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

        return 

    # trigger_dag_task = TriggerDagRunOperator(
    #     task_id = f'source_to_lv0_call_trigger_{job_info["schema"]}.{job_info["table"]}',
    #     trigger_dag_id = f'lv0_dag_{job_info["schema"]}.PUBLIC_INSTITUTION',
    #     trigger_run_id = None,
    #     execution_date = None,
    #     reset_dag_run = True,
    #     wait_for_completion = False,
    #     poke_interval = 60,
    #     allowed_states = ['success'],
    #     failed_states=None
    # )

    request_school() 

school_dag()