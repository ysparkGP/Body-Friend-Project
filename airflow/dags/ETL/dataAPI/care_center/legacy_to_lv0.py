import requests
import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.models import Connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow.exceptions import AirflowException
import json

db_user = 'udaivbbgsjgrg9'
db_pass = 'pa1b14ec99ca23d8f899c8b07a9bf01b21ab9959611d0ab160019b951d7792bae'
db_host = 'ec2-18-176-179-213.ap-northeast-1.compute.amazonaws.com'
db_port = '5432'
db_name = 'doq46gccfbqdp'

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
    dag_id = "legacy_to_lv0_DATAAPI.CARE_CENTER",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['DATAAPI'],
    catchup=False
)
def care_center_dag():
    job_info = {
        'schema' : 'DATAAPI',
        'table' : 'CARE_CENTER'
    }
    def make_record(result_df, row):
        record = {
            "번호": row.get("번호"),
            "시도": row.get("시도"),
            "시군구": row.get("시군구"),
            "산후조리원": row.get("산후조리원"),
            "주소": row.get("주소"),
            "전화번호": row.get("전화번호"),
            "일반실": row.get("일반실"),
            "특실": row.get("특실"),
        }
        df = pd.DataFrame.from_dict([record])
        return pd.concat([result_df, df], ignore_index=True)

    @task
    def request_care_center(**context):

            connection = Connection.get_connection_from_secrets(conn_id='DATAHUB')
            post_host = connection.host
            post_user = connection.login
            post_pass = connection.password
            post_db = connection.schema
            post_port = connection.port
            post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)
            error_cnt = 0
            result_df = pd.DataFrame([])
            
            # 1 페이지부터
            page = 1 
            temp = None
            while 1:
                api_url = 'https://api.odcloud.kr/api/15004303/v1/uddi:e1f4ce85-64fd-46c7-b563-ffb8cfd55bf7_201708031707'
                params = {
                    'serviceKey': '1JmE4RUDRWm3eLgidMR5THWVeZeR/0z8DI0NXPt8cdLzykxiw/YHIxypNG+CkHLqR6rI/fCUHA1m3d1QWkdb+g==',
                    'page' : page,
                    'perPage' : 1000
                }
                response = requests.get(api_url, params=params)

                if response.status_code == 200:
                    error_cnt = 0

                    temp_json = response.content.decode('utf8')
                    data = json.loads(temp_json)
                    rows = data.get('data')

                    if len(rows) == 0:
                        print('complete')
                        result_df.to_sql('care_center',post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False, method='multi')
                        
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

                    for row in rows:
                        result_df = make_record(result_df, row)
                    page += 1

                else:
                    error_cnt += 1
                    if error_cnt >= 3:
                        raise Exception('API Call Error')
                    print('Error')

                
    request_care_center()

care_center_dag()