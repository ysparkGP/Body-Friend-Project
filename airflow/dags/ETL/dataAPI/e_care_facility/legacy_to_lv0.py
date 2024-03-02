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
import ssl

db_user = 'udaivbbgsjgrg9'
db_pass = 'pa1b14ec99ca23d8f899c8b07a9bf01b21ab9959611d0ab160019b951d7792bae'
db_host = 'ec2-18-176-179-213.ap-northeast-1.compute.amazonaws.com'
db_port = '5432'
db_name = 'doq46gccfbqdp'


default_args = {
    'owner': 'goldenplanet',
    'email': ['yspark@goldenplanet.co.kr','dhlee@goldenplanet.co.kr'],
	'email_on_failure': True,
	'email_on_retry':False,
	'retries': 3,
	'retry_delay': timedelta(minutes=30)
}
@dag(
    dag_id = "legacy_to_lv0_DATAAPI.E_CARE_FACILITY",
    default_args=default_args,
    schedule_interval=None, # 혹은 "0 12 * * *" 와 같이 cron 표현식 사용
    start_date=datetime(2024,1,4),
    tags=['DATAAPI']
)
def e_care_facility_dag():
    job_info = {
        'schema' : 'DATAAPI',
        'table' : 'E_CARE_FACILITY'
    }

    def make_record(result_df, row):
        record = {
            'geometry_x' : row.get('geometry').get('coordinates')[0],
            'geometry_y' : row.get('geometry').get('coordinates')[1],
            'cat_nm' : row.get('properties').get('cat_nam'),
            'fac_nam' : row.get('properties').get('fac_nam'),
            'fac_tel' : row.get('properties').get('fac_tel'),
            'fac_o_add' : row.get('properties').get('fac_o_add'),
            'fac_n_add' : row.get('properties').get('fac_n_add'),
            'id' : row.get('id')
        }
        df = pd.DataFrame.from_dict([record])
        return pd.concat([result_df, df], ignore_index=True)

    @task
    def request_e_care_facility(**context):
            print(ssl.OPENSSL_VERSION)
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
            size = 1000
            temp = None
            while 1:
                api_url = 'https://api.vworld.kr/req/data'
                params = {
                    'service':'data',
                    'version':'2.0',
                    'request':'GetFeature',
                    'key':'2D258BE6-AB8D-3DD5-B861-F526C2FA3CFD',
                    'format':'json',
                    'errorformat':'json',
                    'size':size,
                    'page':page,
                    'data':'LT_P_MGPRTFB',
                    'geomfilter':'BOX(124,33,132,43)',
                    'geometry':'true',
                    'attribute':'true',
                    'crs':'EPSG:4326',
                    'attrFilter':'cat_nam:=:노인복지시설'
                }
                response = requests.get(api_url, params=params)

                temp_json = response.content.decode('utf8')
                data = json.loads(temp_json)
                res = data.get('response')

                if res.get('status') == 'OK':
                    # 페이징 확인 후 종료 처리(유효 페이지 넘겨도 계속 조회됨)
                    current = res.get('page').get('current')
                    if page != int(current):
                        result_df.to_sql('e_care_facility',post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False, method='multi')
                        
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
                    
                    rows = res.get('result').get('featureCollection').get('features')
                    for row in rows:
                        result_df = make_record(result_df, row)

                    page += 1


                else:
                    error_cnt += 1
                    if error_cnt >= 3:
                        raise AirflowException('API Call Error')
                    print('Error')
                
    request_e_care_facility()

e_care_facility_dag()