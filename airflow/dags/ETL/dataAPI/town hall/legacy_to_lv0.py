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
    dag_id = "legacy_to_lv0_DATAAPI.TOWN_HALL",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['DATAAPI'],
    catchup=False
)
def town_hall_dag():
    job_info = {
        'schema' : 'DATAAPI',
        'table' : 'TOWN_HALL'
    }

    @task
    def request_town_hall(**context):
        url = 'http://api.data.go.kr/openapi/tn_pubr_public_vill_hall_sen_cent_api'

        connection = Connection.get_connection_from_secrets(conn_id='DATAHUB')
        post_host = connection.host
        post_user = connection.login
        post_pass = connection.password
        post_db = connection.schema
        post_port = connection.port
        post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)
        
        pageNo = 0
        return_list = []
        error_cnt = 0
        result_df = pd.DataFrame([])
        while 1:
            params = {
                # 'serviceKey' : '1JmE4RUDRWm3eLgidMR5THWVeZeR%2F0z8DI0NXPt8cdLzykxiw%2FYHIxypNG%2BCkHLqR6rI%2FfCUHA1m3d1QWkdb%2Bg%3D%3D', 
                'serviceKey' : '1JmE4RUDRWm3eLgidMR5THWVeZeR/0z8DI0NXPt8cdLzykxiw/YHIxypNG+CkHLqR6rI/fCUHA1m3d1QWkdb+g==',
                'pageNo' : pageNo, 
                'numOfRows' : 1000, 
                'type' : 'json', 
                # 'FLCT_NM' : '', 
                # 'FLCT_TYP' : '', 
                # 'LCTN_ROAD_NM_ADDR' : '', 
                # 'LCTN_LOTNO_ADDR' : '', 
                # 'LAT' : '', 
                # 'LOT' : '', 
                # 'BUSI_COD_NM' : '', 
                # 'TELNO' : '', 
                # 'BUIL_YMD' : '', 
                # 'BUIL_AREA' : '', 
                # 'MNG_INST_NM' : '', 
                # 'CRTR_YMD' : '', 
                # 'instt_code' : '', 
                # 'instt_nm' : '' 
            }
            response = requests.get(url, params=params)
            temp_json = response.content.decode('utf8')
            data = json.loads(temp_json)
            checkCode = data['response']['header']['resultCode']
        
            # 페이징 끝
            if checkCode == '03': 
                result_df.to_sql('town_hall', post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False)
                
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

            # 정상 호출
            elif checkCode == '00':
                '''
                    공공기관 이름, 전화번호, 주소(위도, 경도 있으면 수집), 타입(병원/경로당/마을회관/etc)
                '''
                rows = data['response']['body']['items']
                for row in rows:
                    flctNm = row['flctNm']
                    flctTyp = row['flctTyp']
                    lctnRoadNmAddr = row['lctnRoadNmAddr']
                    lctnLotnoAddr = row['lctnLotnoAddr']
                    lat = row['lat']
                    lot = row['lot']
                    busiCodNm = row['busiCodNm']
                    telno = row['telno']
                    builYmd = row['builYmd']
                    builArea = row['builArea']
                    mngInstNm = row['mngInstNm']
                    crtrYmd = row['crtrYmd']
                    insttCode = row['insttCode']
                    record = {
                        'flctNm' : flctNm,
                        'flctTyp' : flctTyp,
                        'lctnRoadNmAddr' : lctnRoadNmAddr,
                        'lctnLotnoAddr' : lctnLotnoAddr,
                        'lat' : lat,
                        'lot' : lot,
                        'busiCodNm' : busiCodNm,
                        'telno' : telno,
                        'builYmd' : builYmd,
                        'builArea' : builArea,
                        'mngInstNm' : mngInstNm,
                        'crtrYmd' : crtrYmd,
                        'insttCode' : insttCode
                    }
                    df = pd.DataFrame.from_dict([record])
                    result_df = pd.concat([result_df, df], ignore_index=True)
                    # return_list.append(record)
                pageNo += 1
        
            else:
                error_cnt += 1
                if error_cnt >= 3:
                    errorMessage = data['response']['header']['resultMsg']
                    raise AirflowException(f'API Error {checkCode} : {errorMessage}')
        
        
    trigger_dag_task = TriggerDagRunOperator(
        task_id = f'source_to_lv0_call_trigger_{job_info["schema"]}.{job_info["table"]}',
        trigger_dag_id = f'lv0_dag_{job_info["schema"]}.PUBLIC_INSTITUTION',
        trigger_run_id = None,
        execution_date = None,
        reset_dag_run = True,
        wait_for_completion = False,
        poke_interval = 60,
        allowed_states = ['success'],
        failed_states=None
    )

    request_town_hall() >> trigger_dag_task

town_hall_dag()