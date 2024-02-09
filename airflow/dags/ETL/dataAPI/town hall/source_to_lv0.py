import requests
import json
import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.models import Connection

default_args = {
    'owner': 'goldenplanet',
    'email': ['yspark@goldenplanet.co.kr','dhlee@goldenplanet.co.kr'],
	'email_on_failure': True,
	'retries': 3,
	'retry_delay': timedelta(minutes=30)
}
@dag(
    dag_id = "api_to_lv0.TOWN_HALL",
    default_args=default_args,
    schedule_interval=None, # 혹은 "0 12 * * *" 와 같이 cron 표현식 사용
    start_date=datetime(2024,1,4),
    tags=['dataAPI']
)
def town_hall_dag():

    @task
    def request_town_hall():
        try:
            # engine = create_engine(f'postgresql://airflow:pa89160f15d9ab55ab4722502dabdec9ac74cdaf61ed4f2efc6338189a387a708@ec2-54-199-22-248.ap-northeast-1.compute.amazonaws.com:5432/d55s2du2773jun', pool_size=40, max_overflow=55)
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
                        raise Exception(f'API Error {checkCode} : {errorMessage}')
                    
            return
        
        except Exception as e:
            print(e)

    request_town_hall()

town_hall_dag()