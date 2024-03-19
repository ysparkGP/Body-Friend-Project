import requests
import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.models import Connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow.exceptions import AirflowException
import xmltodict
import json
import ssl

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
    dag_id = "legacy_to_lv0_DATAAPI.HOSPITAL",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['DATAAPI'],
    catchup=False
)
def hospital_dag():
    job_info = {
        'schema' : 'DATAAPI',
        'table' : 'HOSPITAL'
    }

    def make_record(result_df, row):
        record = {
            'dgidIdName': row.get('dgidIdName'),
            'dutyAddr': row.get('dutyAddr'),
            'dutyEryn': row.get('dutyEryn'),
            'dutyHano': row.get('dutyHano'),
            'dutyHayn': row.get('dutyHayn'),
            'dutyName': row.get('dutyName'),
            'dutyTel1': row.get('dutyTel1'),
            'dutyTel3': row.get('dutyTel3'),
            'dutyTime1c': row.get('dutyTime1c'),
            'dutyTime1s': row.get('dutyTime1s'),
            'dutyTime2c': row.get('dutyTime2c'),
            'dutyTime2s': row.get('dutyTime2s'),
            'dutyTime3c': row.get('dutyTime3c'),
            'dutyTime3s': row.get('dutyTime3s'),
            'dutyTime4c': row.get('dutyTime4c'),
            'dutyTime4s': row.get('dutyTime4s'),
            'dutyTime5c': row.get('dutyTime5c'),
            'dutyTime5s': row.get('dutyTime5s'),
            'dutyTime6c': row.get('dutyTime6c'),
            'dutyTime6s': row.get('dutyTime6s'),
            'hpbdn': row.get('hpbdn'),
            'hperyn': row.get('hperyn'),
            'hpgryn': row.get('hpgryn'),
            'hpid': row.get('hpid'),
            'hpopyn': row.get('hpopyn'),
            'MKioskTy1': row.get('MKioskTy1'),
            'MKioskTy10': row.get('MKioskTy10'),
            'MKioskTy11': row.get('MKioskTy11'),
            'MKioskTy2': row.get('MKioskTy2'),
            'MKioskTy25': row.get('MKioskTy25'),
            'MKioskTy3': row.get('MKioskTy3'),
            'MKioskTy4': row.get('MKioskTy4'),
            'MKioskTy5': row.get('MKioskTy5'),
            'MKioskTy6': row.get('MKioskTy6'),
            'MKioskTy7': row.get('MKioskTy7'),
            'MKioskTy8': row.get('MKioskTy8'),
            'MKioskTy9': row.get('MKioskTy9'),
            'o001': row.get('o001'),
            'o003': row.get('o003'),
            'o004': row.get('o004'),
            'o006': row.get('o006'),
            'o007': row.get('o007'),
            'o015': row.get('o015'),
            'o018': row.get('o018'),
            'o022': row.get('o022'),
            'o025': row.get('o025'),
            'o027': row.get('o027'),
            'o028': row.get('o028'),
            'o029': row.get('o029'),
            'o030': row.get('o030'),
            'o033': row.get('o033'),
            'o034': row.get('o034'),
            'o036': row.get('o036'),
            'o038': row.get('o038'),
            'postCdn1': row.get('postCdn1'),
            'postCdn2': row.get('postCdn2'),
            'wgs84Lat': row.get('wgs84Lat'),
            'wgs84Lon': row.get('wgs84Lon')
        }
        df = pd.DataFrame.from_dict([record])
        return pd.concat([result_df, df], ignore_index=True)

    @task
    def request_hospital(**context):

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
        pageNo = 1 
        temp = None
        while 1:
            try:
                api_url = 'http://apis.data.go.kr/B552657/HsptlAsembySearchService/getHsptlMdcncFullDown'
                params = {
                    'serviceKey': '1JmE4RUDRWm3eLgidMR5THWVeZeR/0z8DI0NXPt8cdLzykxiw/YHIxypNG+CkHLqR6rI/fCUHA1m3d1QWkdb+g==',
                    'pageNo' : pageNo,
                    'numOfRows' : '1000'
                }
                response = requests.get(api_url, params=params, verify=False)
                xml_data = response.text
                data_dict = xmltodict.parse(xml_data)

                # print(data_dict)
                if data_dict.get('response'):
                    error_cnt = 0
                    items = data_dict['response']['body']['items']

                    if items is not None:
                        rows = items['item']

                        # 일관되게 딕셔너리 리스트를 반환 안함
                        # 1건이면 딕셔너리 한 건만 반환 됨
                        if (type(rows)) is list:
                            for row in rows:
                                temp = row
                                result_df = make_record(result_df, row)
                        else:
                            result_df = make_record(result_df, rows)
                        
                        print(f'{pageNo} complete')
                        pageNo += 1
                        
                    # 페이징 끝
                    else:
                        print('complete')
                        result_df.to_sql('hospital',post_engine, schema='lv0', if_exists='replace', chunksize=1000, index=False, method='multi')
                        
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
                    
            except Exception as e:
                error_cnt += 1
                if error_cnt >= 3:
                    raise AirflowException('API Call Error')
            
    request_hospital()

hospital_dag()