import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
import pandas as pd
import json
import requests
import re
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_tz = pendulum.timezone("Asia/Seoul")
address_api_key = Variable.get("kakao_rest_api_key")

default_args = {
    'owner': 'goldenplanet',
    'email': ['yspark@goldenplanet.co.kr','dhlee@goldenplanet.co.kr'],
	'email_on_failure': True,
	'email_on_retry':False,
	'retries': 3,
	'retry_delay': timedelta(minutes=30)
}

@dag(
    dag_id = 'lv1ex_dag_RENTAL.V_CUSTOMER',
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval=None,
    max_active_runs=1, 
    tags=['lv1','api'],
    catchup=False
)
def refine_address():
    job_info = {
        'schema' : 'RENTAL',
        'table' : 'V_CUSTOMER'
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
        
        if target_cnt != 0 and target_cnt != 0 and target_cnt == check_cnt : return 'check_address'
        else: return 'no_task'
        
    branch = BranchPythonOperator(
        task_id='check_condition',
        python_callable=check_condition
    )

    not_condition_task = DummyOperator(task_id="no_task")

    def check_address():
        # PostgresHook을 사용하여 데이터베이스 연결 설정
        pg_hook = PostgresHook(postgres_conn_id='DATAHUB')
        
        query = '''select distinct A.legacy_addr from lv1.v_customer A left join lv1.address_master B on B.legacy_address = A.legacy_addr where B.seq is null and A.legacy_addr is not null'''
        pg_engine = pg_hook.get_conn()
        df = pd.read_sql(query, pg_engine)

        return df
    
    address_check = PythonOperator(
        task_id='check_address',
        python_callable=check_address
    )
    
    @task
    def search_address(**context):
        # 검색해야 하는 주소 정보 가져오기
        address_list = context['task_instance'].xcom_pull(task_ids='check_address')

        # 주소 검색
        def get_address(keyword):
            try:
                URL = 'https://dapi.kakao.com/v2/local/search/address'
                headers = {'Authorization': 'KakaoAK ' + address_api_key}
                params = {'query': keyword, 'size':1}
                res = requests.get(URL, headers= headers,params=params)
                return res
            except:
                return None
            
        # 빈값을 NULL로 치환
        def replace_empty_with_none(value):
            return None if value == '' else value
        
        # 검색 처리 함수
        def run_address_func():
            
            # DATA HUB DB 연결
            pg_hook = PostgresHook(postgres_conn_id='DATAHUB')
            # 검색해야 하는 주소 반복문 실행
            for _, row in address_list.iterrows():
                original_address = row['legacy_addr']
                # 뛰어쓰기 기준으로 단위 쪼갬
                chunks = original_address.split(' ')
                result_tf = False
                apierror_tf = False
                while len(chunks) >= 1 and result_tf is False and apierror_tf is False:
                    new_address = ' '.join(chunks)
                    print(new_address)
                    res = get_address(new_address)
                    if res.status_code != 200:
                        if 'errorType'in json.loads(res.text):
                            apierror_tf = True
                            break
                        else:    
                            pass

                    cnt = json.loads(res.text)['meta']['total_count']
                    #REQUEST 결과 헤더가 200이고, results가 1개 이상일 경우
                    if cnt > 0:
                        result_tf = True
                        #데이터를 추출하여 우편번호, 시, 도, 군을 추출
                        data = json.loads(res.text)['documents'][0]
                        address_name,building_name,region_1depth_name,region_2depth_name,region_3depth_name,x,y,zone_no,region_3depth_h_name,dong,ho = [None] * 11
                        road_address_tf = False
                        if data['road_address']:
                            road_address = data['road_address']
                            road_address = {key: replace_empty_with_none(value) for key, value in road_address.items()}
                            road_address_tf = True
                            address_name = road_address['address_name']
                            building_name = road_address['building_name']
                            region_1depth_name = road_address['region_1depth_name']
                            region_2depth_name = road_address['region_2depth_name']
                            region_3depth_name = road_address['region_3depth_name']
                            x = road_address['x']
                            y = road_address['y']
                            zone_no = road_address['zone_no']
                            if building_name:
                                pattern_dong = re.compile(r'(\d+)동\s*')
                                pattern_ho = re.compile(r'(\d+)호\s*')

                                # 정규표현식 매칭
                                matches_dong = pattern_dong.search(row['legacy_addr'])
                                matches_ho = pattern_ho.search(row['legacy_addr'])

                                if matches_dong:
                                    dong = matches_dong.group(1) if matches_dong.group(1) else None
                                if matches_ho:
                                    ho = matches_ho.group(1) if matches_ho.group(1) else None


                        elif data['address']:
                            address = data['address']
                            address = {key: replace_empty_with_none(value) for key, value in address.items()}
                            address_name = address['address_name']
                            region_1depth_name = address['region_1depth_name']
                            region_2depth_name = address['region_2depth_name']
                            region_3depth_name = address['region_3depth_name']
                            region_3depth_h_name = address['region_3depth_h_name']
                            x = address['x']
                            y = address['y']

                        pg_hook.insert_rows('lv1.address_master',
                            rows=[
                                (
                                    row['legacy_addr'],
                                    cnt,
                                    road_address_tf,
                                    address_name,
                                    region_1depth_name,
                                    region_2depth_name,
                                    region_3depth_name,
                                    region_3depth_h_name,
                                    building_name,
                                    dong,
                                    ho,
                                    zone_no,
                                    x,
                                    y,
                                )
                            ],
                            target_fields=[
                                'legacy_address',
                                'accuracy',
                                'road_address_tf',
                                'address_name',
                                'region_1depth_name',
                                'region_2depth_name',
                                'region_3depth_name',
                                'region_3depth_h_name',
                                'building_name',
                                'building_dong',
                                'building_ho',
                                'zone_no',
                                'x',
                                'y'
                            ],
                            replace=True,
                            replace_index="legacy_address"
                            # on_conflict=[
                            #     ('road_address_tf', 'EXCLUDED.road_address_tf'),
                            #     ('address_name', 'EXCLUDED.address_name'),
                            #     ('region_1depth_name', 'EXCLUDED.region_1depth_name'),
                            #     ('region_2depth_name', 'EXCLUDED.region_2depth_name'),
                            #     ('region_3depth_name', 'EXCLUDED.region_3depth_name'),
                            #     ('region_3depth_h_name', 'EXCLUDED.region_3depth_h_name'),
                            #     ('building_name', 'EXCLUDED.building_name'),
                            #     ('building_dong', 'EXCLUDED.building_dong'),
                            #     ('building_ho', 'EXCLUDED.building_ho'),
                            #     ('zone_no', 'EXCLUDED.zone_no'),
                            #     ('x', 'EXCLUDED.x'),
                            #     ('y', 'EXCLUDED.y'),
                            # ]
                        )
                    else:
                        chunks.pop()

                if apierror_tf:
                    print("API 오류 발생 원인 확인 필요")
                    print(res.text)
                    break

                if not result_tf:
                    print(row['legacy_addr'])
                    print(result_tf)
                    print(apierror_tf)
                    pass
                    pg_hook.insert_rows('lv1.address_master',
                        rows=[
                            (
                                row['legacy_addr'],
                            )
                        ],
                        target_fields=[
                            'legacy_address'
                        ]
                    )
            now_timestamp = datetime.now() + timedelta(hours=9)
            now_date = now_timestamp.date()
            postgres_conn = pg_hook.get_conn()
            with postgres_conn.cursor() as postgres_cursor:
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
            return True
        run_address_func()

    trigger_dag_task = TriggerDagRunOperator(
        task_id = f'lv0_to_lv1_call_trigger_{job_info["schema"]}.{job_info["table"]}',
        trigger_dag_id = f'lv1_dag_{job_info["schema"]}.{job_info["table"]}',
        trigger_run_id = None,
        execution_date = None,
        reset_dag_run = True,
        wait_for_completion = False,
        poke_interval = 60,
        allowed_states = ['success'],
        failed_states=None
    )


    branch >> [not_condition_task, address_check]
    address_check >> search_address() >> trigger_dag_task

refine_address()


