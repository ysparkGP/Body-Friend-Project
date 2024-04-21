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
import psycopg2
from pytz import timezone
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from airflow.models import Connection

connection = Connection.get_connection_from_secrets(conn_id='DATAHUB-ROOT')
db_host = connection.host
db_user = connection.login
db_pass = connection.password
db_name = connection.schema
db_port = connection.port

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

class Databases():
    def __init__(self):
        # self.db = psycopg2.connect(host=os.environ['db_host'], dbname=os.environ['db_name'],user=os.environ['db_user'],password=os.environ['db_pass'],port=os.environ['db_port'])
        self.db = psycopg2.connect(host=db_host, dbname=db_name,user=db_user,password=db_pass,port=db_port)
        self.cursor = self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()

class DDL(Databases):
    #테이블 생성
    def createTB(self,schema,table,startDate,endDate):
        table_date = startDate.replace("-","")
        sql = " CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}_{table_date}\" PARTITION OF \"{schema}\".\"{table}\" FOR VALUES FROM ('{startDate}') TO ('{endDate}');".format(schema=schema,table=table,startDate=startDate,endDate=endDate,table_date=table_date)
        try:
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            self.db.rollback()
            print(" create TB err ",e) 
            raise AirflowException()

    #테이블 드랍
    def dropTB(self,schema,table,date):
        date = date.replace(",","")
        sql = " DROP TABLE IF EXISTS \"{schema}\".\"{table}_{date}\";".format(schema=schema,table=table,date=date)
        try :
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print( "drop TB err", e)
            raise AirflowException()

    def dropTB_sql(self,schema,table):
        sql = " DROP TABLE IF EXISTS \"{schema}\".\"{table}\";".format(schema=schema,table=table)
        try :
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print( "drop TB err", e)
            raise AirflowException()

class DML(Databases):
    #execute function
    def executeFunction(self,function):
        sql = f"{function}"
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(e)
            self.db.rollback()
            raise AirflowException()
    #select
    def readDB(self,schema,table,colum,condition):
        sql = " SELECT {colum} from \"{schema}\".\"{table}\" {condition}".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            print(sql)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            result = (" read DB err",e)
            print(e)
            raise AirflowException()
        return result
    
    def insertDB(self,sql):
        try:
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(e)
            self.db.rollback()
            raise AirflowException()

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
    dag_id = "single_dag.CREATE_ADDRESS_MASTER_LOG",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['SINGLE'],
    catchup=False
)
def etl_job():
    job_info = {
        'schema' : 'public',
        'table' : 'partition_manager'
    }

    def create_table(**context):
        #클래스 호출
        db_ddl = DDL()
        db_dml = DML()
        
        #오늘 날짜
        today = datetime.now(timezone('Asia/Seoul'))
        
        #미래 1주
        plus_one_weeks = today + relativedelta(weeks=1)

        # 일요일에 실행
        # if today.weekday() == 6:
        #     print("task manager vacuum full 실행")
        #     db_dml.executeFunction(f"vacuum full gprtm.task_manager")

        #존재하는 테이블 조회
        for i in db_dml.readDB(schema='public',table='partition_manager',colum='"schema_nm","table_nm","store_d_num"',condition='where partition_tf = ''true'''):
            print(today)
            print(i[0], i[1], i[2])
            create_start_date = today - relativedelta(days=i[2])

            for table in db_dml.readDB(schema='pg_catalog',table='pg_tables',colum='"tablename"',condition="where schemaname = '" + i[0] + "' and tablename like '" + i[1] +"_%'"):
                last_underscore_index = table[0].rfind('_')

                # 마지막 '_' 뒤의 8글자 숫자를 추출합니다.
                if last_underscore_index != -1 and len(table[0]) - last_underscore_index == 9:
                    last_8_digits = table[0][last_underscore_index + 1:]
                    if last_8_digits.isdigit():
                        table_date = datetime.strptime(last_8_digits, '%Y%m%d').replace(tzinfo=timezone('Asia/Seoul'))
                        if table_date < create_start_date - relativedelta(days=1) :
                            # 테이블 삭제 SQL 실행
                            db_ddl.dropTB_sql(i[0], table[0])

            for single_date in daterange(create_start_date, plus_one_weeks):
                # 일 간격으로 날짜 잘라서 추출 (추후 일,주,월,년 단위로 조건걸어야함)
                tomorrow = single_date + relativedelta(days=1)
                today_str = single_date.strftime('%Y-%m-%d')  # YYYY-MM-DD 형식으로 변경
                tomorrow_str = tomorrow.strftime('%Y-%m-%d')  # YYYY-MM-DD 형식으로 변경
                # 테이블 생성
                db_ddl.createTB(i[0], i[1], today_str, tomorrow_str)

        now_timestamp = datetime.now() + timedelta(hours=9)
        now_date = now_timestamp.date()
        insert_log_query = f"insert into public.dag_log values('{context['dag_run'].dag_id}', '{now_date}', '{now_timestamp}')\
                                on conflict (dag_id, completion_date) DO\
                                UPDATE\
                                set dag_id = EXCLUDED.dag_id,\
                                completion_date = EXCLUDED.completion_date,\
                                completion_datetime = EXCLUDED.completion_datetime;\
                            "
        db_dml.insertDB(insert_log_query)

    etl = PythonOperator(
        task_id=f'lv0_task_{job_info["schema"]}.{job_info["table"]}',
        python_callable=create_table
    )


    # branch >> [not_condition_task, lv0_job]
    # lv0_job >> trigger_dag_task
    etl

etl_job()