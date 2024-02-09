# Body Friend (Phase 1)

## 개발 기간
* 20240102 ~ 202403(말)

## 개요
* Phase 2를 위해 Body Friend 원천 데이터를 Heroku Data Hub 에 적재하여 정재하고 Salesforce Service Cloud 에 최종 적재하는 프로젝트
* Airflow Python 으로 Workflow 를 작성하고, 스케줄하여 모니터링
    * legacy(원천) -> lv0(Heroku Data Hub) : Extract And Load
    * lv0(Heroku Data Hub) -> lv1(Heroku Data Hub) : Transform
    * lv1(Heroku Data Hub) -> lv2(Heroku Data Hub) : Transform
    * lv2(Heroku Data Hub) -> hc(Heroku Data Hub, Heroku Connect) : Load

## Airflow DAG 흐름도
<img width="951" alt="Airflow DAG 흐름도" src="https://github.com/ysparkGP/Body-Friend-Project/assets/64354998/5f32b5e4-3019-4a9d-9a83-a2e35d5122bc">

## Airflow DAG 의존성을 위해 필요 작업
* DAG 간의 의존성은 dependency_manager 테이블과 dag_log 테이블로 관리
    * Trigger Task(check condition) 가 다음 DAG 를 실행시키는데, 이 DAG 의 첫 번째 step 은 dependency_manager 테이블과 dag_log 테이블을 확인하여 현재 step 을 진행할 지 결정함
    * dependency_manager 에는 tobe_dag(다음 step DAG)에 여러 asis_dag(이전 step DAG) 가 매핑된 레코드들 존재
    * dag_log 에는 DAG 가 성공했으면 log 를 남기고 실패했으면 log 를 남기지 않음
* Legacy → L0, L0 → L1 작업 시, Python 코드 레벨에서 Log UPSERT 구문 필히 넣어야 함
    * ex)  
        ```
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
        ```
        <img width="810" alt="스크린샷 2024-02-09 17 48 51" src="https://github.com/ysparkGP/Body-Friend-Project/assets/64354998/cbea9ccb-c614-47da-b9fc-e0d045963e76">
* dependency_manager 테이블에 다음 레벨 DAG와 의존성 삽입해야 함 
    * ex) 
        ```
            insert into public.dependency_manager values('legacy_to_lv0_UNIERP5.B_MAJOR', 'lv0_dag_UNIERP5.B_MINOR');
        ```
        <img width="1449" alt="dependency_manager table" src="https://github.com/ysparkGP/Body-Friend-Project/assets/64354998/843ba60b-679d-4f88-9091-9fa7e0210db1">

## Airflow 병렬 실행 시 이슈
* 대용량 테이블 목록(160만 건 ~ 230만 건)
    * R_ORDER_CONTRACT : 컬럼 수 83개
    * R_SERVICE_RECEIPT : 컬럼 수 86개
    * R_ORDER_CONTRACT_ADD : 컬럼 수 64개
    * V_CUSTOMER : 컬럼 수 43개
* 바디프렌드 Airflow Worker Dyno의 메모리(RAM)가 현재 14GB
* 위 4개의 테이블을 한꺼번에 갖고오면 메모리 부족으로 인해 프로세스가 죽어버려 Airflow Scheduler가 얘를 fail 상태로 만듬
* 그래서 원천에서 PK로 정렬시켜 페이징 단위로 쪼개는 쿼리를 날려 결과 값을 순차적으로 가져와 L0에 넣는 방식 채택
* airflow.cfg 설정에서 동시 실행을 조절할 수 있는데 그 중 celery_worker_concurrency 인자를 설정해야함

## Airflow DAG 내 페이징 테이블 이슈
* 몇 백, 몇 천만 건에 대해서 한 번에 전부 fetch 하여 데이터를 airflow 에 가져오면 heroku worker dyno 가 메모리 부하를 못 버텨 뻗어버림
* SELECT A.* FROM( SELECT *, ROW_NUMBER() OVER(ORDER BY ORDER_NO) AS row_num FROM {schema_name}.DBO.{table_name} ) AS A WHERE 1=1 AND A.row_num BETWEEN {start_page} AND {start_page+per_page}-1
    * 현재 10만 건 이상인 테이블들은 페이징 쿼리로 테이블 pk 기준 오름차순으로 1만 건 씩 가져와 적재하고있음
    * R_ORDER_CONTRACT_ADD 테이블의 PK 는 ORDER_NO 인데 앞의 2글자만 따서 그룹핑을 해보니 '02', '82', 'HI', 'K2', 'ON' 등 어떠한 구분자가 앞에 붙여나오는 걸 확인
    * 이렇게 되면 'HI' 부분 페이징 처리가 끝나고 'ON' 부분 정렬 페이징을 가져올 때, 알파벳 순 'HI' 부분 ORDER_NO 이 삽입이 이루어지면 페이징이 밀려 중복과 손실이 동시에 발생 가능성이 있음
    * INSRT_DT 컬럼으로 페이징 정렬 기준을 바꿔야 함
