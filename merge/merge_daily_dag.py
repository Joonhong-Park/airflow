"""
Small File Merge Daily DAG

Impala 테이블의 소파일을 날짜 파티션 단위로 병합한다.
Spark 작업(Livy)으로 병합 결과를 temp 경로에 생성한 뒤,
HDFS swap을 통해 base 경로와 교체하고 Impala partition refresh를 수행한다.

실행 시각별로 DAG를 분리하여 테이블 목록을 분산 처리한다.
    2am: Small-File-Merge-Daily-2am (daily_table_2am_config)
    3am: Small-File-Merge-Daily-3am (daily_table_3am_config)
    ...

새 DAG 추가 시 파일 하단의 create_daily_dag() 호출을 한 줄 추가하면 된다.

흐름:
    load_refresh_flags_task
        └─ [테이블별 table_group]
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ get_partitions_task
            └─ swap_refresh_task
"""

import os
import sys
import subprocess
import json
import logging
import concurrent.futures
import time
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'common'))

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.exceptions import AirflowFailException, AirflowException, AirflowSkipException
from airflow.utils.weight_rule import WeightRule
from livy import SessionState

from postgres_wrapper import postgres_query
from impyla_wrapper import impala_query
from livy_wrapper import create_livy_batch, get_livy_batch_id_by_name, get_livy_batch_by_id
from alarm_wrapper import send_alarm

log = logging.getLogger(__name__)

def dag_failure_alarm(context):
    """
    태스크 실패 시 Airflow가 자동으로 호출하는 콜백 함수.
    실패 정보를 메시지로 구성하여 send_alarm으로 전달한다.
    default_args의 on_failure_callback에 등록되어 모든 태스크에 적용된다.

    Args:
        context (dict): Airflow가 주입하는 실행 컨텍스트.
                        dag, task_instance, exception, execution_date 등 포함.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']  # TODO(Airflow 3): logical_date로 변경
    exception = context.get('exception')
    log_url = context['task_instance'].log_url

    message = (
        f"[DAG 실패 알람]\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"실행일시: {execution_date}\n"
        f"오류: {exception}\n"
        f"로그: {log_url}"
    )

    send_alarm(context, message=message)


pg_conn_id = "pg"
base_cluster = "cluster1"                          # count 조회 기준 클러스터
impala_refresh_variable = "refresh_flags"

# 도메인별 HDFS base URL. 도메인 추가 시 여기에만 추가하면 된다.
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
}


@task(retries=3, retry_delay=timedelta(seconds=10))
def load_refresh_flags_task():
    """
    Airflow Variable에서 클러스터별 refresh 실행 여부를 로드한다.

    Returns:
        dict: {cluster_name: flag} 형태. flag=True인 클러스터만 이후 태스크에서 사용된다.
              flag 필터링은 impala_health_check_task에서 수행한다.

    Variable 형식 예시:
        [{"cluster": "cluster1", "flag": true}, {"cluster": "cluster2", "flag": false}]
    """
    try:
        refresh_flags = Variable.get(impala_refresh_variable, deserialize_json=True)
    except Exception as e:
        raise AirflowFailException(f"Variable '{impala_refresh_variable}' 로드 실패: {e}")

    result = {item['cluster']: item['flag'] for item in refresh_flags}

    if not result:
        raise AirflowFailException(f"'{impala_refresh_variable}' Variable이 비어 있습니다.")

    active = [c for c, f in result.items() if f]
    inactive = [c for c, f in result.items() if not f]
    log.info(f"refresh 대상 클러스터: {active}")
    if inactive:
        log.info(f"refresh 비활성 클러스터 (skip): {inactive}")

    return result


@task
def get_metadata_task(table_config, data_interval_end=None):
    """
    Postgres table_meta 테이블에서 병합 대상 테이블의 메타정보를 조회하고,
    HDFS 경로와 target_date를 계산하여 반환한다.

    Args:
        table_config (dict): Airflow Variable의 테이블 설정 항목 1개.
                             days_ago 기준으로 target_date를 계산한다.
        data_interval_end: Airflow context에서 자동 주입되는 실행 기준 시각.

    Returns:
        dict: 이후 태스크에서 공통으로 사용하는 메타정보.
              save_path, temp_path, backup_path, target_date, partition_cols 등 포함.

    HDFS 경로 구조:
        base:   {domain_path}{save_path}/{table_name}   (save_path는 '/'로 시작)
        temp:   hdfs://temp_path.com/impala/merge/temp/{table_name}
        backup: hdfs://temp_path.com/impala/merge/backup/{table_name}
    """
    table_id = table_config['table_id']

    query = f"""select db, table_name, save_path, partitions, domain
    from table_meta where table_id = {table_id}"""

    result = postgres_query(pg_conn_id, query, fetch_result=True)

    if not result:
        raise AirflowFailException(f"table_meta에 table_id={table_id} 정보가 없습니다.")

    row = result[0]
    db_name = row[0].lower()
    table_name = row[1].lower()
    save_path = row[2]
    partition_cols = [p.strip() for p in row[3].split(',')]
    domain = row[4]

    domain_path = DOMAIN_PATH_MAP.get(domain)
    if not domain_path:
        raise AirflowFailException(f'not support domain: {domain}')

    save_full_path = f"{domain_path}{save_path}/{table_name}"
    temp_full_path = f"hdfs://temp_path.com/impala/merge/temp/{table_name}"
    backup_full_path = f"hdfs://temp_path.com/impala/merge/backup/{table_name}"

    # data_interval_end 기준으로 KST 변환 후 days_ago만큼 빼서 target_date 계산
    run_date = data_interval_end.in_timezone('Asia/Seoul')  # TODO(Airflow 3): in_timezone() → in_tz() (pendulum 3)
    days_ago = table_config.get('days_ago')
    if days_ago is None:
        raise AirflowFailException(f"table_config에 'days_ago' 키가 없습니다. (table_id={table_id})")
    target_date = run_date.subtract(days=days_ago).to_date_string()

    metadata = {
        'table_id': table_id,
        'db_name': db_name,
        'table_name': table_name,
        'partition_cols': partition_cols,   # 최대 2개, 첫번째는 항상 날짜 컬럼
        'save_path': save_full_path,
        'temp_path': temp_full_path,
        'backup_path': backup_full_path,
        'target_date': target_date,
        'sort_columns': table_config.get('sort_columns'),
        'compression': table_config.get('compression', 'snappy')
    }

    log.info(
        f"metadata 조회 완료 | table: {db_name}.{table_name} | target_date: {target_date} | "
        f"partition_cols: {partition_cols} | save_path: {save_full_path}"
    )

    return metadata


@task(retries=3, retry_delay=timedelta(seconds=10))
def impala_health_check_task(metadata, refresh_flags):
    """
    refresh_flags에서 flag=True인 클러스터에 대해 Impala 연결 및 테이블 존재 여부를 확인한다.
    하나의 클러스터라도 실패하면 전체 태스크를 실패 처리한다.

    Args:
        metadata (dict): get_metadata_task 반환값.
        refresh_flags (dict): {cluster_name: flag} 형태.

    Returns:
        list: health check를 통과한 클러스터 이름 목록.
              이후 swap_refresh_task에서 refresh 대상 클러스터로 사용된다.
    """
    db_name = metadata['db_name']
    table_name = metadata['table_name']

    # Impala에서는 테이블명 뒤에 '_t' suffix를 붙여 관리한다.
    query = f"show tables in {db_name} like '{table_name}_t'"

    passed_clusters = []
    failed_clusters = {}

    for cluster, is_active in refresh_flags.items():
        if not is_active:
            continue
        result_df = impala_query(query, cluster, True)
        if result_df is None:
            failed_clusters[cluster] = "impala connect failed"
        elif result_df.empty:
            failed_clusters[cluster] = "table not exist"
        else:
            passed_clusters.append(cluster)

    if failed_clusters:
        log.error(f"health check 실패 클러스터: {failed_clusters}")
        raise AirflowFailException("health check failed")

    if not passed_clusters:
        raise AirflowFailException("active 클러스터가 없습니다. refresh_flags Variable을 확인하세요.")

    log.info(f"health check 통과 클러스터: {passed_clusters}")
    return passed_clusters


@task(retries=3, retry_delay=timedelta(seconds=10))
def log_before_count_task(metadata):
    """
    병합 전 target_date 파티션의 row count를 조회하여 merge_log에 기록한다.
    count가 0이면 병합 대상 데이터가 없는 것으로 판단하여 이후 태스크를 skip 처리한다.

    merge_log upsert 시 before_count만 갱신하며 after_count는 건드리지 않는다.
    retry/backfill 시 after_count가 초기화되는 것을 방지하기 위함이다.
    """
    db_name = metadata['db_name']
    table_name = metadata['table_name']
    table_id = metadata['table_id']
    part1_column = metadata['partition_cols'][0]
    target_date = metadata['target_date']

    count_query = f"select count(*) from {db_name}.{table_name}_t where {part1_column} = '{target_date}'"
    result_df = impala_query(count_query, base_cluster, True)

    if result_df is None:
        raise AirflowFailException(f"count 조회 실패: {db_name}.{table_name}_t / {target_date}")

    count = int(result_df.iloc[0, 0])

    if count == 0:
        raise AirflowSkipException(f"count가 0입니다. 병합 대상 데이터 없음으로 skip 처리: {db_name}.{table_name}_t / {target_date}")

    log.info(f"before merge count for {table_name} / {target_date}: {count}")

    insert_query = f"""
        insert into merge_log (table_id, before_count, after_count, create_time, update_time, target_date)
        values('{table_id}',{count},0,now(),now(),'{target_date}') on conflict (table_id, target_date)
        do update set before_count = excluded.before_count, update_time = now();
        """

    try:
        postgres_query(pg_conn_id, insert_query, commit=True)
    except Exception as e:
        raise AirflowFailException(f"merge_log before_count 기록 실패 (table_id={table_id}, target_date={target_date}): {e}")

    log.info(f"merge_log before_count 기록 완료 | table_id: {table_id} | target_date: {target_date} | before_count: {count}")


@task(retries=3, retry_delay=timedelta(minutes=3))
def livy_task(metadata):
    """
    Livy를 통해 Spark 소파일 병합 작업을 제출하고 완료까지 대기한다.

    병합 결과는 temp 경로에 저장되며, 이후 swap_refresh_task에서 base 경로와 교체된다.
    create_livy_batch 호출 직후 5초 대기는 Livy 서버의 배치 등록 지연에 대응하기 위함이다.

    retry 시 동명 배치(Small-file-merge-daily-{table_name})가 Livy에 잔존할 수 있으므로
    create_livy_batch wrapper의 중복 처리 방식을 확인해야 한다.
    """
    table_name = metadata['table_name']
    sort_columns = metadata.get('sort_columns')
    batch_name = f"Small-file-merge-daily-{table_name}"
    spark_args = [
        "--save-path", metadata['save_path'],
        "--temp-path", metadata['temp_path'],
        "--start-date", metadata['target_date'],
        "--end-date", metadata['target_date'],
        "--partition-cols", ','.join(metadata['partition_cols']),
        "--compression", metadata['compression']
    ]

    if sort_columns:
        spark_args.extend(['--sort-columns', sort_columns])

    log.info(f"Livy 배치 제출 시작 | batch_name: {batch_name} | args: {spark_args}")

    livy_status = create_livy_batch(
        name=batch_name,
        file="hdfs://path1/impala/merge/script/merge.py",
        cluster="livy_cluster",
        args=spark_args,
    )

    if not livy_status:
        raise AirflowFailException(f"Livy 배치 제출 실패 또는 RUNNING 상태 미달: {batch_name}")

    # Livy 서버에 배치가 등록되기까지 약간의 지연이 있어 대기 후 조회
    time.sleep(5)
    try:
        (livy_batch_id, livy_state) = get_livy_batch_id_by_name(batch_name, "livy_cluster")
    except Exception as e:
        raise AirflowFailException(f"Livy API 오류로 batch ID 조회 실패: {batch_name}: {e}")

    # get_livy_batch_id_by_name은 배치를 찾지 못하면 (-1, None)을 반환한다.
    if livy_batch_id == -1:
        raise AirflowFailException(f"Livy batch를 찾을 수 없습니다: {batch_name}")

    log.info(f"livy batch id: {livy_batch_id}, initial state: {livy_state}")

    # get_livy_batch_by_id는 API 오류 시 None을 반환한다.
    livy_batch_obj = get_livy_batch_by_id(livy_batch_id, "livy_cluster")
    if livy_batch_obj is None:
        raise AirflowFailException(f"Livy batch 객체 조회 실패 (id={livy_batch_id})")

    livy_batch_obj.wait()
    final_state = livy_batch_obj.state
    log.info(f"livy batch final state: {final_state}")

    if final_state != SessionState.SUCCESS:
        raise AirflowFailException(f"livy batch failed with state: {final_state}")


@task
def get_partitions_task(metadata):
    """
    Spark 작업이 temp 경로에 생성한 manifest 파일을 읽어 파티션 목록을 반환한다.
    manifest는 1줄 1 JSON 형식이며, 파티션 컬럼 순서가 보장된 상태로 저장된다.

    Returns:
        list[dict]: 파티션 정보 목록. 예: [{"dt": "2024-01-01", "hour": "00"}, ...]
                    swap_refresh_task에서 Impala refresh 쿼리 생성에 사용된다.
    """
    manifest_path = f"{metadata['temp_path']}/manifest/*.json"

    try:
        manifest_json = subprocess.run(
            ["hdfs", "dfs", "-cat", manifest_path],
            capture_output=True,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        raise AirflowFailException(f"manifest HDFS 읽기 실패: {e.stderr}")
    except Exception as e:
        raise AirflowFailException(f"manifest 읽기 실패: {e}")

    try:
        partition_list = [json.loads(line) for line in manifest_json.stdout.splitlines() if line.strip()]
    except Exception as e:
        raise AirflowFailException(f"manifest JSON 파싱 실패: {e}")

    if not partition_list:
        raise AirflowFailException(f"manifest가 비어있습니다. Spark 작업 결과를 확인하세요: {manifest_path}")

    log.info(f"manifest 파티션 {len(partition_list)}개 로드 완료: {partition_list}")

    return partition_list


@task
def swap_refresh_task(cluster_list, partition_list, metadata):
    """
    HDFS swap 후 Impala partition refresh 및 after_count 검증을 수행한다.

    [HDFS Swap 순서]
        1. base/{part1}={date} → backup/{part1}={date}  (기존 데이터 백업)
        2. temp/{part1}={date} → base/{part1}={date}    (병합 결과 반영)
        실패 시: backup/{part1}={date} → base/{part1}={date} 롤백

    [retry 안전성]
        retry 진입 시 temp 경로 존재 여부를 먼저 확인한다.
        temp가 없으면 이미 swap이 완료된 것으로 판단하고 refresh 단계로 바로 진행한다.
        롤백까지 실패한 경우에는 수동 복구가 필요하다.

    [Impala Refresh]
        health check를 통과한 클러스터들에 대해 ThreadPoolExecutor로 병렬 refresh.
        모든 클러스터의 after_count가 일치해야 정상으로 판단한다.
    """
    db_name = metadata['db_name']
    table_name = metadata['table_name']
    table_id = metadata['table_id']
    partition_cols = metadata['partition_cols']   # 최대 2개, 첫번째는 항상 날짜
    part1_column = partition_cols[0]
    target_date = metadata['target_date']
    base_path = metadata['save_path']
    temp_path = metadata['temp_path']
    backup_path = metadata['backup_path']

    temp_target_path = f"{temp_path}/{part1_column}={target_date}"
    base_target_path = f"{base_path}/{part1_column}={target_date}"
    backup_target_path = f"{backup_path}/{part1_column}={target_date}"

    # retry 시 swap이 이미 완료된 경우 refresh 단계로 바로 진행
    temp_exists = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", temp_target_path],
        capture_output=True
    ).returncode == 0

    if not temp_exists:
        log.info(f"temp 경로가 존재하지 않음, 이미 swap 완료된 것으로 판단하고 refresh로 진행: {temp_target_path}")
    else:
        try:
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", backup_path], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise AirflowFailException(f"backup 디렉토리 생성 실패: {e.stderr}")

        try:
            subprocess.run(["hdfs", "dfs", "-mv", base_target_path, backup_path], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise AirflowFailException(f"base→backup 이동 실패: {e.stderr}")

        try:
            subprocess.run(["hdfs", "dfs", "-mv", temp_target_path, base_path], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            log.error(f"temp→base 이동 실패, 롤백 시도: {e.stderr}")
            try:
                subprocess.run(["hdfs", "dfs", "-mv", backup_target_path, base_path], check=True, capture_output=True, text=True)
                log.info(f"롤백 완료: {backup_target_path} -> {base_path}")
            except subprocess.CalledProcessError as rollback_e:
                log.warning(
                    f"롤백 실패 - 수동 복구 필요. 아래 명령어를 순서대로 실행하세요:\n"
                    f"  1. hdfs dfs -mv {backup_target_path} {base_path}\n"
                    f"  2. hdfs dfs -rm -r {temp_target_path}  # 병합 결과 정리 (선택)\n"
                    f"롤백 실패 원인: {rollback_e.stderr}"
                )
            raise AirflowFailException(f"HDFS swap 실패: {e.stderr}")
        log.info(f"hdfs swap complete: {base_target_path} -> {backup_path}, {temp_target_path} -> {base_path}")

    def _refresh_partition(cluster):
        """
        단일 클러스터에 대해 파티션 refresh 후 after_count를 반환한다.
        ThreadPoolExecutor에 의해 클러스터별로 병렬 호출된다.
        """
        max_retries = 3

        # manifest 파티션 순서(첫번째=날짜)가 보장된 상태로 저장되어 있어 순서대로 refresh
        for partition_dict in partition_list:
            spec_items = [f"{key}='{value}'" for key, value in partition_dict.items()]
            partition_spec = ",".join(spec_items)
            for attempt in range(1, max_retries + 1):
                try:
                    refresh_query = f"refresh {db_name}.{table_name}_t partition ({partition_spec})"
                    impala_query(refresh_query, cluster)
                    log.info(f"[{cluster}] partition refresh success: {partition_spec}")
                    break
                except Exception as e:
                    log.warning(f"[{cluster}] partition refresh attempt {attempt}/{max_retries} failed: {e}")
                    time.sleep(5)
            else:
                raise AirflowFailException(f"[{cluster}] partition refresh failed: {partition_spec}")

        # refresh 후 count 조회로 데이터 반영 확인
        for attempt in range(1, max_retries + 1):
            try:
                count_query = f"select count(*) from {db_name}.{table_name}_t where {part1_column} = '{target_date}'"
                result_df = impala_query(count_query, cluster, True)
                count = int(result_df.iloc[0, 0])
                return {cluster: count}
            except Exception as e:
                log.warning(f"[{cluster}] count query attempt {attempt}/{max_retries} failed: {e}")
                time.sleep(5)
        else:
            raise AirflowFailException(f"[{cluster}] count query failed after {max_retries} retries")

    # 클러스터별 병렬 refresh 실행
    cluster_count_dict = {}
    failed_clusters = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(cluster_list)) as executor:
        future_map = {executor.submit(_refresh_partition, cluster): cluster for cluster in cluster_list}
        for future in concurrent.futures.as_completed(future_map):
            cluster = future_map[future]
            try:
                cluster_count_dict.update(future.result())
            except Exception as e:
                log.error(f"[{cluster}] refresh/count 실패: {e}")
                failed_clusters.append(cluster)

    if failed_clusters:
        raise AirflowFailException(f"다음 클러스터 refresh 실패: {failed_clusters}")

    log.info(f"cluster count results: {cluster_count_dict}")

    count_list = list(cluster_count_dict.values())

    if not count_list:
        raise AirflowFailException("클러스터 count 결과가 없습니다.")

    main_count = count_list[0]

    # 클러스터 간 count 불일치는 refresh 누락 또는 복제 지연을 의미한다.
    # AirflowException(retry 가능)으로 처리하여 일시적 지연 상황에 대응한다.
    if not all(count == main_count for count in count_list):
        raise AirflowException("클러스터간 count 불일치")

    update_query = f"""
            update merge_log set after_count = {main_count}, update_time = now()
            where table_id = '{table_id}' and target_date = '{target_date}'
            """

    try:
        postgres_query(pg_conn_id, update_query, commit=True)
    except Exception as e:
        raise AirflowFailException(f"merge_log after_count 업데이트 실패 (table_id={table_id}, target_date={target_date}): {e}")

    log.info(f"merge_log after_count 업데이트 완료 | table_id: {table_id} | target_date: {target_date} | after_count: {main_count}")


@task_group
def table_group(table_config, refresh_flags):
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    partition_list = get_partitions_task(metadata)
    swap_refresh = swap_refresh_task(cluster_list, partition_list, metadata)

    # impala_health_check_task 완료 후 count_before 실행 (cluster_list 의존)
    # 이후 livy → get_partitions → swap_refresh 순서로 직렬 실행
    cluster_list >> log_before >> livy_job >> partition_list >> swap_refresh


def create_daily_dag(dag_id, config_variable, schedule):
    """
    일별 병합 DAG를 생성하는 팩토리 함수.

    Args:
        dag_id (str): Airflow DAG ID. 예: 'Small-File-Merge-Daily-2am'
        config_variable (str): 테이블 설정을 담은 Airflow Variable 이름.
                               예: 'daily_table_2am_config'
        schedule (str): cron 표현식 또는 preset. 예: '0 2 * * *'

    Returns:
        DAG 인스턴스 (Airflow가 전역 스코프에서 자동 인식)

    Variable 형식 예시:
        [
            {"table_id": 1, "days_ago": 1, "sort_columns": "col1,col2", "compression": "snappy"},
            {"table_id": 2, "days_ago": 1, "compression": "zstd"}
        ]
    """
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        default_args={
            'depends_on_past': False,
            'weight_rule': WeightRule.UPSTREAM,
            'on_failure_callback': dag_failure_alarm,
        },
        max_active_runs=1,      # 동일 DAG 중복 실행 방지
        max_active_tasks=10
    )
    def daily_merge_dag():
        refresh_flags_dict = load_refresh_flags_task()
        table_config_list = Variable.get(config_variable, deserialize_json=True, default_var=[])

        # Variable이 비어있으면 태스크 없이 DAG만 생성됨 (정상 동작)
        for table_config in table_config_list:
            table_id = table_config['table_id']
            table_group.override(group_id=f"table_{table_id}")(table_config, refresh_flags_dict)

    return daily_merge_dag()


# ─────────────────────────────────────────────────────────────────────────────
# DAG 목록
# 테이블이 많아 실행 시각을 분산할 경우 아래에 한 줄씩 추가한다.
# 동일 테이블이 여러 Variable에 중복 등록되지 않도록 운영 관리 필요.
# ─────────────────────────────────────────────────────────────────────────────
create_daily_dag('Small-File-Merge-Daily-2am', 'daily_table_2am_config', '0 2 * * *')
create_daily_dag('Small-File-Merge-Daily-3am', 'daily_table_3am_config', '0 3 * * *')
