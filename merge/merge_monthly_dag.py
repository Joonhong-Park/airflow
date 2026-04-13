"""
Small File Merge Monthly DAG

매월 지정된 날짜에 실행되어 특정 달의 전체 파티션을 한 번에 병합한다.
병합은 Livy(Spark)로 월 전체를 처리하고, swap/refresh는 날짜별로 병렬 실행한다.

테이블 목록이 많은 경우를 대비해 실행일을 나눈 DAG로 분리한다.
    Day1: 매월 1일 새벽 1시 실행 (일별 DAG 2시·3시 등과 시간차 확보)
    Day2: 매월 2일 새벽 1시 실행
    ...

새 DAG 추가 시 파일 하단의 create_monthly_dag() 호출을 한 줄 추가하면 된다.
daily DAG와 코드 내용은 동일하더라도 merge_daily_dag.py를 직접 import하지 않는다.
daily/monthly DAG는 완전히 독립 실행된다.

흐름 (테이블별 table_group):
    load_refresh_flags_task
        └─ [테이블별 table_group]
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task               (max_active_tis_per_dag=5)
            ├─ get_partitions_task
            └─ swap_refresh_task[0..N-1] (날짜별 동적 확장, max_active_tis_per_dag=10)

Airflow UI:
    swap_refresh_task는 .expand()로 확장되므로 UI에서 [0], [1], ... 블록으로 표시된다.
    각 인덱스는 target_date_list의 순서(월 첫째날부터)에 대응한다.
"""

import os
import sys
import subprocess
import json
import logging
import concurrent.futures
import time
from collections import defaultdict
from datetime import timedelta

_merge_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_merge_dir, '..', 'common'))

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.exceptions import AirflowFailException, AirflowException, AirflowSkipException
from airflow.utils.weight_rule import WeightRule

from postgres_wrapper import postgres_query
from impyla_wrapper import impala_query
from livy_wrapper import create_livy_batch, get_livy_batch_id_by_name, get_livy_batch_by_id
from livy import SessionState
from alarm_wrapper import send_alarm

log = logging.getLogger(__name__)

# daily DAG와 동일한 값이지만 독립성 유지를 위해 중복 선언
# 변경 시 merge_daily_dag.py의 동일 상수도 함께 수정할 것
pg_conn_id = "pg"
base_cluster = "cluster1"              # count 조회 기준 클러스터
impala_refresh_variable = "refresh_flags"

# 도메인별 HDFS base URL. 도메인 추가 시 merge_daily_dag.py의 동일 딕셔너리도 함께 수정할 것.
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
}


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
    Postgres table_meta에서 메타정보를 조회하고, months_ago 기반으로 병합 대상 월의
    시작일/종료일 및 날짜 목록을 계산하여 반환한다.

    daily와의 차이점:
        - days_ago 대신 months_ago를 사용한다.
        - target_date 단일값 대신 start_date, end_date를 반환한다.

    Args:
        table_config (dict): Airflow Variable의 테이블 설정 항목 1개.
                             months_ago 기준으로 대상 월을 계산한다.
        data_interval_end: Airflow context에서 자동 주입되는 실행 기준 시각.

    Returns:
        dict: start_date, end_date 등 월별 태스크 공통 메타정보.
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

    # data_interval_end 기준으로 KST 변환 후 months_ago만큼 빼서 대상 월 계산
    run_date = data_interval_end.in_timezone('Asia/Seoul')  # TODO(Airflow 3): in_timezone() → in_tz() (pendulum 3)
    months_ago = table_config.get('months_ago')
    if months_ago is None:
        raise AirflowFailException(f"table_config에 'months_ago' 키가 없습니다. (table_id={table_id})")

    target_month = run_date.subtract(months=months_ago)
    start_date = target_month.start_of('month').to_date_string()   # 예: "2024-01-01"
    end_date = target_month.end_of('month').to_date_string()       # 예: "2024-01-31"

    metadata = {
        'table_id': table_id,
        'db_name': db_name,
        'table_name': table_name,
        'partition_cols': partition_cols,   # 최대 2개, 첫번째는 항상 날짜 컬럼
        'save_path': save_full_path,
        'temp_path': temp_full_path,
        'backup_path': backup_full_path,
        'start_date': start_date,
        'end_date': end_date,
        'sort_columns': table_config.get('sort_columns'),
        'compression': table_config.get('compression', 'snappy'),
    }

    log.info(
        f"metadata 조회 완료 | table: {db_name}.{table_name} | "
        f"대상 월: {start_date} ~ {end_date} | "
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
    병합 전 대상 월 전체의 날짜별 row count를 GROUP BY로 한 번에 조회하여
    merge_log에 단일 bulk insert(upsert)한다.

    daily와의 차이점:
        - 날짜 단일 조회 대신 start_date ~ end_date 범위를 GROUP BY로 조회한다.
        - 유효한 날짜 전체를 VALUES (...), (...) 형태로 단일 쿼리로 insert한다.

    케이스 구분:
        - result_df is None  → Impala 연결 오류. 재시도 필요 → AirflowFailException
        - result_df is empty → 대상 월 전체에 데이터 없음 → AirflowSkipException (이후 태스크 전체 skip)
        - count == 0인 날짜  → 해당 날짜만 insert 제외하고 경고 로그. 다른 날짜는 계속 처리
        - 유효 행 전체가 0   → 모든 날짜 count = 0. 병합 불필요 → AirflowSkipException
    """
    db_name = metadata['db_name']
    table_name = metadata['table_name']
    table_id = metadata['table_id']
    part1_column = metadata['partition_cols'][0]
    start_date = metadata['start_date']
    end_date = metadata['end_date']

    # 대상 월 전체를 날짜별로 GROUP BY하여 한 번에 조회
    count_query = f"""
        select {part1_column}, count(*)
        from {db_name}.{table_name}_t
        where {part1_column} between '{start_date}' and '{end_date}'
        group by {part1_column}
    """
    result_df = impala_query(count_query, base_cluster, True)

    # None: Impala 연결 실패 또는 쿼리 실행 오류
    if result_df is None:
        raise AirflowFailException(
            f"Impala 연결 오류로 count 조회 실패: {db_name}.{table_name}_t / {start_date} ~ {end_date}"
        )

    # empty: GROUP BY 결과가 없음 = 해당 월에 데이터 자체가 없음
    if result_df.empty:
        raise AirflowSkipException(
            f"대상 월 데이터 없음. 병합 대상 없으므로 skip 처리: "
            f"{db_name}.{table_name}_t / {start_date} ~ {end_date}"
        )

    log.info(f"before merge count 조회 완료 | {table_name} | {start_date} ~ {end_date} | 총 {len(result_df)}일")

    # count == 0인 날짜는 insert 제외 (경고만 기록)
    valid_rows = []
    for _, row in result_df.iterrows():
        date_val = str(row[0])
        count = int(row[1])
        if count == 0:
            log.warning(f"count = 0. merge_log insert 제외: {table_name} / {date_val}")
        else:
            log.info(f"before merge count | {table_name} / {date_val}: {count}")
            valid_rows.append((date_val, count))

    # 유효한 날짜가 하나도 없으면 skip (전체 월 count = 0)
    if not valid_rows:
        raise AirflowSkipException(
            f"모든 날짜의 count = 0. 병합 대상 없으므로 skip 처리: "
            f"{db_name}.{table_name}_t / {start_date} ~ {end_date}"
        )

    # 유효한 날짜 전체를 단일 bulk insert
    values_clause = ", ".join(
        f"('{table_id}', {count}, 0, now(), now(), '{date_val}')"
        for date_val, count in valid_rows
    )
    insert_query = f"""
        insert into merge_log (table_id, before_count, after_count, create_time, update_time, target_date)
        values {values_clause}
        on conflict (table_id, target_date)
        do update set before_count = excluded.before_count, update_time = now();
    """
    try:
        postgres_query(pg_conn_id, insert_query, commit=True)
    except Exception as e:
        raise AirflowFailException(
            f"merge_log before_count bulk insert 실패 (table_id={table_id}, {start_date} ~ {end_date}): {e}"
        )

    log.info(f"merge_log before_count 기록 완료 | table_id: {table_id} | {start_date} ~ {end_date} | {len(valid_rows)}일")


@task(retries=3, retry_delay=timedelta(minutes=3), max_active_tis_per_dag=5)
def livy_task(metadata):
    """
    Livy를 통해 Spark 소파일 병합 작업을 제출하고 완료까지 대기한다.

    daily와의 차이점:
        - batch_name에 'monthly'가 포함된다.
        - --start-date, --end-date에 월 전체 범위(start_date ~ end_date)를 전달한다.
          (daily는 target_date를 start/end 모두에 동일하게 전달한다.)

    병합 결과는 temp 경로에 날짜별 파티션으로 저장되며,
    이후 swap_refresh_task가 날짜별로 병렬 실행되어 base 경로와 교체한다.

    max_active_tis_per_dag=5:
        한 DAG run 내에서 livy_task 인스턴스를 최대 5개까지만 동시에 실행한다.
        Livy는 테이블당 가장 오래 실행되는 태스크이므로, 이 설정이 사실상 동시 처리 테이블 수를 제어한다.
        (Airflow 2.2+ 지원)
    """
    table_name = metadata['table_name']
    sort_columns = metadata.get('sort_columns')
    batch_name = f"Small-file-merge-monthly-{table_name}"

    spark_args = [
        "--save-path", metadata['save_path'],
        "--temp-path", metadata['temp_path'],
        "--start-date", metadata['start_date'],
        "--end-date", metadata['end_date'],
        "--partition-cols", ','.join(metadata['partition_cols']),
        "--compression", metadata['compression'],
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
    Spark 작업이 temp 경로에 생성한 manifest 파일을 읽어 날짜별로 그룹화하여 반환한다.
    manifest는 1줄 1 JSON 형식이며, 파티션 컬럼 순서가 보장된 상태로 저장된다.

    Returns:
        list[dict]: 날짜별 그룹화된 파티션 목록.
                    예: [{"target_date": "2025-01-01", "partitions": [{"dt": "2025-01-01", "hour": "00"}, ...]}, ...]
                    swap_refresh_task.expand_kwargs()로 날짜 수만큼 동적 확장된다.
                    각 task 인스턴스는 해당 날짜의 partitions만 수신한다.
    """
    part1_column = metadata['partition_cols'][0]
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

    groups = defaultdict(list)
    for p in partition_list:
        groups[str(p[part1_column])].append(p)

    date_groups = [
        {"target_date": target_date, "partitions": partitions}
        for target_date, partitions in groups.items()
    ]

    log.info(f"manifest 파티션 {len(partition_list)}개 → {len(date_groups)}개 날짜 그룹으로 로드 완료")

    return date_groups


@task(max_active_tis_per_dag=10)
def swap_refresh_task(cluster_list, metadata, target_date, partitions):
    """
    특정 날짜(target_date)의 HDFS swap 후 Impala partition refresh 및 after_count 검증을 수행한다.

    daily와의 차이점:
        - target_date, partitions를 명시적 파라미터로 수신한다.
          → get_partitions_task가 날짜별로 그룹화하여 반환한 date_groups로 expand_kwargs() 확장됨.
          → Airflow UI에서 [0], [1], ... 형태로 날짜 수만큼 개별 태스크 블록으로 표시됨.
        - partitions는 이미 해당 날짜 것만 포함되어 있어 별도 필터링이 불필요하다.

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

    max_active_tis_per_dag=10:
        한 DAG run 내에서 swap_refresh_task 인스턴스를 최대 10개까지만 동시에 실행한다.
        .expand()로 날짜별 동적 확장되므로 테이블 수 × 월 일수만큼 인스턴스가 생성될 수 있어
        반드시 동시 실행 수를 제한해야 한다. (Airflow 2.2+ 지원)
    """
    db_name = metadata['db_name']
    table_name = metadata['table_name']
    table_id = metadata['table_id']
    partition_cols = metadata['partition_cols']   # 최대 2개, 첫번째는 항상 날짜
    part1_column = partition_cols[0]
    base_path = metadata['save_path']
    temp_path = metadata['temp_path']
    backup_path = metadata['backup_path']

    date_partition_list = partitions
    if not date_partition_list:
        raise AirflowFailException(
            f"manifest에 target_date={target_date}에 해당하는 파티션이 없습니다. "
            f"Livy 결과를 확인하세요: {table_name}"
        )

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
        단일 클러스터에 대해 target_date의 파티션 refresh 후 after_count를 반환한다.
        ThreadPoolExecutor에 의해 클러스터별로 병렬 호출된다.
        """
        max_retries = 3

        # manifest 파티션 순서(첫번째=날짜)가 보장된 상태로 저장되어 있어 순서대로 refresh
        for partition_dict in date_partition_list:
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
        raise AirflowFailException(
            f"merge_log after_count 업데이트 실패 (table_id={table_id}, target_date={target_date}): {e}"
        )

    log.info(
        f"merge_log after_count 업데이트 완료 | table_id: {table_id} | "
        f"target_date: {target_date} | after_count: {main_count}"
    )


@task_group
def table_group(table_config, refresh_flags):
    """
    테이블 1개에 대한 월별 병합 전체 흐름을 묶은 task_group.

    테이블별로 table_group.override(group_id=...)로 호출된다.
    swap_refresh_task는 get_partitions_task의 날짜별 그룹 수만큼 동적으로 확장(.expand_kwargs())된다.
    """
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    date_groups = get_partitions_task(metadata)

    # swap_refresh_task는 date_groups의 날짜 수만큼 동적으로 병렬 확장된다.
    # date_groups: [{"target_date": "2025-01-01", "partitions": [...]}, ...]
    swap_refresh = swap_refresh_task.partial(
        cluster_list=cluster_list,
        metadata=metadata,
    ).expand_kwargs(date_groups)

    # impala_health_check 완료 후 count_before 실행 (cluster_list 의존)
    # 이후 livy → get_partitions → swap_refresh 순서로 직렬 실행
    cluster_list >> log_before >> livy_job >> date_groups >> swap_refresh


def create_monthly_dag(dag_id, config_variable, schedule):
    """
    월별 병합 DAG를 생성하는 팩토리 함수.

    Args:
        dag_id (str): Airflow DAG ID. 예: 'Small-File-Merge-Monthly-Day1'
        config_variable (str): 테이블 설정을 담은 Airflow Variable 이름.
                               예: 'monthly_merge_table_config_day1'
        schedule (str): cron 표현식. 예: '0 1 1 * *' (매월 1일 01:00)

    Returns:
        DAG 인스턴스 (Airflow가 전역 스코프에서 자동 인식)

    Variable 형식 예시:
        [
            {"table_id": 1, "months_ago": 1, "sort_columns": "col1,col2", "compression": "snappy"},
            {"table_id": 2, "months_ago": 1, "compression": "zstd"}
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
        max_active_tasks=10,
    )
    def monthly_merge_dag():
        refresh_flags_dict = load_refresh_flags_task()
        table_config_list = Variable.get(config_variable, deserialize_json=True, default_var=[])

        # Variable이 비어있으면 태스크 없이 DAG만 생성됨 (정상 동작)
        for table_config in table_config_list:
            table_id = table_config['table_id']
            table_group.override(group_id=f"table_{table_id}")(table_config, refresh_flags_dict)

    return monthly_merge_dag()


# ─────────────────────────────────────────────────────────────────────────────
# DAG 목록
# 테이블이 많아 실행일을 분산할 경우 아래에 한 줄씩 추가한다.
# 동일 테이블이 여러 dayN Variable에 중복 등록되지 않도록 운영 관리 필요.
# ─────────────────────────────────────────────────────────────────────────────
create_monthly_dag('Small-File-Merge-Monthly-Day1', 'monthly_merge_table_config_day1', '0 1 1 * *')
create_monthly_dag('Small-File-Merge-Monthly-Day2', 'monthly_merge_table_config_day2', '0 1 2 * *')
# create_monthly_dag('Small-File-Merge-Monthly-Day3', 'monthly_merge_table_config_day3', '0 1 3 * *')
