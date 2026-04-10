"""
월별 소파일 병합 DAG 전용 태스크 모음

merge_daily_dag.py의 태스크와 동일한 로직은 주석으로 표기.
이 모듈의 상수(pg_conn_id, DOMAIN_PATH_MAP 등)는 merge_daily_dag에서 import하지 않고
의도적으로 중복 선언한다. merge_daily_dag를 import하면 daily DAG가 Airflow에 재등록되는
부작용이 발생할 수 있기 때문이다.
"""

import os
import sys
import subprocess
import json
import logging
import concurrent.futures
import time
from datetime import timedelta

import pendulum

# tasks/ 디렉토리 기준 두 단계 상위의 common/ 디렉토리를 참조한다.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'common'))

from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowException, AirflowSkipException

from postgres_wrapper import postgres_query
from impyla_wrapper import impala_query
from livy_wrapper import create_livy_batch, get_livy_batch_id_by_name, get_livy_batch_by_id
from livy import SessionState

log = logging.getLogger(__name__)

# merge_daily_dag와 동일한 값이지만 import 부작용을 피하기 위해 중복 선언
pg_conn_id = "pg"
base_cluster = "cluster1"   # count 조회 기준 클러스터

# 도메인별 HDFS base URL. 도메인 추가 시 merge_daily_dag.py의 동일 딕셔너리도 함께 수정할 것.
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
}


@task
def get_metadata_task(table_config, data_interval_end=None):
    """
    Postgres table_meta에서 메타정보를 조회하고, months_ago 기반으로 병합 대상 월의
    시작일/종료일 및 날짜 목록을 계산하여 반환한다.

    daily와의 차이점:
        - days_ago 대신 months_ago를 사용한다.
        - target_date 단일값 대신 start_date, end_date, target_date_list를 반환한다.
        - target_date_list는 swap_refresh_task를 날짜별로 동적 확장(.expand())하는 데 사용된다.

    Args:
        table_config (dict): Airflow Variable의 테이블 설정 항목 1개.
                             months_ago 기준으로 대상 월을 계산한다.
        data_interval_end: Airflow context에서 자동 주입되는 실행 기준 시각.

    Returns:
        dict: start_date, end_date, target_date_list 등 월별 태스크 공통 메타정보.
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
    run_date = data_interval_end.in_timezone('Asia/Seoul')
    months_ago = table_config.get('months_ago')
    if months_ago is None:
        raise AirflowFailException(f"table_config에 'months_ago' 키가 없습니다. (table_id={table_id})")

    target_month = run_date.subtract(months=months_ago)
    start_date = target_month.start_of('month').to_date_string()   # 예: "2024-01-01"
    end_date = target_month.end_of('month').to_date_string()       # 예: "2024-01-31"

    # start_date ~ end_date 사이의 날짜를 하루 단위로 열거
    # swap_refresh_task.expand(target_date=...) 에 전달되어 날짜별 병렬 태스크로 확장됨
    period = pendulum.period(pendulum.parse(start_date), pendulum.parse(end_date))
    target_date_list = [dt.to_date_string() for dt in period.range('days')]

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
        'target_date_list': target_date_list,
        'sort_columns': table_config.get('sort_columns'),
        'compression': table_config.get('compression', 'snappy'),
    }

    log.info(
        f"metadata 조회 완료 | table: {db_name}.{table_name} | "
        f"대상 월: {start_date} ~ {end_date} ({len(target_date_list)}일) | "
        f"partition_cols: {partition_cols} | save_path: {save_full_path}"
    )

    return metadata


@task(retries=3, retry_delay=timedelta(seconds=10))
def log_before_count_task(metadata):
    """
    병합 전 대상 월 전체의 날짜별 row count를 GROUP BY로 한 번에 조회하여
    각 날짜를 merge_log에 개별 insert(upsert)한다.

    daily와의 차이점:
        - 날짜 단일 조회 대신 start_date ~ end_date 범위를 GROUP BY로 조회한다.
        - 조회된 각 날짜별로 merge_log에 insert한다.

    count=0인 날짜:
        - 실제로 발생할 가능성은 낮지만, 0이면 merge_log insert를 skip하고 경고만 남긴다.
        - 특정 날짜가 0이어도 다른 날짜 처리를 계속 진행하기 위해 태스크 전체를 중단하지 않는다.

    결과가 전혀 없는 경우(result_df가 None이거나 비어있는 경우):
        - result_df is None → Impala 연결 오류로 AirflowFailException
        - result_df가 비어있음 → 대상 월 데이터 전체 없음으로 AirflowSkipException
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

    if result_df is None:
        raise AirflowFailException(
            f"count 조회 실패 (Impala 연결 오류): {db_name}.{table_name}_t / {start_date} ~ {end_date}"
        )

    if result_df.empty:
        raise AirflowSkipException(
            f"대상 월 데이터가 없습니다. 병합 대상 없음으로 skip 처리: "
            f"{db_name}.{table_name}_t / {start_date} ~ {end_date}"
        )

    log.info(f"before merge count 조회 완료 | {table_name} | {start_date} ~ {end_date} | 총 {len(result_df)}일")

    for _, row in result_df.iterrows():
        date_val = str(row[0])
        count = int(row[1])

        if count == 0:
            # 해당 날짜만 skip하고 다른 날짜 처리는 계속 진행
            log.warning(f"count가 0입니다. merge_log insert skip: {table_name} / {date_val}")
            continue

        log.info(f"before merge count | {table_name} / {date_val}: {count}")

        insert_query = f"""
            insert into merge_log (table_id, before_count, after_count, create_time, update_time, target_date)
            values('{table_id}', {count}, 0, now(), now(), '{date_val}')
            on conflict (table_id, target_date)
            do update set before_count = excluded.before_count, update_time = now();
        """
        try:
            postgres_query(pg_conn_id, insert_query, commit=True)
        except Exception as e:
            raise AirflowFailException(
                f"merge_log before_count 기록 실패 (table_id={table_id}, target_date={date_val}): {e}"
            )

    log.info(f"merge_log before_count 기록 완료 | table_id: {table_id} | {start_date} ~ {end_date}")


@task(retries=3, retry_delay=timedelta(minutes=3))
def livy_task(metadata):
    """
    Livy를 통해 Spark 소파일 병합 작업을 제출하고 완료까지 대기한다.

    daily와의 차이점:
        - batch_name에 'monthly'가 포함된다.
        - --start-date, --end-date에 월 전체 범위(start_date ~ end_date)를 전달한다.
          (daily는 target_date를 start/end 모두에 동일하게 전달한다.)

    병합 결과는 temp 경로에 날짜별 파티션으로 저장되며,
    이후 swap_refresh_task가 날짜별로 병렬 실행되어 base 경로와 교체한다.
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
def swap_refresh_task(cluster_list, partition_list, metadata, target_date):
    """
    특정 날짜(target_date)의 HDFS swap 후 Impala partition refresh 및 after_count 검증을 수행한다.

    daily와의 차이점:
        - target_date를 metadata에서 읽지 않고 명시적 파라미터로 수신한다.
          → merge_monthly_dag.py에서 .expand(target_date=target_date_list)로 날짜별 동적 확장됨.
          → Airflow UI에서 [0], [1], ... 형태로 날짜 수만큼 개별 태스크 블록으로 표시됨.
        - partition_list(월 전체 manifest)에서 target_date에 해당하는 파티션만 필터링하여 처리한다.

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
    base_path = metadata['save_path']
    temp_path = metadata['temp_path']
    backup_path = metadata['backup_path']

    # monthly는 partition_list가 월 전체를 포함하므로 해당 날짜 파티션만 필터링
    date_partition_list = [p for p in partition_list if str(p.get(part1_column)) == target_date]
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
                    if attempt == max_retries:
                        raise AirflowFailException(f"[{cluster}] partition refresh failed: {partition_spec}")
                    time.sleep(5)

        # refresh 후 count 조회로 데이터 반영 확인
        for attempt in range(1, max_retries + 1):
            try:
                count_query = f"select count(*) from {db_name}.{table_name}_t where {part1_column} = '{target_date}'"
                result_df = impala_query(count_query, cluster, True)
                count = int(result_df.iloc[0, 0])
                return {cluster: count}
            except Exception as e:
                log.warning(f"[{cluster}] count query attempt {attempt}/{max_retries} failed: {e}")
                if attempt == max_retries:
                    raise AirflowFailException(f"[{cluster}] count query failed after {max_retries} retries")
                time.sleep(5)

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
