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
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.operators.python import get_current_context
from livy import LivyBatch, SessionState

from postgres_wrapper import postgres_query
from impyla_wrapper import impala_query
from livy_wrapper import create_livy_batch, get_livy_batch_id_by_name, get_livy_batch_by_id

log = logging.getLogger(__name__)

pg_conn_id = "pg"
base_cluster = "cluster1"
impala_refresh_variable = "refresh_flags"
table_config_variable = "daily_merge_table_config"

DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
}


@task(retries=3, retry_delay=timedelta(seconds=10))
def load_refresh_flags_task():
    try:
        refresh_flags = Variable.get(impala_refresh_variable, deserialize_json=True)
    except Exception as e:
        raise AirflowFailException(e)

    result = {item['cluster']: item['flag'] for item in refresh_flags}

    if not result:
        raise AirflowFailException(f"'{impala_refresh_variable}' Variable이 비어 있습니다.")

    return result


@task
def get_metadata_task(table_config):
    context = get_current_context()
    data_interval_end = context['data_interval_end']

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

    run_date = data_interval_end.in_timezone('Asia/Seoul')
    days_ago = table_config['days_ago']
    target_date = run_date.subtract(days=days_ago).to_date_string()

    metadata = {
        'table_id': table_id,
        'db_name': db_name,
        'table_name': table_name,
        'partition_cols': partition_cols,
        'save_path': save_full_path,
        'temp_path': temp_full_path,
        'backup_path': backup_full_path,
        'target_date': target_date,
        'sort_columns': table_config.get('sort_columns'),
        'compression': table_config.get('compression', 'snappy')
    }

    return metadata


@task(retries=3, retry_delay=timedelta(seconds=10))
def impala_health_check_task(metadata, refresh_flags):
    db_name = metadata['db_name']
    table_name = metadata['table_name']

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
        log.error(f"health check failed clusters: {failed_clusters}")
        raise AirflowFailException("health check failed")

    return passed_clusters


@task(retries=3, retry_delay=timedelta(seconds=10))
def log_before_count_task(metadata):
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
        raise AirflowFailException(f"count가 0입니다. 데이터 적재 여부를 확인하세요: {db_name}.{table_name}_t / {target_date}")

    log.info(f"before merge count for {table_name} / {target_date}: {count}")

    insert_query = f"""
        insert into merge_log (table_id, before_count, after_count, create_time, update_time, target_date)
        values('{table_id}',{count},0,now(),now(),'{target_date}') on conflict (table_id, target_date)
        do update set before_count = excluded.before_count, update_time = now();
        """

    try:
        postgres_query(pg_conn_id, insert_query, commit=True)
    except Exception as e:
        raise AirflowFailException(e)


@task(retries=3, retry_delay=timedelta(minutes=3))
def livy_task(metadata):
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

    livy_status = create_livy_batch(
        name=batch_name,
        file="hdfs://path1/impala/merge/script/merge.py",
        cluster="livy_cluster",
        args=spark_args,
    )

    if not livy_status:
        raise AirflowFailException(f"Livy 배치 제출 실패 또는 RUNNING 상태 미달: {batch_name}")

    time.sleep(5)
    (livy_batch_id, livy_state) = get_livy_batch_id_by_name(batch_name, "livy_cluster")
    log.info(f"livy batch id: {livy_batch_id}, initial state: {livy_state}")

    livy_batch_obj = get_livy_batch_by_id(livy_batch_id, "livy_cluster")
    livy_batch_obj.wait()
    final_state = livy_batch_obj.state
    log.info(f"livy batch final state: {final_state}")

    if final_state != SessionState.SUCCESS:
        raise AirflowFailException(f"livy batch failed with state: {final_state}")


@task
def get_partitions_task(metadata):
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

    return partition_list


@task
def swap_refresh_task(cluster_list, partition_list, metadata):
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
        max_retries = 3

        # refresh
        # manifest에 파티션 순서(첫번째=날짜)가 보장된 상태로 저장됨
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
                    if attempt == max_retries:
                        raise AirflowFailException(f"[{cluster}] partition refresh failed: {partition_spec}")
                    time.sleep(5)

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

    # count
    cluster_count_dict = {}
    failed_clusters = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(cluster_list)) as executor:
        future_map = {}
        for cluster in cluster_list:
            future = executor.submit(_refresh_partition, cluster)
            future_map[future] = cluster
        for future in concurrent.futures.as_completed(future_map):
            cluster = future_map[future]
            try:
                result = future.result()
                if result is not None:
                    cluster_count_dict.update(result)
            except Exception as e:
                log.error(f"[{cluster}] refresh/count 실패: {e}")
                failed_clusters.append(cluster)

    if failed_clusters:
        raise AirflowFailException(f"다음 클러스터 refresh 실패: {failed_clusters}")

    log.info(f"cluster count results: {cluster_count_dict}")
    log.info(f"cluster count results: {cluster_count_dict}")

    count_list = list(cluster_count_dict.values())

    if not count_list:
        raise AirflowFailException("클러스터 count 결과가 없습니다.")

    main_count = count_list[0]

    if not all(count == main_count for count in count_list):
        raise AirflowException("클러스터간 count 불일치")

    update_query = f"""
            update merge_log set after_count = {main_count}, update_time = now()
            where table_id = '{table_id}' and target_date = '{target_date}'
            """

    try:
        postgres_query(pg_conn_id, update_query, commit=True)
    except Exception as e:
        raise AirflowFailException(e)


@task_group
def table_group(table_config, refresh_flags):
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    partition_list = get_partitions_task(metadata)
    swap_refresh = swap_refresh_task(cluster_list, partition_list, metadata)

    cluster_list >> log_before >> livy_job >> partition_list >> swap_refresh


@dag(
    dag_id='Small-File-Merge-Daily',
    schedule='@daily',
    default_args={'depends_on_past': False, 'weight_rule': 'upstream'},
    max_active_runs=1,
    max_active_tasks=10
)
def daily_merge_dag():
    refresh_flags_dict = load_refresh_flags_task()
    table_config_list = Variable.get(table_config_variable, deserialize_json=True, default_var=[])

    for table_config in table_config_list:
        table_id = table_config['table_id']
        table_group.override(group_id=f"table_{table_id}")(table_config, refresh_flags_dict)


daily_merge_dag()
