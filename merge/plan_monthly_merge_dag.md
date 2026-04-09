# Monthly Small File Merge DAG 구현 계획

## 개요

일별 DAG(`merge_daily_dag.py`)를 기반으로, 특정 달의 전체 파티션을 한 번에 병합하는 월별 DAG.  
- 일별 DAG는 수정하지 않음
- 일별 DAG와 완전히 동일한 태스크는 import하여 재사용
- 월별 DAG끼리 공유하는 태스크는 `tasks/` 디렉토리에서 관리

---

## 파일 구조

```
merge/
├── tasks/
│   └── monthly_tasks.py        # 월별 DAG 전용 공통 태스크
├── merge_daily_dag.py          # 수정 없음
├── merge_monthly_dag.py        # day1/day2 DAG 정의
├── CLAUDE.md
└── plan_monthly_merge_dag.md
```

---

## 태스크 공유 전략

| 태스크 | 출처 | 비고 |
|---|---|---|
| `load_refresh_flags_task` | `merge_daily_dag` import | 완전 동일 |
| `impala_health_check_task` | `merge_daily_dag` import | 완전 동일 |
| `get_partitions_task` | `merge_daily_dag` import | 완전 동일 |
| `get_metadata_task` | `tasks/monthly_tasks.py` | `months_ago` 처리, `start_date`/`end_date`/`target_date_list` 반환 |
| `livy_task` | `tasks/monthly_tasks.py` | `start_date`/`end_date` 사용, batch_name에 monthly 포함 |
| `swap_refresh_task` | `tasks/monthly_tasks.py` | `target_date`를 명시적 파라미터로 수신 (`.expand()` 대응) |
| `log_before_count_task` | `tasks/monthly_tasks.py` | GROUP BY 날짜별 count → 날짜별 insert |

---

## `tasks/monthly_tasks.py`

### `get_metadata_task`

```python
@task
def get_metadata_task(table_config, data_interval_end=None):
    # DB 조회 (merge_daily_dag와 동일)
    ...

    run_date = data_interval_end.in_timezone('Asia/Seoul')
    target_month = run_date.subtract(months=table_config['months_ago'])
    start_date = target_month.start_of('month').to_date_string()
    end_date = target_month.end_of('month').to_date_string()

    period = pendulum.period(pendulum.parse(start_date), pendulum.parse(end_date))
    target_date_list = [dt.to_date_string() for dt in period.range('days')]

    metadata = {
        ...,
        'start_date': start_date,
        'end_date': end_date,
        'target_date_list': target_date_list,
    }
    return metadata
```

### `log_before_count_task`

GROUP BY로 월 전체 날짜별 count 조회 후 각 날짜를 `merge_log`에 insert.

```python
@task(retries=3, retry_delay=timedelta(seconds=10))
def log_before_count_task(metadata):
    start_date = metadata['start_date']
    end_date = metadata['end_date']

    count_query = f"""
        select {part1_column}, count(*)
        from {db_name}.{table_name}_t
        where {part1_column} between '{start_date}' and '{end_date}'
        group by {part1_column}
    """
    result_df = impala_query(count_query, base_cluster, True)

    if result_df is None:
        raise AirflowFailException(...)

    for _, row in result_df.iterrows():
        date_val, count = row[0], int(row[1])
        if count == 0:
            raise AirflowFailException(f"count가 0입니다: {table_name} / {date_val}")
        insert_query = f"""
            insert into merge_log (table_id, before_count, after_count, create_time, update_time, target_date)
            values('{table_id}', {count}, 0, now(), now(), '{date_val}')
            on conflict (table_id, target_date)
            do update set before_count = excluded.before_count, update_time = now();
        """
        postgres_query(pg_conn_id, insert_query, commit=True)
```

### `livy_task`

```python
@task(retries=3, retry_delay=timedelta(minutes=3))
def livy_task(metadata):
    table_name = metadata['table_name']
    batch_name = f"Small-file-merge-monthly-{table_name}"
    spark_args = [
        "--save-path", metadata['save_path'],
        "--temp-path", metadata['temp_path'],
        "--start-date", metadata['start_date'],
        "--end-date",   metadata['end_date'],
        "--partition-cols", ','.join(metadata['partition_cols']),
        "--compression", metadata['compression'],
    ]
    ...  # 이하 merge_daily_dag의 livy_task와 동일
```

### `swap_refresh_task`

`target_date`를 명시적 파라미터로 수신하여 `.expand()` 동적 확장 대응.  
swap/refresh/count 로직은 `merge_daily_dag`의 `swap_refresh_task`와 동일.

```python
@task
def swap_refresh_task(cluster_list, partition_list, metadata, target_date):
    # target_date: .expand()로 날짜별 주입
    # 이하 로직은 merge_daily_dag의 swap_refresh_task와 동일
    ...
```

---

## `merge_monthly_dag.py`

```python
from merge_daily_dag import (
    load_refresh_flags_task,
    impala_health_check_task,
    get_partitions_task,
)
from tasks.monthly_tasks import (
    get_metadata_task,
    log_before_count_task,
    livy_task,
    swap_refresh_task,
)

@task_group
def table_group(table_config, refresh_flags):
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    partition_list = get_partitions_task(metadata)
    swap_refresh = swap_refresh_task.partial(
        cluster_list=cluster_list,
        partition_list=partition_list,
        metadata=metadata,
    ).expand(target_date=metadata['target_date_list'])

    cluster_list >> log_before >> livy_job >> partition_list >> swap_refresh


def create_monthly_dag(dag_id, config_variable, schedule):
    @dag(dag_id=dag_id, schedule=schedule,
         default_args={'depends_on_past': False, 'weight_rule': 'upstream'},
         max_active_runs=1, max_active_tasks=10)
    def monthly_merge_dag():
        refresh_flags_dict = load_refresh_flags_task()
        table_config_list = Variable.get(config_variable, deserialize_json=True, default_var=[])
        for table_config in table_config_list:
            table_id = table_config['table_id']
            table_group.override(group_id=f"table_{table_id}")(table_config, refresh_flags_dict)
    return monthly_merge_dag()


create_monthly_dag('Small-File-Merge-Monthly-Day1', 'monthly_merge_table_config_day1', '0 1 1 * *')
create_monthly_dag('Small-File-Merge-Monthly-Day2', 'monthly_merge_table_config_day2', '0 1 2 * *')
```

---

## DAG 분리 전략

| DAG | 스케줄 | Variable |
|---|---|---|
| `Small-File-Merge-Monthly-Day1` | `0 1 1 * *` | `monthly_merge_table_config_day1` |
| `Small-File-Merge-Monthly-Day2` | `0 1 2 * *` | `monthly_merge_table_config_day2` |

- 새벽 1시 실행으로 일별 DAG(`@daily` = 00:00)와 시간차 확보
- 동일 테이블이 `day1`, `day2` 양쪽 Variable에 중복 등록되지 않도록 운영 관리 필요

---

## 주의사항

- `metadata['target_date_list']` XComArg subscript는 Airflow 2.3+ 지원
- `swap_refresh_task`가 날짜별 병렬 실행되므로 `max_active_tasks` 값 조정 검토 (테이블 수 × 월 일수 기준)
- 일별/월별 DAG가 동일 테이블을 동시에 실행하지 않도록 스케줄 관리 필요
- `task_group` 내부에 `expand()`가 있으므로 `task_group` 자체를 `expand_kwargs()`로 중첩 매핑하면 Airflow 2.x 미지원
