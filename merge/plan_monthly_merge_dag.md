# Monthly Small File Merge DAG 구현 계획

## 개요

일별 DAG(`merge_daily_dag.py`)를 기반으로, 특정 달의 전체 파티션을 한 번에 병합하는 월별 DAG.  
Spark는 테이블당 1회 실행(해당 월 전체), `swap_refresh_task`는 해당 월의 일수만큼 날짜별로 확장 실행.

---

## 일별 DAG와의 차이점

| 항목 | 일별 DAG | 월별 DAG |
|---|---|---|
| Variable 파라미터 | `days_ago` | `months_ago` |
| target 범위 | 하루 | 한 달 전체 |
| Spark `--start-date` | target_date | 해당 월 1일 |
| Spark `--end-date` | target_date | 해당 월 마지막일 |
| `swap_refresh_task` 실행 수 | 테이블당 1회 | 테이블당 N회 (해당 월 일수) |
| `swap_refresh_task` 매핑 방식 | 정적 | `.expand()` 동적 매핑 |
| Livy 배치명 | `Small-file-merge-daily-{table_name}` | `Small-file-merge-monthly-{table_name}` |

---

## Variable 스키마

`daily_merge_table_config`와 별도로 `monthly_merge_table_config` Variable 사용.  
`days_ago` 대신 `months_ago` 사용.

```json
[
  {
    "table_id": 1,
    "months_ago": 1,
    "sort_columns": "col1,col2",
    "compression": "snappy"
  }
]
```

---

## target_date 계산 로직

```python
run_date = data_interval_end.in_timezone('Asia/Seoul')
target_month = run_date.subtract(months=months_ago)

start_date = target_month.start_of('month').to_date_string()   # 예: "2024-01-01"
end_date   = target_month.end_of('month').to_date_string()     # 예: "2024-01-31"

# swap_refresh_task 확장용 날짜 리스트
period = pendulum.period(
    pendulum.parse(start_date),
    pendulum.parse(end_date)
)
target_date_list = [dt.to_date_string() for dt in period.range('days')]
# ["2024-01-01", "2024-01-02", ..., "2024-01-31"]
```

---

## task 구조

```
일별 DAG와 동일한 정적 루프 구조 유지
(table_group 정적 생성 + 내부 swap_refresh_task.expand() 1단계 동적 매핑)

for table_config in table_config_list:          # 파싱 시점, 정적 생성
    table_group_monthly(table_config, refresh_flags_list)
        ├─ get_metadata_task                    # start_date, end_date, target_date_list 포함
        ├─ impala_health_check_task             # 일별과 동일
        ├─ log_before_count_task                # 월 전체 count (start_date ~ end_date)
        ├─ livy_task                            # Spark 1회 (start_date ~ end_date)
        ├─ get_partitions_task                  # manifest에서 파티션 목록 읽기
        └─ swap_refresh_task.partial(...).expand(  # 일수만큼 동적 확장
               target_date=target_date_list
           )
```

> **주의**: `table_group` 자체를 `expand_kwargs()`로 동적 매핑하면 내부 `swap_refresh_task.expand()`와 중첩 동적 매핑이 되어 Airflow 2.x에서 미지원. 반드시 정적 루프 구조를 유지해야 한다.

---

## `get_metadata_task` 변경 사항

월별 metadata에 `start_date`, `end_date`, `target_date_list` 추가 반환.  
`target_date` 키는 월별 DAG에서는 사용하지 않으므로 제거하거나 `start_date`로 대체한다.

```python
metadata = {
    ...
    'start_date': start_date,
    'end_date': end_date,
    'target_date_list': target_date_list,   # swap_refresh expand용
}
```

---

## `log_before_count_task` 변경 사항

count 쿼리 범위를 월 전체로 변경.

```python
count_query = f"""
    select count(*) from {db_name}.{table_name}_t
    where {part1_column} between '{start_date}' and '{end_date}'
"""
```

---

## `livy_task` 변경 사항

`--start-date`, `--end-date`를 각각 월의 첫날/마지막날로 설정.  
배치명에 `monthly` 포함.

```python
batch_name = f"Small-file-merge-monthly-{table_name}"
spark_args = [
    "--save-path", metadata['save_path'],
    "--temp-path", metadata['temp_path'],
    "--start-date", metadata['start_date'],   # 월 1일
    "--end-date",   metadata['end_date'],     # 월 마지막일
    "--partition-cols", ','.join(metadata['partition_cols']),
    "--compression", metadata['compression']
]
```

---

## `swap_refresh_task` 변경 사항

`target_date`를 metadata에서 꺼내는 대신 파라미터로 직접 수신.  
`cluster_list`, `partition_list`, `metadata`는 `.partial()`로 고정하고 `target_date`만 `.expand()`로 확장.

```python
@task
def swap_refresh_task(cluster_list, partition_list, metadata, target_date):
    # target_date는 파라미터로 수신 (metadata에서 꺼내지 않음)
    ...
```

`table_group` 내 호출:

```python
target_date_list = metadata['target_date_list']  # XCom에서 수신

swap_refresh_task.partial(
    cluster_list=cluster_list,
    partition_list=partition_list,
    metadata=metadata
).expand(target_date=target_date_list)
```

---

## 주의사항

- 일별 DAG와 월별 DAG가 같은 테이블을 동시에 실행하지 않도록 스케줄 및 `max_active_runs` 관리 필요.
- `swap_refresh_task`가 날짜별로 병렬 실행되므로 `max_active_tasks` 값 조정 검토 (테이블 수 × 일수 기준).
- 일별 DAG의 `swap_refresh_task`는 `target_date`를 `metadata`에서 꺼내므로, 월별 DAG용으로 시그니처를 변경할 때 일별 DAG 호환성 확인 필요. 공통 함수로 분리하거나 DAG별 별도 함수로 관리하는 방향 검토.
