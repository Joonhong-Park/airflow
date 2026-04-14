# merge

소파일 병합(Small File Merge) DAG 모음.

## 파일

| 파일 | 역할 |
|---|---|
| `merge_daily_dag.py` | 일별 병합 DAG (`Small-File-Merge-Daily-2am`, `3am`, ...) |
| `merge_monthly_dag.py` | 월별 병합 DAG (`Small-File-Merge-Monthly-Day1`, `Day2`, ...) |

## 등록된 DAG 목록

| DAG ID | 스케줄 | Variable |
|---|---|---|
| `Small-File-Merge-Daily-2am` | `0 2 * * *` | `daily_table_2am_config` |
| `Small-File-Merge-Daily-3am` | `0 3 * * *` | `daily_table_3am_config` |
| `Small-File-Merge-Monthly-Day1` | `0 1 1 * *` | `monthly_merge_table_config_day1` |
| `Small-File-Merge-Monthly-Day2` | `0 1 2 * *` | `monthly_merge_table_config_day2` |

새 DAG 추가 시 각 파일 하단의 `create_daily_dag()` / `create_monthly_dag()` 한 줄 추가.

## DAG 공통 설정

```python
default_args={
    'depends_on_past': False,
    'weight_rule': WeightRule.UPSTREAM,
    'on_failure_callback': dag_failure_alarm,   # 태스크 실패 시 알람 전송
}
max_active_runs=1       # 동일 DAG 중복 실행 방지
max_active_tasks=10
```

## Airflow Variables

| Variable | 형식 | 설명 |
|---|---|---|
| `refresh_flags` | JSON array | 클러스터별 refresh 실행 여부 (`[{"cluster": "...", "flag": true}]`) |
| `daily_table_{H}am_config` | JSON array | 일별 병합 테이블 설정 (H시 실행, 예: 2am, 3am) |
| `monthly_merge_table_config_day{N}` | JSON array | 월별 병합 테이블 설정 (N일 실행, N=1,2,3,...) |

## 테이블 설정 스키마

```json
// daily_table_2am_config / daily_table_3am_config / ...
[
  {
    "table_id": 1,
    "days_ago": 1,
    "sort_columns": "col1,col2",
    "compression": "snappy"
  }
]

// monthly_merge_table_config_day1 / day2 / ...
[
  {
    "table_id": 1,
    "months_ago": 1,
    "sort_columns": "col1,col2",
    "compression": "snappy"
  }
]
```

`sort_columns`는 선택 항목. `compression` 기본값은 `snappy`.

## DAG 흐름 (일별)

```
load_refresh_flags_task
    └─ for each table_config:
        table_group (group_id=table_{table_id})
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ get_partitions_task
            └─ swap_refresh_task(cluster_list, partition_list, metadata)
```

태스크 의존 순서: `get_metadata_task → impala_health_check_task → count_before → livy_task → get_partitions_task → swap_refresh_task`

- `get_partitions_task`는 단순 파티션 목록 반환 (`[{"dt": "2024-01-01", "hour": "00"}, ...]`)
- `swap_refresh_task`는 단일 인스턴스 실행 (expand 없음)

## DAG 흐름 (월별)

```
load_refresh_flags_task
    └─ for each table_config:
        table_group (group_id=table_{table_id})
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task                          (max_active_tis_per_dag=5)
            ├─ get_partitions_task
            └─ swap_refresh_task.partial(cluster_list, metadata)
                   .expand_kwargs(date_groups)    (날짜별 동적 확장, max_active_tis_per_dag=10)
```

태스크 의존 순서: `get_metadata_task → impala_health_check_task → count_before → livy_task → get_partitions_task → swap_refresh_task[0..N-1]`

- `get_partitions_task`는 날짜별 그룹화 후 반환 (`[{"target_date": "2025-01-01", "partitions": [...]}, ...]`)
- `swap_refresh_task`는 날짜 수만큼 동적 확장됨 (UI에서 `[0]`, `[1]`, ... 블록으로 표시)

`merge_daily_dag`를 직접 import하지 않는다.

## 태스크별 retry 설정

| 태스크 | retries | retry_delay | 비고 |
|---|---|---|---|
| `load_refresh_flags_task` | 3 | 10s | |
| `impala_health_check_task` | 3 | 10s | |
| `log_before_count_task` | 3 | 10s | |
| `livy_task` | 3 | 3min | Spark 작업 제출/대기 |
| `swap_refresh_task` | 없음 | - | retry 안전성은 temp 경로 체크로 보장 |

## 월별 DAG 동시 실행 제한

`max_active_tis_per_dag` 파라미터로 태스크별 동시 실행 수를 제한. (Airflow 2.2+, 외부 리소스 생성 불필요)

| 태스크 | max_active_tis_per_dag | 설명 |
|---|---|---|
| `livy_task` | 5 | 동시 처리 테이블 수 제한 (Livy가 테이블당 가장 오래 실행되는 태스크) |
| `swap_refresh_task` | 10 | 날짜별 동적 확장 인스턴스 동시 실행 수 제한 |

## HDFS 경로 구조

- **base path**: `{domain_path}{save_path}/{table_name}` (`save_path`는 `/`로 시작)
- **temp path**: `hdfs://temp_path.com/impala/merge/temp/{table_name}`
- **backup path**: `hdfs://temp_path.com/impala/merge/backup/{table_name}`

swap 순서: `base → backup` 이동 후 `temp → base` 이동. 실패 시 `backup → base` 롤백.
retry 시 temp 경로 존재 여부로 swap 완료 판단 → 미존재 시 refresh 단계로 바로 진행.

## domain → HDFS path 매핑

모듈 상단 `DOMAIN_PATH_MAP` 딕셔너리에서 관리. 도메인 추가 시 두 파일 모두 수정.

```python
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
}
```

## refresh_flags 처리 방식

- `load_refresh_flags_task`: 전체 클러스터를 `{cluster: flag}` 딕셔너리로 반환 (active 클러스터 없으면 fail)
- `impala_health_check_task`: `flag=False`인 클러스터는 skip, `flag=True`인 클러스터만 health check 수행
- health check: Impala 연결 + `show tables in {db} like '{table}_t'`로 테이블 존재 여부 확인
- 하나의 클러스터라도 실패 시 전체 태스크 fail

## count_before 처리 방식

- **일별**: 단일 날짜 `count(*)` 조회 → `merge_log` 1건 upsert
  - `result_df is None` → `AirflowFailException` (Impala 연결 오류)
  - `result_df is empty` → `AirflowFailException` (비정상, `count(*)` 는 항상 1행 반환)
  - `count == 0` → `AirflowSkipException` (이후 태스크 전체 skip)
- **월별**: `GROUP BY` 날짜별 조회 → 유효한 날짜 전체 bulk upsert
  - `result_df is None` → `AirflowFailException`
  - `result_df is empty` → `AirflowSkipException` (해당 월 데이터 없음)
  - count=0인 날짜만 insert 제외 (경고 로그), 유효 날짜 전부 0이면 `AirflowSkipException`
- upsert 시 `before_count`만 갱신, `after_count`는 건드리지 않음 (retry/backfill 시 보존)

## swap_refresh_task 처리 방식

1. temp 경로 존재 여부 확인 (retry 안전성)
   - temp 없음 → swap 완료로 판단, refresh 단계로 바로 진행
   - temp 있음 → HDFS swap 수행
2. HDFS swap: `base → backup`, `temp → base`. 실패 시 `backup → base` 롤백
3. Impala partition refresh: `ThreadPoolExecutor`로 클러스터별 병렬 실행, 각 파티션 최대 3회 재시도
4. after_count 조회 후 클러스터 간 count 일치 여부 확인
   - 불일치 → `AirflowException` (retry 가능, 복제 지연 대응)
5. `merge_log.after_count` 업데이트

## Airflow 3 마이그레이션 시 수정 필요 항목

현재 Airflow 2 기준으로 작성. Airflow 3 전환 시 아래 항목 수정 필요.

| 항목 | 현재 (Airflow 2) | 변경 후 (Airflow 3) |
|---|---|---|
| `data_interval_end.in_timezone()` | pendulum 2 메서드 | `in_tz()`로 변경 (pendulum 3) |
| `from airflow.utils.weight_rule import WeightRule` | 두 DAG 파일 상단 | Airflow 3에서도 동일 경로 유지, 변경 불필요 |
| `weight_rule` in `default_args` | - | Airflow 3에서도 동작 확인, 변경 불필요 |
| `data_interval_end.in_timezone()` | 두 DAG 파일 `get_metadata_task` | `in_tz()`로 변경 (pendulum 3) |

## 주의사항

- 일별/월별 DAG가 동일 테이블을 동시에 실행하지 않도록 스케줄 관리 필요 (월별은 새벽 1시, 일별은 2시·3시 등으로 분리)
- 동일 테이블이 여러 Variable에 중복 등록되지 않도록 운영 관리 필요
- `DOMAIN_PATH_MAP` 수정 시 `merge_daily_dag.py`와 `merge_monthly_dag.py` 두 파일 모두 반영
- `livy_task`에서 `create_livy_batch` 후 `time.sleep(5)` 후 ID 조회 — Livy 등록 지연 대응
- `livy_task` retry 시 동명 배치가 Livy에 잔존할 수 있으므로 `create_livy_batch` wrapper의 중복 처리 방식 확인 필요
- `task_group` 내부에 `expand_kwargs()`가 있으므로 `task_group` 자체를 `expand_kwargs()`로 중첩 매핑하면 Airflow 2.x 미지원
- PostgreSQL 테이블: `table_meta`(메타정보), `merge_log`(before/after count 기록)
