# merge

소파일 병합(Small File Merge) DAG 모음.

## 파일

| 파일 | 역할 |
|---|---|
| `merge_daily_dag.py` | 일별 병합 DAG (`Small-File-Merge-Daily`) |
| `plan_monthly_merge_dag.md` | 월별 병합 DAG 구현 계획 |

## Airflow Variables

| Variable | 형식 | 설명 |
|---|---|---|
| `refresh_flags` | JSON array | 클러스터별 refresh 실행 여부 (`[{"cluster": "...", "flag": true}]`) |
| `daily_merge_table_config` | JSON array | 일별 병합 테이블 설정 |
| `monthly_merge_table_config` | JSON array | 월별 병합 테이블 설정 (`months_ago` 사용) |

## 테이블 설정 스키마

```json
// daily_merge_table_config
[
  {
    "table_id": 1,
    "days_ago": 1,
    "sort_columns": "col1,col2",
    "compression": "snappy"
  }
]

// monthly_merge_table_config
[
  {
    "table_id": 1,
    "months_ago": 1,
    "sort_columns": "col1,col2",
    "compression": "snappy"
  }
]
```

## DAG 흐름 (일별)

```
load_refresh_flags_task
    └─ for each table_config:
        table_group
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ get_partitions_task
            └─ swap_refresh_task
```

태스크 의존 순서: `get_metadata_task → impala_health_check_task → count_before → livy_task → get_partitions_task → swap_refresh_task`

## 태스크별 retry 설정

| 태스크 | retries | retry_delay |
|---|---|---|
| `load_refresh_flags_task` | 3 | 10s |
| `impala_health_check_task` | 3 | 10s |
| `log_before_count_task` | 3 | 10s |
| `livy_task` | 3 | 3min |

## HDFS 경로 구조

- **base path**: `{domain_path}{save_path}/{table_name}` (`save_path`는 `/`로 시작)
- **temp path**: `hdfs://temp_path.com/impala/merge/temp/{table_name}`
- **backup path**: `hdfs://temp_path.com/impala/merge/backup/{table_name}`

swap 순서: `base → backup` 이동 후 `temp → base` 이동. 실패 시 `backup → base` 롤백.

## domain → HDFS path 매핑

모듈 상단 `DOMAIN_PATH_MAP` 딕셔너리에서 관리. 도메인 추가 시 딕셔너리만 확장.

```python
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
}
```

## refresh_flags 처리 방식

- `load_refresh_flags_task`: 전체 클러스터를 `{cluster: flag}` 딕셔너리로 반환
- `impala_health_check_task`: `flag=False`인 클러스터는 skip, `flag=True`인 클러스터만 health check 수행

## 주의사항

- 일별/월별 DAG가 동일 테이블을 동시에 실행하지 않도록 스케줄 관리 필요
- `swap_refresh_task` 내 Impala refresh는 클러스터별 병렬 실행 (`ThreadPoolExecutor`)
- `livy_task`에서 `create_livy_batch` 후 `time.sleep(5)` 후 ID 조회 — Livy 등록 지연 대응
- `livy_task` retry 시 동명 배치(`Small-file-merge-daily-{table_name}`)가 Livy에 잔존할 수 있으므로 `create_livy_batch` wrapper의 중복 처리 방식 확인 필요
- `merge_log` upsert 시 `after_count`는 갱신하지 않음 — retry/backfill 시 기존 after_count 보존
- 월별 DAG에서 `swap_refresh_task`는 `.partial().expand(target_date=target_date_list)`로 날짜별 동적 확장
- `task_group` 내부에 `expand()`가 있으므로 `task_group` 자체를 `expand_kwargs()`로 중첩 매핑하면 Airflow 2.x 미지원
- PostgreSQL 테이블: `table_meta`(메타정보), `merge_log`(before/after count 기록)
