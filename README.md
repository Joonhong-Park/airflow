# Airflow DAGs

Impala 테이블의 소파일(Small File)을 주기적으로 병합하는 Airflow DAG 모음.

## 개요

Impala 테이블에 누적된 소파일을 Spark(Livy)로 병합하고, HDFS swap 및 Impala partition refresh를 수행한다.

```
Impala(base) → Spark(Livy) 병합 → HDFS temp 저장
    → HDFS swap(base ↔ temp) → Impala partition refresh → after_count 검증
```

- **일별 병합**: 매일 새벽 지정 시각에 전날(또는 N일 전) 파티션 병합
- **월별 병합**: 매월 지정일 새벽 1시에 전월(또는 N개월 전) 전체 파티션 병합

## 디렉토리 구조

```
airflow/
├── common/                 # 공통 유틸리티 모듈
│   ├── Impyla_wrapper.py   # Impala 쿼리 실행
│   ├── livy_wrapper.py     # Livy 배치 생성/조회/대기
│   └── postgres_wrapper.py # PostgreSQL 쿼리 실행
└── merge/                  # 소파일 병합 DAG
    ├── merge_daily_dag.py  # 일별 병합 DAG
    ├── merge_monthly_dag.py# 월별 병합 DAG
    └── ops_manual.md       # 운영 매뉴얼
```

## DAG 목록 및 스케줄

| DAG ID | 유형 | 스케줄 | Airflow Variable |
|---|---|---|---|
| `Small-File-Merge-Daily-2am` | 일별 | 매일 02:00 | `daily_table_2am_config` |
| `Small-File-Merge-Daily-3am` | 일별 | 매일 03:00 | `daily_table_3am_config` |
| `Small-File-Merge-Monthly-Day1` | 월별 | 매월 1일 01:00 | `monthly_merge_table_config_day1` |
| `Small-File-Merge-Monthly-Day2` | 월별 | 매월 2일 01:00 | `monthly_merge_table_config_day2` |

> 스케줄 설계 원칙: 월별(01:00) → 일별(02:00~)로 시간차를 두어 동일 테이블 동시 실행 방지.

## 아키텍처

### 컴포넌트

| 컴포넌트 | 연결 ID | 역할 |
|---|---|---|
| PostgreSQL | `pg` | 테이블 메타정보(`table_meta`), 병합 로그(`merge_log`) 저장 |
| Impala | `cluster1` (base), 다중 클러스터 | 대상 테이블 조회 및 partition refresh |
| Livy (Spark) | `livy_cluster` | 소파일 병합 실행 |
| HDFS | - | base / temp / backup 경로 관리 |

### HDFS 경로 구조

| 경로 | 설명 |
|---|---|
| base: `{domain_path}{save_path}/{table_name}` | 실제 서비스 데이터 |
| temp: `hdfs://temp_path.com/impala/merge/temp/{table_name}` | Spark 병합 결과 임시 저장 |
| backup: `hdfs://temp_path.com/impala/merge/backup/{table_name}` | swap 전 기존 데이터 백업 |

## DAG 흐름

### 일별

```
load_refresh_flags_task
    └─ [테이블별 table_group]
        ├─ get_metadata_task
        ├─ impala_health_check_task
        ├─ count_before
        ├─ livy_task
        ├─ get_partitions_task
        └─ swap_refresh_task
```

### 월별

```
load_refresh_flags_task
    └─ [테이블별 table_group]
        ├─ get_metadata_task
        ├─ impala_health_check_task
        ├─ count_before
        ├─ livy_task               (max_active_tis_per_dag=5)
        ├─ get_partitions_task
        └─ swap_refresh_task[0..N] (날짜별 동적 확장, max_active_tis_per_dag=10)
```

## Airflow Variables

### `refresh_flags`

클러스터별 Impala refresh 활성화 여부.

```json
[
  {"cluster": "cluster1", "flag": true},
  {"cluster": "cluster2", "flag": false}
]
```

### 일별 테이블 설정 (`daily_table_{H}am_config`)

```json
[
  {"table_id": 1, "days_ago": 1, "sort_columns": "col1,col2", "compression": "snappy"},
  {"table_id": 2, "days_ago": 1}
]
```

### 월별 테이블 설정 (`monthly_merge_table_config_day{N}`)

```json
[
  {"table_id": 1, "months_ago": 1, "sort_columns": "col1,col2", "compression": "snappy"}
]
```

| 필드 | 필수 | 설명 |
|---|---|---|
| `table_id` | ✓ | `table_meta.table_id`와 매핑 |
| `days_ago` / `months_ago` | ✓ | 병합 대상 날짜/월 기준 |
| `sort_columns` | - | Spark 정렬 컬럼 (콤마 구분). 미입력 시 정렬 없음 |
| `compression` | - | 압축 형식. 기본값 `snappy` |

## 빠른 시작

### 테이블 추가

**1. PostgreSQL `table_meta`에 등록**

```sql
INSERT INTO table_meta (table_id, db, table_name, save_path, partitions, domain)
VALUES (10, 'mydb', 'my_table', '/mydb/my_table', 'dt,hour', 'path1');
```

**2. Airflow Variable에 항목 추가**

Airflow UI → Admin → Variables → 해당 Variable 편집

```json
[{"table_id": 10, "days_ago": 1, "compression": "snappy"}]
```

### DAG 추가

`merge_daily_dag.py` 또는 `merge_monthly_dag.py` 하단에 한 줄 추가:

```python
# 일별
create_daily_dag('Small-File-Merge-Daily-4am', 'daily_table_4am_config', '0 4 * * *')

# 월별
create_monthly_dag('Small-File-Merge-Monthly-Day3', 'monthly_merge_table_config_day3', '0 1 3 * *')
```

### 새 도메인 추가

`merge_daily_dag.py`와 `merge_monthly_dag.py` 두 파일의 `DOMAIN_PATH_MAP` 모두 수정:

```python
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
    "path3": "hdfs://path3.com/impala",  # 추가
}
```

## 운영

장애 대응, 수동 복구 절차, 모니터링 쿼리 등 상세 내용은 [merge/ops_manual.md](merge/ops_manual.md) 참조.

## 주의사항

- 동일 `table_id`를 여러 Variable에 중복 등록 시 중복 병합 발생 — Variable 수정 시 전체 목록 교차 확인
- `DOMAIN_PATH_MAP` 수정 시 `merge_daily_dag.py`와 `merge_monthly_dag.py` 두 파일 모두 반영
- `merge_daily_dag.py`와 `merge_monthly_dag.py`는 서로 직접 import하지 않는다 (독립 실행 원칙)
- 현재 Airflow 2.x 기준. Airflow 3 전환 시 `in_timezone()` → `in_tz()` 등 수정 필요 (각 DAG 파일 TODO 주석 참고)
