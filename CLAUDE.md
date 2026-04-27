# Airflow DAGs

## 디렉토리 구조

```
airflow/
├── common/                 # 공통 유틸리티 (Impala, Livy, Postgres 래퍼)
│   ├── Impyla_wrapper.py
│   ├── livy_wrapper.py
│   └── postgres_wrapper.py
├── merge/                  # 소파일 병합 DAG (일별, 월별)
│   ├── merge_daily_dag.py
│   ├── merge_monthly_dag.py
│   └── ops_manual.md       # 운영 매뉴얼 (장애 대응, 테이블 추가 절차 등)
└── <future>/               # 이후 추가되는 DAG는 별도 디렉토리로 관리
```

## 규칙

- 공통으로 사용하는 wrapper/utility는 `common/`에 위치
- 각 DAG 그룹은 독립된 디렉토리에서 관리하며, 해당 디렉토리에 `CLAUDE.md` 포함
- 새로운 DAG 추가 시 디렉토리 생성 후 `CLAUDE.md` 작성
- `merge_daily_dag.py`와 `merge_monthly_dag.py`는 독립 실행 원칙 — 두 파일 간 직접 import 금지

## Airflow 연결 정보

| 연결 ID | 용도 |
|---|---|
| `pg` | PostgreSQL (table_meta, merge_log) |
| `cluster1` | Base Impala 클러스터 (count 조회 기준) |
| `livy_cluster` | Livy 클러스터 (Spark 병합 작업 제출) |

## Airflow 버전 및 호환성

- 현재: **Airflow 2.x** (pendulum 2 기반)
- Airflow 3 전환 시 수정 필요: `in_timezone()` → `in_tz()`, `execution_date` → `logical_date`
- `max_active_tis_per_dag` → Airflow 3에서 `max_active_tis_per_dagrun`으로 변경 (`merge_monthly_dag.py` 이미 주석 반영)

## PostgreSQL 테이블

| 테이블 | 역할 |
|---|---|
| `table_meta` | 병합 대상 테이블 메타정보 (db, table_name, save_path, partitions, domain) |
| `merge_log` | 병합 전후 row count 기록 (table_id, target_date, before_count, after_count) |

## alarm_wrapper

DAG 파일에서 `from alarm_wrapper import send_alarm`으로 사용. `common/`에 파일 없음 — 별도 경로에 위치하거나 환경별로 제공됨. 태스크 실패 시 `dag_failure_alarm` 콜백에서 호출.

## 등록된 DAG 목록

| DAG ID | 유형 | 스케줄 | Airflow Variable |
|---|---|---|---|
| `Small-File-Merge-Daily-2am` | 일별 | `0 2 * * *` | `daily_table_2am_config` |
| `Small-File-Merge-Daily-3am` | 일별 | `0 3 * * *` | `daily_table_3am_config` |
| `Small-File-Merge-Monthly-Day1` | 월별 | `0 1 1 * *` | `monthly_merge_table_config_day1` |
| `Small-File-Merge-Monthly-Day2` | 월별 | `0 1 2 * *` | `monthly_merge_table_config_day2` |

새 DAG 추가 시 각 파일 하단의 `create_daily_dag()` / `create_monthly_dag()` 한 줄 추가.
