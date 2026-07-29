# Monthly Merge: 운영/소급 DAG 분리 + execute_date 필터링 설계

- Date: 2026-07-29
- Scope: `merge/merge_monthly_dag.py` (및 `merge/CLAUDE.md` 문서 갱신)
- Out of scope: `merge/merge_daily_dag.py` (변경 없음)

## 배경 / 문제

현재 `merge_monthly_dag.py`는 테이블 목록이 많아질 경우를 대비해 `Day1`, `Day2`, ... 형태로
여러 DAG(각각 별도 Variable, 월 1회 cron 스케줄)를 등록하고, 테이블을 Variable 단위로
수동 분산 배치하는 구조다. 테이블 수가 약 70개로 늘어나면서:

- 테이블을 어느 `dayN` Variable에 넣을지 운영자가 수동으로 관리해야 하고, 중복 등록 위험이 있다.
- `livy_task`/`swap_refresh_task`에 설정된 `max_active_tis_per_dagrun`이 기대대로 동작하지 않는
  것이 확인되어, 동시성 제어를 다른 방식으로 일원화할 필요가 있다.
- 특정 테이블만 재실행(소급 처리)하고 싶을 때 기존 월 1회 스케줄 DAG를 그대로 쓰기 어렵다.

## 목표

1. 테이블별로 "매월 며칠에 실행할지"(`execute_date`)를 Variable에서 직접 관리하고, 단일 DAG가
   매일 실행되며 그날 해당하는 테이블만 병합, 나머지는 skip.
2. 위 자동 실행("운영용")과는 별개로, 운영자가 필요할 때 수동으로 트리거하는 "소급용" DAG를
   `Day1`/`Day2`... 식으로 여러 개 유지.
3. `max_active_tis_per_dagrun` 제거, `max_active_tasks=10`으로 동시성 제어 일원화 (운영/소급 공통).

## Variable 스키마 변경

운영용/소급용 모두 동일한 스키마를 사용한다. 기존 `months_ago`에 `execute_date`를 추가한다.

```json
[
  {"table_id": 1, "execute_date": 5, "months_ago": 1, "sort_columns": "col1,col2", "compression": "snappy"},
  {"table_id": 2, "execute_date": 12, "months_ago": 1, "compression": "zstd"}
]
```

- `execute_date` (int, 1~31, 필수): 매월 며칠에 이 테이블을 실행할지. DAG 실행일의 "일(day)"과
  비교해 일치할 때만 병합을 진행한다.
- `months_ago` (기존과 동일, 필수): 병합 대상 월을 결정 (`data_interval_end`에서 N개월 전).
- 31일처럼 모든 달에 존재하지 않는 날짜를 `execute_date`로 지정한 테이블은 해당 달에 실행되지
  않고 자연히 skip된다 (알려진 제약, 별도 보정 로직 없음).

## 아키텍처 변경

### 새 태스크: `check_execute_date_task`

`table_group` 내부, `get_metadata_task` 이전에 배치되는 신규 태스크.

```python
@task
def check_execute_date_task(table_config, data_interval_end=None):
    table_id = table_config['table_id']
    execute_date = table_config.get('execute_date')
    if execute_date is None:
        raise AirflowFailException(f"table_config에 'execute_date' 키가 없습니다. (table_id={table_id})")

    run_day = data_interval_end.in_timezone('Asia/Seoul').day  # TODO(Airflow 3): in_timezone() -> in_tz()
    if run_day != execute_date:
        raise AirflowSkipException(
            f"execute_date 불일치로 skip: table_id={table_id}, execute_date={execute_date}, 오늘={run_day}"
        )
    log.info(f"execute_date 일치, 병합 진행: table_id={table_id}, execute_date={execute_date}")
```

- retries 없음 (외부 I/O 없는 순수 로직 비교, `get_metadata_task`와 동일한 패턴).
- `get_metadata_task`보다 먼저 실행되어, 불일치 테이블은 Postgres/Impala 조회 없이 즉시 skip된다.
- `table_group`에서 명시적 순서 지정: `check_execute_date_task(table_config) >> get_metadata_task(table_config)`.

### `table_group` 태스크 순서 변경

```
check_execute_date_task → get_metadata_task → impala_health_check_task → count_before
  → livy_task → get_partitions_task → swap_refresh_task[0..N-1]
```

`check_execute_date_task`가 skip되면 downstream 태스크 전체가 Airflow의 기본 skip 전파 규칙에
따라 skip 처리된다 (기존 `count_before`의 `AirflowSkipException` 처리 방식과 동일한 원리).

### DAG 팩토리: 운영용 vs 소급용

`merge_monthly_dag.py` 한 파일 안에 두 팩토리 함수가 공존한다. 기존 `create_monthly_dag`는
그대로 유지하고(운영용으로 재사용), 소급용은 이를 감싸는 얇은 래퍼로 추가한다.

```python
def create_monthly_dag(dag_id, config_variable, schedule):
    """기존 팩토리. 운영용 단일 DAG 등록에 사용."""
    ...  # 기존 구현 + max_active_tasks=10 반영, table_group에 check_execute_date_task 포함

def create_monthly_backfill_dag(dag_id, config_variable):
    """소급용: schedule=None 고정. 내부적으로 create_monthly_dag에 위임(중복 방지)."""
    return create_monthly_dag(dag_id, config_variable, schedule=None)
```

### DAG 등록 (파일 하단)

기존 `Small-File-Merge-Monthly-Day1`/`Day2` 등록과 `monthly_merge_table_config_day1`/`day2`
Variable은 삭제하고 아래로 교체한다.

```python
# 운영용: 매일 실행, execute_date로 대상 테이블 필터링
create_monthly_dag('Small-File-Merge-Monthly', 'monthly_merge_table_config', '0 1 * * *')

# 소급용: 수동 트리거 전용, Day1/Day2 스타일로 Variable 분리 유지
create_monthly_backfill_dag('Small-File-Merge-Monthly-Backfill-Day1', 'monthly_merge_table_config_backfill_day1')
create_monthly_backfill_dag('Small-File-Merge-Monthly-Backfill-Day2', 'monthly_merge_table_config_backfill_day2')
```

## 동시성 설정 변경

- `livy_task`: `max_active_tis_per_dagrun=5` 제거.
- `swap_refresh_task`: `max_active_tis_per_dagrun=10` 제거.
- `@dag(...)` 데코레이터의 `max_active_tasks`를 5 → 10으로 상향. 운영용/소급용 DAG 모두 동일하게
  적용 (두 DAG 모두 `create_monthly_dag`를 거치므로 자동 반영됨).
- `merge_daily_dag.py`는 애초에 `max_active_tis_per_dagrun`을 사용하지 않으므로 변경 없음.

## 에러 처리 / 엣지 케이스

- `execute_date` 키 누락: `AirflowFailException` (기존 `months_ago` 누락 처리와 동일 패턴).
- `execute_date`가 해당 월에 존재하지 않는 날짜(예: 31일, 2월)인 경우: 그 달은 자연히 계속 skip
  (매월 재계산되므로 존재하는 달에는 정상 실행됨). 별도 예외 처리 불필요.
- 소급용 DAG를 특정 날짜에 맞춰 강제로 실행하고 싶다면, Airflow UI의 "Trigger DAG w/ config"로
  logical date를 원하는 날짜로 지정해 트리거한다 (운영용과 동일한 비교 로직을 그대로 재사용하기
  위함 — 소급용 별도의 필터 예외 로직을 두지 않는다).
- `refresh_flags`, `impala_health_check_task` 등 기존 로직/에러 처리는 변경 없음.

## 문서 갱신 대상

`merge/CLAUDE.md`에서 아래 섹션이 새 구조를 반영하도록 갱신 필요:

- "등록된 DAG 목록" 표: Small-File-Merge-Monthly(운영), Backfill-Day1/Day2(소급)로 교체.
- "테이블 설정 스키마": monthly 설정에 `execute_date` 필드 추가.
- "DAG 흐름 (월별)": `check_execute_date_task` 추가된 태스크 체인으로 갱신.
- "월별 DAG 동시 실행 제한": `max_active_tis_per_dagrun` 관련 표 제거, `max_active_tasks=10` 설명으로 교체.
- "태스크별 retry 설정": `check_execute_date_task` 행 추가 (retries 없음).

## 테스트 / 검증 계획

- 로컬에는 Airflow 실행 환경이 없으므로 `python3 -m py_compile merge/merge_monthly_dag.py`로 문법
  검증.
- `check_execute_date_task`의 날짜 비교 로직은 별도 유닛 테스트 없이 코드 리뷰로 검증 (레포에
  테스트 스위트 없음, CLAUDE.md 명시).
- 실제 동작 검증은 원격 Airflow 클러스터에서 운영자가 수행 (기존 관례와 동일).
