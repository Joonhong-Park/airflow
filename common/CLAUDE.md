# common

여러 DAG에서 공통으로 사용하는 wrapper 모듈.

## 파일

| 파일 | 역할 |
|---|---|
| `Impyla_wrapper.py` | Impala 쿼리 실행 (`impala_query`) |
| `livy_wrapper.py` | Livy 배치 생성/조회/대기 (`create_livy_batch`, `get_livy_batch_id_by_name`, `get_livy_batch_by_id`) |
| `postgres_wrapper.py` | PostgreSQL 쿼리 실행 (`postgres_query`, `get_db_connection`) |

## 사용 방법

DAG 파일에서 직접 import:

```python
from postgres_wrapper import postgres_query
from impyla_wrapper import impala_query
from livy_wrapper import create_livy_batch, get_livy_batch_id_by_name, get_livy_batch_by_id
```

## 주의사항

- Airflow connection은 `BaseHook.get_connection()`으로 조회 (하드코딩 금지)
- `impala_query`: 실패 시 `None` 반환 (예외를 올리지 않음) — 호출부에서 None 체크 필요
- `postgres_wrapper`: 실패 시 예외를 올림 — 호출부에서 try/except 처리
- `livy_wrapper`: RUNNING 상태 도달 실패 시 배치를 kill하고 `False` 반환
