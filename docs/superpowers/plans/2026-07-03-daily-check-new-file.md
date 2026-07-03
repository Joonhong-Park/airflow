# check_new_file_task Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect files landing in a table's base HDFS path while `livy_task` is running, so the daily merge DAG fails loudly instead of silently losing that data during the HDFS swap.

**Architecture:** `log_before_count_task` captures a `cutoff_time` timestamp and returns it via XCom (previously returned nothing). A new task `check_new_file_task`, inserted between `livy_task` and `get_partitions_task`, recursively lists the base target path (`hdfs dfs -ls -R`), filters to directory entries only (a new file always bumps its parent directory's mtime, so file entries don't need to be inspected individually), and fails the task if any directory's mtime is newer than `cutoff_time` (minus a 1-minute buffer for `hdfs dfs -ls`'s minute-precision timestamps).

**Tech Stack:** Python 3, Airflow TaskFlow API (`@task`), `subprocess` + `hdfs dfs -ls -R` (same pattern already used by `get_partitions_task` / `swap_refresh_task` in this file).

## Global Constraints

- Scope is `merge/merge_daily_dag.py` only — do NOT touch `merge/merge_monthly_dag.py` (per `merge/CLAUDE.md`, the two files intentionally duplicate logic and must not import each other; monthly's per-date `swap_refresh_task.expand_kwargs()` structure needs separate design, out of scope here).
- No pytest / no local Airflow install in this repo. The only local verification available is `python3 -m py_compile` (per top-level `CLAUDE.md`). Do not attempt to `import` or run `merge_daily_dag.py` directly — it imports `airflow`, `livy`, and `alarm_wrapper`, none of which are installed locally; that ImportError is expected, not a bug.
- New task must have `retries` unset (defaults to 0) — retrying re-checks the same already-computed HDFS state and can't change the outcome, so a retry loop is pointless here. This is a deliberate deviation from the retry pattern used by neighboring tasks like `impala_health_check_task`.
- Follow existing code style in the file: task docstrings in Korean, f-strings for query/path building, `AirflowFailException` for non-retryable failures (see `get_partitions_task` for the closest existing example of this exact pattern: `subprocess.run([...], capture_output=True, text=True, check=True)` wrapped in try/except raising `AirflowFailException`).

---

## Task 1: Capture and return `cutoff_time` from `log_before_count_task`

**Files:**
- Modify: `merge/merge_daily_dag.py:34` (import line)
- Modify: `merge/merge_daily_dag.py:239-294` (`log_before_count_task`)

**Interfaces:**
- Produces: `log_before_count_task(metadata) -> str` — an ISO-8601 timestamp string (`datetime.now().isoformat()`), captured at the very start of the function, before the count query runs. This is the XCom value that Task 3's `check_new_file_task` will consume as its `cutoff_time` argument.

- [ ] **Step 1: Add `datetime` to the existing import**

Current (`merge/merge_daily_dag.py:34`):
```python
from datetime import timedelta
```

Change to:
```python
from datetime import datetime, timedelta
```

- [ ] **Step 2: Capture `cutoff_time` at the start of `log_before_count_task` and return it**

Current (`merge/merge_daily_dag.py:239-294`):
```python
@task(retries=3, retry_delay=timedelta(seconds=10))
def log_before_count_task(metadata):
    """
    병합 전 target_date 파티션의 row count를 조회하여 merge_log에 기록한다.

    케이스 구분:
        - result_df is None  → Impala 연결 오류. 재시도 필요 → AirflowFailException
        - result_df is empty → count(*) 쿼리가 행을 반환하지 않은 비정상 상황 → AirflowFailException
        - count == 0         → 데이터 없음. 병합 불필요 → AirflowSkipException (이후 태스크 전체 skip)

    merge_log upsert 시 before_count만 갱신하며 after_count는 건드리지 않는다.
    retry/backfill 시 after_count가 초기화되는 것을 방지하기 위함이다.
    """
    db_name = metadata['db_name']
    table_name = metadata['table_name']
    table_id = metadata['table_id']
    part1_column = metadata['partition_cols'][0]
    target_date = metadata['target_date']

    count_query = f"select count(*) from {db_name}.{table_name}_t where {part1_column} = '{target_date}'"
    result_df = impala_query(count_query, base_cluster, True)
```

Change the docstring and add the capture line right after it, before `db_name = ...`:
```python
@task(retries=3, retry_delay=timedelta(seconds=10))
def log_before_count_task(metadata):
    """
    병합 전 target_date 파티션의 row count를 조회하여 merge_log에 기록한다.

    케이스 구분:
        - result_df is None  → Impala 연결 오류. 재시도 필요 → AirflowFailException
        - result_df is empty → count(*) 쿼리가 행을 반환하지 않은 비정상 상황 → AirflowFailException
        - count == 0         → 데이터 없음. 병합 불필요 → AirflowSkipException (이후 태스크 전체 skip)

    merge_log upsert 시 before_count만 갱신하며 after_count는 건드리지 않는다.
    retry/backfill 시 after_count가 초기화되는 것을 방지하기 위함이다.

    함수 시작 시점의 timestamp를 cutoff_time으로 반환한다. livy_task 실행 도중
    base 경로에 새 파일이 유입되는지 판단하는 기준선으로 check_new_file_task에서 사용된다.
    """
    cutoff_time = datetime.now().isoformat()

    db_name = metadata['db_name']
    table_name = metadata['table_name']
    table_id = metadata['table_id']
    part1_column = metadata['partition_cols'][0]
    target_date = metadata['target_date']

    count_query = f"select count(*) from {db_name}.{table_name}_t where {part1_column} = '{target_date}'"
    result_df = impala_query(count_query, base_cluster, True)
```

Then at the end of the function (`merge/merge_daily_dag.py:294`), current:
```python
    log.info(f"merge_log before_count 기록 완료 | table_id: {table_id} | target_date: {target_date} | before_count: {count}")
```

Change to:
```python
    log.info(f"merge_log before_count 기록 완료 | table_id: {table_id} | target_date: {target_date} | before_count: {count}")

    return cutoff_time
```

(The `AirflowFailException` / `AirflowSkipException` branches earlier in the function are unaffected — they still raise before reaching this return, exactly as before.)

- [ ] **Step 3: Verify syntax**

Run: `cd /mnt/sdcard/workspace/airflow && python3 -m py_compile merge/merge_daily_dag.py`
Expected: no output, exit code 0.

- [ ] **Step 4: Commit**

```bash
cd /mnt/sdcard/workspace/airflow
git add merge/merge_daily_dag.py
git commit -m "Return cutoff_time from log_before_count_task for new-file detection"
```

---

## Task 2: Add `_find_new_directory` helper and `check_new_file_task`

**Files:**
- Modify: `merge/merge_daily_dag.py` — insert new code between `livy_task` (ends line 358) and `get_partitions_task` (starts line 361)

**Interfaces:**
- Consumes: `metadata` dict from `get_metadata_task` (keys used: `save_path`, `partition_cols`, `target_date`) and `cutoff_time: str` from Task 1's `log_before_count_task`.
- Produces:
  - `_find_new_directory(ls_output: str, cutoff: datetime) -> tuple[str, datetime] | None` — pure function, no I/O, used only by `check_new_file_task`.
  - `check_new_file_task(metadata, cutoff_time) -> None` (Airflow `@task`, raises `AirflowFailException` on detection or HDFS error).

- [ ] **Step 1: Insert the helper function and task**

Insert this block immediately after `livy_task` ends (`merge/merge_daily_dag.py:358`, the blank lines before `@task\ndef get_partitions_task`):

```python
def _find_new_directory(ls_output, cutoff):
    """
    'hdfs dfs -ls -R' 출력에서 cutoff 이후 mtime을 가진 디렉토리 항목을 찾는다.
    디렉토리 항목만 확인한다 — 새 파일이 추가되면 그 파일의 부모 디렉토리 mtime이
    갱신되므로, 파일 항목까지 개별 확인할 필요가 없다.

    Returns:
        (path, mtime) 튜플. 새 디렉토리가 없으면 None.
    """
    for line in ls_output.splitlines():
        if not line.startswith("d"):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        mtime = datetime.strptime(f"{parts[5]} {parts[6]}", "%Y-%m-%d %H:%M")
        if mtime > cutoff:
            return parts[-1], mtime
    return None


@task
def check_new_file_task(metadata, cutoff_time):
    """
    livy_task 실행 도중 base 경로에 새 파일이 적재됐는지 확인한다.

    감지되면 AirflowFailException으로 fail 처리한다. retry는 설정하지 않는다 —
    재시도해도 동일한 HDFS 상태를 다시 보는 것뿐이라 의미가 없다. 복구는 운영자가
    Airflow UI에서 count_before 태스크부터 clear하여 수동으로 재실행한다.
    """
    base_target_path = f"{metadata['save_path']}/{metadata['partition_cols'][0]}={metadata['target_date']}"
    cutoff = datetime.fromisoformat(cutoff_time) - timedelta(minutes=1)

    result = subprocess.run(
        ["hdfs", "dfs", "-ls", "-R", base_target_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise AirflowFailException(f"source 경로 조회 실패: {result.stderr}")

    detected = _find_new_directory(result.stdout, cutoff)
    if detected:
        path, mtime = detected
        raise AirflowFailException(
            f"livy_task 실행 중 새 파일 유입 감지: {path} (mtime={mtime}). "
            f"count_before부터 재시작 필요."
        )

    log.info(f"신규 파일 유입 없음 확인: {base_target_path}")


```

- [ ] **Step 2: Sanity-check `_find_new_directory`'s parsing against a real `hdfs dfs -ls -R` line format, in a throwaway shell — not committed**

`hdfs dfs -ls -R` lines look like:
```
drwxr-xr-x   - user group          0 2024-01-01 00:00 /base/dt=2024-01-01
-rw-r--r--   3 user group        123 2024-01-01 00:05 /base/dt=2024-01-01/part-0.parquet
```

Run this standalone (does not import the DAG file, so no airflow dependency issue):
```bash
python3 - <<'EOF'
from datetime import datetime, timedelta

def _find_new_directory(ls_output, cutoff):
    for line in ls_output.splitlines():
        if not line.startswith("d"):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        mtime = datetime.strptime(f"{parts[5]} {parts[6]}", "%Y-%m-%d %H:%M")
        if mtime > cutoff:
            return parts[-1], mtime
    return None

ls_output = (
    "drwxr-xr-x   - user group          0 2024-01-01 00:00 /base/dt=2024-01-01\n"
    "-rw-r--r--   3 user group        123 2024-01-01 00:05 /base/dt=2024-01-01/part-0.parquet\n"
)

# case 1: cutoff after the directory's mtime -> no detection
assert _find_new_directory(ls_output, datetime(2024, 1, 2)) is None

# case 2: cutoff before the directory's mtime -> detection, returns the directory path
result = _find_new_directory(ls_output, datetime(2023, 12, 31))
assert result == ("/base/dt=2024-01-01", datetime(2024, 1, 1, 0, 0)), result

# case 3: a file-only line with a newer mtime than any directory must NOT trigger detection
file_only = "-rw-r--r--   3 user group        123 2024-01-01 00:05 /base/dt=2024-01-01/part-0.parquet\n"
assert _find_new_directory(file_only, datetime(2023, 12, 31)) is None

print("OK")
EOF
```
Expected output: `OK`

- [ ] **Step 3: Verify syntax of the real file**

Run: `cd /mnt/sdcard/workspace/airflow && python3 -m py_compile merge/merge_daily_dag.py`
Expected: no output, exit code 0.

- [ ] **Step 4: Commit**

```bash
cd /mnt/sdcard/workspace/airflow
git add merge/merge_daily_dag.py
git commit -m "Add check_new_file_task to detect files landing during livy_task"
```

---

## Task 3: Wire `check_new_file_task` into `table_group` and update the module docstring

**Files:**
- Modify: `merge/merge_daily_dag.py:15-23` (module docstring flow diagram)
- Modify: `merge/merge_daily_dag.py:546-557` (`table_group`)

**Interfaces:**
- Consumes: `check_new_file_task(metadata, cutoff_time)` and `log_before_count_task(metadata) -> cutoff_time` from Tasks 1-2.

- [ ] **Step 1: Update the module docstring flow diagram**

Current (`merge/merge_daily_dag.py:15-23`):
```
흐름:
    load_refresh_flags_task
        └─ [테이블별 table_group]
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ get_partitions_task
            └─ swap_refresh_task
```

Change to:
```
흐름:
    load_refresh_flags_task
        └─ [테이블별 table_group]
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ check_new_file_task     (livy_task 실행 중 base 경로에 새 파일 유입 감지 시 fail)
            ├─ get_partitions_task
            └─ swap_refresh_task
```

- [ ] **Step 2: Wire the new task into `table_group`**

Current (`merge/merge_daily_dag.py:546-557`):
```python
@task_group
def table_group(table_config, refresh_flags):
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    partition_list = get_partitions_task(metadata)
    swap_refresh = swap_refresh_task(cluster_list, partition_list, metadata)

    # impala_health_check_task 완료 후 count_before 실행 (cluster_list 의존)
    # 이후 livy → get_partitions → swap_refresh 순서로 직렬 실행
    cluster_list >> log_before >> livy_job >> partition_list >> swap_refresh
```

Change to:
```python
@task_group
def table_group(table_config, refresh_flags):
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    source_check = check_new_file_task(metadata, log_before)
    partition_list = get_partitions_task(metadata)
    swap_refresh = swap_refresh_task(cluster_list, partition_list, metadata)

    # impala_health_check_task 완료 후 count_before 실행 (cluster_list 의존)
    # 이후 livy → check_new_file → get_partitions → swap_refresh 순서로 직렬 실행
    # log_before가 반환하는 cutoff_time을 check_new_file_task가 소비한다
    cluster_list >> log_before >> livy_job >> source_check >> partition_list >> swap_refresh
```

- [ ] **Step 3: Verify syntax**

Run: `cd /mnt/sdcard/workspace/airflow && python3 -m py_compile merge/merge_daily_dag.py`
Expected: no output, exit code 0.

- [ ] **Step 4: Commit**

```bash
cd /mnt/sdcard/workspace/airflow
git add merge/merge_daily_dag.py
git commit -m "Wire check_new_file_task into daily table_group"
```

---

## Task 4: Update `merge/CLAUDE.md` documentation

**Files:**
- Modify: `merge/CLAUDE.md`

**Interfaces:** None (documentation only).

- [ ] **Step 1: Update the daily flow diagram**

Current:
```
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
```

Change to:
```
## DAG 흐름 (일별)

```
load_refresh_flags_task
    └─ for each table_config:
        table_group (group_id=table_{table_id})
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ check_new_file_task
            ├─ get_partitions_task
            └─ swap_refresh_task(cluster_list, partition_list, metadata)
```

태스크 의존 순서: `get_metadata_task → impala_health_check_task → count_before → livy_task → check_new_file_task → get_partitions_task → swap_refresh_task`

- `check_new_file_task`는 livy_task 실행 도중 base 경로에 새 파일이 유입됐는지 확인한다.
  `count_before`가 반환한 cutoff_time 이후 mtime을 가진 디렉토리가 있으면 `AirflowFailException`으로
  fail 처리(retry 없음) — swap 시 신규 데이터가 backup으로 밀려나 유실되는 것을 막기 위함이다.
  복구는 운영자가 `count_before`부터 수동으로 clear하여 재실행한다. monthly DAG에는 아직 적용되지 않았다.
```

- [ ] **Step 2: Add a row to the retry settings table**

Current (`## 태스크별 retry 설정` table):
```
| 태스크 | retries | retry_delay | 비고 |
|---|---|---|---|
| `load_refresh_flags_task` | 3 | 10s | |
| `impala_health_check_task` | 3 | 10s | |
| `log_before_count_task` | 3 | 10s | |
| `livy_task` | 3 | 3min | Spark 작업 제출/대기 |
| `swap_refresh_task` | 없음 | - | retry 안전성은 temp 경로 체크로 보장 |
```

Change to:
```
| 태스크 | retries | retry_delay | 비고 |
|---|---|---|---|
| `load_refresh_flags_task` | 3 | 10s | |
| `impala_health_check_task` | 3 | 10s | |
| `log_before_count_task` | 3 | 10s | |
| `livy_task` | 3 | 3min | Spark 작업 제출/대기 |
| `check_new_file_task` | 없음 | - | 같은 HDFS 상태를 재검사할 뿐이라 retry 무의미 (일별 DAG 전용) |
| `swap_refresh_task` | 없음 | - | retry 안전성은 temp 경로 체크로 보장 |
```

- [ ] **Step 3: Commit**

```bash
cd /mnt/sdcard/workspace/airflow
git add merge/CLAUDE.md
git commit -m "Document check_new_file_task in merge/CLAUDE.md"
```
