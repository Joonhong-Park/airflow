# Design: livy_task 실행 중 신규 파일 유입 감지 (`check_new_file_task`)

Date: 2026-07-03
Scope: `merge/merge_daily_dag.py`만. `merge_monthly_dag.py`는 날짜별 `swap_refresh_task` expand 구조가 달라 별도 설계 필요 — 이번 작업에서 제외.

## 문제

`livy_task`는 base 경로의 데이터를 읽어 temp 경로에 병합 결과를 만든다. `swap_refresh_task`는 이후
`base→backup`, `temp→base` 순으로 HDFS 경로를 교체한다. 만약 `livy_task` 실행 도중(즉 `count_before` 조회
이후) base 경로에 새 파일이 적재되면, 그 신규 데이터는 병합 결과에 반영되지 않은 채로 swap 시 backup으로
밀려나 유실된다.

Airflow DAG는 비순환 구조이므로 태스크 그래프상에서 `count_before`로 자동으로 되돌아갈 수 없다. 따라서 신규
파일 유입을 감지하면 명확한 에러 메시지와 함께 태스크를 fail 처리하고, 운영자가 Airflow UI에서
`count_before` 태스크부터 clear하여 수동으로 재실행하는 방식을 택한다.

## 태스크 체인 변경

```
count_before(cutoff_time 반환) >> livy_task >> check_new_file_task(metadata, cutoff_time) >> get_partitions_task >> swap_refresh_task
```

## 1. `log_before_count_task` 수정

- 함수 시작 시점에 `cutoff_time = datetime.now()` 캡처.
- 기존 로직(merge_log upsert)은 그대로 수행.
- 반환값을 `None` → `cutoff_time`(isoformat 문자열)으로 변경.
- `count == 0`으로 `AirflowSkipException`이 발생하는 경로는 그대로 skip이므로 영향 없음.

## 2. 신규 태스크 `check_new_file_task(metadata, cutoff_time)`

- 위치: `livy_task` 완료 직후, `get_partitions_task` 이전.
- 대상 경로: `{save_path}/{partition_cols[0]}={target_date}` (base_target_path).
- 감지 방식: `hdfs dfs -ls -R base_target_path` 결과 중 **디렉토리 항목만** 필터링(`d`로 시작하는 라인)해서
  각 디렉토리의 mtime을 확인한다. 파일 항목은 확인하지 않는다 — 새 파일이 추가되면 그 파일의 바로 위
  디렉토리의 mtime이 갱신되므로, 디렉토리 mtime만 보면 충분하다. `partition_cols`가 1개든 2개든
  `-R`이 하위 depth를 전부 훑으므로 depth 하드코딩이 필요 없다.
- cutoff 비교: `hdfs dfs -ls`의 mtime은 분 단위 정밀도이므로, `cutoff_time - 1분`을 기준으로 비교(경계
  오탐 방지 버퍼).
- 감지 시 `AirflowFailException`으로 fail 처리, retries=0 (재시도해도 같은 hdfs 상태를 다시 보는 것뿐이라
  의미 없음). 에러 메시지에 감지된 디렉토리 경로와 mtime, "count_before부터 재시작 필요" 안내 포함.

```python
@task
def check_new_file_task(metadata, cutoff_time):
    """
    livy_task 실행 도중 base 경로에 새 파일이 적재됐는지 확인한다.
    새 파일이 추가되면 그 파일의 부모 디렉토리 mtime이 갱신되므로,
    재귀적으로 조회한 디렉토리 항목의 mtime만 cutoff_time과 비교한다.
    감지되면 AirflowFailException으로 fail 처리 — count_before부터 수동 재실행 필요.
    """
    base_target_path = f"{metadata['save_path']}/{metadata['partition_cols'][0]}={metadata['target_date']}"
    cutoff = datetime.fromisoformat(cutoff_time) - timedelta(minutes=1)

    result = subprocess.run(
        ["hdfs", "dfs", "-ls", "-R", base_target_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise AirflowFailException(f"source 경로 조회 실패: {result.stderr}")

    for line in result.stdout.splitlines():
        if not line.startswith("d"):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        mtime = datetime.strptime(f"{parts[5]} {parts[6]}", "%Y-%m-%d %H:%M")
        if mtime > cutoff:
            raise AirflowFailException(
                f"livy_task 실행 중 새 파일 유입 감지: {parts[-1]} (mtime={mtime}). "
                f"count_before부터 재시작 필요."
            )
```

## 3. `table_group` 수정

```python
cluster_list >> log_before >> livy_job >> source_check >> partition_list >> swap_refresh
```

## 스코프 아웃

- 자동 재시도 루프 (count_before로 자동으로 돌아가는 기능은 만들지 않음 — DAG 비순환 제약)
- `merge_monthly_dag.py` 반영 (별도 요청 시 진행)
- mtime 버퍼 값(1분)의 설정화 — 하드코딩 유지
