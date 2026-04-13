# 소파일 병합(Small File Merge) 운영 매뉴얼

> Confluence 페이지 기준으로 작성. 대상 독자: 데이터 엔지니어링 팀.

---

## 목차

1. [개요](#1-개요)
2. [아키텍처](#2-아키텍처)
3. [DAG 목록 및 스케줄](#3-dag-목록-및-스케줄)
4. [Airflow Variables 설정](#4-airflow-variables-설정)
5. [테이블 추가 / 변경](#5-테이블-추가--변경)
6. [DAG 추가](#6-dag-추가)
7. [장애 대응](#7-장애-대응)
8. [수동 복구 절차 (HDFS)](#8-수동-복구-절차-hdfs)
9. [모니터링](#9-모니터링)
10. [주의사항 및 운영 규칙](#10-주의사항-및-운영-규칙)

---

## 1. 개요

Impala 테이블에 누적된 소파일(Small File)을 주기적으로 병합하여 쿼리 성능을 개선한다.

- **일별 병합**: 매일 새벽 지정 시각에 전날(또는 N일 전) 파티션을 병합
- **월별 병합**: 매월 지정일 새벽 1시에 전월(또는 N개월 전) 전체 파티션을 병합

병합 흐름 요약:

```
Impala(base) → Spark(Livy)로 병합 → HDFS temp 저장
    → HDFS swap(base ↔ temp) → Impala partition refresh → after_count 검증
```

---

## 2. 아키텍처

### 2.1 컴포넌트

| 컴포넌트 | 역할 |
|---|---|
| Airflow | DAG 스케줄링 및 태스크 오케스트레이션 |
| Livy (Spark) | 소파일 병합 실행 (`livy_cluster`) |
| Impala | 대상 테이블 조회 및 partition refresh (`cluster1`, 다중 클러스터) |
| HDFS | base / temp / backup 경로 관리 |
| PostgreSQL | 테이블 메타정보(`table_meta`) 및 병합 로그(`merge_log`) 저장 (`pg` connection) |

### 2.2 HDFS 경로 구조

| 경로 | 위치 | 설명 |
|---|---|---|
| base | `{domain_path}{save_path}/{table_name}` | 실제 서비스 데이터 경로 |
| temp | `hdfs://temp_path.com/impala/merge/temp/{table_name}` | Spark 병합 결과 임시 저장 |
| backup | `hdfs://temp_path.com/impala/merge/backup/{table_name}` | swap 전 기존 데이터 백업 |

### 2.3 PostgreSQL 테이블

**table_meta** — 병합 대상 테이블 메타정보

| 컬럼 | 설명 |
|---|---|
| `table_id` | 테이블 식별자 (Variable의 `table_id`와 매핑) |
| `db` | Impala 데이터베이스명 |
| `table_name` | Impala 테이블명 (Impala 내부는 `{table_name}_t`) |
| `save_path` | HDFS 저장 경로 (`/`로 시작) |
| `partitions` | 파티션 컬럼 목록 (콤마 구분, 첫번째가 날짜 컬럼) |
| `domain` | 도메인 식별자 (`path1`, `path2` 등) |

**merge_log** — 병합 전후 row count 기록

| 컬럼 | 설명 |
|---|---|
| `table_id` | 테이블 식별자 |
| `target_date` | 병합 대상 날짜 |
| `before_count` | 병합 전 row count |
| `after_count` | 병합 후 row count |
| `create_time` | 최초 기록 시각 |
| `update_time` | 최종 업데이트 시각 |

---

## 3. DAG 목록 및 스케줄

| DAG ID | 유형 | 스케줄 | Airflow Variable |
|---|---|---|---|
| `Small-File-Merge-Daily-2am` | 일별 | 매일 02:00 | `daily_table_2am_config` |
| `Small-File-Merge-Daily-3am` | 일별 | 매일 03:00 | `daily_table_3am_config` |
| `Small-File-Merge-Monthly-Day1` | 월별 | 매월 1일 01:00 | `monthly_merge_table_config_day1` |
| `Small-File-Merge-Monthly-Day2` | 월별 | 매월 2일 01:00 | `monthly_merge_table_config_day2` |

> 스케줄 설계 원칙: 월별(01:00) → 일별(02:00~)로 시간차를 두어 동일 테이블 동시 실행 방지.

---

## 4. Airflow Variables 설정

### 4.1 refresh_flags

클러스터별 Impala refresh 활성화 여부. DAG 시작 시 로드.

```json
[
  {"cluster": "cluster1", "flag": true},
  {"cluster": "cluster2", "flag": true},
  {"cluster": "cluster3", "flag": false}
]
```

- `flag: true` — health check 후 refresh 수행
- `flag: false` — 해당 클러스터 skip (점검 중인 클러스터 임시 제외 등에 사용)
- 모든 클러스터가 `flag: false`이면 DAG fail

### 4.2 일별 테이블 설정 (`daily_table_{H}am_config`)

```json
[
  {
    "table_id": 1,
    "days_ago": 1,
    "sort_columns": "col1,col2",
    "compression": "snappy"
  },
  {
    "table_id": 2,
    "days_ago": 1,
    "compression": "zstd"
  }
]
```

| 필드 | 필수 | 설명 |
|---|---|---|
| `table_id` | ✓ | `table_meta.table_id`와 매핑 |
| `days_ago` | ✓ | 실행일 기준 N일 전 파티션 병합 (보통 1) |
| `sort_columns` | - | Spark 정렬 컬럼 (콤마 구분). 미입력 시 정렬 없음 |
| `compression` | - | 압축 형식. 기본값 `snappy` |

### 4.3 월별 테이블 설정 (`monthly_merge_table_config_day{N}`)

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

| 필드 | 필수 | 설명 |
|---|---|---|
| `table_id` | ✓ | `table_meta.table_id`와 매핑 |
| `months_ago` | ✓ | 실행월 기준 N개월 전 전체 파티션 병합 (보통 1) |
| `sort_columns` | - | 위와 동일 |
| `compression` | - | 위와 동일 |

---

## 5. 테이블 추가 / 변경

### 5.1 신규 테이블 병합 추가

**Step 1.** PostgreSQL `table_meta`에 테이블 등록

```sql
INSERT INTO table_meta (table_id, db, table_name, save_path, partitions, domain)
VALUES (
    10,                -- 신규 table_id (기존과 중복 불가)
    'mydb',
    'my_table',
    '/mydb/my_table',  -- '/'로 시작
    'dt,hour',         -- 첫번째가 날짜 컬럼
    'path1'
);
```

**Step 2.** Airflow Variable에 항목 추가

Airflow UI → Admin → Variables → 해당 Variable 편집

```json
[
  {"table_id": 10, "days_ago": 1, "compression": "snappy"}
]
```

**Step 3.** Impala에서 테이블 존재 여부 확인

```sql
SHOW TABLES IN mydb LIKE 'my_table_t';
```

health check가 `{table_name}_t` 형식으로 조회하므로 Impala 테이블명이 일치해야 함.

### 5.2 테이블 제거

1. 해당 Variable에서 `table_id` 항목 삭제
2. `table_meta`에서 행 삭제 (선택, 이력 보존 목적으로 유지 가능)

### 5.3 compression / sort_columns 변경

Variable의 해당 항목 수정 후 다음 실행부터 반영됨. 재기동 불필요.

---

## 6. DAG 추가

### 6.1 일별 DAG 추가 (새 시각 추가)

`merge_daily_dag.py` 하단에 한 줄 추가:

```python
create_daily_dag('Small-File-Merge-Daily-4am', 'daily_table_4am_config', '0 4 * * *')
```

Variable `daily_table_4am_config`를 Airflow에 등록.

### 6.2 월별 DAG 추가 (새 실행일 추가)

`merge_monthly_dag.py` 하단에 한 줄 추가:

```python
create_monthly_dag('Small-File-Merge-Monthly-Day3', 'monthly_merge_table_config_day3', '0 1 3 * *')
```

Variable `monthly_merge_table_config_day3`를 Airflow에 등록.

### 6.3 새 도메인 추가

`merge_daily_dag.py`와 `merge_monthly_dag.py` 두 파일의 `DOMAIN_PATH_MAP` 모두 수정:

```python
DOMAIN_PATH_MAP = {
    "path1": "hdfs://path1.com/impala",
    "path2": "hdfs://path2.com/impala",
    "path3": "hdfs://path3.com/impala",  # 추가
}
```

---

## 7. 장애 대응

### 7.1 태스크별 실패 원인 및 조치

#### `load_refresh_flags_task` 실패

| 원인 | 조치 |
|---|---|
| Variable `refresh_flags` 없음 또는 파싱 오류 | Airflow UI에서 Variable 내용 확인 및 수정 |
| 모든 클러스터 `flag: false` | 최소 1개 이상 `flag: true`로 설정 후 재실행 |

#### `impala_health_check_task` 실패

| 원인 | 조치 |
|---|---|
| Impala 연결 실패 | 해당 클러스터 상태 확인. 점검 중이면 `refresh_flags`에서 `flag: false` 처리 |
| 테이블 존재하지 않음 (`table not exist`) | Impala에서 `{table_name}_t` 테이블 존재 여부 확인. `table_meta`의 `db`, `table_name` 값 확인 |

#### `count_before` 실패

| 원인 | 조치 |
|---|---|
| Impala 연결 오류 | `base_cluster`(`cluster1`) 상태 확인 후 재시도 |
| 대상 날짜 데이터 없음 (skip) | 정상 동작. 이후 태스크 전체 skip됨 |

#### `livy_task` 실패

| 원인 | 조치 |
|---|---|
| Livy 배치 제출 실패 | Livy 클러스터 상태 확인 |
| Spark 작업 실패 (`state != SUCCESS`) | Livy UI에서 해당 배치 로그 확인. 배치명: `Small-file-merge-daily-{table_name}` 또는 `monthly-` |
| 동명 배치 잔존 (retry 시) | Livy에서 동명 배치 수동 종료 후 재시도 |

#### `get_partitions_task` 실패

| 원인 | 조치 |
|---|---|
| manifest 파일 없음 | `livy_task` 성공 여부 재확인. temp 경로 직접 조회: `hdfs dfs -ls {temp_path}/manifest/` |
| manifest JSON 파싱 오류 | manifest 파일 내용 확인 |

#### `swap_refresh_task` 실패

| 원인 | 조치 |
|---|---|
| `base→backup` 이동 실패 | base 또는 backup 경로 상태 확인. [수동 복구 절차](#8-수동-복구-절차-hdfs) 참고 |
| `temp→base` 이동 실패 + 롤백 성공 | 로그에 롤백 완료 메시지 확인 후 원인 분석, 재시도 |
| `temp→base` 이동 실패 + 롤백 실패 | **수동 복구 필요**. [수동 복구 절차](#8-수동-복구-절차-hdfs) 참고 |
| 클러스터간 count 불일치 | `AirflowException`으로 자동 retry. 복제 지연이면 retry에서 해소됨. 반복 실패 시 각 클러스터 count 직접 조회 |
| `merge_log` 업데이트 실패 | PostgreSQL 연결 상태 확인 |

### 7.2 클러스터 점검 시 처리

1. Airflow UI → Admin → Variables → `refresh_flags` 편집
2. 점검 클러스터를 `"flag": false`로 변경
3. 점검 완료 후 `"flag": true`로 복원
4. 점검 기간 실행된 DAG는 해당 클러스터 refresh 없이 완료되므로, 복원 후 수동 refresh 필요 시 Impala에서 직접 실행

---

## 8. 수동 복구 절차 (HDFS)

롤백까지 실패한 경우 (`base→backup`은 성공, `temp→base`는 실패, `backup→base` 롤백도 실패) 아래 순서로 수동 복구.

```bash
# 1. backup → base 복원 (기존 데이터 복구)
hdfs dfs -mv hdfs://temp_path.com/impala/merge/backup/{table_name}/{part1}={date} \
             {base_path}/{part1}={date}

# 2. (선택) temp 경로 정리
hdfs dfs -rm -r hdfs://temp_path.com/impala/merge/temp/{table_name}/{part1}={date}

# 3. Impala partition refresh
REFRESH {db}.{table_name}_t PARTITION ({part1}='{date}');
```

복구 후 Airflow에서 해당 태스크를 **Clear** 하여 재실행.

> `{base_path}`는 `table_meta.save_path` 기준으로 `{domain_path}{save_path}/{table_name}` 형식.

---

## 9. 모니터링

### 9.1 병합 결과 확인 (merge_log)

```sql
-- 특정 테이블의 최근 병합 이력
SELECT target_date, before_count, after_count, update_time
FROM merge_log
WHERE table_id = 1
ORDER BY target_date DESC
LIMIT 10;

-- before/after count 차이가 큰 경우 (소파일 병합 효과 확인)
SELECT m.table_id, t.table_name, m.target_date,
       m.before_count, m.after_count,
       m.before_count - m.after_count AS reduced
FROM merge_log m
JOIN table_meta t USING (table_id)
WHERE m.target_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY reduced DESC;

-- after_count = 0 인 이상 케이스 조회
SELECT * FROM merge_log
WHERE after_count = 0 AND before_count > 0
ORDER BY update_time DESC;
```

### 9.2 Airflow UI 확인 포인트

- DAG 실행 이력: 각 DAG의 Grid View에서 성공/실패/skip 확인
- 월별 DAG `swap_refresh_task`: `[0]`, `[1]`, ... 각 날짜 인스턴스 개별 확인 가능
- skip된 테이블: `count_before` 태스크가 노란색(skipped)이면 해당 날짜 데이터 없음

### 9.3 알람

태스크 실패 시 `dag_failure_alarm` 콜백이 자동 호출되어 알람 전송.
알람 내용: DAG ID, Task ID, 실행일시, 오류 메시지, 로그 URL.

---

## 10. 주의사항 및 운영 규칙

| 항목 | 내용 |
|---|---|
| 일별/월별 시간 분리 | 월별 01:00, 일별 02:00~. 동일 테이블을 동시에 실행하지 않도록 스케줄 설계 |
| 중복 등록 금지 | 동일 `table_id`를 여러 Variable에 등록하면 중복 병합 발생. Variable 수정 시 전체 목록 교차 확인 |
| `DOMAIN_PATH_MAP` 동기화 | `merge_daily_dag.py`와 `merge_monthly_dag.py` 두 파일 모두 수정 |
| DAG 중복 실행 방지 | `max_active_runs=1`로 설정. backfill 시 동시에 여러 날짜 실행되지 않음 |
| Livy 배치명 중복 | retry 시 동명 배치가 Livy에 잔존할 수 있음. `create_livy_batch` wrapper 동작 확인 필요 |
| Airflow 3 전환 시 | `data_interval_end.in_timezone()` → `in_tz()`, `execution_date` → `logical_date` 변경 필요 |
