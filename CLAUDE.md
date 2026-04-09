# Airflow DAGs

## 디렉토리 구조

```
airflow/
├── common/     # 공통 유틸리티 (Impala, Livy, Postgres 래퍼)
├── merge/      # 소파일 병합 DAG (일별, 월별)
└── <future>/   # 이후 추가되는 DAG는 별도 디렉토리로 관리
```

## 규칙

- 공통으로 사용하는 wrapper/utility는 `common/`에 위치
- 각 DAG 그룹은 독립된 디렉토리에서 관리하며, 해당 디렉토리에 `CLAUDE.md` 포함
- 새로운 DAG 추가 시 디렉토리 생성 후 `CLAUDE.md` 작성

## Airflow 연결 정보

- Postgres 연결: `pg` (connection id)
- Base 클러스터: `cluster1`
- Livy 클러스터: `livy_cluster`
