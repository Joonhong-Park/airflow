"""
Small File Merge Monthly DAG

매월 지정된 날짜에 실행되어 특정 달의 전체 파티션을 한 번에 병합한다.
병합은 Livy(Spark)로 월 전체를 처리하고, swap/refresh는 날짜별로 병렬 실행한다.

테이블 목록이 많은 경우를 대비해 실행일을 나눈 DAG로 분리한다.
    Day1: 매월 1일 새벽 1시 실행 (일별 DAG 00:00과 시간차 확보)
    Day2: 매월 2일 새벽 1시 실행
    ...

새 DAG 추가 시 파일 하단의 create_monthly_dag() 호출을 한 줄 추가하면 된다.

흐름 (테이블별 table_group):
    load_refresh_flags
        └─ [테이블별 table_group]
            ├─ get_metadata_task
            ├─ impala_health_check_task
            ├─ count_before (log_before_count_task)
            ├─ livy_task
            ├─ get_partitions_task
            └─ swap_refresh_task[0..N-1] (날짜별 동적 확장)

Airflow UI:
    swap_refresh_task는 .expand()로 확장되므로 UI에서 [0], [1], ... 블록으로 표시된다.
    각 인덱스는 target_date_list의 순서(월 첫째날부터)에 대응한다.
"""

import os
import sys

# merge/ 디렉토리를 sys.path에 추가하여 tasks 패키지를 참조 가능하게 한다.
_merge_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_merge_dir, '..', 'common'))  # common 래퍼
sys.path.insert(0, _merge_dir)                                 # tasks 패키지

from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.utils.weight_rule import WeightRule

# 모든 태스크는 tasks/monthly_tasks.py에서 import한다.
# daily DAG와 완전히 분리하여 merge_daily_dag를 직접 import하지 않는다.
from tasks.monthly_tasks import (
    dag_failure_alarm,
    load_refresh_flags_task,
    impala_health_check_task,
    get_metadata_task,
    log_before_count_task,
    livy_task,
    get_partitions_task,
    swap_refresh_task,
)


@task_group
def table_group(table_config, refresh_flags):
    """
    테이블 1개에 대한 월별 병합 전체 흐름을 묶은 task_group.

    merge_monthly_dag.py에서 테이블별로 table_group.override(group_id=...)로 호출된다.
    swap_refresh_task는 target_date_list의 각 날짜에 대해 동적으로 확장(.expand())된다.
    → Airflow 2.3+ 에서 XComArg subscript(metadata['target_date_list'])를 지원한다.
    """
    metadata = get_metadata_task(table_config)
    cluster_list = impala_health_check_task(metadata, refresh_flags)
    log_before = log_before_count_task.override(task_id="count_before")(metadata)
    livy_job = livy_task(metadata)
    partition_list = get_partitions_task(metadata)

    # swap_refresh_task는 target_date_list의 날짜 수만큼 동적으로 병렬 확장된다.
    # metadata['target_date_list']는 XComArg subscript로 Airflow 2.3+ 필요.
    swap_refresh = swap_refresh_task.partial(
        cluster_list=cluster_list,
        partition_list=partition_list,
        metadata=metadata,
    ).expand(target_date=metadata['target_date_list'])

    # impala_health_check 완료 후 count_before 실행 (cluster_list 의존)
    # 이후 livy → get_partitions → swap_refresh 순서로 직렬 실행
    cluster_list >> log_before >> livy_job >> partition_list >> swap_refresh


def create_monthly_dag(dag_id, config_variable, schedule):
    """
    월별 병합 DAG를 생성하는 팩토리 함수.

    Args:
        dag_id (str): Airflow DAG ID. 예: 'Small-File-Merge-Monthly-Day1'
        config_variable (str): 테이블 설정을 담은 Airflow Variable 이름.
                               예: 'monthly_merge_table_config_day1'
        schedule (str): cron 표현식. 예: '0 1 1 * *' (매월 1일 01:00)

    Returns:
        DAG 인스턴스 (Airflow가 전역 스코프에서 자동 인식)

    Variable 형식 예시:
        [
            {"table_id": 1, "months_ago": 1, "sort_columns": "col1,col2", "compression": "snappy"},
            {"table_id": 2, "months_ago": 1, "compression": "zstd"}
        ]
    """
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        default_args={
            'depends_on_past': False,
            'weight_rule': WeightRule.UPSTREAM,
            'on_failure_callback': dag_failure_alarm,
        },
        max_active_runs=1,      # 동일 DAG 중복 실행 방지
        max_active_tasks=10,
    )
    def monthly_merge_dag():
        refresh_flags_dict = load_refresh_flags_task()
        table_config_list = Variable.get(config_variable, deserialize_json=True, default_var=[])

        # Variable이 비어있으면 태스크 없이 DAG만 생성됨 (정상 동작)
        for table_config in table_config_list:
            table_id = table_config['table_id']
            table_group.override(group_id=f"table_{table_id}")(table_config, refresh_flags_dict)

    return monthly_merge_dag()


# ─────────────────────────────────────────────────────────────────────────────
# DAG 목록
# 테이블이 많아 실행일을 분산할 경우 아래에 한 줄씩 추가한다.
# 동일 테이블이 여러 dayN Variable에 중복 등록되지 않도록 운영 관리 필요.
# ─────────────────────────────────────────────────────────────────────────────
create_monthly_dag('Small-File-Merge-Monthly-Day1', 'monthly_merge_table_config_day1', '0 1 1 * *')
create_monthly_dag('Small-File-Merge-Monthly-Day2', 'monthly_merge_table_config_day2', '0 1 2 * *')
# create_monthly_dag('Small-File-Merge-Monthly-Day3', 'monthly_merge_table_config_day3', '0 1 3 * *')
