# dags/impala_connection_test.py

from __future__ import annotations

import json
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from impala.dbapi import connect

log = logging.getLogger(__name__)

IMPALA_CONN_IDS: list[str] = [
    "impala_cluster1_ddl",
    "impala_cluster1_user",
    "impala_cluster2_ddl",
    "impala_cluster2_user",
]


@dag(
    dag_id="impala_connection_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["impala", "connection", "test"],
)
def impala_connection_test():

    @task()
    def test_connection(conn_id: str) -> dict:
        conn  = BaseHook.get_connection(conn_id)
        extra = json.loads(conn.extra) if conn.extra else {}

        try:
            with connect(
                host=conn.host,
                port=conn.port or 21050,
                user=conn.login,
                password=conn.password,
                auth_mechanism=extra.get("auth_mechanism", "LDAP"),
                use_ssl=extra.get("ssl", False),
                timeout=30,
            ) as impala_conn:
                with impala_conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    row = cursor.fetchone()

            log.info("[성공] %s | SELECT 1 = %s", conn_id, row)
            return {"conn_id": conn_id, "success": True, "message": str(row)}

        except Exception as e:
            log.error("[실패] %s | %s", conn_id, e)
            return {"conn_id": conn_id, "success": False, "message": str(e)}

    @task(trigger_rule="all_done")
    def summarize(results: list[dict]) -> None:
        success = [r for r in results if r["success"]]
        fail    = [r for r in results if not r["success"]]

        log.info("=" * 60)
        log.info("총 %d건 | 성공 %d건 | 실패 %d건", len(results), len(success), len(fail))
        for r in fail:
            log.info("  [실패] %s | %s", r["conn_id"], r["message"])
        log.info("=" * 60)

        if fail:
            raise RuntimeError(f"접속 실패: {[r['conn_id'] for r in fail]}")

    test_results = [test_connection(conn_id=c) for c in IMPALA_CONN_IDS]
    summarize(results=test_results)


impala_connection_test()