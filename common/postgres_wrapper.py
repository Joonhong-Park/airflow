import logging
from contextlib import contextmanager

import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


@contextmanager
def get_db_connection(connection_id):
    conn = None
    hook = PostgresHook(postgres_conn_id=connection_id)

    try:
        conn = hook.get_conn()
        yield conn

    except (psycopg2.Error, Exception) as e:
        log.error(e)
        raise

    finally:
        if conn is not None:
            try:
                conn.close()
            except psycopg2.Error as e:
                log.error(e)


def postgres_query(connection_id, statement, fetch_result=False, commit=False):
    results = None
    try:
        with get_db_connection(connection_id) as conn:
            with conn.cursor() as cursor:
                cursor.execute(statement)
                if fetch_result:
                    results = cursor.fetchall()
                else:
                    log.debug(f"쿼리 실행 성공. no fetch")
            if commit:
                conn.commit()
            return results
    except psycopg2.Error as e:
        log.error(e)
        raise
