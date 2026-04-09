import logging

from impala import dbapi
from impala.util import as_pandas
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


def impala_query(statement, cluster, fetch_result=False):
    try:
        impala_connstring = BaseHook.get_connection(cluster)
        host = impala_connstring.host
        port = impala_connstring.port
        user = impala_connstring.login
        password = impala_connstring.get_password()
    except Exception as e:
        log.error(f'fail to retrieve information of cluster {cluster}: {e}')
        return None

    try:
        with dbapi.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            auth_mechanism='LDAP'
        ) as impala_connection:
            with impala_connection.cursor() as impala_cursor:
                impala_cursor.execute(statement)

                if fetch_result:
                    return as_pandas(impala_cursor)
    except Exception as e:
        log.error(e)

    return None
