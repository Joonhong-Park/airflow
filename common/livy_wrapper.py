import json
import logging
import time

import requests
from airflow.hooks.base import BaseHook
from livy import LivyBatch, SessionState

log = logging.getLogger(__name__)


def get_airflow_connection(cluster):
    try:
        livy_connection = BaseHook.get_connection(cluster)
        livy_host = livy_connection.host
        livy_port = livy_connection.port
        livy_extra = json.loads(livy_connection.extra)

        return {'host': livy_host, 'port': livy_port, 'extra': livy_extra}
    except Exception as e:
        log.error(e)
        return None


def create_livy_batch(name, file, cluster, args=None, py_files=None, files=None,
                      driver_memory='1G', driver_cores=1, executor_memory='1G',
                      executor_cores=1, num_executors=1, queue=None, spark_conf=None):
    livy_info = get_airflow_connection(cluster)

    livy_url = f'https://{livy_info["host"]}:{livy_info["port"]}'
    if queue is None:
        livy_queue = livy_info['extra']['queue']
    else:
        livy_queue = queue

    livy_spark_conf = livy_info['extra']['conf']
    if spark_conf is not None:
        livy_spark_conf = livy_spark_conf | spark_conf

    livy_batch = LivyBatch.create(
        url=livy_url,
        file=file,
        name=name,
        args=args,
        py_files=py_files,
        files=files,
        driver_memory=driver_memory,
        driver_cores=driver_cores,
        executor_memory=executor_memory,
        executor_cores=executor_cores,
        num_executors=num_executors,
        queue=livy_queue,
        conf=livy_spark_conf
    )

    for i in range(60):
        if livy_batch.state == SessionState.RUNNING:
            return True
        time.sleep(1)

    livy_batch.kill()
    log.error(f"Livy batch '{name}' did not reach RUNNING state within timeout")

    return False


def get_livy_batch_id_by_name(name, cluster):
    livy_batch_id = -1
    livy_info = get_airflow_connection(cluster)
    livy_url = f'https://{livy_info["host"]}:{livy_info["port"]}'
    livy_params = {'size': livy_info['extra']['size']}
    livy_response = requests.get(f'{livy_url}/batches', params=livy_params)
    if livy_response.status_code != 200:
        raise Exception(f"Livy API returned status {livy_response.status_code}")
    livy_dict = livy_response.json()

    for entry in livy_dict['sessions']:
        livy_id = entry['id']
        livy_name = entry['name']
        livy_state = entry['state']

        if livy_name == name:
            return (livy_id, livy_state)

    return (livy_batch_id, None)


def get_livy_batch_by_id(id, cluster):
    livy_info = get_airflow_connection(cluster)
    livy_url = f'https://{livy_info["host"]}:{livy_info["port"]}'
    livy_response = requests.get(f'{livy_url}/batches/{id}')
    if livy_response.status_code != 200:
        log.error(f"Livy API returned status {livy_response.status_code} for batch id {id}")
        return None
    return LivyBatch(url=livy_url, batch_id=id)
