from pycarol import Carol, ApiKeyAuth, PwdAuth, Tasks, Staging, Connectors
from collections import defaultdict
import random
import time
import logging

def track_tasks(login, task_list, do_not_retry=False, logger=None):
    if logger is None:
        logger = logging.getLogger(login.tenant)

    retry_tasks = defaultdict(int)
    n_task = len(task_list)
    max_retries = set()
    carol_task = Tasks(login)
    while True:
        task_status = defaultdict(list)
        for task in task_list:
            status = carol_task.get_task(task).task_status
            task_status[status].append(task)
        for task in task_status['FAILED'] + task_status['CANCELED']:
            logger.warning(f'Something went wrong while processing: {task}')
            retry_tasks[task] += 1
            if do_not_retry:
                logger.error(f'Task: {task} failed. It wll not be restarted.')
                continue
            if retry_tasks[task] > 3:
                max_retries.update([task])
                logger.error(f'Task: {task} failed 3 times. will not restart')
                continue

            logger.info(f'Retry task: {task}')
            login.call_api(path=f'v1/tasks/{task}/reprocess', method='POST')

        if len(task_status['COMPLETED']) == n_task:
            logger.debug(f'All task finished')
            return task_status, False

        elif len(max_retries) + len(task_status['COMPLETED']) == n_task:
            logger.warning(f'There are {len(max_retries)} failed tasks.')
            return task_status, True
        else:
            time.sleep(round(10 + random.random() * 5, 2))
            logger.debug('Waiting for tasks')


def drop_staging(login, staging_list, logger=None):
    """
    Drop a list of stagings

    Args:
        login: pycarol.Carol
            Carol() instance.
        staging_list: list
            List of stagings to drop
        logger:
            Logger to be used. If None will use
                logger = logging.getLogger(login.tenant)

    Returns: list, status
        List of tasks created, fail status.

    """

    if logger is None:
        logger = logging.getLogger(login.tenant)

    tasks = []
    for i in staging_list:
        stag = Staging(login)
        try:
            r = stag.drop_staging(staging_name=i, connector_name='protheus_carol', )
            tasks += [r['taskId']]
            logger.debug(f"dropping {i} - {r['taskId']}")
        except Exception as e:
            logger.error("error dropping staging", exc_info=1)
            return tasks, True

    return tasks, False

def get_all_stagings(login, connector_name):
    """
    Get all staging tables from a connector.

    Args:
        login: pycarol.Carol
            Carol() instance.
        connector_name: str
            Connector Name

    Returns: list
        list of staging for the connector.

    """

    conn_stats = Connectors(login).stats(connector_name=connector_name)
    st = [i for i in list(conn_stats.values())[0]]
    return sorted(st)

def get_all_etls(login, connector_name):
    """
    get all ETLs from a connector.

    Args:
        login: pycarol.Carol
            Carol() instance.
        connector_name: str
            Connector Name

    Returns: list
        list of ETLs

    """

    connector_id = Connectors(login).get_by_name(connector_name)['mdmId']
    etls = login.call_api(f'v1/etl/connector/{connector_id}', method='GET')
    return etls

def drop_etls(login, etl_list):
    """

    Args:
        login: login: pycarol.Carol
            Carol() instance.
        etl_list: list
            list of ETLs to delete.

    Returns: None

    """
    for i in etl_list:
        mdm_id = i['mdmId']
        try:
            #Delete drafts.
            login.call_api(f'v2/etl/{mdm_id}', method='DELETE', params={'entitySpace': 'WORKING'})
        except Exception as e:
            pass
        login.call_api(f'v2/etl/{mdm_id}', method='DELETE', params={'entitySpace': 'PRODUCTION'})


