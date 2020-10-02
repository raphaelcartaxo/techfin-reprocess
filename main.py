from pycarol import Carol, ApiKeyAuth, PwdAuth, CDSStaging
from pycarol import Tasks
import random
import time
import os
from dotenv import load_dotenv
from joblib import Parallel, delayed
import sheet_utils
import argparse
from slacker_log_handler import SlackerLogHandler
import logging
from pycarol.apps import Apps
from collections import defaultdict
import gspread

load_dotenv('.env', override=True)

gc = gspread.oauth()
sh = gc.open("status_techfin_reprocess")
techfin_worksheet = sh.worksheet("status")
# folder with creds /Users/rafarui/.config/gspread


# Arguments to run via console.
parser = argparse.ArgumentParser(
    description='reprocess techfin tenants')
parser.add_argument("-t", '--tenant',
                    type=str,  # required=True,
                    help='Tenant Name')
parser.add_argument("-o", '--org',
                    type=str,  # required=True,
                    help='organization')

parser.add_argument("--skip-consolidate",
                    action='store_true',
                    help='Skip Consolidate')

args = parser.parse_args()




def get_login(domain, org):
    email = os.environ['CAROLUSER']
    password = os.environ['CAROLPWD']
    carol_app = 'techfinplatform'
    login = Carol(domain, carol_app, auth=PwdAuth(email, password), organization=org, )
    api_key = login.issue_api_key()

    login = Carol(domain, carol_app, auth=ApiKeyAuth(api_key['X-Auth-Key']),
                  connector_id=api_key['X-Auth-ConnectorId'], organization=org, )
    return login


def track_tasks(login, task_list, do_not_retry=False, logger=None):
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


def update_app(login, app_name, app_version, logger):
    current_cell = sheet_utils.find_tenant(techfin_worksheet, login.domain)

    #check if stall task is running.
    uri = 'v1/queries/filter?indexType=MASTER&scrollable=false&pageSize=25&offset=0&sortBy=mdmLastUpdated&sortOrder=DESC'

    query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": "mdmTask"},
                          {"mdmKey": "mdmTaskType.raw", "mdmFilterType": "TERMS_FILTER",
                           "mdmValue": ["INSTALL_CAROL_APP"]},
                          {"mdmKey": "mdmTaskStatus.raw", "mdmFilterType": "TERMS_FILTER", "mdmValue": ["RUNNING"]}],
             "mustNotList": [{"mdmKey": "mdmUserId.raw", "mdmFilterType": "MATCH_FILTER", "mdmValue": ""}],
             "shouldList": []}

    r = login.call_api(path=uri, method='POST', data=query)
    if len(r['hits']) >= 1:
        task_id = r['hits'][0]['mdmId']

    try:
        task_list, fail = track_tasks(login, [task_id], logger=logger)
    except:
        fail = True

    to_install = login.call_api("v1/tenantApps/subscribableCarolApps", method='GET')
    to_install = [i for i in to_install['hits'] if i["mdmName"] == app_name]
    if to_install:
        to_install = to_install[0]
        assert to_install["mdmAppVersion"] == app_version
        to_install_id = to_install['mdmId']
    else:
        logger.error("Error trying to update app")
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED - did not found app to install')
        return [], True

    updated = login.call_api(f"v1/tenantApps/subscribe/carolApps/{to_install_id}", method='POST')
    params = {"publish": True, "connectorGroup": "protheus"}
    install_task = login.call_api(f"v1/tenantApps/{updated['mdmId']}/install", method='POST', params=params)
    install_task = install_task['mdmId']
    task_list = []
    try:
        task_list, fail = track_tasks(login, [install_task], logger=logger)
    except:
        fail = True

    if fail:
        logger.error(f"Problem with {login.domain} during App installation.")
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED - app install')
        return [], fail

    return task_list, False


def par_processing(login, staging_name, connector_name, delete_realtime_records=False,
                   delete_target_folder=False):
    cds_stag = CDSStaging(login)
    n_r = cds_stag.count(staging_name=staging_name, connector_name=connector_name)
    if n_r > 5000000:
        worker_type = 'n1-highmem-16'
        max_number_workers = 16
    else:
        worker_type = 'n1-highmem-4'
        max_number_workers = 16
    number_shards = round(n_r / 100000) + 1
    number_shards = max(16, number_shards)
    task_id = cds_stag.process_data(staging_name=staging_name, connector_name=connector_name, worker_type=worker_type,
                                    number_shards=number_shards, max_number_workers=max_number_workers,
                                    delete_target_folder=delete_target_folder,
                                    delete_realtime_records=delete_realtime_records)
    return task_id


def get_app_version(login, app_name, version):
    app = Apps(login)
    app_info = app.get_by_name(app_name)
    return app_info['mdmAppVersion']


def run(domain, org='totvstechfin'):

    # avoid all tasks starting at the same time.
    time.sleep(round(1 + random.random() * 5, 4))
    org = 'totvstechfin'
    app_name = "techfinplatform"
    app_version = '0.0.58'
    connector_name = 'protheus_carol'
    # Create slack handler
    slack_handler = SlackerLogHandler(os.environ["SLACK"], '#techfin-reprocess',  # "@rafael.rui",
                                      username='TechFinBot')
    slack_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    slack_handler.setFormatter(formatter)
    logger = logging.getLogger(domain)
    logger.addHandler(slack_handler)
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

    login = get_login(domain, org)

    current_cell = sheet_utils.find_tenant(techfin_worksheet, login.domain)
    status = techfin_worksheet.row_values(current_cell.row)[-1]

    if 'done' in status.strip().lower() or 'failed' in status.strip().lower() or 'running' in status.strip().lower():
        logger.info(f"Nothing to do in {domain}")
        return

    sheet_utils.update_status(techfin_worksheet, current_cell.row, "Running")
    sheet_utils.update_start_time(techfin_worksheet, current_cell.row)

    logger.info(f"Starting process {domain}")

    # Intall app.
    current_version = get_app_version(login, app_name, app_version)
    fail = False
    if current_version != app_version:
        logger.info(f"Updating app from {current_version} to {app_version}")
        sheet_utils.update_version(techfin_worksheet, current_cell.row, current_version)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "Installing app")
        _, fail = update_app(login, app_name, app_version, logger)
        sheet_utils.update_version(techfin_worksheet, current_cell.row, app_version)
    else:
        logger.info(f"Running version {app_version}")
        sheet_utils.update_version(techfin_worksheet, current_cell.row, app_version)

    if fail:
        return

    to_reprocess = [
        'fk5_transferencia',
        'fkd_1',
        'se1_payments_abatimentos',
        'fk1',
        'se1_acresc_1',
        'fk5_estorno_transferencia_pagamento',
        'fkd_deletado',
        'se1_decresc_1',
        'se1_payments',
        'sea_1_frv_descontado_deletado_invoicepayment',
        'sea_1_frv_descontado_naodeletado_invoicepayment',
    ]

    sheet_utils.update_status(techfin_worksheet, current_cell.row, "reprocessing stagings")

    tasks_to_track = []
    for i, staging_name in enumerate(to_reprocess):
        if i == 0:
            task = par_processing(login, staging_name, connector_name, delete_realtime_records=True,
                                  delete_target_folder=True)
            time.sleep(5) #time to delete RT.
        else:
            task = par_processing(login, staging_name, connector_name, delete_realtime_records=False,
                                  delete_target_folder=False)
        tasks_to_track.append(task['data']['mdmId'])

    try:
        task_list, fail = track_tasks(login, tasks_to_track, logger=logger)
    except:
        fail = True

    if fail:
        logger.error(f"Problem with {login.domain} durring consolidate.")
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED')
        return

    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(techfin_worksheet, current_cell.row, "Done")
    sheet_utils.update_end_time(techfin_worksheet, current_cell.row)

    return task_list


if __name__ == "__main__":

    table = techfin_worksheet.get_all_records()
    table = [t['environmentName (tenantID)'] for t in table if t.get('environmentName (tenantID)', None) is not None]

    import multiprocessing
    pool = multiprocessing.Pool(6)
    pool.map(run, table)
    pool.close()
    pool.join()

