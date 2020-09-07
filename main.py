import sys
from pycarol import Carol, ApiKeyAuth, DataModel, Connectors, Staging, PwdAuth, CDSStaging
from pycarol import Tasks
import random, time
import os
from dotenv import load_dotenv
from joblib import Parallel, delayed
import sheet_utils


load_dotenv('.env', override=True)
import argparse
from slacker_log_handler import SlackerLogHandler
import logging
from pycarol.apps import Apps
from collections import defaultdict

import gspread


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


def update_settings(login):
    app = Apps(login)
    app_name = 'techfinplatform'
    app_id = app.get_by_name(app_name)['mdmId']

    data = [{"mdmName": "skip_pause", "mdmParameterValue": False},
            {"mdmName": "clean_dm", "mdmParameterValue": True},
            {"mdmName": "clean_etls", "mdmParameterValue": True}]

    settings_id = login.call_api(path=f'v1/tenantApps/{app_id}/settings', )['mdmId']

    _ = login.call_api(path=f'v1/tenantApps/{app_id}/settings/{settings_id}?publish=true', method='PUT', data=data)


def get_login(domain, org):

    email = os.environ['CAROLUSER']
    password = os.environ['CAROLPWD']
    carol_app = 'techfinplatform'
    login = Carol(domain, carol_app, auth=PwdAuth(email, password), organization=org, )
    api_key = login.issue_api_key()

    login = Carol(domain, carol_app, auth=ApiKeyAuth(api_key['X-Auth-Key']),
                  connector_id=api_key['X-Auth-ConnectorId'], organization=org, )
    return login

def par_consolidate(login, staging_name, connector_name):
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
    task_id = cds_stag.consolidate(staging_name=staging_name, connector_name=connector_name, worker_type=worker_type,
                                   number_shards=number_shards, rehash_ids=True, max_number_workers=max_number_workers )
    return task_id

def consolidate_staggins(login):

    current_cell = sheet_utils.find_tenant(techfin_worksheet, login.domain)
    main_tables = ['ar1', 'cko', 'company', 'ct1', 'ctl', 'ctt', 'currency', 'cv3', 'cvd', 'fk1',
                   'fk2', 'fk5', 'fk7', 'fkc', 'fkd', 'frv', 'invoicexml', 'mapping', 'organization',
                   'paymentstype', 'sa1', 'sa2', 'sa6', 'sd1', 'sd2', 'se1', 'se2', 'se8', 'sea', 'sf1', 'sf2',
                   'sf4', 'protheus_sharing']

    tasks = defaultdict(list)
    task_id = Parallel(n_jobs=5, backend='threading')(delayed(par_consolidate)(login, staging_name=i, connector_name='protheus_carol')
                                 for i in main_tables)
    tasks[domain].extend(task_id)
    task_list = [i['data']['mdmId'] for i in tasks[login.domain]]

    try:
        task_list, fail = track_tasks(login, task_list)
    except:
        fail = True

    if fail:
        logger.error(f"Problem with {login.domain} durring consolidate." )
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED')
        exit(1)

    return task_list

def track_tasks(login, task_list, do_not_retry=False):

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


def run_app(login, app_name, process_name, ):
    current_cell = sheet_utils.find_tenant(techfin_worksheet, login.domain)

    app = Apps(login)
    app_id = app.get_by_name(app_name)['mdmId']

    params = {"entitySpace": "WORKING", "checkAllSpaces": True}
    process_id = login.call_api(f"v1/tenantApps/{app_id}/aiprocesses", method='GET', params=params)["mdmId"]
    process_id = login.call_api(f"v1/tenantApps/{app_id}/aiprocesses/{process_id}/execute/{process_name}",
                                method='POST')["data"]["mdmId"]

    try:
        task_list, fail = track_tasks(login, [process_id], do_not_retry=True)
    except:
        fail=True

    if fail:
        logger.error(f"Problem with {login.domain} during Processing.")
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED')
        exit(1)
    return task_list


def update_app(login, app_name, app_version):

    current_cell = sheet_utils.find_tenant(techfin_worksheet, login.domain)

    to_install = login.call_api("v1/tenantApps/subscribableCarolApps", method='GET')
    to_install = [i for i in to_install['hits'] if i["mdmName"] == app_name]
    if to_install:
        to_install = to_install[0]
        assert to_install["mdmAppVersion"]==app_version
        to_install_id = to_install['mdmId']
    else:
        logger.error("Error trying to update app")
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED')
        exit(1)
    updated = login.call_api(f"v1/tenantApps/subscribe/carolApps/{to_install_id}", method='POST')
    params = {"publish": True, "connectorGroup": "protheus"}
    install_task = login.call_api(f"v1/tenantApps/{updated['mdmId']}/install", method='POST', params=params)
    install_task = install_task['mdmId']

    try:
        task_list, fail = track_tasks(login, [install_task])
    except:
        fail = True

    if fail:
        logger.error(f"Problem with {login.domain} during App installation.")
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED')
        exit(1)
    return task_list



def get_app_version(login, app_name, version):
    app = Apps(login)
    app_info = app.get_by_name(app_name)
    return app_info['mdmAppVersion']

if __name__ == "__main__":

    domain = args.tenant
    org = args.org
    skip_consolidate = args.skip_consolidate

    app_name = "techfinplatform"
    process_name = "processAll"
    app_version = '0.0.54'

    # Create slack handler
    slack_handler = SlackerLogHandler(os.environ["SLACK"], '#techfin-reprocess', #"@rafael.rui",
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
    if not current_cell:
        logger.error(f"{login.domain} not found in the sheet")
        raise ValueError

    sheet_utils.update_start_time(techfin_worksheet, current_cell.row)
    logger.info(f"Starting process {domain}")

    #Intall app.
    current_version = get_app_version(login, app_name, app_version)
    if current_version!=app_version:
        logger.info(f"Updating app from {current_version} to {app_version}")
        sheet_utils.update_version(techfin_worksheet, current_cell.row, current_version)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "Installing+consolidate+appRunning")
        update_app(login, app_name, app_version)
        sheet_utils.update_version(techfin_worksheet, current_cell.row, app_version)
    else:
        logger.info(f"Running version {app_version}")
        sheet_utils.update_version(techfin_worksheet, current_cell.row, app_version)

    #Update app params.
    update_settings(login)

    #consolidate.
    if not skip_consolidate:
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "Consolidating + appRunning")
        logger.info(f"Staging consolidate {domain}")
        _ = consolidate_staggins(login)
        logger.info(f"Done consolidate {domain}")
    sheet_utils.update_status(techfin_worksheet, current_cell.row, "Process")
    logger.info(f"Starting app {domain}")

    #run app.
    _ = run_app(login, app_name=app_name, process_name=process_name)
    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(techfin_worksheet, current_cell.row, "Done")
    sheet_utils.update_end_time(techfin_worksheet, current_cell.row)
