from pycarol import Carol, ApiKeyAuth, PwdAuth, CDSStaging, Connectors
from pycarol import Tasks
import random
import time
import os
from dotenv import load_dotenv
from joblib import Parallel, delayed
from functions import sheet_utils, carol_login, carol_apps, carol_task, custom_pipeline
import argparse
from slacker_log_handler import SlackerLogHandler
import logging
from functools import reduce

load_dotenv('.env', override=True)

def run(domain, org='totvstechfin'):
    # avoid all tasks starting at the same time.
    time.sleep(round(1 + random.random() * 6, 2))
    org = 'totvstechfin'
    app_name = "techfinplatform"
    app_version = '0.0.63'
    connector_name = 'protheus_carol'
    connector_group = 'protheus'

    consolidate_list = ['se1', 'fk1', 'se2', 'fk2']

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

    current_cell = sheet_utils.find_tenant(sheet_utils.techfin_worksheet, domain)
    status = sheet_utils.techfin_worksheet.row_values(current_cell.row)[-1].strip().lower()

    skip_status = ['done', 'failed', 'wait', 'running', 'installing', 'reprocessing']
    if any(i in status for i in skip_status):
        logger.info(f"Nothing to do in {domain}, status {status}")
        return

    login = carol_login.get_login(domain, org)
    sheet_utils.update_start_time(sheet_utils.techfin_worksheet, current_cell.row)


    dag = custom_pipeline.get_dag()
    dag = list(reduce(set.union, custom_pipeline.get_dag()))
    dms = [i.replace('DM_', '') for i in dag if i.startswith('DM_')]
    staging_list = [i for i in dag if not i.startswith('DM_')]

    current_version = carol_apps.get_app_version(login, app_name, app_version)

    if current_version != app_version:
        # Dropping stagings.
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - drop stagings")
        logger.info(f"Starting process {domain}")
        st = carol_task.get_all_stagings(login, connector_name=connector_name)
        st = [i for i in st if i.startswith('se1_') or i.startswith('se2_')]
        tasks, fail = carol_task.drop_staging(login, staging_list=st, logger=logger)
        if fail:
            logger.error(f"error dropping staging {domain}")
            sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - dropping stagings")
            return
        try:
            task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
        except Exception as e:
            logger.error("error dropping staging", exc_info=1)
            sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - dropping stagings")
            return

        # Drop ETL SE1, SE2.
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - drop ETLs")
        to_drop = ['se1', 'se2']
        to_delete = [i for i in carol_task.get_all_etls(login, connector_name=connector_name) if
                     (i['mdmSourceEntityName'] in to_drop)]

        try:
            carol_task.drop_etls(login, etl_list=to_delete)
        except:
            logger.error("error dropping ETLs", exc_info=1)
            sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - dropping ETLs")
            return


    # Stop pub/sub if any.
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - stop pubsub")
    try:
        carol_task.pause_and_clear_subscriptions(login, dms, logger)
    except Exception as e:
        logger.error("error stop pubsub", exc_info=1)
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - stop pubsub")
        return
    try:
        carol_task.play_subscriptions(login, dms, logger)
    except Exception as e:
        logger.error("error playing pubsub", exc_info=1)
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - playing pubsub")
        return


    # Install app.
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - app install")
    current_version = carol_apps.get_app_version(login, app_name, app_version)
    fail = False
    task_list = '__unk__'
    if current_version != app_version:
        logger.info(f"Updating app from {current_version} to {app_version}")
        sheet_utils.update_version(sheet_utils.techfin_worksheet, current_cell.row, current_version)
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - app install")
        task_list, fail = carol_apps.update_app(login, app_name, app_version, logger, connector_group=connector_group)
        sheet_utils.update_version(sheet_utils.techfin_worksheet, current_cell.row, app_version)
    else:
        logger.info(f"Running version {app_version}")
        sheet_utils.update_version(sheet_utils.techfin_worksheet, current_cell.row, app_version)

    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row,
                                  'failed - app install')
        sheet_utils.update_task_id(sheet_utils.techfin_worksheet, current_cell.row, task_list)
        return

    # Cancel unwanted tasks.
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - canceling tasks")
    pross_tasks = carol_task.find_task_types(login)
    pross_task = [i['mdmId'] for i in pross_tasks]
    if pross_task:
        carol_task.cancel_tasks(login, pross_task)

    # pause ETLs.
    carol_task.pause_etls(login, etl_list=staging_list, connector_name=connector_name, logger=logger)
    # pause mappings.
    carol_task.pause_dms(login, dm_list=dms, connector_name=connector_name,)

    # consolidate
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - consolidate")
    task_list = carol_task.consolidate_stagings(login, connector_name=connector_name, staging_list=consolidate_list,
                                    n_jobs=1, logger=logger)

    try:
        task_list, fail = carol_task.track_tasks(login, task_list, logger=logger)
    except Exception as e:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after app install", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after app install")
        return

    # delete DMs
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - delete DMs")
    task_list = carol_task.par_delete_golden(login, dm_list=dms, n_jobs=1)
    try:
        task_list, fail = carol_task.track_tasks(login, task_list, logger=logger)
    except Exception as e:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - delete DMs")
        logger.error("error after app install", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after app install")
        return


    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - processing")
    try:
        fail = custom_pipeline.run_custom_pipeline(login, connector_name=connector_name, logger=logger)
    except Exception:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after app install", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after app install")
        return


    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "Done")
    sheet_utils.update_end_time(sheet_utils.techfin_worksheet, current_cell.row)

    return task_list


if __name__ == "__main__":
    table = sheet_utils.techfin_worksheet.get_all_records()

    skip_status = ['done', 'failed', 'running', 'installing', 'reprocessing']

    table = [t['environmentName (tenantID)'].strip() for t in table
             if t.get('environmentName (tenantID)', None) is not None
             and t.get('environmentName (tenantID)', 'None') != ''
             and not any(i in t.get('Status', '').lower().strip() for i in skip_status)
             ]

    import multiprocessing

    pool = multiprocessing.Pool(6)
    pool.map(run, table)
    pool.close()
    pool.join()
