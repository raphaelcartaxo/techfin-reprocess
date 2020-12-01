from pycarol import Carol, ApiKeyAuth, PwdAuth, CDSStaging, Connectors
from pycarol import Tasks
import random
import time
import os
from dotenv import load_dotenv
from joblib import Parallel, delayed
from functions import sheet_utils, carol_login, carol_apps, carol_task, custom_pipeline, techfin_task
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
    app_version = '0.0.68'
    connector_name = 'protheus_carol'
    connector_group = 'protheus'

    consolidate_list = ['se1', 'se2', ]
    compute_transformations = True  # need to force the old data to the stagings transformation.

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

    if current_version != app_version and current_version < "0.0.63":
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
    carol_task.pause_dms(login, dm_list=dms, connector_name=connector_name, )
    time.sleep(round(10 + random.random() * 6, 2))  # pause have affect


    # consolidate
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - consolidate")
    task_list = carol_task.consolidate_stagings(login, connector_name=connector_name, staging_list=consolidate_list,
                                                n_jobs=1, logger=logger, compute_transformations=compute_transformations)

    try:
        task_list, fail = carol_task.track_tasks(login, task_list, logger=logger)
    except Exception as e:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after consolidate", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after consolidate")
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

    # delete stagings.
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - delete stagings")
    st = carol_task.get_all_stagings(login, connector_name=connector_name)
    st = [i for i in st if i.startswith('se1_') or i.startswith('se2_')]
    task_list = carol_task.par_delete_staging(login, staging_list=st, connector_name=connector_name, n_jobs=1)
    try:
        task_list, fail = carol_task.track_tasks(login, task_list, logger=logger)
    except Exception as e:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - delete stagings")
        logger.error("error after delete DMs", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - delete stagings")
        logger.error("error after delete DMs")
        return

    # delete DMs
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - delete DMs")
    task_list = carol_task.par_delete_golden(login, dm_list=dms, n_jobs=1)
    try:
        task_list, fail = carol_task.track_tasks(login, task_list, logger=logger)
    except Exception as e:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - delete DMs")
        logger.error("error after delete DMs", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - delete DMs")
        logger.error("error after delete DMs")
        return

    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - delete payments techfin")
    try:
        res = techfin_task.delete_payments(login.domain)
    except Exception as e:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - delete payments techfin")
        logger.error("error after delete payments techfin", exc_info=1)
        return


    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - processing")
    try:
        fail = custom_pipeline.run_custom_pipeline(login, connector_name=connector_name, logger=logger)
    except Exception:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after processing", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after processing")
        return

    sync_type = sheet_utils.get_sync_type(sheet_utils.techfin_worksheet, current_cell.row)
    if 'painel' in sync_type.lower().strip():
        sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "running - add pub/sub")
        try:
            techfin_task.add_pubsub(login.domain)
        except Exception:
            sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "failed - add pub/sub")
            logger.error("error after add pub/sub", exc_info=1)
            return

    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(sheet_utils.techfin_worksheet, current_cell.row, "Done")
    sheet_utils.update_end_time(sheet_utils.techfin_worksheet, current_cell.row)

    return task_list


if __name__ == "__main__":
    table = sheet_utils.techfin_worksheet.get_all_records()

    skip_status = ['done', 'failed', 'running', 'installing', 'reprocessing']

    # run("tenant70827589d8a611eabbf10a586460272f")

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

