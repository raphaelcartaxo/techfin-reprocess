from toposort import toposort_flatten, toposort
from . import carol_task
from pycarol import CDSStaging, Connectors
from functools import reduce
import logging
import time

def get_dag():
    rel = {}
    rel['DM_arinvoice'] = ['se1_invoice']
    rel['se1_invoice'] = ['se1']
    rel['DM_arinvoiceinstallment'] = ['se1_installments', 'DM_arinvoice']
    rel['se1_installments'] = ['DM_arinvoice', 'se1']
    rel['DM_arinvoicepayments'] = [
        'DM_arinvoiceinstallment',
        'fk1',
        'fk5_estorno_transferencia_pagamento',
        'fk5_transferencia',
        'fkd_1',
        'fkd_deletado',
        'se1_acresc',
        'se1_decresc',
        'se1_payments',
        'se1_payments_abatimentos',
        'sea_1_frv_descontado_deletado_invoicepayment',
        'sea_1_frv_descontado_naodeletado_invoicepayment']

    rel['fk1'] = ['DM_arinvoiceinstallment']
    rel['fk5_estorno_transferencia_pagamento'] = ['DM_arinvoiceinstallment']
    rel['fk5_transferencia'] = ['DM_arinvoiceinstallment']
    rel['fkd_1'] = ['DM_arinvoiceinstallment']
    rel['fkd_deletado'] = ['DM_arinvoiceinstallment']
    rel['se1_acresc'] = ['DM_arinvoiceinstallment', 'se1']
    rel['se1_decresc'] = ['DM_arinvoiceinstallment', 'se1']
    rel['se1_payments'] = ['DM_arinvoiceinstallment', 'se1']
    rel['se1_payments_abatimentos'] = ['DM_arinvoiceinstallment', 'se1']
    rel['sea_1_frv_descontado_deletado_invoicepayment'] = ['DM_arinvoiceinstallment']
    rel['sea_1_frv_descontado_naodeletado_invoicepayment'] = ['DM_arinvoiceinstallment']

    # APs
    rel['DM_apinvoice'] = ['se2_invoice']
    rel['se2_invoice'] = ['se2']

    rel['DM_apinvoiceinstallment'] = ['DM_apinvoice', 'se2_installments']
    rel['se2_installments'] = ['DM_apinvoice', 'se2']

    rel['DM_apinvoicepayments'] = [
        'DM_apinvoiceinstallment',
        'fk2',
        'se2_acresc',
        'se2_decresc',
        'se2_payments',
        'se2_payments_abatimentos'
    ]

    rel['fk2'] = ['DM_apinvoiceinstallment']
    rel['se2_acresc'] = ['DM_apinvoiceinstallment', 'se2']
    rel['se2_decresc'] = ['DM_apinvoiceinstallment', 'se2']
    rel['se2_payments'] = ['DM_apinvoiceinstallment', 'se2']
    rel['se2_payments_abatimentos'] = ['DM_apinvoiceinstallment', 'se2']

    rel['sea_1_frv_descontado_deletado_payments_bank'] = ['DM_apinvoicepayments']
    rel['sea_1_frv_descontado_naodeletado_payments_bank'] = ['DM_apinvoicepayments']
    rel['DM_arpaymentsbank'] = ['DM_apinvoicepayments', 'sea_1_frv_descontado_deletado_payments_bank', 'sea_1_frv_descontado_naodeletado_payments_bank']

    rel = {i: set(j) for i, j in rel.items()}
    dag_order = toposort(rel)

    return dag_order


def run_custom_pipeline(login, connector_name, logger):

    if logger is None:
        logger = logging.getLogger(login.domain)
    dag = get_dag()

    for p in dag:
        dm = [i for i in p if i.startswith('DM_')]
        stagings = [i for i in p if not i.startswith('DM_')]
        tasks = []
        if dm:
            #play integration
            # TODO: reprocess rejected?
            pass

        for staging_name in stagings:
            mappings_ = carol_task.resume_process(login, connector_name=connector_name,
                                                  staging_name=staging_name,logger=logger, delay=1)
        #wait for play.
        time.sleep(120)


        for staging_name in stagings:
            logger.debug(f"processing {staging_name}")
            task_id = CDSStaging(login).process_data(staging_name, connector_name=connector_name, max_number_workers=16,
                                                     delete_target_folder=False, delete_realtime_records=False,
                                                     recursive_processing=False)
            tasks += [task_id['data']['mdmId']]

        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)

        if fail:
            return True

    return False



