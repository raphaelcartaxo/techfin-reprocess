from toposort import toposort_flatten, toposort
from functools import reduce

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

    rel = {i: set(j) for i, j in rel.items()}
    dag_order = toposort(rel)

    return dag_order



def get_stagings():
    dag = list(reduce(set.union, get_dag()))
