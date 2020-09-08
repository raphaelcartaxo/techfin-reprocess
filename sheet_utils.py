import gspread
import datetime


def find_tenant(techfin_worksheet, domain):
    if techfin_worksheet is None:
        return
    try:
        match = techfin_worksheet.find(domain)
        return match
    except gspread.CellNotFound:
        return


def update_status(techfin_worksheet, row, status):
    col = 9
    techfin_worksheet.update_cell(row, col, status)


def update_start_time(techfin_worksheet, row):
    col = 6
    techfin_worksheet.update_cell(row, col, str(datetime.datetime.now())[:-7])


def update_end_time(techfin_worksheet, row):
    col = 7
    techfin_worksheet.update_cell(row, col, str(datetime.datetime.now())[:-7])


def update_version(techfin_worksheet, row, version):
    col = 3
    techfin_worksheet.update_cell(row, col, version)
