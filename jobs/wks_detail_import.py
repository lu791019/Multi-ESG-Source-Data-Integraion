
import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from dateutil.relativedelta import relativedelta
from jobs.plant_cal import FEM_plant_ratio

from models import engine, engine_source

connect_csr_string = engine_source.get_connect_string_csr()
connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)
db_csr = create_engine(connect_csr_string, echo=True)


def wks_detail_update(df_target, raw_table, period_start, update_plant):

    try:

        conn = db_eco.connect()
        conn.execute(
            f"""Delete From raw.{raw_table} where period_start = '{period_start}' and plant in {tuple(update_plant)}""")
        df_target.to_sql(str(raw_table), conn, index=False,
                         if_exists='append', schema='raw', chunksize=10000)
        conn.close()

        return True

    except Exception as e:
        error = str(e)
        print(error)

        return False


# def useful_datetime(i):

#     period_start = (date(dt.now().year, dt.now().month, 1) -
#                     relativedelta(months=i)).strftime("%Y-%m-%d")
#     last_year_period_start = (date(
#         dt.now().year-1, dt.now().month, 1) - relativedelta(months=i)).strftime("%Y-%m-%d")
#     period_start1 = (date(dt.now().year, dt.now().month, 1) -
#                      relativedelta(months=i)).strftime("%Y%m%d")
#     period = (date(dt.now().year-1, dt.now().month, 1) -
#               relativedelta(months=i)).strftime("%Y-%m")
#     period_year = (date(dt.now().year, dt.now().month, 1) -
#                    relativedelta(months=i)).strftime("%Y")

#     return period_start, last_year_period_start, period_start1, period, period_year





def csr_item_import_pre(item, raw_table, WKS_plant, period_start):



    # WKS_plant = ('WKS-5', 'WKS-6')

    plant_ratio = pd.read_sql(
        f"""select plant, category, ratio, period_start from raw.plant_ratio where plant in  ('WKS-1','WKS-5', 'WKS-6') and period_start = '{period_start}'""", db_eco)

    df_detail = pd.read_sql(
        f"""SELECT item, category, unit, site, amount, period_start FROM raw.csr_kpidetail where period_start = '{period_start}'""", db_eco)

    df_detail = df_detail[df_detail['site'] == 'WKS']

    df_detail['period_start'] = df_detail['period_start'].astype(str)

    plant_ratio['period_start'] = plant_ratio['period_start'].astype(str)

    csr = df_detail.merge(
        plant_ratio, on=['category', 'period_start'], how='left')

    csr.dropna(inplace=True)

    csr['amount'] = csr['ratio'] * csr['amount']

    csr = csr[['item', 'category', 'unit',
               'amount', 'period_start', 'plant']]

    csr = csr[(csr['item'] != 'Electricity') | (csr['category'] != '廠區')]

    # FEM_ratio = FEM_plant_ratio(
    #     'WKS', '用電量', '廠區', plant=('WKS-5', 'WKS-6'))
    FEM_ratio = pd.read_sql(f"""SELECT plant, category, ratio, period_start FROM raw.fem_ratio where plant in  ('WKS-1','WKS-5', 'WKS-6') and period_start = '{period_start}'""", db_eco)
    FEM_ratio = FEM_ratio[['plant', 'ratio', 'category', 'period_start']]
    FEM_ratio['period_start'] = period_start
    FEM_ratio['period_start'] = FEM_ratio['period_start'].astype(str)
    df_detail['period_start'] = df_detail['period_start'].astype(str)
    FEM_ratio['category'] = '廠區'

    fem_elec = df_detail.merge(
        FEM_ratio, on=['period_start', 'category'], how='left')
    fem_elec.dropna(inplace=True)
    fem_elec['amount'] = fem_elec['amount'] * fem_elec['ratio']
    fem_elec = fem_elec[['item', 'category',
                         'unit', 'amount', 'period_start', 'plant']]
    fem_elec = fem_elec[(fem_elec['item'] == 'Electricity')
                        & (fem_elec['category'] == '廠區')]

    csr = csr.append(fem_elec)

    csr_backstage = csr.copy()

    csr_backstage['amount'] = csr_backstage.groupby(
        ['plant', 'unit', 'period_start', 'item'])['amount'].transform('sum')

    csr_backstage = csr_backstage[[
        'item', 'period_start', 'plant', 'amount']]

    csr_backstage.drop_duplicates(inplace=True)

    csr_elec = csr_backstage[['amount', 'period_start', 'plant']
                             ][csr_backstage['item'] == str(item)]

    csr_elec['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    csr_elec['unit'] = '度'

    csr_elec['type'] = 'CSR'

    update_plant = csr_elec['plant']

    if csr_elec.size != 0:
        wks_detail_update(csr_elec, str(raw_table), period_start, update_plant)

def useful_datetime(i):

    period_start = (date(2022, 6, 1) + relativedelta(months=i-1)).strftime("%Y-%m-%d")
    last_year_period_start = (date(2021, 6, 1) + relativedelta(months=i-1)).strftime("%Y-%m-%d")
    period_start1 = (date(2022, 6, 1) + relativedelta(months=i-1)).strftime("%Y%m%d")
    period = (date(2021, 6, 1) + relativedelta(months=i-1)).strftime("%Y-%m")
    period_year = (date(2022, 6, 1) + relativedelta(months=i-1)).strftime("%Y")

    return period_start, last_year_period_start, period_start1, period, period_year


def csr_item_import(item, raw_table, WKS_plant, stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    current_month = dt.now().month
    current_day = dt.now().day

    if stage == 'development':  # DEV - 10號更新上個月
        checkpoint = 10
    else:  # PRD - 15號更新上個月
        checkpoint = 12

    try:

        if current_day < checkpoint:

            start_date = dt(2022, 6, 1)
            end_date = dt(dt.now().year, dt.now().month -2, 1)

            current_date = start_date
            while current_date <= end_date:


                i = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month) + 1

                period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(i)

                csr_item_import_pre(item, raw_table, WKS_plant, period_start)

                current_date += relativedelta(months=1)

        #     if current_month == 1:

        #         for i in range(2, dt.now().month+3):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_item_import_pre(
        #                 item, raw_table, WKS_plant, period_start)
        # #

        #     else:

        #         for i in range(2, dt.now().month+1):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_item_import_pre(
        #                 item, raw_table, WKS_plant, period_start)
        #

        else:

            start_date = dt(2022, 6, 1)
            end_date = dt(dt.now().year, dt.now().month -1, 1)

            current_date = start_date
            while current_date <= end_date:


                i = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month) + 1

                period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(i)

                csr_item_import_pre(item, raw_table, WKS_plant, period_start)

                current_date += relativedelta(months=1)

        #     if current_month == 1:

        #         for i in range(1, dt.now().month+2):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_item_import_pre(
        #                 item, raw_table, WKS_plant, period_start)
        # #

        #     else:

        #         for i in range(1, dt.now().month):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_item_import_pre(
        #                 item, raw_table, WKS_plant, period_start)

        return True

    except:

        return False
