import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import *
import calendar
from models import engine

from models import engine, engine_source

connect_eco_string = engine.get_connect_string()
db_eco = create_engine(connect_eco_string, echo=True)

connect_csr_string = engine_source.get_connect_string_csr()
db_csr = create_engine(connect_csr_string, echo=True)


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


def site2plant_ratio(df, plant_cnt):
    df['sum'] = df.groupby(['plant'])['amount'].transform('sum')
    df['day_count'] = df['plant'].map(plant_cnt)
    df['amount'] = df['sum'] / df['day_count']

    df.drop(['period_start', 'sum', 'day_count'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    df['ratio'] = df['amount'].div(df['amount'].sum())

    return df


def update_ratio_data(df_target, category, period_start, plant, table='plant_ratio'):

    try:
        conn = db_eco.connect()
        conn.execute(
            f"""delete from raw.{table} where category = '{category}' and period_start = '{period_start}' and plant in {plant}""")

        df_target.to_sql(str(table), con=db_eco, schema='raw',
                         if_exists='append', index=False, chunksize=1000)
        conn.close()

        return True

    except:
        return False


def plant_ratio_cal(df, category, start_time, WKS_plant):

    plant_cnt = dict(df['plant'].value_counts())
    df = site2plant_ratio(df, plant_cnt)

    df['category'] = str(category)
    df['period_start'] = start_time
    df['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    update_ratio_data(df, str(category), start_time, WKS_plant)


def site2plant_ratio_cal(site, plant=('WKS-5', 'WKS-6')):

    if dt.now().month == 1:
        start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")
    else:
        start_time = date(dt.now().year, dt.now().month -
                          1, 1).strftime("%Y-%m-%d")
        end_time = date(dt.now().year, dt.now().month-1,
                        calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

    WKS_plant = plant

    try:
        df_living = pd.read_sql(
            f"""select plant,amount,version as period_start from raw.living_hc where plant in {WKS_plant} and plant in (select plant_name from raw.plant_mapping pm where site ='{site}') and version >='{start_time}' and version <= '{end_time}' """, db_eco)
        df_employee = pd.read_sql(
            f"""select plant,(dl+idl) as "amount",period_start from raw.employeeinfo_count where plant in {WKS_plant} and plant in (select plant_name from raw.plant_mapping pm where site ='{site}') and period_start >='{start_time}' and period_start <= '{end_time}' """, db_eco)

        plant_ratio_cal(df_living, '宿舍', start_time, WKS_plant)
        plant_ratio_cal(df_employee, '廠區', start_time, WKS_plant)

        return True

    except:
        return False


def FEM_plant_ratio(site, consumetype, category, plant=('WKS-5', 'WKS-6')):

    if dt.now().month == 1:
        start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")
    else:
        start_time = date(dt.now().year, dt.now().month -
                          1, 1).strftime("%Y-%m-%d")
        end_time = date(dt.now().year, dt.now().month-1,
                        calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

    try:
        FEM = pd.read_sql(
            f"SELECT DISTINCT plant as plant_code,power as amount FROM raw.wks_mfg_fem_dailypower WHERE consumetype = '用電量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{start_time}' AND datadate <= '{end_time}' AND site in ('{site}') AND plant not in ('{site}')", con=db_eco)
        plant_mapping = pd.read_sql(
            f"""SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping where site in ('WKS')""", con=db_eco)

        FEM = FEM.groupby('plant_code').sum().reset_index()

        FEM = FEM.merge(plant_mapping, on='plant_code', how='left')
        FEM.dropna(inplace=True)
        FEM['ratio'] = FEM['amount'].div(FEM['amount'].sum())
    #         FEM['item'] = '用電量'
        FEM['item'] = str(consumetype)
    #         FEM['category'] = '廠區'
        FEM['category'] = str(category)
        FEM['period_start'] = start_time

        return FEM

    except:
        return False


def csr_insert(df_target, raw_table, period_start, plant, schema):

    try:

        conn = db_eco.connect()
        conn.execute(
            f"""Delete From {schema}.{raw_table} where period_start = '{period_start}' and plant = '{plant}'""")
        df_target.to_sql(str(raw_table), conn, index=False,
                         if_exists='append', schema=str(schema), chunksize=10000)
        conn.close()

        return True

    except:
        return False


def wks_insert(df_target, raw_table, period_start, plant, schema):

    try:

        conn = db_eco.connect()
        conn.execute(
            f"""Delete From {schema}.{raw_table} where period_start = '{period_start}' and plant in {plant}""")
        df_target.to_sql(str(raw_table), conn, index=False,
                         if_exists='append', schema=str(schema), chunksize=10000)
        conn.close()

        return True

    except:
        return False


def csr_detail_integration_pre(item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6')):

    # period_start = (date(dt.now().year, dt.now().month, 1) -
    #                 relativedelta(months=1)).strftime("%Y-%m-%d")
    '''
    WKS拆分邏輯
    -----------
    廠區
        用電 FEM工廠用電比例
        用水 計薪人力
    宿舍
        用電 住宿人力
        用水 住宿人力
    '''
    try:
        if site == 'WKS':

            plant_ratio = pd.read_sql(
                f"""select plant, category, ratio, period_start from raw.plant_ratio where plant in {WKS_plant} and period_start = '{period_start}'""", db_eco)
            df_detail = pd.read_sql(
                f"""SELECT item, category, unit, site, amount, period_start FROM raw.csr_kpidetail where site in ('{site}') and period_start = '{period_start}'""", db_eco)

            if df_detail.shape[0] > 0:

                df_detail['period_start'] = df_detail['period_start'].astype(
                    str)
                plant_ratio['period_start'] = plant_ratio['period_start'].astype(
                    str)

                csr = df_detail.merge(
                    plant_ratio, on=['category', 'period_start'], how='left')

                csr.dropna(inplace=True)

                csr['amount'] = csr['ratio'] * csr['amount']

                csr = csr[['item', 'category', 'unit',
                           'amount', 'period_start', 'plant']]

                csr = csr[(csr['item'] != 'Electricity')
                          | (csr['category'] != '廠區')]

                FEM_ratio = pd.read_sql(f"""SELECT plant, category, ratio, period_start FROM raw.fem_ratio where plant in ('WKS-5', 'WKS-6') and period_start = '{period_start}'""", db_eco)

                # FEM_ratio = FEM_plant_ratio(
                #     'WKS', '用電量', '廠區', plant=('WKS-1', 'WKS-5', 'WKS-6'))

                FEM_ratio = FEM_ratio[['plant', 'ratio', 'category', 'period_start']]
                FEM_ratio['category'] = '廠區'
                FEM_ratio['period_start'] = FEM_ratio['period_start'].astype(str)

                fem_elec = df_detail.merge(
                    FEM_ratio, on=['period_start', 'category'], how='left')
                fem_elec.dropna(inplace=True)
                fem_elec['amount'] = fem_elec['amount'] * fem_elec['ratio']
                fem_elec = fem_elec[['item', 'category',
                                    'unit', 'amount', 'period_start', 'plant']]
                fem_elec = fem_elec[(fem_elec['item'] == 'Electricity')
                                    & (fem_elec['category'] == '廠區')]

                csr = csr.append(fem_elec)

                # water/elec to raw
                csr_item = csr[['category', 'amount', 'period_start',
                                'plant']][csr['item'] == str(item)]
                csr_item['last_update_time'] = dt.strptime(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                if csr_item.size != 0:
                    wks_insert(csr_item, str(raw_target1),
                               period_start, WKS_plant, 'raw')

                csr['amount'] = csr.groupby(['plant', 'unit', 'period_start', 'item'])[
                    'amount'].transform('sum')
                csr = csr[['item', 'period_start', 'plant', 'amount', 'unit']]
                csr.drop_duplicates(inplace=True)

                csr_app = csr[['amount', 'period_start', 'plant', 'unit']
                              ][csr['item'] == str(item)]
                csr_app['type'] = 'CSR'
                csr_app['last_update_time'] = dt.strptime(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                if csr_app.size != 0:
                    wks_insert(csr_app, str(raw_target2),
                               period_start, WKS_plant, schema='raw')

        else:

            df_detail = pd.read_sql(
                f"""SELECT item, category, unit, site AS plant, amount, period_start FROM raw.csr_kpidetail where site = '{site}' and period_start = '{period_start}'""", db_eco)

            if df_detail.shape[0] > 0:
                csr_item = df_detail[['category', 'amount',
                                      'period_start', 'plant']][df_detail['item'] == str(item)]
                csr_item['last_update_time'] = dt.strptime(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                if csr_item.size != 0:
                    csr_insert(csr_item, str(raw_target1),
                               period_start, site, 'raw')
        return True

    except:
        return False

def useful_datetime(i):

    period_start = (date(2022, 6, 1) + relativedelta(months=i-1)).strftime("%Y-%m-%d")
    last_year_period_start = (date(2021, 6, 1) + relativedelta(months=i-1)).strftime("%Y-%m-%d")
    period_start1 = (date(2022, 6, 1) + relativedelta(months=i-1)).strftime("%Y%m%d")
    period = (date(2021, 6, 1) + relativedelta(months=i-1)).strftime("%Y-%m")
    period_year = (date(2022, 6, 1) + relativedelta(months=i-1)).strftime("%Y")

    return period_start, last_year_period_start, period_start1, period, period_year

def csr_detail_integration(item, raw_target1, raw_target2, stage, site='WKS', WKS_plant=('WKS-5', 'WKS-6')):

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
                csr_detail_integration_pre(item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))
                current_date += relativedelta(months=1)


        #     if current_month == 1:

        #         for i in range(2, dt.now().month+3):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_detail_integration_pre(
        #                 item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))
        # #

        #     else:

        #         for i in range(2, dt.now().month+1):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_detail_integration_pre(
        #                 item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))
        #

        else:

            start_date = dt(2022, 6, 1)
            end_date = dt(dt.now().year, dt.now().month -1, 1)

            current_date = start_date
            while current_date <= end_date:


                i = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month) + 1

                period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(i)
                csr_detail_integration_pre(item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))
                current_date += relativedelta(months=1)

        #     if current_month == 1:

        #         for i in range(1, dt.now().month+2):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_detail_integration_pre(
        #                 item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))
        # #

        #     else:

        #         for i in range(1, dt.now().month):

        #             period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
        #                 i)
        #             csr_detail_integration_pre(
        #                 item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))

        return True

    except:

        return False
