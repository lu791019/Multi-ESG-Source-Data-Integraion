import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
import calendar
from models import engine


connect_eco_string = engine.get_connect_string()
db_eco = create_engine(connect_eco_string, echo=True)


def insertcol_raw(df, unit, period_start, source_type):

    df['unit'] = str(unit)
    df['period_start'] = period_start
    df['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    df['type'] = str(source_type)

    return df


def site2plant_ratio(df, col, plant_cnt):

    df['sum'] = df.groupby([str(col)])['cnt'].transform('sum')
    df['day_count'] = df[str(col)].map(plant_cnt)
    df['cnt'] = df['sum'] / df['day_count']

    df.drop(['period_start', 'sum', 'day_count'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    df['ratio'] = df['cnt'].div(df['cnt'].sum())

    return df


def update_to_raw(df_target, raw_table, period_start, plant):

    try:

        conn = db_eco.connect()
        conn.execute(
        f"""Delete From raw.{raw_table} where period_start = '{period_start}' AND plant in ('WCD-1','WVN-1') """)
        conn.execute(
            f"""Delete From raw.{raw_table} where period_start = '{period_start}' AND plant = '{plant}' """)
        df_target.to_sql(str(raw_table), conn, index=False,
                         if_exists='append', schema='raw', chunksize=10000)
        conn.close()

        return True

    except:
        return False


def payrollcnt_other(plant_exclude, period_start, period_end):

    df = pd.read_sql(
        f"""select plant,(dl+idl) as "amount",period_start from raw.employeeinfo_count where plant not in {plant_exclude} and period_start >='{period_start}' and period_start <= '{period_end}' """, db_eco)

    plant_cnt = dict(df['plant'].value_counts())

    df['sum'] = df.groupby(['plant'])['amount'].transform('sum')

    df['day_count'] = df['plant'].map(plant_cnt)

    df['amount'] = df['sum'] / df['day_count']

    df.drop(['period_start', 'sum', 'day_count'], axis=1, inplace=True)

    df.drop_duplicates(inplace=True)

    return df


def payrollcnt_etl(site, unit='人', source_type='employee_info'):

    if dt.now().month == 1:
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")
    else:
        period_start = date(dt.now().year, dt.now().month -
                            1, 1).strftime("%Y-%m-%d")
        period_end = date(dt.now().year, dt.now().month-1,
                          calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

    try:

        if site == 'WKS':

            site_include = 'WKS'
            plant = ('WKS-5', 'WKS-6')

            df = pd.read_sql(
                f"""select site, period_start, dl+idl as "cnt" from raw.employeeinfo_count where period_start >='{period_start}' and period_start <= '{period_end}' and site ='{site_include}' and plant = '{site_include}' """, db_eco)

            df['site'] = site
            df_plant_cnt = dict(df['site'].value_counts())
            df = site2plant_ratio(df, 'site', df_plant_cnt)
            df.drop(['ratio'], axis=1, inplace=True)

            payrollcnt = pd.read_sql(
                f"""select plant,period_start, (dl+idl) as "cnt" from raw.employeeinfo_count where period_start >='{period_start}' and period_start <= '{period_end}' and plant in {plant} """, db_eco)
            plant_cnt = dict(payrollcnt['plant'].value_counts())
            df_ratio = site2plant_ratio(payrollcnt, 'plant', plant_cnt)

            df_ratio['site'] = site
            payrollcnt = df_ratio.merge(df, on=['site'], how='left')
            payrollcnt['amount'] = payrollcnt['cnt_x'] + \
                (payrollcnt['cnt_y']*payrollcnt['ratio'])
            payrollcnt.drop(['ratio', 'site', 'cnt_x', 'cnt_y'],
                            axis=1, inplace=True)

        elif site == 'WZS':

            site_include = ('WZS', 'WZSOPT')
            plant = ('WZS-1', 'WZS-3', 'WZS-6', 'WZS-8')

            df = pd.read_sql(
                f"""select period_start, sum(dl+idl) as "cnt" from raw.employeeinfo_count where period_start >='{period_start}' and period_start <= '{period_end}' and site in {site_include} and plant in {site_include} group by period_start""", db_eco)

            df['site'] = site
            df_plant_cnt = dict(df['site'].value_counts())
            df = site2plant_ratio(df, 'site', df_plant_cnt)
            df.drop(['ratio'], axis=1, inplace=True)

            payrollcnt = pd.read_sql(
                f"""select plant,period_start, sum(dl+idl) as "cnt" from raw.employeeinfo_count where period_start >='{period_start}' and period_start <= '{period_end}' and plant in {plant} group by plant,period_start """, db_eco)
            plant_cnt = dict(payrollcnt['plant'].value_counts())
            df_ratio = site2plant_ratio(payrollcnt, 'plant', plant_cnt)

            df_ratio['site'] = site
            payrollcnt = df_ratio.merge(df, on=['site'], how='left')
            payrollcnt['amount'] = payrollcnt['cnt_x'] + \
                (payrollcnt['cnt_y']*payrollcnt['ratio'])
            payrollcnt.drop(['ratio', 'site', 'cnt_x', 'cnt_y'],
                            axis=1, inplace=True)

        elif site == 'other':

            plant_exclude = ('WMY', 'WKS', 'WKS-1', 'WKS-5', 'WKS-6',
                             'WZS', 'WZS-1', 'WZS-3', 'WZS-6', 'WZS-8', 'WZSOPT')

            payrollcnt = payrollcnt_other(
                plant_exclude, period_start, period_end)

        payrollcnt.reset_index(inplace=True, drop=True)

        payrollcnt = insertcol_raw(payrollcnt, unit, period_start, source_type)

        if payrollcnt.size != 0:
            for i in range(0, len(payrollcnt)):
                update_to_raw(payrollcnt[payrollcnt['plant'] == payrollcnt['plant']
                                         [i]], 'payrollcnt', period_start, payrollcnt['plant'][i])

        return True

    except:
        return False


def livingcnt_etl(unit='人', source_type='dormAPI'):

    if dt.now().month == 1:
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")
    else:
        period_start = date(dt.now().year, dt.now().month -
                            1, 1).strftime("%Y-%m-%d")
        period_end = date(dt.now().year, dt.now().month-1,
                          calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

    try:

        site = 'WKS'

        plant = 'Site_KS'

        plant_include = ('WKS-5', 'WKS-6')

        df = pd.read_sql(
            f"""select amount as cnt,version as period_start from raw.living_hc where version >='{period_start}' and version <= '{period_end}' and plant = '{plant}' """, db_eco)

        df['site'] = site
        df_plant_cnt = dict(df['site'].value_counts())
        df = site2plant_ratio(df, 'site', df_plant_cnt)
        df.drop(['ratio'], axis=1, inplace=True)

        living_wks = pd.read_sql(
            f"""select plant,amount as cnt,version as period_start from raw.living_hc where version >='{period_start}' and version <= '{period_end}' and plant in {plant_include} """, db_eco)
        plant_cnt = dict(living_wks['plant'].value_counts())
        df_ratio = site2plant_ratio(living_wks, 'plant', plant_cnt)

        df_ratio['site'] = site
        living_wks = df_ratio.merge(df, on=['site'], how='left')
        living_wks['cnt'] = living_wks['cnt_x'] + \
            (living_wks['cnt_y']*living_wks['ratio'])
        living_wks.drop(['ratio', 'site', 'cnt_x', 'cnt_y'],
                        axis=1, inplace=True)

        plant_exclude = ('Site_KS', 'WKS-1', 'WKS-5', 'WKS-6')

        living = pd.read_sql(
            f"""select plant,amount as cnt,version as period_start from raw.living_hc where version >='{period_start}' and version <= '{period_end}' and plant not in {plant_exclude} """, db_eco)

        plant_cnt = dict(living['plant'].value_counts())

        living = site2plant_ratio(living, 'plant', plant_cnt)

        living.drop(['ratio'], axis=1, inplace=True)

        living = living.append(living_wks).reset_index(drop=True)

        living.rename(columns={'cnt': 'amount'}, inplace=True)

        living = insertcol_raw(living, unit, period_start, source_type)

        if living.size != 0:
            for i in range(0, len(living)):
                update_to_raw(living[living['plant'] == living['plant'][i]],
                              'livingcnt', period_start, living['plant'][i])

        return True

    except:
        return False
