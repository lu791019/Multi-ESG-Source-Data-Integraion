import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from dateutil.relativedelta import relativedelta

from models import engine

connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)


def useful_datetime(i):

    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%Y-%m-%d")

    return period_start


def update_to_scope1(shcema, df_target, raw_table, period_start, plant):

    conn = db_eco.connect()
    conn.execute(
        f"""Delete From {shcema}.{raw_table} where period_start = '{period_start}' AND plant = '{plant}' AND category ='scope1'""")
    df_target.to_sql(str(raw_table), conn, index=False,
                     if_exists='append', schema=shcema, chunksize=10000)
    conn.close()


def update_to_raw(shcema, df_target, raw_table, period_start, plant):

    conn = db_eco.connect()
    conn.execute(
        f"""Delete From {shcema}.{raw_table} where period_start = '{period_start}' AND plant = '{plant}'""")
    df_target.to_sql(str(raw_table), conn, index=False,
                     if_exists='append', schema=shcema, chunksize=10000)
    conn.close()


def fix_scope1(current_months, staging_table='carbon_emission'):

    period_start_new = useful_datetime(current_months)

    period_start_old = useful_datetime(5)

    scope1_new = pd.read_sql(
        f"""SELECT * FROM staging.{staging_table} where category ='scope1' and period_start = '{period_start_new}'""", con=db_eco)
    scope1_old = pd.read_sql(
        f"""SELECT * FROM staging.{staging_table} where category ='scope1' and period_start = '{period_start_old}'""", con=db_eco)

    for i in set(scope1_old['plant']).difference(scope1_new['plant']):
        if scope1_old['amount'][scope1_old['plant'] == str(i)].sum() > 0:
            scope1_new['plant'] = str(i)
            scope1_new['amount'] = 0
            scope1_new['period_start'] = period_start_new
            scope1_new['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            scope1_new['category'] = 'scope1'
            scope1_new.drop_duplicates(inplace=True)
            update_to_scope1('staging', scope1_new,
                             staging_table, period_start_new, i)


def fix_raw(current_months, raw_table):

    period_start_new = useful_datetime(current_months)

    period_start_old = useful_datetime(6)

    df_new = pd.read_sql(f"""SELECT * FROM raw.{raw_table} where  period_start = '{period_start_new}'""", con=db_eco)
    df_new['type'] = ''
    # include WKS-1
    df_old = pd.read_sql(f"""SELECT * FROM raw.{raw_table} where  period_start = '{period_start_old}'""", con=db_eco)
    df_old['type'] = ''

    df_fix = df_new.copy()

    if raw_table == 'revenue':
        df_fix.drop(columns = ['id'],inplace=True)

    if raw_table == 'renewable_energy':
        df_fix.drop(columns = ['id'],inplace=True)

    for i in set(df_old['plant']).difference(df_new['plant']):


        df_fix['plant'] = str(i)
        df_fix['amount'] = 0
        df_fix['period_start'] = period_start_new
        df_fix['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        df_fix.drop_duplicates(inplace=True)


        if df_fix.size !=0:

            update_to_raw('raw', df_fix, raw_table, period_start_new, i)

def change_plant_process(table_name,start_time,schema='raw'):
    #廠區異動 2023後的資料 WCD to WCD-1、WVN to WVN-1、 WMI 不分廠
    plant_dict = {'WVN': 'WVN-1', 'WCD': 'WCD-1', 'WMI-1':'WMI','WMI-2':'WMI'}

    # connect_string = engine.get_connect_string()
    # db = create_engine(connect_string, echo=True)

    df = pd.read_sql(f"""SELECT * FROM {schema}.{table_name} WHERE period_start >= '{start_time}'""", db_eco)
    # df.drop(columns=['last_update_time'],inplace=True)
    df = df.replace({'plant': plant_dict})
    df.drop_duplicates(inplace = True)
    df1 = df[~((df['amount'] == 0) & (df.duplicated(subset='period_start')))]
    group_col = df1.columns.tolist()
    group_col.remove('amount')
    if table_name=='revenue':
        group_col.remove('id')
    df1 = df1.groupby(group_col).sum().reset_index()

    df2 = df[((df['amount'] == 0) & (df.duplicated(subset='period_start')))]

    df_final = df1.append(df2).reset_index(drop=True)



    conn = db_eco.connect()
    conn.execute(f"DELETE FROM {schema}.{table_name} WHERE period_start >= '{start_time}'")
    df_final.to_sql(table_name, conn, index=False, if_exists='append', schema=schema, chunksize=10000)
    conn.close()


def current_csr_update(table_name):

    plant_dict = {'WMYP2': 'WMY-2', 'WMYP1': 'WMY-1', 'WIHK2': 'WIHK-2', 'WIHK1': 'WIHK-1','WMIP1':'WMI-1','WMIP2':'WMI-2','WCD':'WCD-1'}

    if table_name == 'electricity_total':

        df_csr = pd.read_sql(f"""SELECT sitename as "plant", indicatorvalue as "amount", updatedate as "update", period_start FROM raw.csr_electricity_indicator where sitename not in ('WKS','WZS')""",db_eco)

        df_csr.dropna(inplace=True)

        if df_csr.size !=0:

            df_csr = df_csr[df_csr['update'].dt.date>=dt.now().date()].reset_index(drop=True)

            df_csr = df_csr.replace({'plant': plant_dict})

            df_csr = df_csr[['plant', 'amount','period_start']]

            df_csr['unit'] = '度'
            df_csr['type'] = 'CSR'

            df_csr['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            df_csr['period_start'] = df_csr['period_start'].astype(str)

            condition_col1 = df_csr['period_start']
            condition_col2 = df_csr['plant']


            if len(condition_col1) == 0 or len(condition_col2) == 0:
                # print('no exec')
                pass

            elif df_csr.size ==0:
                pass

            else:

                delete_query = f"""DELETE FROM raw.electricity_total WHERE plant IN {tuple(condition_col2)} AND period_start IN {tuple(condition_col1)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_csr.to_sql('electricity_total', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close()


    if table_name == 'water':

        df_csr = pd.read_sql(f"""SELECT sitename as "plant", indicatorvalue as "amount", updatedate as "update", period_start FROM raw.csr_water_indicator where sitename not in ('WKS','WZS')""",db_eco)

        df_csr.dropna(inplace=True)

        if df_csr.size !=0:

            df_csr = df_csr[df_csr['update'].dt.date>=dt.now().date()].reset_index(drop=True)

            df_csr = df_csr.replace({'plant': plant_dict})

            df_csr = df_csr[['plant', 'amount','period_start']]

            df_csr['unit'] = '立方米'
            df_csr['type'] = 'CSR'

            df_csr['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            df_csr['period_start'] = df_csr['period_start'].astype(str)

            condition_col1 = df_csr['period_start']
            condition_col2 = df_csr['plant']

            if len(condition_col1) == 0 or len(condition_col2) == 0:
                # print('no exec')
                pass

            elif df_csr.size ==0:
                pass

            else:

                delete_query = f"""DELETE FROM raw.water WHERE plant IN {tuple(condition_col2)} AND period_start IN {tuple(condition_col1)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_csr.to_sql('water', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close