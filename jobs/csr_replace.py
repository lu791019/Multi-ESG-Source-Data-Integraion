import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from dateutil.relativedelta import relativedelta

from models import engine, engine_source


connect_csr_string = engine_source.get_connect_string_csr()
connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)
db_csr = create_engine(connect_csr_string, echo=True)

plant_exclude = ('WKS-1', 'WKS-5', 'WKS-6', 'WKS-6A',
                 'WKS-6B', 'WZS-1', 'WZS-3', 'WZS-6', 'WZS-8')


def update_to_raw(df_target, raw_table, period_start, plant):

    conn = db_eco.connect()
    conn.execute(
        f"""Delete From raw.{raw_table} where period_start = '{period_start}' AND plant in ('WCD-1') """)
    conn.execute(
        f"""Delete From raw.{raw_table} where period_start = '{period_start}' AND plant = '{plant}' """)
    df_target.to_sql(str(raw_table), conn, index=False,
                     if_exists='append', schema='raw', chunksize=10000)
    conn.close()


def useful_datetime(i):
    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%Y-%m-%d")

    return period_start


def to_raw(raw_table, csr_table):

    start_date = dt(2022, 1, 1)
    end_date = dt(dt.now().year, dt.now().month -1, 1)

    period_start = start_date

    while period_start <= end_date:

        raw_data = pd.read_sql(
            f"""SELECT * FROM raw.{raw_table} WHERE period_start = '{period_start}'  """, con=db_eco)
        for i in range(0, len(raw_data)):
            csr_data = pd.read_sql(
                f"""SELECT plant,period_start,indicatorvalue as amount FROM {csr_table} WHERE plant = '{raw_data['plant'][i]}' AND period_start = '{period_start}'""", con=db_eco)
        #   驗證CSR Data非空值
            if csr_data.size != 0:
                #   if csr data amount > 0 then replace
                if csr_data['amount'][0].sum() > 0:

                    raw_data.loc[i, 'amount'] = csr_data['amount'][0].sum()
                    raw_data.loc[i, 'last_update_time'] = dt.strptime(
                        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                    raw_data.loc[i, 'type'] = 'CSR'

                elif csr_data['amount'][0] <= 0:
                    pass
            else:
                pass
            update_to_raw(raw_data[raw_data['plant'] == raw_data['plant']
                          [i]], raw_table, period_start, raw_data['plant'][i])

        period_start += relativedelta(months=1)


def append_to_raw(raw_table, csr_table):

    plant_exclude = ('F237', 'XTRKS', 'WKS', 'WZS', 'WKS-6A', 'WKS-6B',
                     'WZS-1', 'WZS-3', 'WZS-6', 'WZS-8', 'WKS-1', 'WKS-5', 'WKS-6')

    start_date = dt(2022, 1, 1)
    end_date = dt(dt.now().year, dt.now().month -1, 1)

    period_start = start_date
    while period_start <= end_date:

        raw_data = pd.read_sql(
            f"""SELECT * FROM raw.{raw_table} WHERE period_start = '{period_start}'  """, con=db_eco)

        if csr_table == 'app.electricity_backstage_update':

            csr_data = pd.read_sql(f"""SELECT plant , indicatorvalue as amount,'度' as "unit",period_start FROM {csr_table} WHERE period_start = '{period_start}' and plant not in {plant_exclude}""", con=db_eco)

        elif csr_table == 'app.water_backstage_update':

            csr_data = pd.read_sql(f"""SELECT plant , indicatorvalue as amount,'立方米' as "unit",period_start FROM {csr_table} WHERE period_start = '{period_start}' and plant not in {plant_exclude}""", con=db_eco)

        for i in set(csr_data['plant']).difference(raw_data['plant']):

            csr_new_data = csr_data[csr_data['plant'] == i]
            csr_new_data['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            csr_new_data['type'] = 'CSR'

            if csr_new_data.size != 0:

                update_to_raw(csr_new_data, raw_table, period_start, i)

        period_start += relativedelta(months=1)
