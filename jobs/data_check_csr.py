import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from dateutil.relativedelta import relativedelta
from models import engine

connect_string = engine.get_connect_string()
db = create_engine(connect_string, echo=True)

plant_map = {"WKS-1": "F230", "WKS-5": "F232", "WKS-6": "F236", "WKS-6A": "F237",
             "WOK": "F741", "WTZ": "F261",
             "WZS-1": "F136", "WZS-3": "F130", "WZS-6": "F138", "WZS-8": "F139",
             "WCQ": "F710", "WCD": "F721"}


def update_data_status_csr(data_status, period_start):
    db = create_engine(connect_string, echo=True)
    conn = db.connect()
    conn.execute(
        f"""Delete From staging.data_status where system = 'CSR' and period_start = '{period_start}'""")
    data_status.to_sql('data_status', conn, index=False,
                       if_exists='append', schema='staging', chunksize=10000)
    conn.close()


def useful_datetime(i):

    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%Y-%m-%d")

    # last_year_period_start = ( date(dt.now().year-1, dt.now().month, 1) - relativedelta(months=i) ).strftime("%Y-%m-%d")
    # period_start1 = ( date(dt.now().year, dt.now().month, 1) - relativedelta(months=i) ).strftime("%Y%m%d")
    # period = ( date(dt.now().year-1, dt.now().month, 1) - relativedelta(months=i) ).strftime("%Y-%m")
    # period_year = ( date(dt.now().year, dt.now().month, 1) - relativedelta(months=i) ).strftime("%Y")

    return period_start


def csr_data_status(data_status, period_start):
    for i in range(0, len(data_status)):

        if data_status['system'][i] == 'CSR':
            if data_status['item'][i] == '電費帳單':
                table_name = 'raw.electricity_total'
                check_data = pd.read_sql(
                    f"""SELECT * FROM {table_name} WHERE plant = '{data_status['plant'][i]}' AND period_start = '{period_start}' and type in ('CSR')""", con=db)

                if check_data.size != 0:

                    if check_data['amount'][0] > 0:

                        data_status.loc[i, 'status'] = 2
                        data_status.loc[i,
                                        'amount'] = check_data['amount'].sum()

                    elif check_data['amount'][0] < 0:
                        data_status.loc[i, 'status'] = 3
                        data_status.loc[i, 'amount'] = check_data['amount'].sum(
                        )

                    else:
                        # if current_day >= this_month_end_day:  # 無資料
                        data_status.loc[i, 'status'] = 0
                        data_status.loc[i, 'amount'] = 0
                        # else:  # 尚未更新
                        #     data_status.loc[i, 'status'] = 0
                        #     data_status.loc[i, 'amount'] = 0
                else:
                    data_status.loc[i, 'status'] = 1
                    data_status.loc[i, 'amount'] = 0
            # else:  # 水費帳單
            elif data_status['item'][i] == '水費帳單':

                table_name = 'raw.water'
                check_data = pd.read_sql(
                    f"""SELECT * FROM {table_name} WHERE plant = '{data_status['plant'][i]}' AND period_start = '{period_start}' and type in ('CSR')""", con=db)

                if check_data.size != 0:
                    if check_data['amount'][0] > 0:

                        data_status.loc[i, 'status'] = 2
                        data_status.loc[i,
                                        'amount'] = check_data['amount'].sum()

                    elif check_data['amount'][0] < 0:

                        data_status.loc[i, 'status'] = 3
                        data_status.loc[i,
                                        'amount'] = check_data['amount'].sum()
                    else:
                        # if current_day >= this_month_end_day:  # 無資料
                        data_status.loc[i, 'status'] = 0
                        data_status.loc[i, 'amount'] = 0
                        # else:  # 尚未更新
                        #     data_status.loc[i, 'status'] = 0
                        #     data_status.loc[i, 'amount'] = 0
                else:
                    data_status.loc[i, 'status'] = 1
                    data_status.loc[i, 'amount'] = 0


def data_check_csr():

    for i in range(1, 4):
        period_start = useful_datetime(i)

        data_status = pd.read_sql(
            f"""SELECT * FROM staging.data_status where system ='CSR' and period_start = '{period_start}'""", con=db)
        if data_status.size != 0:
            csr_data_status(data_status, period_start)
        else:
            data_status = pd.read_sql(
                f"""SELECT * FROM staging.data_status where system in ('CSR')""", con=db)
            data_status = data_status.drop('id', axis=1)

            data_status = data_status[data_status['period_start']
                                      == data_status['period_start'][0]]
            data_status = data_status.reset_index(drop=True)

            csr_data_status(data_status, period_start)
            data_status['period_start'] = period_start

        data_status['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        update_data_status_csr(data_status, period_start)
