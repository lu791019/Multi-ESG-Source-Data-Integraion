import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
import calendar

from models import engine

connect_string = engine.get_connect_string()
db = create_engine(connect_string, echo=True)


def update_source_status(df_target, target_table, item):

    conn = db.connect()
    if df_target.size != 0:
        conn.execute(
            f"""Delete From staging.{target_table} where item = '{item}'""")
        df_target.to_sql(str(target_table), conn, index=False,
                         if_exists='append', schema='staging', chunksize=10000)
    conn.close()

# class source_import:
#     def __init__(self,source_table,item,target_table):
#         self.source_table = source_table
#         self.item = item
#         self.target_table = target_table


def insert_col(df, item):
    df.insert(0, "item", item)
    df.insert(1, "last_update_time", dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"))
    df.insert(2, "comment", "")

    return df


def update_source_status_plant(df_target, target_table, item, period_start, plant):
    conn = db.connect()
    if df_target.size != 0:
        conn.execute(
            f"""Delete From staging.{target_table} where item = '{item}' and plant in ('{plant}') and period_start >='{period_start}'""")
        df_target.to_sql(str(target_table), conn, index=False,
                         if_exists='append', schema='staging', chunksize=10000)
    conn.close()


def source_add_type(data_status, period_start):

    if dt.now().month == 1:
        last_month_days = 31
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m%d")
        period_end1 = date(dt.now().year-1, 12, 31).strftime("%Y%m%d")
        period = date(dt.now().year-1, 12, 1).strftime("%Y-%m")

    else:
        last_month_days = calendar.mdays[dt.now().month-1]
        period_start = date(
            dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")
        period_start1 = date(
            dt.now().year, dt.now().month-1, 1).strftime("%Y%m%d")
        period_end1 = date(dt.now().year, dt.now().month-1,
                           calendar.mdays[dt.now().month-1]).strftime("%Y%m%d")
        period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

    plant_mapping = pd.read_sql(
        'SELECT plant_name,plant_code FROM raw.plant_mapping', con=db)

    opm = pd.read_sql(
        f"""SELECT * FROM raw."wks_opm_ui_finparam" WHERE batch_id = (SELECT MAX(batch_id) FROM raw."wks_opm_ui_finparam" ) AND period  = '{period}'""", con=db)
    dpm = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_dpm_upphndetail WHERE batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_dpm_upphndetail )  AND period >= '{period_start1}' AND period <= '{period_end1}'""", con=db)

    ebg_revenue = pd.read_sql(
        f"""SELECT * FROM raw."wks_opm_raw_ui_revenue" WHERE period  = '{period}'""", con=db)
    ebg_invoice_qty = pd.read_sql(
        f"""SELECT * FROM raw.ebg_invoice_qty WHERE period_start  = '{period_start}'""", con=db)

    opm_item_map = {"營業額": "revenue", "出貨量": "output"}

    for i in range(0, len(data_status)):
        #     print(invoice['type'][i])
        plant = list(
            plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
        plant.append(data_status['plant'][i])
        item = opm_item_map.get(data_status['item'][i])
        check_data = opm[opm['plant'].isin(plant)]

        if data_status['type'][i] == '月報表':
            pass

        elif check_data[item].shape[0] > 0:
            if check_data[item].sum() > 0 and data_status['type'][i] != '月報表':
                data_status.loc[i, 'type'] = 'OPM'

        elif data_status['plant'][i] in ['WZS-1', 'WIH']:

            if data_status['item'][i] == '營業額':
                plant = list(
                    plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
                plant.append(data_status['plant'][i])
                check_data = ebg_revenue[ebg_revenue['plant'].isin(plant)]
                if check_data['rev'].shape[0] > 0:
                    if check_data['rev'].sum() > 0 and data_status['type'][i] != '月報表':
                        data_status.loc[i, 'type'] = 'OPM'
                elif data_status['type'][i] == '月報表':
                    pass

        else:
            if data_status['item'][i] == '出貨量':
                check_data = ebg_invoice_qty[ebg_invoice_qty['plant']
                                             == data_status['plant'][i]]
                if check_data['amount'].shape[0] > 0:
                    if check_data['amount'].sum() > 0 and data_status['type'][i] != '月報表':
                        data_status.loc[i, 'type'] = 'ebg_invoice'
                elif data_status['type'][i] == '月報表':
                    pass
    return data_status


def data_import(source_table, item):

    df = pd.read_sql(
        f"""select distinct plant ,period_start,type from raw.{source_table}""", con=db)
    df_target = insert_col(df, str(item))

    df_target_all = df_target.copy()
    df_target_all['bo'] = 'ALL'

    plant_mapping = pd.read_sql(f"""SELECT DISTINCT bo,plant_name AS "plant" FROM raw.plant_mapping """, con=db)

    df_target = df_target.merge(plant_mapping, on='plant', how='left')
    df_target = df_target.append(df_target_all)
    update_source_status(df_target, 'source_status', str(item))


def data_import_OPM(item):

    if dt.now().month == 1:

        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

    else:

        period_start = date(
            dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")

    data_status = pd.read_sql(
        f"""SELECT * FROM staging.source_status where item in ('出貨量','營業額') and type not in ('月報表') and period_start = '{period_start}'""", con=db)

    if data_status.size != 0:
        data_status = source_add_type(data_status, period_start)

    elif data_status.size == 0:

        data_status = pd.read_sql(
            f"""SELECT * FROM staging.source_status where item in ('出貨量','營業額') and period_start = '{period_start}'""", con=db)

        data_status['type'] = ''

        data_status = source_add_type(data_status, period_start)

        data_status['period_start'] = period_start

    elif data_status.size == 0 and dt.now().day == 1:

        data_status = pd.read_sql(
            f"""SELECT * FROM staging.source_status where item in ('出貨量','營業額')""", con=db)
        data_status = data_status.drop('id', axis=1)

        data_status = data_status[data_status['period_start']
                                  == data_status['period_start'][0]]
        data_status = data_status.reset_index(drop=True)

        data_status['type'] = ''

        data_status = source_add_type(data_status, period_start)

        data_status['period_start'] = period_start

    data_status['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    data_status = data_status[data_status['item']
                              == str(item)].reset_index(drop=True)
    # revenue = data_status[data_status['item']=='營業額'].reset_index(drop=True)
    # invoice = data_status[data_status['item']=='出貨量'].reset_index(drop=True)

    for i in data_status['plant']:
        update_source_status_plant(data_status[data_status['plant'] == str(
            i)], 'source_status', str(item), period_start, str(i))
