import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
import calendar

from models import engine

connect_string = engine.get_connect_string()


opm_item_map = {"營收": "revenue", "人力": "dlnum", "出貨量": "output"}
fem_item_map = {"智慧電表": "用電量", "智慧水表": "用水量", "太陽能": "太陽能發電"}


def update_data_status(data_status, period_start):
    db = create_engine(connect_string, echo=True)
    conn = db.connect()
    #conn.execute("TRUNCATE TABLE staging.data_status")
    conn.execute(
        f"""DELETE FROM staging.data_status WHERE period_start  = '{period_start}' and system not in ('CSR')""")  # 刪除DB昨天的check結果(同月份保留最新一份)
    data_status.to_sql('data_status', conn, index=False,
                       if_exists='append', schema='staging', chunksize=10000)
    conn.close()


def data_check(current_day=1):

    db = create_engine(connect_string, echo=True)
    #only get max period_start data_status
    data_status = pd.read_sql(f"""SELECT * FROM staging.data_status where system not in ('CSR') AND period_start = (SELECT MAX(period_start) FROM staging.data_status where system not in ('CSR'))""", con=db)
    data_status = data_status.drop('id', axis=1)
    data_status = data_status.reset_index(drop=True)

    plant_mapping = pd.read_sql(
        'SELECT plant_name,plant_code FROM raw.plant_mapping', con=db)

    # set time
    if current_day < 10:
        # 檢查2個月前
        if dt.now().month == 1:
            last_month_days = 30
            period_start = date(dt.now().year-1, 11, 1).strftime("%Y-%m-%d")
            period_start_WIH = date(
                dt.now().year-1, 10, 15).strftime("%Y-%m-%d")
            period_start_WZS = date(
                dt.now().year-1, 10, 25).strftime("%Y-%m-%d")
            period_start_WCD = date(
                dt.now().year-1, 10, 25).strftime("%Y-%m-%d")
            period_start1 = date(dt.now().year-1, 11, 1).strftime("%Y%m%d")
            period_end = date(dt.now().year-1, 11, 30).strftime("%Y-%m-%d")
            period_end_WIH = date(dt.now().year-1, 11, 14).strftime("%Y-%m-%d")
            period_end_WZS = date(dt.now().year-1, 11, 24).strftime("%Y-%m-%d")
            period_end_WCD = date(dt.now().year-1, 11, 24).strftime("%Y-%m-%d")
            period_end1 = date(dt.now().year-1, 11, 30).strftime("%Y%m%d")
            period = date(dt.now().year-1, 11, 1).strftime("%Y-%m")
        elif dt.now().month == 2:
            last_month_days = 31
            period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            period_start_WIH = date(
                dt.now().year-1, 11, 15).strftime("%Y-%m-%d")
            period_start_WZS = date(
                dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
            period_start_WCD = date(
                dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
            period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m%d")
            period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")
            period_end_WIH = date(dt.now().year-1, 12, 14).strftime("%Y-%m-%d")
            period_end_WZS = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
            period_end_WCD = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
            period_end1 = date(dt.now().year-1, 12, 31).strftime("%Y%m%d")
            period = date(dt.now().year-1, 12, 1).strftime("%Y-%m")
        else:
            last_month_days = calendar.mdays[dt.now().month-2]
            period_start = date(
                dt.now().year, dt.now().month-2, 1).strftime("%Y-%m-%d")
            period_start_WIH = date(
                dt.now().year, dt.now().month-2, 15).strftime("%Y-%m-%d")
            period_start_WZS = date(
                dt.now().year, dt.now().month-2, 25).strftime("%Y-%m-%d")
            period_start_WCD = date(
                dt.now().year, dt.now().month-2, 25).strftime("%Y-%m-%d")
            period_start1 = date(
                dt.now().year, dt.now().month-2, 1).strftime("%Y%m%d")
            period_end = date(dt.now().year, dt.now().month-2,
                              calendar.mdays[dt.now().month-2]).strftime("%Y-%m-%d")
            period_end_WIH = date(
                dt.now().year, dt.now().month-1, 14).strftime("%Y-%m-%d")
            period_end_WZS = date(
                dt.now().year, dt.now().month-1, 24).strftime("%Y-%m-%d")
            period_end_WCD = date(
                dt.now().year, dt.now().month-1, 24).strftime("%Y-%m-%d")
            period_end1 = date(dt.now().year, dt.now().month-2,
                               calendar.mdays[dt.now().month-2]).strftime("%Y%m%d")
            period = date(dt.now().year, dt.now().month-2, 1).strftime("%Y-%m")
        # set time
    else:
        if dt.now().month == 1:
            last_month_days = 31
            period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            period_start_WIH = date(
                dt.now().year-1, 11, 15).strftime("%Y-%m-%d")
            period_start_WZS = date(
                dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
            period_start_WCD = date(
                dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
            period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m%d")
            period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")
            period_end_WIH = date(dt.now().year-1, 12, 14).strftime("%Y-%m-%d")
            period_end_WZS = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
            period_end_WCD = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
            period_end1 = date(dt.now().year-1, 12, 31).strftime("%Y%m%d")
            period = date(dt.now().year-1, 12, 1).strftime("%Y-%m")

        elif dt.now().month == 2:
            last_month_days = calendar.mdays[dt.now().month-1]
            period_start = date(
                dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")
            period_start_WIH = date(
                dt.now().year-1, 12, 15).strftime("%Y-%m-%d")
            period_start_WZS = date(
                dt.now().year-1, 12, 25).strftime("%Y-%m-%d")
            period_start_WCD = date(
                dt.now().year-1, 12, 25).strftime("%Y-%m-%d")
            period_start1 = date(
                dt.now().year, dt.now().month-1, 1).strftime("%Y%m%d")
            period_end = date(dt.now().year, dt.now().month-1,
                              calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")
            period_end_WIH = date(dt.now().year, 1, 14).strftime("%Y-%m-%d")
            period_end_WZS = date(dt.now().year, 1, 24).strftime("%Y-%m-%d")
            period_end_WCD = date(dt.now().year, 1, 24).strftime("%Y-%m-%d")
            period_end1 = date(dt.now().year, dt.now().month-1,
                               calendar.mdays[dt.now().month-1]).strftime("%Y%m%d")
            period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

        else:
            last_month_days = calendar.mdays[dt.now().month-1]
            period_start = date(
                dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")
            period_start_WIH = date(
                dt.now().year, dt.now().month-2, 15).strftime("%Y-%m-%d")
            period_start_WZS = date(
                dt.now().year, dt.now().month-2, 25).strftime("%Y-%m-%d")
            period_start_WCD = date(
                dt.now().year, dt.now().month-2, 25).strftime("%Y-%m-%d")
            period_start1 = date(
                dt.now().year, dt.now().month-1, 1).strftime("%Y%m%d")
            period_end = date(dt.now().year, dt.now().month-1,
                              calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")
            period_end_WIH = date(
                dt.now().year, dt.now().month-1, 14).strftime("%Y-%m-%d")
            period_end_WZS = date(
                dt.now().year, dt.now().month-1, 24).strftime("%Y-%m-%d")
            period_end_WCD = date(
                dt.now().year, dt.now().month-1, 24).strftime("%Y-%m-%d")
            period_end1 = date(dt.now().year, dt.now().month-1,
                               calendar.mdays[dt.now().month-1]).strftime("%Y%m%d")
            period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

    current_date = dt.now().strftime("%Y%m%d")

    benefit = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.electricity_saving_digital WHERE period_start  = '{period_start}'""", con=db)
    opm = pd.read_sql(
        f"""SELECT * FROM raw."wks_opm_ui_finparam" WHERE batch_id = (SELECT MAX(batch_id) FROM raw."wks_opm_ui_finparam" ) AND period  = '{period}'""", con=db)
    fem_other_water = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_fem_dailypower WHERE  batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start}' AND datadate <= '{period_end}' AND site not in ('WIH','WZS','WCD')""", con=db)
    fem_WIH = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_fem_dailypower WHERE  batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start_WIH}' AND datadate <= '{period_end_WIH}' AND site in ('WIH')""", con=db)
    fem_WZS = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_fem_dailypower WHERE  batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start_WZS}' AND datadate <= '{period_end_WZS}' AND site in ('WZS')""", con=db)
    fem_WCD = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_fem_dailypower WHERE  batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start_WCD}' AND datadate <= '{period_end_WCD}' AND site in ('WCD')""", con=db)
    fem_other_elect = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_fem_dailypower WHERE  batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower) AND datadate >= '{period_start}' AND datadate <= '{period_end}' AND site not in ('WZS','WCD')""", con=db)
    fem_water = fem_other_water.append(fem_WZS).append(fem_WCD).append(fem_WIH).reset_index(drop=True)
    fem_elect = fem_other_elect.append(fem_WZS).append(fem_WCD).reset_index(drop=True)
    dpm = pd.read_sql(
        f"""SELECT DISTINCT * FROM raw.wks_mfg_dpm_upphndetail WHERE batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_dpm_upphndetail )  AND period >= '{period_start1}' AND period <= '{period_end1}'""", con=db)
    waste = pd.read_sql(f"""SELECT * FROM raw.waste WHERE period_start  = '{period_start}'""", con=db)
    invoice_qty = pd.read_sql(f"""SELECT * FROM raw.invoice_qty WHERE period_start  = '{period_start}'""", con=db)
    payrollcnt = pd.read_sql(f"""SELECT * FROM raw.payrollcnt WHERE period_start  = '{period_start}'""", con=db)
    opm_revenue = pd.read_sql(f"SELECT period AS period_start,plant AS plant_code,rev AS amount FROM raw.wks_opm_raw_ui_revenue WHERE  batch_id = (SELECT MAX(batch_id) FROM raw.wks_opm_raw_ui_revenue) AND period = '{period}' ", con=db)
    opm_revenue = opm_revenue.merge(plant_mapping, on='plant_code', how='inner')
    opm_revenue['plant'] = opm_revenue['plant_name']
    if opm_revenue.shape[0] > 0:
        opm_revenue = opm_revenue.groupby(['period_start', 'plant']).sum().reset_index()

    for i in range(0, len(data_status)):
        # 智能系統效益追蹤平台 - WKS-COMMON only
        if data_status['system'][i] == 'benefit':
            if 'WKS' in data_status['plant'][i]:
                plant = 'WKS-COMMON'
                check_data = benefit[benefit['plant'] == plant]
                if check_data.shape[0] > 0:
                    data_status.loc[i, 'status'] = 2
                else:  # 無資料
                    data_status.loc[i, 'status'] = 1
            else:
                plant = data_status['plant'][i]
                data_status.loc[i, 'status'] = 0

        # OPM
        if data_status['system'][i] == 'OPM':
            #plant not in OPM
            if (data_status['plant'][i] in ['WNH','WHC']) & (data_status['item'][i] != '人力'):
                data_status.loc[i, 'status'] = 0
            else :
                #營收
                if data_status['item'][i] == '營收':
                    if data_status['plant'][i] == 'WZS-8':
                        plant = list(plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
                        plant.append(data_status['plant'][i])
                        item = opm_item_map.get(data_status['item'][i])
                        check_data = opm[opm['plant'].isin(plant)]
                        if check_data[item].shape[0] > 0:
                            if check_data[item].sum().sum() > 0:
                                data_status.loc[i, 'status'] = 2
                            else:
                                data_status.loc[i, 'status'] = 1
                        else:
                            data_status.loc[i, 'status'] = 1
                    else:    
                        check_data = opm_revenue[opm_revenue['plant'] == data_status['plant'][i]]
                        if check_data.shape[0] > 0:
                            data_status.loc[i, 'status'] = 2
                        else:
                            data_status.loc[i, 'status'] = 1
                #人力 - 使用全球HR資料源
                elif data_status['item'][i] == '人力' :
                    check_data = payrollcnt[payrollcnt['plant'] == data_status['plant'][i]].reset_index(drop = True)
                    if check_data.shape[0] > 0 :
                        if check_data['amount'][0] > 0 :
                            data_status.loc[i, 'status'] = 2
                        else:
                            data_status.loc[i, 'status'] = 1
                    else:
                        data_status.loc[i, 'status'] = 1
                #出貨 - 統一檢查 raw.invoice_qty
                else :
                    check_data = invoice_qty[invoice_qty['plant'] == data_status['plant'][i]].reset_index(drop = True)
                    if check_data.shape[0] > 0:
                        if check_data['amount'][0] > 0:
                            data_status.loc[i, 'status'] = 2
                        else:
                            data_status.loc[i, 'status'] = 1
                    else:
                        data_status.loc[i, 'status'] = 1

        # FEM
        if data_status['system'][i] == 'FEM':
            #plant not in FEM
            if data_status['plant'][i] in ['WCZ','WMX']: #'WNH', 'WHC', 'WMX', 
                data_status.loc[i, 'amount'] = 0
                data_status.loc[i, 'status'] = 0
            else:
                item = fem_item_map.get(data_status['item'][i])
                if item == '太陽能發電':
                    # 無太陽能發電
                    if data_status['plant'][i] in ['WIH', 'WTZ', 'WCQ', 'WCD', 'WNH', 'WHC', 'WMX']:
                        data_status.loc[i, 'amount'] = 0
                        data_status.loc[i, 'status'] = 0
                    else:
                        # WKS、WZS暫無拆分plant
                        if 'WKS' in data_status['plant'][i]:
                            plant = ['WKS']
                        elif 'WZS' in data_status['plant'][i]:
                            plant = ['WZS']
                        else:
                            plant = list(
                                plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
                            plant.append(data_status['plant'][i])
                        fem = fem_elect
                        check_data = fem[(fem['consumetype'] == item) & (
                            fem['plant'].isin(plant))]
                        # 每一天都要有資料
                        if check_data.shape[0] == last_month_days:
                            data_status.loc[i, 'amount'] = check_data['power'].sum()
                            data_status.loc[i, 'status'] = 2
                            if check_data['power'].sum() == 0:
                                data_status.loc[i, 'status'] = 1
                        elif check_data.shape[0] > 0:  # FEM黃燈
                            data_status.loc[i,
                                            'amount'] = check_data['power'].sum()
                            data_status.loc[i, 'status'] = 3
                        else:  # 無資料
                            data_status.loc[i, 'amount'] = 0
                            data_status.loc[i, 'status'] = 1

                elif item == '用水量':
                    fem = fem_water
                    #WMX 無智慧水表
                    if data_status['plant'][i] in ['WMX']: 
                        data_status.loc[i, 'amount'] = 0
                        data_status.loc[i, 'status'] = 0
                    else:
                        # WKS、WZS暫無拆分plant
                        if 'WKS' in data_status['plant'][i]:
                            plant = ['WKS']
                        elif 'WZS' in data_status['plant'][i]:
                            plant = ['WZS']
                        else:
                            plant = list(
                                plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
                            plant.append(data_status['plant'][i])
                        check_data = fem[(fem['consumetype'] == item)
                                        & (fem['plant'].isin(plant))]
                        # 每一天都要有資料
                        if check_data.shape[0] == last_month_days:
                            data_status.loc[i, 'amount'] = check_data['power'].sum()
                            data_status.loc[i, 'status'] = 2
                            if check_data['power'].sum() == 0:
                                data_status.loc[i, 'status'] = 1
                        elif check_data.shape[0] > 0:  # FEM黃燈
                            data_status.loc[i,
                                            'amount'] = check_data['power'].sum()
                            data_status.loc[i, 'status'] = 3
                        else:  # 無資料
                            data_status.loc[i, 'amount'] = 0
                            data_status.loc[i, 'status'] = 1
                # 用電
                else:
                    fem = fem_elect
                    plant = list(
                        plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
                    plant.append(data_status['plant'][i])
                    check_data = fem[(fem['consumetype'] == item)
                                     & (fem['plant'].isin(plant))]
                    # 每一天都要有資料
                    if check_data.shape[0] == last_month_days:
                        data_status.loc[i,
                                        'amount'] = check_data['power'].sum()
                        data_status.loc[i, 'status'] = 2
                    elif check_data.shape[0] > 0:  # FEM黃燈
                        data_status.loc[i,
                                        'amount'] = check_data['power'].sum()
                        data_status.loc[i, 'status'] = 3
                    else:  # 無資料
                        data_status.loc[i, 'amount'] = 0
                        data_status.loc[i, 'status'] = 1
        # DPM
        if data_status['system'][i] == 'DPM':
            #plant not in FEM
            if data_status['plant'][i] in ['WNH', 'WHC', 'WMX', 'WCZ', 'WZS-8']:
                data_status.loc[i, 'status'] = 0
            else:
                plant = list(
                    plant_mapping['plant_code'][plant_mapping['plant_name'] == data_status['plant'][i]].unique())
                plant.append(data_status['plant'][i])
                check_data = dpm[dpm['plant'].isin(plant)]
                if check_data.shape[0] > 0:
                    data_status.loc[i, 'status'] = 2
                else:  # 無資料
                    data_status.loc[i, 'status'] = 1

        # 廢棄物
        if data_status['system'][i] == 'waste':
            plant = data_status['plant'][i]
            check_data = waste[waste['plant'] == plant]
            if check_data.shape[0] > 0:
                if check_data['amount'].sum() > 0:
                    data_status.loc[i, 'status'] = 2
                else:
                    data_status.loc[i, 'status'] = 1
            else:  # 無資料
                data_status.loc[i, 'status'] = 1

    data_status['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    data_status['period_start'] = period_start

    update_data_status(data_status, period_start)  # 更新結果至資料庫
