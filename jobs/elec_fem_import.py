import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
import calendar
from models import engine


def cal_bo_site(data, category):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    plant_mapping = pd.read_sql(
        'SELECT DISTINCT bo,site,plant_name AS "plant" FROM raw.plant_mapping', con=db)

    # 計算BO site
    data = data.merge(plant_mapping, on='plant', how='left')

    # for bo=all
    data_bo_all = data.copy()
    data_bo_all = data_bo_all[data_bo_all['bo'] != 'Others']
    data_bo_all['bo'] = 'ALL'
    data = data.append(data_bo_all)

    # site
    if category == 0:
        data_site = data.groupby(
            ['bo', 'site', 'period_start']).sum().reset_index()

    elif category == 1:
        data_site = data.groupby(
            ['bo', 'site', 'category', 'period_start']).sum().reset_index()

    else:
        data_site = data.groupby(
            ['bo', 'site', 'category1', 'category2', 'period_start']).sum().reset_index()
    data_site['plant'] = 'ALL'
    # bo
    if category == 0:
        data_bo = data.groupby(['bo', 'period_start']).sum().reset_index()

    elif category == 1:
        data_bo = data.groupby(
            ['bo', 'category', 'period_start']).sum().reset_index()

    else:
        data_bo = data.groupby(
            ['bo', 'category1', 'category2', 'period_start']).sum().reset_index()
    data_bo['site'] = 'ALL'
    data_bo['plant'] = 'ALL'

    # all
    data = data.append(data_site).append(data_bo).reset_index(drop=True)

    return data


def elec_FEM_to_raw(stage):

    # plant_exclude = ('KOE', 'WKS', 'WZS', 'WKS-6A', 'WKS-6B', 'WZS-1',
    #                  'WZS-3', 'WZS-6', 'WZS-8', 'WKS-1', 'WKS-5', 'WKS-6', 'WMY-2', 'WCZ', 'XTRKS', 'WGKS', 'WMI', 'WIH')
    site_exclude = ('WZS', 'WCD', 'WKS')
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)
    conn = db.connect()
    # table_name = str(table_name)

    current_day = dt.now().day

    if stage == 'development':  # DEV - 10號前抓2個月
        checkpoint = 10
    else:  # QAS、PRD - 15號前抓2個月
        checkpoint = 12

    # set time - data in current year
    if dt.now().month == 1:
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

        period_start_WZS = date(dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
        period_start_WCD = date(dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
        period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m")
        period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        period_end_WZS = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
        period_end_WCD = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
        period = date(dt.now().year-1, 12, 1).strftime("%Y-%m")
    elif (dt.now().month == 2) & (current_day < checkpoint):
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")

        period_start_WZS = date(dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
        period_start_WCD = date(dt.now().year-1, 11, 25).strftime("%Y-%m-%d")
        period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m")
        period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        period_end_WZS = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
        period_end_WCD = date(dt.now().year-1, 12, 24).strftime("%Y-%m-%d")
        period = date(dt.now().year-1, 12, 1).strftime("%Y-%m")
    elif (dt.now().month == 2) & (current_day >= checkpoint):
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year, dt.now().month -
                            1, 1).strftime("%Y-%m-%d")

        period_start_WZS = date(dt.now().year - 1, 12, 25).strftime("%Y-%m-%d")
        period_start_WCD = date(dt.now().year - 1, 12, 25).strftime("%Y-%m-%d")

        period_start1 = date(
            dt.now().year, dt.now().month-1, 1).strftime("%Y%m")
        period_end = date(dt.now().year, dt.now().month-1,
                          calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        period_end_WZS = date(dt.now().year, 1,  24).strftime("%Y-%m-%d")
        period_end_WCD = date(dt.now().year, 1,  24).strftime("%Y-%m-%d")
        period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

    else:
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year, dt.now().month -
                            1, 1).strftime("%Y-%m-%d")

        period_start_WZS = date(dt.now().year, dt.now().month -
                                2, 25).strftime("%Y-%m-%d")
        period_start_WCD = date(dt.now().year, dt.now().month -
                                2, 25).strftime("%Y-%m-%d")
        period_start1 = date(
            dt.now().year, dt.now().month-1, 1).strftime("%Y%m")
        period_end = date(dt.now().year, dt.now().month-1,
                          calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        period_end_WZS = date(dt.now().year, dt.now().month-1,
                              24).strftime("%Y-%m-%d")
        period_end_WCD = date(dt.now().year, dt.now().month-1,
                              24).strftime("%Y-%m-%d")
        period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

    electricity_other = pd.read_sql(
        f"SELECT DISTINCT plant as plant_code,power as amount FROM raw.wks_mfg_fem_dailypower WHERE consumetype = '用電量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start}' AND datadate <= '{period_end}' AND site not in {site_exclude}", con=db)
    electricity_SRBG = pd.read_sql(
        f"SELECT DISTINCT plant as plant_code,power as amount FROM raw.wks_mfg_fem_dailypower WHERE consumetype != '用水量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start}' AND datadate <= '{period_end}' AND site = 'SRBG' ", con=db)
    electricity_XTRKS = pd.read_sql(
        f"SELECT DISTINCT plant as plant_code,power as amount FROM raw.wks_mfg_fem_dailypower WHERE consumetype = '用電量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start}' AND datadate <= '{period_end}' AND plant in ('F2C1')", con=db)
    electricity_WZS = pd.read_sql(
        f"SELECT DISTINCT plant as plant_code,power as amount FROM raw.wks_mfg_fem_dailypower WHERE consumetype = '用電量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start_WZS}' AND datadate <= '{period_end_WZS}' AND site in ('WZS')", con=db)
    electricity_WCD = pd.read_sql(
        f"SELECT DISTINCT plant as plant_code,power as amount FROM raw.wks_mfg_fem_dailypower WHERE consumetype = '用電量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start_WCD}' AND datadate <= '{period_end_WCD}' AND site in ('WCD')", con=db)
    electricity = electricity_other.append(electricity_WCD).append(electricity_XTRKS).append(electricity_SRBG).reset_index(drop=True)
    if electricity.shape[0] > 0:
        electricity = electricity.groupby('plant_code').sum().reset_index()
        electricity['plant_code'] = electricity['plant_code'].replace(
            'WCD', 'F721')
        electricity['plant_code'] = electricity['plant_code'].replace(
            '170', 'F170')
        electricity['plant_code'] = electricity['plant_code'].replace(
            'WCQ', 'F710')
        electricity['plant_code'] = electricity['plant_code'].replace(
            'WHC', 'F600')
        plant_mapping = pd.read_sql(
            f"""SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping """, con=db)
        electricity = electricity.merge(
            plant_mapping, on='plant_code', how='left')
        # electricity = electricity[electricity['plant'] != 'XTRKS']
        electricity = electricity.dropna()
        electricity = electricity.groupby('plant').sum().reset_index()
        # electricity = electricity.drop('plant_code', axis=1)
        electricity['period_start'] = period_start
        electricity['unit'] = '度'

        electricity['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        electricity['type'] = 'FEM'

        if electricity.shape[0] > 0:
            check_type = pd.read_sql(
                f"SELECT * FROM raw.electricity_total WHERE period_start = '{period_start}' AND type in ('月報表','CSR','wzs_api') ", con=db)
            # 如為月報表則不更新該廠數據
            if check_type.shape[0] > 0:
                plant_exclude = check_type['plant'].unique()
                electricity = electricity[~electricity['plant'].isin(
                    plant_exclude)].reset_index(drop=True)
                plant_exclude = "','".join(plant_exclude)
                conn.execute(
                    f"DELETE FROM raw.electricity_total WHERE plant NOT IN ('{plant_exclude}') AND period_start = '{period_start}'")
                electricity.to_sql('electricity_total', conn, index=False,
                                   if_exists='append', schema='raw', chunksize=10000)
            else:
                conn.execute(
                    f"DELETE FROM raw.electricity_total WHERE period_start = '{period_start}'")
                electricity.to_sql('electricity_total', conn, index=False,
                                   if_exists='append', schema='raw', chunksize=10000)

    conn.close()
    # conn = db.connect()
    # conn.execute(
    #     f"""DELETE FROM raw.electricity_total WHERE period_start = '{period_start}' and plant not in {plant_exclude}""")
    # electricity.to_sql('electricity_total', conn, index=False,
    #                    if_exists='append', schema='raw', chunksize=10000)
