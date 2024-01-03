import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
import calendar
from models import engine
from jobs.fix_data import fix_raw


def cal_bo_site(data, category):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    plant_mapping = pd.read_sql('SELECT DISTINCT bo,site,plant_name AS "plant" FROM raw.plant_mapping WHERE boundary = true', con=db)

    # 計算BO site
    data = data.merge(plant_mapping, on='plant', how='inner')
    data_copy = data.copy()

    # for bo=all
    data_bo_all = data_copy.copy()
    # data_bo_all = data_bo_all[data_bo_all['bo'] != 'Others']
    data_bo_all['bo'] = 'ALL'
    data = data.append(data_bo_all)

    #for bo=all
    # data_bo_all_other = data_copy.copy()
    # data_bo_all_other['bo'] = 'ALL+新邊界'
    # data = data.append(data_bo_all_other)

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


def raw_to_staging(table_name, stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)
    table_name = str(table_name)

    current_day = dt.now().day

    if stage == 'development':  # DEV - 10號前抓2個月
        checkpoint = 10
    else:  # PRD - 15號前抓2個月
        checkpoint = 12

    # set time - data in current year
    if dt.now().month == 1:
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")


        period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m")
        period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")


        period = date(dt.now().year-1, 12, 1).strftime("%Y-%m")
    elif (dt.now().month == 2) & (current_day < checkpoint):  # 10或15號前抓2個月前
        period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")


        period_start1 = date(dt.now().year-1, 12, 1).strftime("%Y%m")
        period_end = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")


        period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")
    else:
        period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        period_start = date(dt.now().year, dt.now().month -
                            1, 1).strftime("%Y-%m-%d")


        period_start1 = date(
            dt.now().year, dt.now().month-1, 1).strftime("%Y%m")
        period_end = date(dt.now().year, dt.now().month-1,
                          calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")


        period = date(dt.now().year, dt.now().month-1, 1).strftime("%Y-%m")

    #廢棄物 - waste
    if table_name == 'waste':
        waste = pd.read_sql(
            f"""SELECT plant,category,amount,period_start FROM raw.waste WHERE period_start >='{period_year_start}' """, con=db)
        if waste.shape[0] > 0:
            waste.loc[waste['category'] ==
                      '一般廢棄物(焚化&掩埋)', 'category1'] = 'unrecyclable'
            waste.loc[waste['category'] == '有害廢棄物',
                      'category1'] = 'unrecyclable'
            waste.loc[waste['category'] ==
                      '一般廢棄物(廚餘)', 'category1'] = 'recyclable'
            waste.loc[waste['category'] == '資源廢棄物', 'category1'] = 'recyclable'

            waste.loc[waste['category'] ==
                      '一般廢棄物(焚化&掩埋)', 'category2'] = 'general'
            waste.loc[waste['category'] == '有害廢棄物', 'category2'] = 'hazardous'
            waste.loc[waste['category'] ==
                      '一般廢棄物(廚餘)', 'category2'] = 'general'
            waste.loc[waste['category'] == '資源廢棄物', 'category2'] = 'hazardous'

            waste = waste.drop('category', axis=1)
            waste['amount'] = waste['amount'].fillna(0)
            # 計算YTM
            waste['Year'] = waste['period_start'].apply(lambda x: x.year)
            waste = waste.sort_values(
                by=['Year', 'plant', 'category1', 'category2', 'period_start'])
            waste['ytm_amount'] = waste.groupby(
                ['Year', 'plant', 'category1', 'category2'])['amount'].cumsum()
            waste = waste.drop('Year', axis=1)

            #計算bo and site
            waste = cal_bo_site(waste, 2)
            waste = waste[['bo', 'site', 'plant', 'category1',
                           'category2', 'amount', 'ytm_amount', 'period_start']]
            waste['unit'] = "噸"
            waste['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.waste WHERE  period_start >='{period_year_start}'")
            waste.to_sql('waste', conn, index=False,
                         if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    #營業額 - revenue
    if table_name == 'revenue':
        conn = db.connect()
        plant_mapping = pd.read_sql(
            'SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping', con=db)
        # from OPM - opm_ui_finparam : WZS-8、WMI
        plant_list = ('F5A1', 'F5A2', 'F139')
        revenue1 = pd.read_sql(
            f"""SELECT period AS period_start ,plant AS plant_code,revenue AS amount FROM raw.wks_opm_ui_finparam WHERE plant IN {plant_list} AND period = '{period}' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_opm_ui_finparam)""", con=db)
        if revenue1.shape[0] > 0:
            revenue1 = revenue1.drop_duplicates()
            revenue1 = revenue1.merge(
                plant_mapping, on='plant_code', how='inner')
            revenue1 = revenue1[revenue1['amount'] > 0]
            revenue1 = revenue1.drop('plant_code', axis=1)
            revenue1 = revenue1.groupby(
                ['period_start', 'plant']).sum().reset_index()

            revenue1['period_start'] = revenue1['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%m"))
            revenue1 = revenue1[revenue1['period_start'] >= period_year_start]
            # 單位轉換 K->十億
            revenue1['amount'] = revenue1['amount'] / 1000000

        # from OPM - raw_ui_revenue
        revenue2 = pd.read_sql(
            f"SELECT period AS period_start,plant AS plant_code,rev AS amount FROM raw.wks_opm_raw_ui_revenue WHERE  period = '{period}' ", con=db)
        if revenue2.shape[0] > 0:
            revenue2 = revenue2.merge(
                plant_mapping, on='plant_code', how='inner')
            revenue2 = revenue2.groupby(
                ['period_start', 'plant']).sum().reset_index()
            revenue2['period_start'] = revenue2['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%m"))
            revenue2['amount'] = revenue2['amount'] / 1000
        # from user upload WMX - WYHQ
        # revenue_WMX = pd.read_sql(
        #     f"SELECT period_start,plant,amount FROM raw.revenue_wmx WHERE  period_start = '{period_start}' ", con=db)
        # if revenue_WMX.shape[0] > 0:
        #     revenue_WMX['period_start'] = revenue_WMX['period_start'].apply(
        #         lambda x: dt.strptime(dt.strftime(x, "%Y-%m-%d"), "%Y-%m-%d"))

        revenue = revenue1.append(revenue2).reset_index(drop=True)
        revenue = revenue.groupby(
            ['period_start', 'plant']).sum().reset_index()

        if revenue.shape[0] > 0:
            revenue = revenue[['period_start', 'plant', 'amount']]
            revenue['unit'] = '十億台幣'
            revenue['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            revenue['type'] = 'OPM'

            check_type = pd.read_sql(
                f"SELECT * FROM raw.revenue WHERE period_start = '{period_start}' AND type = '月報表' ", con=db)
            # 如為月報表則不更新該廠數據
            if check_type.shape[0] > 0:
                plant_exclude = check_type['plant'].unique()
                revenue = revenue[~revenue['plant'].isin(
                    plant_exclude)].reset_index(drop=True)
                plant_exclude = "','".join(plant_exclude)
                conn.execute(
                    f"DELETE FROM raw.revenue WHERE plant NOT IN ('{plant_exclude}') AND period_start = '{period_start}'")
                revenue.to_sql('revenue', conn, index=False,
                               if_exists='append', schema='raw', chunksize=10000)
            else:
                conn.execute(
                    f"DELETE FROM raw.revenue WHERE period_start = '{period_start}'")
                revenue.to_sql('revenue', db, index=False,
                               if_exists='append', schema='raw', chunksize=10000)

        # 1個月前與2個月前無來源補0
        fix_raw(1, 'revenue')
        fix_raw(2, 'revenue')

        # get current data for YTM
        revenue = pd.read_sql(
            f"SELECT plant,amount,period_start FROM raw.revenue WHERE period_start >= '{period_year_start}' ", con=db)

        revenue_adj = pd.read_sql(
            f"SELECT plant,adj_amount as amount,period_start FROM staging.revenue_adj WHERE period_start >= '{period_year_start}' and comment != 'test' ", con=db)

        #WMX-WYHQ
        revenue_WMX = pd.read_sql(f"SELECT period_start,plant,amount FROM raw.revenue_wmx WHERE  period_start >= '{period_year_start}' ", con=db)

        revenue = revenue.append(revenue_adj).append(revenue_WMX)

        if revenue.shape[0] > 0:
            revenue['amount'] = revenue['amount'].fillna(0)
            revenue = revenue.groupby(
                ['plant', 'period_start']).sum().reset_index()

            # 計算YTM
            revenue['Year'] = revenue['period_start'].apply(lambda x: x.year)
            revenue = revenue.sort_values(by=['Year', 'plant', 'period_start'])
            revenue['ytm_amount'] = revenue.groupby(
                ['Year', 'plant'])['amount'].cumsum()
            revenue = revenue.drop('Year', axis=1)

            #計算bo and site
            revenue = cal_bo_site(revenue, 0)
            revenue = revenue[['bo', 'site', 'plant',
                               'amount', 'ytm_amount', 'period_start']]
            revenue['unit'] = "十億台幣"
            revenue['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn.execute(
                f"DELETE FROM staging.revenue WHERE  period_start >='{period_year_start}'")
            revenue.to_sql('revenue', conn, index=False,
                           if_exists='append', schema='staging', chunksize=10000)
        conn.close()

    #可再生能源 - renewable_energy
    if table_name == 'renewable_energy':
        renewable_energy = pd.read_sql(
            f"""SELECT plant,category2 AS "category",amount,period_start FROM raw.renewable_energy WHERE category1 = '綠色能源' AND period_start >='{period_year_start}' """, con=db)

        if renewable_energy.shape[0] > 0:
            renewable_energy.loc[renewable_energy['category']
                                 == '光伏', 'category'] = 'solar_energy'
            renewable_energy.loc[renewable_energy['category']
                                 == '綠證', 'category'] = 'green_energy'
            renewable_energy.loc[renewable_energy['category']
                                 == '綠電', 'category'] = 'green_electricity'
            renewable_energy.loc[renewable_energy['plant']
                                 == 'WKS-P6A', 'plant'] = 'WKS-6A'
            renewable_energy.loc[renewable_energy['plant']
                                 == 'WKS-P6B', 'plant'] = 'WKS-6B'
            renewable_energy.loc[renewable_energy['plant']
                                 == 'WKS-P6', 'plant'] = 'WKS-6'
            renewable_energy = renewable_energy.fillna(0)

            # 計算YTM
            renewable_energy['Year'] = renewable_energy['period_start'].apply(
                lambda x: x.year)
            renewable_energy = renewable_energy.sort_values(
                by=['Year', 'plant', 'category', 'period_start'])
            renewable_energy['ytm_amount'] = renewable_energy.groupby(
                ['Year', 'plant', 'category'])['amount'].cumsum()
            renewable_energy = renewable_energy.drop('Year', axis=1)

            #計算bo and site
            renewable_energy = cal_bo_site(renewable_energy, 1)
            renewable_energy = renewable_energy[[
                'bo', 'site', 'plant', 'category', 'amount', 'ytm_amount', 'period_start']]
            renewable_energy['unit'] = "度"
            renewable_energy['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.renewable_energy WHERE  period_start >='{period_year_start}'")
            renewable_energy.to_sql('renewable_energy', conn, index=False,
                                    if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    #出貨量 - invoice_qty
    if table_name == 'invoice_qty':
        conn = db.connect()

        # from OPM
        invoice_qty = pd.read_sql(
            f"""SELECT period AS period_start ,plant AS plant_code,output AS amount FROM raw.wks_opm_ui_finparam WHERE   period = '{period}' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_opm_ui_finparam)""", con=db)
        invoice_qty = invoice_qty.drop_duplicates()
        if invoice_qty.shape[0] > 0:
            plant_mapping = pd.read_sql(
                'SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping', con=db)
            invoice_qty = invoice_qty.merge(
                plant_mapping, on='plant_code', how='inner')
            invoice_qty = invoice_qty[invoice_qty['amount'] > 0]
            invoice_qty = invoice_qty.drop('plant_code', axis=1)
            invoice_qty = invoice_qty.groupby(
                ['period_start', 'plant']).sum().reset_index()
            invoice_qty['period_start'] = invoice_qty['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%m"))
            # 單位轉換 K->台
            invoice_qty['amount'] = invoice_qty['amount'] * 1000
            invoice_qty['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            invoice_qty['type'] = 'OPM'

            check_type = pd.read_sql(
                f"SELECT * FROM raw.invoice_qty WHERE period_start = '{period_start}' AND type = '月報表' ", con=db)
            # 如為月報表則不更新該廠數據
            if check_type.shape[0] > 0:
                plant_exclude = check_type['plant'].unique()
                invoice_qty = invoice_qty[~invoice_qty['plant'].isin(
                    plant_exclude)].reset_index(drop=True)
                plant_exclude = "','".join(plant_exclude)
                conn.execute(
                    f"DELETE FROM raw.invoice_qty WHERE plant NOT IN ('{plant_exclude}') AND period_start = '{period_start}'")
                invoice_qty.to_sql('invoice_qty', conn, index=False,
                                   if_exists='append', schema='raw', chunksize=10000)
            else:
                conn.execute(
                    f"DELETE FROM raw.invoice_qty WHERE period_start = '{period_start}'")
                invoice_qty.to_sql('invoice_qty', db, index=False,
                                   if_exists='append', schema='raw', chunksize=10000)

        # 1個月前與2個月前無來源補0
        fix_raw(1, 'invoice_qty')
        fix_raw(2, 'invoice_qty')
        # 計算YTM
        # get current data for YTM
        invoice_qty = pd.read_sql(
            f"SELECT plant,amount,period_start FROM raw.invoice_qty WHERE period_start >= '{period_year_start}' ", con=db)

        invoice_qty['Year'] = invoice_qty['period_start'].apply(
            lambda x: x.year)
        invoice_qty = invoice_qty.sort_values(
            by=['Year', 'plant', 'period_start'])
        invoice_qty['ytm_amount'] = invoice_qty.groupby(['Year', 'plant'])[
            'amount'].cumsum()
        invoice_qty = invoice_qty.drop('Year', axis=1)

        #計算bo and site
        invoice_qty = cal_bo_site(invoice_qty, 0)
        invoice_qty = invoice_qty[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'period_start']]

        invoice_qty['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        conn.execute(
            f"DELETE FROM staging.invoice_qty WHERE  period_start >='{period_year_start}'")
        invoice_qty.to_sql('invoice_qty', conn, index=False,
                           if_exists='append', schema='staging', chunksize=10000)
        conn.close()

    #生產量 - production_qty
    if table_name == 'production_qty':
        # from DPM
        production_qty = pd.read_sql(
            f"SELECT period ,plant AS plant_code,output AS amount FROM raw.wks_mfg_dpm_upphndetail WHERE period LIKE '{period_start1}%%' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_dpm_upphndetail)", con=db)
        production_qty = production_qty.groupby(
            'plant_code').sum().reset_index()
        plant_mapping = pd.read_sql(
            'SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping', con=db)
        production_qty = production_qty.merge(
            plant_mapping, on='plant_code', how='left')
        production_qty = production_qty.groupby('plant').sum().reset_index()
        production_qty['period_start'] = dt.strptime(period_start1, "%Y%m")
        production_qty['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        production_qty["type"] = 'DPM'

        if production_qty.shape[0] > 0:
            conn = db.connect()
            # 先將當月數據寫入raw.production_qty
            check_type = pd.read_sql(
                f"SELECT * FROM raw.production_qty WHERE period_start = '{period_start}' AND type = '月報表' ", con=db)
            # 如為月報表則不更新該廠數據
            if check_type.shape[0] > 0:
                plant_exclude = check_type['plant'].unique()
                production_qty = production_qty[~production_qty['plant'].isin(
                    plant_exclude)].reset_index(drop=True)
                plant_exclude = "','".join(plant_exclude)
                conn.execute(
                    f"DELETE FROM raw.production_qty WHERE plant NOT IN ('{plant_exclude}') AND period_start = '{period_start}'")
                production_qty.to_sql(
                    'production_qty', conn, index=False, if_exists='append', schema='raw', chunksize=10000)
            else:
                conn.execute(
                    f"DELETE FROM raw.production_qty WHERE period_start = '{period_start}'")
                production_qty.to_sql(
                    'production_qty', db, index=False, if_exists='append', schema='raw', chunksize=10000)

            # 1個月前與2個月前無來源補0
            fix_raw(1, 'production_qty')
            fix_raw(2, 'production_qty')

            # get current data for YTM
            production_qty = pd.read_sql(
                f"SELECT plant,amount,period_start FROM raw.production_qty WHERE period_start >= '{period_year_start}' ", con=db)

            production_qty['amount'] = production_qty['amount'].fillna(0)
            # 計算YTM
            production_qty['Year'] = production_qty['period_start'].apply(
                lambda x: x.year)
            production_qty = production_qty.sort_values(
                by=['Year', 'plant', 'period_start'])
            production_qty['ytm_amount'] = production_qty.groupby(['Year', 'plant'])[
                'amount'].cumsum()
            production_qty = production_qty.drop('Year', axis=1)

            #計算bo and site
            production_qty = cal_bo_site(production_qty, 0)
            production_qty = production_qty[[
                'bo', 'site', 'plant', 'amount', 'ytm_amount', 'period_start']]

            production_qty['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn.execute(
                f"DELETE FROM staging.production_qty WHERE  period_start >='{period_year_start}'")
            production_qty.to_sql('production_qty', conn, index=False,
                                  if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    #用水 - water
    if table_name == 'water':
        water = pd.read_sql(
            f"SELECT * FROM raw.water WHERE period_start >='{period_year_start}' ", con=db)
        if water.shape[0] > 0:
            water['amount'] = water['amount'].fillna(0)
            # 計算YTM
            water['Year'] = water['period_start'].apply(lambda x: x.year)
            water = water.sort_values(by=['Year', 'plant', 'period_start'])
            water['ytm_amount'] = water.groupby(['Year', 'plant'])[
                'amount'].cumsum()
            water = water.drop('Year', axis=1)

            #計算bo and site
            water = cal_bo_site(water, 0)
            water = water[['bo', 'site', 'plant',
                           'amount', 'ytm_amount', 'period_start']]
            water['unit'] = '立方米'
            water['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.water WHERE  period_start >='{period_year_start}'")
            water.to_sql('water', db, index=False, if_exists='append',
                         schema='staging', chunksize=10000)
            conn.close()

    #節能技改 - electricity_saving_tech
    if table_name == 'electricity_saving_tech':
        electricity_saving_tech = pd.read_sql(
            f"SELECT * FROM raw.electricity_saving_tech WHERE period_start >='{period_year_start}' ", con=db)
        if electricity_saving_tech.shape[0] > 0:
            # 計算YTM
            electricity_saving_tech['Year'] = electricity_saving_tech['period_start'].apply(
                lambda x: x.year)
            electricity_saving_tech = electricity_saving_tech.sort_values(
                by=['Year', 'plant', 'period_start'])
            electricity_saving_tech['ytm_amount'] = electricity_saving_tech.groupby(
                ['Year', 'plant'])['amount'].cumsum()
            electricity_saving_tech = electricity_saving_tech.drop(
                'Year', axis=1)

            #計算bo and site
            electricity_saving_tech = cal_bo_site(electricity_saving_tech, 0)
            electricity_saving_tech = electricity_saving_tech[[
                'bo', 'site', 'plant', 'amount', 'ytm_amount', 'period_start']]

            electricity_saving_tech['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.electricity_saving_tech WHERE  period_start >='{period_year_start}'")
            electricity_saving_tech.to_sql(
                'electricity_saving_tech', conn, index=False, if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    #節電-數位化 - electricity_saving_digital
    if table_name == 'electricity_saving_digital':
        electricity_saving_digital = pd.read_sql(
            f"SELECT plant,kpi AS amount,period_start FROM raw.electricity_saving_digital WHERE period_start >='{period_year_start}' ", con=db)
        if electricity_saving_digital.shape[0] > 0:
            electricity_saving_digital = electricity_saving_digital.groupby(
                ['plant', 'period_start']).sum().reset_index()

            # 計算YTM
            electricity_saving_digital['Year'] = electricity_saving_digital['period_start'].apply(
                lambda x: x.year)
            electricity_saving_digital = electricity_saving_digital.sort_values(
                by=['Year', 'plant', 'period_start'])
            electricity_saving_digital['ytm_amount'] = electricity_saving_digital.groupby(
                ['Year', 'plant'])['amount'].cumsum()
            electricity_saving_digital = electricity_saving_digital.drop(
                'Year', axis=1)

            # #計算bo and site
            # electricity_saving_tech = cal_bo_site(electricity_saving_tech,0)
            electricity_saving_digital['bo'] = 'ALL'
            electricity_saving_digital['site'] = 'ALL'
            electricity_saving_digital['plant'] = 'ALL'
            electricity_saving_digital = electricity_saving_digital[[
                'bo', 'site', 'plant', 'amount', 'ytm_amount', 'period_start']]

            electricity_saving_digital['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.electricity_saving_digital WHERE  period_start >='{period_year_start}'")
            electricity_saving_digital.to_sql(
                'electricity_saving_digital', conn, index=False, if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    #用電 - electricity
    if table_name == 'electricity':

        electricity = pd.read_sql(
            f"SELECT plant,amount,period_start FROM raw.electricity_total WHERE period_start >='{period_year_start}' ", con=db)

        electricity['amount'] = electricity['amount'].fillna(0)
        # 計算YTM
        electricity['Year'] = electricity['period_start'].apply(
            lambda x: x.year)
        electricity = electricity.sort_values(
            by=['Year', 'plant', 'period_start'])
        electricity['ytm_amount'] = electricity.groupby(['Year', 'plant'])[
            'amount'].cumsum()
        electricity = electricity.drop('Year', axis=1)

        #計算bo and site
        electricity = cal_bo_site(electricity, 0)
        electricity = electricity[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'period_start']]
        electricity['unit'] = "度"
        electricity['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        conn = db.connect()
        conn.execute(
            f"DELETE FROM staging.electricity WHERE  period_start >='{period_year_start}'")
        electricity.to_sql('electricity', conn, index=False,
                           if_exists='append', schema='staging', chunksize=10000)
        conn.close()

    #碳排 - carbon_emission
    if table_name == 'carbon_emission':
        carbon_emission = pd.read_sql(
            f"SELECT plant,category,amount,period_start FROM staging.carbon_emission WHERE period_start >='{period_year_start}' ", con=db)

        if carbon_emission.shape[0] > 0:
            carbon_emission['amount'] = carbon_emission['amount'].fillna(0)
            # 計算YTM
            carbon_emission['Year'] = carbon_emission['period_start'].apply(
                lambda x: x.year)
            carbon_emission = carbon_emission.sort_values(
                by=['Year', 'plant', 'category', 'period_start'])
            carbon_emission['ytm_amount'] = carbon_emission.groupby(
                ['Year', 'plant', 'category'])['amount'].cumsum()
            carbon_emission = carbon_emission.drop('Year', axis=1)

            #計算bo and site
            carbon_emission = cal_bo_site(carbon_emission, 1)
            carbon_emission = carbon_emission[[
                'bo', 'site', 'plant', 'category', 'amount', 'ytm_amount', 'period_start']]

            carbon_emission['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.carbon_emission_group WHERE  period_start >='{period_year_start}'")
            carbon_emission.to_sql('carbon_emission_group', conn, index=False,
                                   if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    # 每日用電量、每日生產量
    if table_name == 'daily':
        conn = db.connect()
        # dialy power
        period_start = (dt.now()-timedelta(days=1)).strftime("%Y-%m-01")

        electricity = pd.read_sql(
            f"""SELECT DISTINCT plant as plant_code,power as amount,datadate FROM raw.wks_mfg_fem_dailypower WHERE consumetype = '用電量' AND batch_id = (SELECT MAX(batch_id) FROM raw.wks_mfg_fem_dailypower ) AND datadate >= '{period_start}' """, con=db)
        if electricity.shape[0] > 0:
            electricity['plant_code'] = electricity['plant_code'].replace(
                'WCD', 'F721')
            electricity['plant_code'] = electricity['plant_code'].replace(
                'WCQ', 'F710')
            electricity['plant_code'] = electricity['plant_code'].replace(
                'WHC', 'F600')
            plant_mapping = pd.read_sql(
                'SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping', con=db)
            electricity = electricity.merge(
                plant_mapping, on='plant_code', how='left')
            # electricity = electricity[electricity['plant'] != 'XTRKS']
            electricity = electricity.dropna()
            # electricity = electricity.drop('plant_code', axis=1)
            electricity = electricity.groupby(
                ['plant', 'datadate']).sum().reset_index()

            electricity['amount'] = electricity['amount'].fillna(0)
            electricity['amount'] = electricity['amount']/1000
            electricity['unit'] = '千度'
            # 計算YTM
            electricity['Year'] = electricity['datadate'].apply(
                lambda x: x[0:4])
            electricity['month'] = electricity['datadate'].apply(
                lambda x: x[5:7])
            electricity = electricity.sort_values(
                by=['Year', 'month', 'plant', 'datadate'])
            electricity['ytm_amount'] = electricity.groupby(
                ['Year', 'month', 'plant'])['amount'].cumsum()
            electricity = electricity.drop(['Year', 'month'], axis=1)
            electricity = electricity[[
                'plant', 'datadate', 'amount', 'ytm_amount', 'unit']]
            electricity['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn.execute(
                f"DELETE FROM staging.daily_power WHERE  datadate >='{period_start}'")
            electricity.to_sql('daily_power', conn, index=False,
                               if_exists='append', schema='staging', chunksize=10000)

        # daily production
        period_start = (dt.now()-timedelta(days=1)).strftime("%Y%m")

        # from DPM
        production_qty = pd.read_sql(
            f"""SELECT period ,plant AS plant_code,output AS amount FROM raw.wks_mfg_dpm_upphndetail WHERE period LIKE '{period_start}%' """, con=db)
        if production_qty.shape[0] > 0:
            production_qty = production_qty.groupby(
                ['plant_code', 'period']).sum().reset_index()
            plant_mapping = pd.read_sql(
                'SELECT plant_name AS "plant",plant_code FROM raw.plant_mapping', con=db)
            production_qty = production_qty.merge(
                plant_mapping, on='plant_code', how='left')
            production_qty = production_qty.groupby(
                ['plant', 'period']).sum().reset_index()

            production_qty['amount'] = production_qty['amount'].fillna(0)

            # 計算YTM
            production_qty['Year'] = production_qty['period'].apply(
                lambda x: x[0:4])
            production_qty['month'] = production_qty['period'].apply(
                lambda x: x[4:6])
            production_qty = production_qty.sort_values(
                by=['Year', 'month', 'plant', 'period'])
            production_qty['ytm_amount'] = production_qty.groupby(
                ['Year', 'month', 'plant'])['amount'].cumsum()
            production_qty = production_qty.drop(['Year', 'month'], axis=1)
            production_qty['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            production_qty['period'] = production_qty['period'].apply(
                lambda x: x[0:4] + "-" + x[4:6] + "-" + x[6:8])

            period_start = (dt.now()-timedelta(days=1)).strftime("%Y-%m-01")
            conn.execute(
                f"DELETE FROM staging.daily_production WHERE  period >='{period_start}'")
            production_qty.to_sql('daily_production', conn, index=False,
                                  if_exists='append', schema='staging', chunksize=10000)
        conn.close()

    if table_name == 'manpower':
        manpower = pd.read_sql(
            f"SELECT * FROM raw.payrollcnt WHERE period_start >='{period_year_start}'", con=db)

        if manpower.shape[0] > 0:
            manpower['amount'] = manpower['amount'].fillna(0)
            # 計算YTM
            manpower['Year'] = manpower['period_start'].apply(lambda x: x.year)
            manpower = manpower.sort_values(
                by=['Year', 'plant', 'period_start'])
            manpower['ytm_amount'] = manpower.groupby(['Year', 'plant'])[
                'amount'].cumsum()
            manpower = manpower.drop('Year', axis=1)

            #計算bo and site
            manpower = cal_bo_site(manpower, 0)
            manpower = manpower[['bo', 'site', 'plant',
                                 'amount', 'ytm_amount', 'period_start']]
            manpower['unit'] = '人'
            manpower['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.payrollcnt WHERE  period_start >='{period_year_start}'")
            manpower.to_sql('payrollcnt', db, index=False, if_exists='append',
                            schema='staging', chunksize=10000)
            conn.close()

    if table_name == 'livingcnt':
        living_hc = pd.read_sql(
            f"SELECT * FROM raw.livingcnt WHERE period_start >='{period_year_start}'", con=db)

        if living_hc.shape[0] > 0:
            living_hc['amount'] = living_hc['amount'].fillna(0)
            # 計算YTM
            living_hc['Year'] = living_hc['period_start'].apply(
                lambda x: x.year)
            living_hc = living_hc.sort_values(
                by=['Year', 'plant', 'period_start'])
            living_hc['ytm_amount'] = living_hc.groupby(['Year', 'plant'])[
                'amount'].cumsum()
            living_hc = living_hc.drop('Year', axis=1)

            #計算bo and site
            living_hc = cal_bo_site(living_hc, 0)
            living_hc = living_hc[['bo', 'site', 'plant',
                                   'amount', 'ytm_amount', 'period_start']]
            living_hc['unit'] = '人'
            living_hc['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.livingcnt WHERE  period_start >='{period_year_start}'")
            living_hc.to_sql('livingcnt', db, index=False, if_exists='append',
                             schema='staging', chunksize=10000)
            conn.close()

    if table_name == 'water_detail':
        water_detail = pd.read_sql(
            f"""SELECT plant,category,amount,period_start FROM raw.water_detail WHERE period_start >='{period_year_start}' """, con=db)

        if water_detail.shape[0] > 0:

            water_detail.loc[water_detail['category']
                             == '宿舍', 'category'] = 'living'
            water_detail.loc[water_detail['category']
                             == '廠區', 'category'] = 'factory'
            water_detail = water_detail.fillna(0)

            water_detail['Year'] = water_detail['period_start'].apply(
                lambda x: x.year)
            water_detail = water_detail.sort_values(
                by=['Year', 'plant', 'category', 'period_start'])
            water_detail['ytm_amount'] = water_detail.groupby(
                ['Year', 'plant', 'category'])['amount'].cumsum()
            water_detail = water_detail.drop('Year', axis=1)

            water_detail = cal_bo_site(water_detail, 1)
            water_detail = water_detail[[
                'bo', 'site', 'plant', 'category', 'amount', 'ytm_amount', 'period_start']]
            water_detail['unit'] = "立方米"
            water_detail['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.water_detail WHERE  period_start >='{period_year_start}'")
            water_detail.to_sql('water_detail', db, index=False,
                                if_exists='append', schema='staging', chunksize=10000)
            conn.close()
