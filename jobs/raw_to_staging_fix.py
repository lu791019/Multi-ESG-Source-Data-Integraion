import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
import calendar
from models import engine
from jobs.raw_to_staging import cal_bo_site



def current_year():
    # 起始年份
    start_year = 2022
    # 结束年份（不包括）
    end_year = dt.now().year+1

    # 使用循环和relativedelta计算每年的起始日期和结束日期
    for year in range(start_year, end_year):

        period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

        if year == end_year - 1:

            if dt.now().month == 1 :

                period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

        else:

            period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

    return period_year_start, period_year_end



def raw_to_staging_fix(table_name, stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)
    table_name = str(table_name)


    # # 起始年份
    # start_year = 2022
    # # 结束年份（不包括）
    # end_year = dt.now().year+1

    # # 使用循环和relativedelta计算每年的起始日期和结束日期
    # for year in range(start_year, end_year):

    #     period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

    #     if year == end_year - 1:

    #         period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

    #     else:

    #         period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

    #廢棄物 - waste
    if table_name == 'waste':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            waste = pd.read_sql(
                f"""SELECT plant,category,amount,period_start FROM raw.waste WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'  """, con=db)
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
                    f"DELETE FROM staging.waste WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'")
                waste.to_sql('waste', conn, index=False,
                            if_exists='append', schema='staging', chunksize=10000)
                conn.close()

    #可再生能源 - renewable_energy
    if table_name == 'renewable_energy':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            renewable_energy = pd.read_sql(
                f"""SELECT plant,category2 AS "category",amount,period_start FROM raw.renewable_energy WHERE category1 = '綠色能源' AND period_start >='{period_year_start}' and period_start <='{period_year_end}' """, con=db)

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
                    f"DELETE FROM staging.renewable_energy WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'")
                renewable_energy.to_sql('renewable_energy', conn, index=False,
                                        if_exists='append', schema='staging', chunksize=10000)
                conn.close()

    #用水 - water
    if table_name == 'water':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            water = pd.read_sql(
                f"SELECT * FROM raw.water WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}'", con=db)
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
                    f"DELETE FROM staging.water WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'")
                water.to_sql('water', db, index=False, if_exists='append',
                            schema='staging', chunksize=10000)
                conn.close()

    #節能技改 - electricity_saving_tech
    if table_name == 'electricity_saving_tech':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")
            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            electricity_saving_tech = pd.read_sql(
                f"SELECT * FROM raw.electricity_saving_tech WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}'", con=db)
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
                    f"DELETE FROM staging.electricity_saving_tech WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
                electricity_saving_tech.to_sql(
                    'electricity_saving_tech', conn, index=False, if_exists='append', schema='staging', chunksize=10000)
                conn.close()

    #節電-數位化 - electricity_saving_digital
    if table_name == 'electricity_saving_digital':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            electricity_saving_digital = pd.read_sql(
                f"SELECT plant,kpi AS amount,period_start FROM raw.electricity_saving_digital WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}' ", con=db)
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
                    f"DELETE FROM staging.electricity_saving_digital WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
                electricity_saving_digital.to_sql(
                    'electricity_saving_digital', conn, index=False, if_exists='append', schema='staging', chunksize=10000)
                conn.close()

    #用電 - electricity
    if table_name == 'electricity':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")
            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            electricity = pd.read_sql(
                f"SELECT plant,amount,period_start FROM raw.electricity_total WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}' ", con=db)

            electricity['amount'] = electricity['amount'].fillna(0)
            # 計算YTM
            electricity['Year'] = electricity['period_start'].apply(
                lambda x: x.year)
            electricity = electricity.sort_values(
                by=['Year', 'plant', 'period_start'])
            electricity['amount'] = pd.to_numeric(electricity['amount'], errors='coerce')
            electricity['ytm_amount'] = electricity.groupby(['Year', 'plant'])['amount'].cumsum()
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
                f"DELETE FROM staging.electricity WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
            electricity.to_sql('electricity', conn, index=False,
                            if_exists='append', schema='staging', chunksize=10000)
            conn.close()

    #碳排 - carbon_emission
    if table_name == 'carbon_emission':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            carbon_emission = pd.read_sql(
                f"SELECT plant,category,amount,period_start FROM staging.carbon_emission WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}' ", con=db)

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
                    f"DELETE FROM staging.carbon_emission_group WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
                carbon_emission.to_sql('carbon_emission_group', conn, index=False,
                                    if_exists='append', schema='staging', chunksize=10000)
                conn.close()

    if table_name == 'manpower':
        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            manpower = pd.read_sql(
                f"SELECT * FROM raw.payrollcnt WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}'", con=db)

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
                    f"DELETE FROM staging.payrollcnt WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
                manpower.to_sql('payrollcnt', db, index=False, if_exists='append',
                                schema='staging', chunksize=10000)
                conn.close()

    if table_name == 'livingcnt':

        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            living_hc = pd.read_sql(
                f"SELECT * FROM raw.livingcnt WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}'", con=db)

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
                    f"DELETE FROM staging.livingcnt WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
                living_hc.to_sql('livingcnt', db, index=False, if_exists='append',
                                schema='staging', chunksize=10000)
                conn.close()

    if table_name == 'water_detail':
        # 起始年份
        start_year = 2022
        # 结束年份（不包括）
        end_year = dt.now().year+1

        for year in range(start_year, end_year):

            period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

            if year == end_year - 1:

                if dt.now().month == 1 :

                    period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

                else:

                    period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")
            else:

                period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")

            water_detail = pd.read_sql(
                f"""SELECT plant,category,amount,period_start FROM raw.water_detail WHERE period_start >='{period_year_start}'and period_start <='{period_year_end}' """, con=db)

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
                    f"DELETE FROM staging.water_detail WHERE  period_start >='{period_year_start}'and period_start <='{period_year_end}'")
                water_detail.to_sql('water_detail', db, index=False,
                                    if_exists='append', schema='staging', chunksize=10000)
                conn.close()