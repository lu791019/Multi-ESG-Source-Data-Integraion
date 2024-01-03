import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from dateutil.relativedelta import relativedelta
import os

from models import engine, engine_source
from jobs import etl_sql
from jobs import csr_to_raw
from jobs.csr_replace import update_to_raw
from jobs.raw_to_staging import cal_bo_site
from jobs.staging_to_app import data_import_app
from jobs.fix_data import fix_raw, fix_scope1
from jobs.plant_cal import csr_detail_integration_pre
from jobs.wks_detail_import import csr_item_import_pre

connect_csr_string = engine_source.get_connect_string_csr()
connect_eco_string = engine.get_connect_string()

db = create_engine(connect_eco_string, echo=True)
db_csr = create_engine(connect_csr_string, echo=True)


plant_exclude = ('WKS-1', 'WKS-5', 'WKS-6', 'WKS-6A', 'WKS-6B', 'WZS-1',
                 'WZS-3', 'WZS-6', 'WZS-8', 'WMY-1', 'WMY-2', 'WCQ', 'WCQ-2')


def useful_datetime(i):

    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%Y-%m-%d")
    last_year_period_start = (date(
        dt.now().year-1, dt.now().month, 1) - relativedelta(months=i)).strftime("%Y-%m-%d")
    period_start1 = (date(dt.now().year, dt.now().month, 1) -
                     relativedelta(months=i)).strftime("%Y%m%d")
    period = (date(dt.now().year-1, dt.now().month, 1) -
              relativedelta(months=i)).strftime("%Y-%m")
    period_year = (date(dt.now().year, dt.now().month, 1) -
                   relativedelta(months=i)).strftime("%Y")

    return period_start, last_year_period_start, period_start1, period, period_year


def update_to_raw(df_target, raw_table, period_start, plant):

    conn = db.connect()
    conn.execute(
        f"""Delete From raw.{raw_table} where period_start = '{period_start}' AND plant = '{plant}' """)
    df_target.to_sql(str(raw_table), conn, index=False,
                     if_exists='append', schema='raw', chunksize=10000)
    conn.close()


def csr_update_raw(raw_table, csr_table):

    # excute 2021-01 ~ 2021-12
    for t in range(dt.now().month, dt.now().month+12):
        period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
            t)

        raw_data = pd.read_sql(
            f"""SELECT * FROM raw.{raw_table} WHERE period_start = '{period_start}' AND plant not in {plant_exclude}""", con=db)
        for i in range(0, len(raw_data)):
            csr_data = pd.read_sql(
                f"""SELECT plant,period_start,indicatorvalue as amount FROM {csr_table} WHERE plant = '{raw_data['plant'][i]}' AND period_start = '{period_start}'""", con=db)
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


def carbon_emission_etl(stage, period_year, period_year_start, period_year_end):
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    electricity = pd.read_sql(
        f"SELECT plant,amount,period_start FROM raw.electricity_total WHERE period_start >= '{period_year_start}' and period_start <='{period_year_end}' ", con=db)
    if electricity.shape[0] > 0:
        # 2022/1後為FEM，包含太陽能發電，計算碳排須額外扣除
        renewable_energy = pd.read_sql(
            f"""SELECT plant,amount,period_start FROM raw.renewable_energy WHERE  category1 = '綠色能源' AND category2 = '光伏' AND period_start >= '{period_year_start}' and period_start <='{period_year_end}'""", con=db)
        if renewable_energy.shape[0] > 0:
            if period_year_start >= '2022-01-01':
                check_electricity_type = pd.read_sql(
                    f"""SELECT plant,period_start,type FROM raw.electricity_total WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)
                # 資料為原FEM
                plant_fem = check_electricity_type[check_electricity_type['type'] == 'FEM']
                if plant_fem.shape[0] > 0:
                    plant_fem = plant_fem.merge(
                        renewable_energy, on=['plant', 'period_start'])
                    if plant_fem.shape[0] > 0:
                        plant_fem['amount'] = plant_fem['amount'] * -1
                        plant_fem = plant_fem.drop('type', axis=1)
                        electricity = electricity.append(
                            plant_fem).reset_index(drop=True)
                #資料為非FEM - WZS須扣除太陽能
                plant_non_fem = check_electricity_type[check_electricity_type['type'] != 'FEM']
                plant_non_fem = plant_non_fem[plant_non_fem['plant'].isin(
                    ['WZS-1', 'WZS-3', 'WZS-6', 'WZS-8'])]
                if plant_non_fem.shape[0] > 0:
                    plant_non_fem = plant_non_fem.merge(
                        renewable_energy, on=['plant', 'period_start'])
                    if plant_non_fem.shape[0] > 0:
                        plant_non_fem['amount'] = plant_non_fem['amount'] * -1
                        plant_non_fem = plant_non_fem.drop('type', axis=1)
                        electricity = electricity.append(
                            plant_non_fem).reset_index(drop=True)

        # 年底須扣除綠證重新計算碳排
        green_energy = pd.read_sql(
            f"""SELECT plant,amount,period_start FROM raw.renewable_energy WHERE category1 = '綠色能源' AND category2 = '綠證' AND period_start >= '{period_year_start}' and period_start <='{period_year_end}' """, con=db)
        if green_energy.shape[0] > 0:
            green_energy['amount'] = green_energy['amount'] * -1
            electricity = electricity.append(
                green_energy).reset_index(drop=True)

        # get carbon coef
        carbon_coef = pd.read_sql(
            f"""SELECT site AS "plant",amount FROM staging.cfg_carbon_coef WHERE year = '{period_year}' """, con=db)

        if carbon_coef.shape[0] > 0:
            carbon_emission = electricity.merge(
                carbon_coef, on='plant', how='left')
            carbon_emission = carbon_emission.dropna()

            # 計算碳排
            carbon_emission['amount'] = carbon_emission['amount_x'] * \
                carbon_emission['amount_y'] / 1000
            carbon_emission = carbon_emission.groupby(
                ['plant', 'period_start']).sum().reset_index()
            carbon_emission = carbon_emission[[
                'plant', 'period_start', 'amount']]
            carbon_emission['category'] = 'scope2'
            carbon_emission['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            conn = db.connect()
            conn.execute(
                f"DELETE FROM staging.carbon_emission WHERE category = 'scope2' AND period_start >='{period_year_start}' and period_start <='{period_year_end}'")
            carbon_emission.to_sql('carbon_emission', conn, index=False,
                                   if_exists='append', schema='staging', chunksize=10000)
            conn.close()


def raw_to_staging(table_name, period_year_start, period_year_end):

    db = create_engine(connect_eco_string, echo=True)
    table_name = str(table_name)

    #廢棄物 - waste
    if table_name == 'waste':
        waste = pd.read_sql(
            f"""SELECT plant,category,amount,period_start FROM raw.waste WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' """, con=db)
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

    #用水 - water
    if table_name == 'water':
        water = pd.read_sql(
            f"SELECT * FROM raw.water WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}' ", con=db)
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

    #用電 - electricity
    if table_name == 'electricity':

        electricity = pd.read_sql(
            f"SELECT plant,amount,period_start FROM raw.electricity_total WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' ", con=db)

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
            f"DELETE FROM staging.electricity WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'")
        electricity.to_sql('electricity', conn, index=False,
                           if_exists='append', schema='staging', chunksize=10000)
        conn.close()

    #碳排 - carbon_emission
    if table_name == 'carbon_emission':
        carbon_emission = pd.read_sql(
            f"SELECT plant,category,amount,period_start FROM staging.carbon_emission WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' ", con=db)

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
    if table_name == 'water_detail':
        water_detail = pd.read_sql(
            f"""SELECT plant,category,amount,period_start FROM raw.water_detail WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' """, con=db)

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
                f"DELETE FROM staging.water_detail WHERE  period_start >='{period_year_start}' and period_start <='{period_year_end}'")
            water_detail.to_sql('water_detail', db, index=False,
                                if_exists='append', schema='staging', chunksize=10000)
            conn.close()


def db_operate(table_name, sqlString, pandaFormat):
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    conn = db.connect()
    conn.execute(sqlString)
    pandaFormat.to_sql(table_name, conn, index=False,
                       if_exists='append', schema='app', chunksize=10000)
    conn.close()

    return True


def is_data_exist(data):
    return data > 0


def waste_operate(waste, table_name, period_start, db, last_year_period_start):
    waste_all = waste.groupby(['bo', 'site', 'plant']).sum().reset_index()
    # 營業額 revenue (十億台幣)
    revenue = pd.read_sql(
        f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.revenue WHERE period_start ='{period_start}'""", con=db)

    #出貨量 (片)
    invoice_qty = pd.read_sql(
        f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.invoice_qty WHERE period_start ='{period_start}'""", con=db)
    #ASP (千台幣/片)
    ASP = revenue.merge(invoice_qty, on=['bo', 'site', 'plant'], how='left')
    ASP['amount'] = (ASP['amount_x'] * 1000000) / (ASP['amount_y'])
    ASP['ytm_amount'] = (ASP['ytm_amount_x'] * 1000000) / (ASP['ytm_amount_y'])
    ASP = ASP[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 計薪人力 payrollcnt (人)
    manpower = pd.read_sql(
        f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.payrollcnt WHERE period_start ='{period_start}'""", con=db)
    # 不確定是否以千人來看
    # manpower['amount'] = manpower['amount']/1000
    # manpower['ytm_amount'] = manpower['ytm_amount']/1000

    # 生產量
    prod_qty = pd.read_sql(
        f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.production_qty WHERE period_start ='{period_start}'""", con=db)

    prod = prod_qty.copy()
    prod['amount'] = prod['amount']/1000
    prod['ytm_amount'] = prod['ytm_amount']/1000

    # 廢棄物產生密度
    waste_intensity = waste_all.merge(
        revenue, on=['bo', 'site', 'plant'], how='left')
    waste_intensity['amount'] = waste_intensity['amount_x'] / \
        waste_intensity['amount_y']
    waste_intensity['ytm_amount'] = waste_intensity['ytm_amount_x'] / \
        waste_intensity['ytm_amount_y']
    waste_intensity = waste_intensity[[
        'bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # ASP還原廢棄物產生密度(頓/十億台幣) = 廢棄物總量(頓) / (去年同期ASP(千台幣/台)*當月生產量(千台))
    last_year_ASP = pd.read_sql(
        f"""SELECT bo,site,plant,amount,ytm_amount FROM app.waste_overview WHERE category1 = 'ASP' AND period_start ='{last_year_period_start}'""", con=db)
    waste_recovery = waste_all.merge(
        last_year_ASP, on=['bo', 'site', 'plant'], how='left')
    waste_recovery = waste_recovery.merge(
        invoice_qty, on=['bo', 'site', 'plant'], how='left')

    waste_recovery['amount'] = waste_recovery['amount_x'] / \
        (waste_recovery['amount_y'] * waste_recovery['amount'] / 1000000)
    waste_recovery['ytm_amount'] = waste_recovery['ytm_amount_x'] / \
        (waste_recovery['ytm_amount_y'] *
         waste_recovery['ytm_amount'] / 1000000)
    waste_recovery = waste_recovery[[
        'bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 廢棄物回收率
    recyclable = waste[waste['category1'] == 'recyclable']
    recyclable = recyclable.groupby(
        ['bo', 'site', 'plant']).sum().reset_index()

    recycling_rate = waste_all.merge(
        recyclable, on=['bo', 'site', 'plant'], how='left')
    recycling_rate = recycling_rate.fillna(0)

    recycling_rate['amount'] = recycling_rate['amount_y'] / \
        recycling_rate['amount_x']
    recycling_rate['ytm_amount'] = recycling_rate['ytm_amount_y'] / \
        recycling_rate['ytm_amount_x']
    recycling_rate = recycling_rate[[
        'bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 約當千台資源廢棄物
    waste_recycle = waste[(waste['category1'] == 'recyclable') & (
        waste['category2'] == 'hazardous')]
    recycl_prd = waste_recycle.merge(
        prod, on=['bo', 'site', 'plant'], how='left')
    recycl_prd['amount'] = recycl_prd['amount_x'] / \
        recycl_prd['amount_y']
    recycl_prd['ytm_amount'] = recycl_prd['ytm_amount_x'] / \
        recycl_prd['ytm_amount_y']
    recycl_prd = recycl_prd[[
        'bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 人均廚餘
    waste_kitchen = waste[(waste['category1'] == 'recyclable')
                          & (waste['category2'] == 'general')]
    kitchen_manpower = waste_kitchen.merge(
        manpower, on=['bo', 'site', 'plant'], how='left')
    kitchen_manpower['amount'] = kitchen_manpower['amount_x'] / \
        kitchen_manpower['amount_y']
    kitchen_manpower['ytm_amount'] = kitchen_manpower['ytm_amount_x'] / \
        kitchen_manpower['ytm_amount_y']
    kitchen_manpower = kitchen_manpower[[
        'bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 單位轉換
    # 出貨量 片->千片
    invoice_qty['amount'] = invoice_qty['amount'] / 1000
    invoice_qty['ytm_amount'] = invoice_qty['ytm_amount'] / 1000

    revenue["category1"] = 'revenue'
    waste_intensity["category1"] = 'waste_intensity'
    recycling_rate["category1"] = 'recycling_rate'
    ASP["category1"] = 'ASP'
    waste_recovery["category1"] = 'waste_recovery'
    invoice_qty["category1"] = 'invoice_qty'
    manpower['category1'] = 'manpower'
    prod_qty['category1'] = 'prod_qty'
    kitchen_manpower['category1'] = 'kitchen_manpower'
    recycl_prd['category1'] = 'recycl_prd'

    waste_overview = waste.append(revenue).append(waste_intensity).append(recycling_rate).append(
        ASP).append(waste_recovery).append(invoice_qty).append(kitchen_manpower).append(recycl_prd).append(manpower).append(prod_qty).reset_index(drop=True)
    waste_overview['period_start'] = period_start
    waste_overview['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    waste_overview = waste_overview[['bo', 'site', 'plant', 'amount',
                                    'ytm_amount', 'category1', 'category2', 'period_start', 'last_update_time']]

    return db_operate(
        table_name,
        f"DELETE FROM app.waste_overview WHERE  period_start ='{period_start}'",
        waste_overview,
    )


def staging_to_app(table_name, stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    table_name = str(table_name)

    # try:
    # excute 2021-01 ~ 2021-12
    for i in range(dt.now().month, dt.now().month+12):
        period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
            i)
        data_import_app(table_name, period_start, last_year_period_start,
                        period_start1, period, period_year, db, stage)

    #     return True

    # except:

    #     return False


def csr_item_import(item, raw_table, WKS_plant):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    try:
        # excute 2021-01 ~ 2021-12
        for i in range(dt.now().month, dt.now().month+12):
            period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
                i)
            csr_item_import_pre(item, raw_table, WKS_plant, period_start)

        return True

    except:

        return False


def csr_detail_integration(item, raw_target1, raw_target2, site='WKS', WKS_plant=('WKS-5', 'WKS-6')):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    try:
        # excute 2021-01 ~ 2021-12
        for i in range(dt.now().month, dt.now().month+12):
            period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(
                i)
            csr_detail_integration_pre(
                item, raw_target1, raw_target2, period_start, site='WKS', WKS_plant=('WKS-5', 'WKS-6'))

        return True

    except:

        return False


def get_stage():
    return os.environ['STAGE'] if 'STAGE' in os.environ else 'qas'


# if __name__ == '__main__':
def main():
    stage = get_stage()
    period_year = '2022'
    period_year_start = '2022-01-01'
    period_year_end = '2022-12-01'

    # csr_to_raw.import_csr_data([131])  # 用電
    # csr_to_raw.import_csr_data([2])  # 用水
    # csr_to_raw.import_csr_data([22, 23, 24, 25, 50, 67, 68, 69, 85, 91])  # 廢棄物
    # csr_to_raw.import_csr_scope1()
    # csr_to_raw.csr_detail_import('csr_kpidetail')
    # csr_to_raw.import_carbon_coef('carbon_coef')

    # etl_sql.run_sql_file('./sqls/waste_csr_to_raw.sql')

    # etl_sql.run_sql_file('./sqls/electricity_backstage_import.sql')

    # etl_sql.run_sql_file('./sqls/water_backstage_import.sql')

    # csr_update_raw('electricity_total', 'app.electricity_backstage_update')

    # csr_update_raw('water', 'app.water_backstage_update')

    # csr_detail_integration('Water', 'water_detail',
    #                        'water', 'WKS')

    # csr_detail_integration('Water', 'water_detail',
    #                        'water_backstage_update', 'WOK')

    # csr_detail_integration('Water', 'water_detail',
    #                        'water_backstage_update', 'WTZ')

    # csr_item_import('Electricity', 'electricity_total', ('WKS-5', 'WKS-6'))
    carbon_emission_etl(stage, period_year, period_year_start, period_year_end)
    etl_sql.run_sql_file('./sqls/staging_carbon_scope1.sql')

    raw_to_staging('waste', period_year_start, period_year_end)  # 廢棄物
    raw_to_staging('water', period_year_start, period_year_end)  # 用水
    raw_to_staging('electricity', period_year_start, period_year_end)  # 用電
    raw_to_staging('carbon_emission', period_year_start, period_year_end)  # 碳排
    raw_to_staging('water_detail', period_year_start, period_year_end)  # 碳排

    staging_to_app('reporting_summary', stage)  # 首頁
    staging_to_app('energy_overview', stage)  # 總覽比較
    staging_to_app('carbon_emission_overview', stage)  # 碳排放量
    staging_to_app('renewable_energy_overview', stage)  # 可再生能源
    staging_to_app('electricity_overview', stage)  # 用電
    staging_to_app('water_overview', stage)  # 用水
    # staging_to_app('electricity_unit_overview', stage)  # 單臺用電
    staging_to_app('waste_overview', stage)  # 廢棄物
    # staging_to_app('decarbon_target', stage)  # 脫碳目標
