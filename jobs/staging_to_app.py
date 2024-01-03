import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from dateutil.relativedelta import relativedelta
from jobs.raw_to_staging import cal_bo_site

from models import engine

def map_site_category(data):
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    #get plant_mapping for map site_category
    plant_mapping = pd.read_sql('SELECT DISTINCT plant_name AS "plant",site_category FROM raw.plant_mapping WHERE boundary = true', con=db)

    data = data.merge(plant_mapping, on='plant', how='left')
    return data


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
    revenue = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.revenue WHERE period_start ='{period_start}'""", con=db)

    #出貨量 (片)
    invoice_qty = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.invoice_qty WHERE period_start ='{period_start}'""", con=db)
    #ASP (千台幣/片)
    ASP = revenue.merge(invoice_qty, on=['bo', 'site', 'plant'], how='left')
    ASP['amount'] = (ASP['amount_x'] * 1000000) / (ASP['amount_y'])
    ASP['ytm_amount'] = (ASP['ytm_amount_x'] * 1000000) / (ASP['ytm_amount_y'])
    ASP = ASP[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 計薪人力 payrollcnt (人)
    manpower = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.payrollcnt WHERE period_start ='{period_start}'""", con=db)
    # 不確定是否以千人來看
    # manpower['amount'] = manpower['amount']/1000
    # manpower['ytm_amount'] = manpower['ytm_amount']/1000

    # 生產量
    prod_qty = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.production_qty WHERE period_start ='{period_start}'""", con=db)

    prod = prod_qty.copy()
    prod['amount'] = prod['amount']/1000
    prod['ytm_amount'] = prod['ytm_amount']/1000

    # 廢棄物產生密度
    waste_intensity = waste_all.merge(revenue, on=['bo', 'site', 'plant'], how='left')
    waste_intensity['amount'] = waste_intensity['amount_x'] / \
        waste_intensity['amount_y']
    waste_intensity['ytm_amount'] = waste_intensity['ytm_amount_x'] / \
        waste_intensity['ytm_amount_y']
    waste_intensity = waste_intensity[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # ASP還原廢棄物產生密度(頓/十億台幣) = 廢棄物總量(頓) / (去年同期ASP(千台幣/台)*當月生產量(千台))
    last_year_ASP = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM app.waste_overview WHERE category1 = 'ASP' AND period_start ='{last_year_period_start}'""", con=db)
    waste_recovery = waste_all.merge(last_year_ASP, on=['bo', 'site', 'plant'], how='left')
    waste_recovery = waste_recovery.merge(invoice_qty, on=['bo', 'site', 'plant'], how='left')

    waste_recovery['amount'] = waste_recovery['amount_x'] / \
        (waste_recovery['amount_y'] * waste_recovery['amount'] / 1000000)
    waste_recovery['ytm_amount'] = waste_recovery['ytm_amount_x'] / \
        (waste_recovery['ytm_amount_y'] *
         waste_recovery['ytm_amount'] / 1000000)
    waste_recovery = waste_recovery[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 廢棄物回收率
    recyclable = waste[waste['category1'] == 'recyclable']
    recyclable = recyclable.groupby(['bo', 'site', 'plant']).sum().reset_index()

    recycling_rate = waste_all.merge(recyclable, on=['bo', 'site', 'plant'], how='left')
    recycling_rate = recycling_rate.fillna(0)

    recycling_rate['amount'] = recycling_rate['amount_y'] / \
        recycling_rate['amount_x']
    recycling_rate['ytm_amount'] = recycling_rate['ytm_amount_y'] / \
        recycling_rate['ytm_amount_x']
    recycling_rate = recycling_rate[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 約當千台資源廢棄物
    waste_recycle = waste[(waste['category1'] == 'recyclable') & (waste['category2'] == 'hazardous')]
    recycl_prd = waste_recycle.merge(prod, on=['bo', 'site', 'plant'], how='left')
    recycl_prd['amount'] = recycl_prd['amount_x'] / \
        recycl_prd['amount_y']
    recycl_prd['ytm_amount'] = recycl_prd['ytm_amount_x'] / \
        recycl_prd['ytm_amount_y']
    recycl_prd = recycl_prd[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

    # 人均廚餘
    waste_kitchen = waste[(waste['category1'] == 'recyclable') & (waste['category2'] == 'general')]
    kitchen_manpower = waste_kitchen.merge(manpower, on=['bo', 'site', 'plant'], how='left')
    kitchen_manpower['amount'] = kitchen_manpower['amount_x'] / \
        kitchen_manpower['amount_y']
    kitchen_manpower['ytm_amount'] = kitchen_manpower['ytm_amount_x'] / \
        kitchen_manpower['ytm_amount_y']
    kitchen_manpower = kitchen_manpower[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

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

    #map site category
    waste_overview = map_site_category(waste_overview)

    return db_operate(
        table_name,
        f"DELETE FROM app.waste_overview WHERE  period_start ='{period_start}'",
        waste_overview,
    )


def data_import_app(table_name, period_start, last_year_period_start, period_start1, period, period_year, db, stage):
    # 首頁
    if table_name == 'reporting_summary':
        # 營業額 revenue (十億台幣)
        revenue = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.revenue WHERE  period_start ='{period_start}'""", con=db)

        # 廢棄物 waste (噸)
        waste = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.waste WHERE period_start ='{period_start}'""", con=db)
        if waste.shape[0] > 0:
            waste = waste.groupby(['bo', 'site', 'plant']).sum().reset_index()

            # 廢棄物產生密度 waste intensity (噸/十億台幣)
            waste_intensity = waste.merge(revenue, on=['bo', 'site', 'plant'], how='left')
            waste_intensity['amount_x'] = waste_intensity['amount_x'] / \
                waste_intensity['amount_y']
            waste_intensity['ytm_amount_x'] = waste_intensity['ytm_amount_x'] / \
                waste_intensity['ytm_amount_y']
            waste_intensity = waste_intensity.rename(
                columns={'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'})
            waste_intensity = waste_intensity[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
            waste_intensity['category'] = 'waste_intensity'
        else:
            waste_intensity = pd.DataFrame(columns=['bo', 'site', 'plant', 'amount', 'ytm_amount'])

        # 用水量 - water 噸->千噸
        water = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.water WHERE period_start ='{period_start}'""", con=db)
        water['amount'] = water['amount'] / 1000
        water['ytm_amount'] = water['ytm_amount'] / 1000

        # 用水強度 - water intensity (千噸/十億台幣)
        water_intensity = water.merge(revenue, on=['bo', 'site', 'plant'], how='left')
        water_intensity['amount_x'] = water_intensity['amount_x'] / \
            water_intensity['amount_y']
        water_intensity['ytm_amount_x'] = water_intensity['ytm_amount_x'] / \
            water_intensity['ytm_amount_y']
        water_intensity = water_intensity.rename(
            columns={'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'})
        water_intensity = water_intensity[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
        water_intensity['category'] = 'water_intensity'

        # 節電 - 技改 度->千度
        electricity_saving_tech = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity_saving_tech WHERE  period_start ='{period_start}'""", con=db)
        # electricity_saving_tech['category'] = 'electricity_saving_tech'
        electricity_saving_tech['amount'] = electricity_saving_tech['amount'] / 1000
        electricity_saving_tech['ytm_amount'] = electricity_saving_tech['ytm_amount'] / 1000

        # 節電 - 數位化 度->千度
        electricity_saving_digital = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity_saving_digital WHERE  period_start ='{period_start}'""", con=db)

        electricity_saving_digital['amount'] = electricity_saving_digital['amount'] / 1000
        electricity_saving_digital['ytm_amount'] = electricity_saving_digital['ytm_amount'] / 1000

        # 節電加總
        electricity_saving = electricity_saving_tech.append(electricity_saving_digital).reset_index(drop=True)
        # electricity_saving = electricity_saving.groupby(['bo']).sum().reset_index()

        # 用電
        electricity = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE period_start ='{period_start}'""", con=db)
        renewable_energy = pd.read_sql(f"""SELECT bo,site,plant,category,amount,ytm_amount FROM staging.renewable_energy WHERE period_start ='{period_start}'""", con=db)
        if period_start <= '2021-12-01':

            # 加上太陽能
            solar = renewable_energy[renewable_energy['category']  == 'solar_energy']
            electricity_all = electricity.append(solar.drop('category', axis=1))
            electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()

        else:
            # 2021-12-01之後用FEM數據 不須綠證也不需太陽能
            # renewable_energy_green為前次修改只有綠證部分 先暫且不使用
            # 如WOK、WKS 非FEM，則須加上太陽能
            check_electricity_type = pd.read_sql(f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()
                temp = renewable_energy[renewable_energy['category'] == 'solar_energy']
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy[renewable_energy['category'] == 'solar_energy']
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy = cal_bo_site(solar_energy,1)
                if solar_energy is None:
                    solar_energy = pd.DataFrame(columns=['bo', 'site', 'plant', 'amount', 'ytm_amount'])
                    electricity_all = electricity.append(solar_energy, axis=1)
                    electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
                else :
                    electricity_all = electricity.append(solar_energy.drop(['category','period_start'], axis=1))
                    electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity

        # 可再生能源占比
        renewable_energy_all = renewable_energy.groupby(['bo', 'site', 'plant']).sum().reset_index()
        non_renewable_electricity = electricity_all.merge(renewable_energy_all, on=['bo', 'site', 'plant'], how='left')
        if renewable_energy_all.shape[0] ==0:
            non_renewable_electricity.rename(columns={'amount': 'amount_x', 'ytm_amount': 'ytm_amount_x'}, inplace=True)
            non_renewable_electricity['amount_y'] = 0
            non_renewable_electricity['ytm_amount_y'] = 0
            non_renewable_electricity['amount'] = non_renewable_electricity['amount_x'] - \
                non_renewable_electricity['amount_y']
            non_renewable_electricity['ytm_amount'] = non_renewable_electricity['ytm_amount_x'] - \
                non_renewable_electricity['ytm_amount_y']

        else:
            non_renewable_electricity['amount'] = non_renewable_electricity['amount_x'] - \
                non_renewable_electricity['amount_y']
            non_renewable_electricity['ytm_amount'] = non_renewable_electricity['ytm_amount_x'] - \
                non_renewable_electricity['ytm_amount_y']

        non_renewable_electricity['category'] = 'nonrenewable_energy'
        non_renewable_electricity = non_renewable_electricity[['bo', 'site', 'plant', 'category', 'amount', 'ytm_amount']]

        # electricity['category'] = 'nonrenewable_energy'
        energy_percent = non_renewable_electricity.append(renewable_energy).reset_index(drop=True)
        energy_percent = energy_percent.merge(electricity_all, on=['bo', 'site', 'plant'], how='left')
        energy_percent = energy_percent.fillna(0)
        energy_percent['amount_x'] = energy_percent['amount_x'] / \
            energy_percent['amount_y']
        energy_percent['ytm_amount_x'] = energy_percent['ytm_amount_x'] / \
            energy_percent['ytm_amount_y']
        energy_percent = energy_percent.rename(columns={'category_x': 'category', 'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'})
        energy_percent = energy_percent[['bo', 'site', 'plant', 'category', 'amount', 'ytm_amount']]
        energy_percent['category'] = energy_percent['category'] + "_percent"

        # 用電單位轉換 度->千度
        electricity_all['amount'] = electricity_all['amount'] / 1000
        electricity_all['ytm_amount'] = electricity_all['ytm_amount'] / 1000

        #用電強度 (千度/十億台幣)
        electricity_intensity = electricity_all.merge(revenue, on=['bo', 'site', 'plant'], how='left')
        electricity_intensity['amount_x'] = electricity_intensity['amount_x'] / \
            electricity_intensity['amount_y']
        electricity_intensity['ytm_amount_x'] = electricity_intensity['ytm_amount_x'] / \
            electricity_intensity['ytm_amount_y']
        electricity_intensity = electricity_intensity.rename(columns={'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'})
        electricity_intensity = electricity_intensity[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
        electricity_intensity['category'] = 'electricity_intensity'

        #單臺用電 (度/台)
        prod_qty = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.production_qty WHERE period_start ='{period_start}'""", con=db)
        electricity_per_unit = electricity_all.merge(prod_qty, on=['bo', 'site', 'plant'], how='left')
        electricity_per_unit['amount_x'] = electricity_per_unit['amount_x'] / \
            electricity_per_unit['amount_y'] * 1000
        electricity_per_unit['ytm_amount_x'] = electricity_per_unit['ytm_amount_x'] / \
            electricity_per_unit['ytm_amount_y'] * 1000
        electricity_per_unit = electricity_per_unit.rename(columns={'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'})
        electricity_per_unit = electricity_per_unit[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
        electricity_per_unit['category'] = 'electricity_per_unit'

        # 碳排
        carbon_emission = pd.read_sql(f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.carbon_emission_group WHERE period_start ='{period_start}'""", con=db)
        carbon_emission = carbon_emission.groupby(['bo', 'site', 'plant']).sum().reset_index()

        revenue['category'] = 'revenue'
        waste['category'] = 'waste'
        water['category'] = 'water'
        electricity_saving_tech['category'] = 'electricity_saving_tech'
        electricity_saving_digital['category'] = 'electricity_saving_digital'
        electricity_saving['category'] = 'electricity_saving_all'
        electricity_all['category'] = 'electricity'
        carbon_emission['category'] = 'carbon_emission'

        reporting_summary = revenue.append(waste).append(waste_intensity).append(water).append(water_intensity).append(electricity_saving_tech).append(electricity_saving_digital).append(
            electricity_saving).append(electricity_all).append(energy_percent).append(electricity_intensity).append(electricity_per_unit).append(carbon_emission).reset_index(drop=True)

        reporting_summary['period_start'] = period_start
        reporting_summary['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        reporting_summary = reporting_summary[['bo', 'site', 'plant', 'amount', 'ytm_amount', 'category', 'period_start', 'last_update_time']]

        return db_operate(
            table_name,
            f"DELETE FROM app.reporting_summary WHERE  period_start ='{period_start}'",
            reporting_summary
        )

    # 總覽比較
    if table_name == 'energy_overview':
        # 用電
        electricity = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE  period_start ='{period_start}'""", con=db)
        renewable_energy = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.renewable_energy WHERE category = 'solar_energy' AND period_start ='{period_start}'""", con=db)

        if period_start <= '2021-12-01':

            # 加上太陽能
            electricity_all = electricity.append(renewable_energy)
            electricity_all = electricity_all.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()

        else:
            # 2021-12-01之後用FEM數據 故只有綠證
            # renewable_energy_green = pd.read_sql(f"""SELECT bo,site,plant,category,amount,ytm_amount FROM staging.renewable_energy WHERE  site = 'ALL' AND period_start = '{period_start}' and category='green_energy'""",con = db)
            # electricity_all = electricity.append(renewable_energy_green)
            # 如WOK、WKS 非FEM，則須加上太陽能
            check_electricity_type = pd.read_sql(
                f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()

                temp= renewable_energy.copy()
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy.copy()
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy = cal_bo_site(solar_energy,0)
                electricity_all = electricity.append(solar_energy.drop(['period_start'], axis=1))
                electricity_all = electricity.append(solar_energy)
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()

        # 用水
        water = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.water WHERE period_start ='{period_start}'""", con=db)

        # 營業額 revenue (十億台幣)
        revenue = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.revenue WHERE period_start ='{period_start}'""", con=db)

        # 出貨量
        invoice_qty = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.invoice_qty WHERE period_start ='{period_start}'""", con=db)
        ASP = revenue.merge(
            invoice_qty, on=['bo', 'site', 'plant'], how='left')
        ASP['amount'] = (ASP['amount_x'] * 1000000) / (ASP['amount_y'])
        ASP['ytm_amount'] = (ASP['ytm_amount_x'] *
                             1000000) / (ASP['ytm_amount_y'])
        ASP = ASP[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
        ASP['category'] = 'ASP'

        revenue['category'] = 'revenue'
        water['category'] = 'water'
        electricity_all['category'] = 'electricity'
        invoice_qty['category'] = 'invoice_qty'

        energy_overview = revenue.append(water).append(electricity_all).append(
            invoice_qty).append(ASP).reset_index(drop=True)
        energy_overview['period_start'] = period_start
        energy_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        energy_overview = energy_overview[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'category', 'period_start', 'last_update_time']]

        #map site category
        energy_overview = map_site_category(energy_overview)


        return db_operate(
            table_name,
            f"DELETE FROM app.energy_overview WHERE  period_start ='{period_start}'",
            energy_overview,
        )

    # 碳排放量
    if table_name == 'carbon_emission_overview':
        # 用電
        electricity = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE  period_start ='{period_start}'""", con=db)
        renewable_energy = pd.read_sql(
            f"""SELECT bo,site,plant,category as category2,amount,ytm_amount FROM staging.renewable_energy WHERE  period_start = '{period_start}'""", con=db)
        if period_start <= '2021-12-01':
            # 加上太陽能
            solar = renewable_energy[renewable_energy['category2']
                                     == 'solar_energy']
            electricity_all = electricity.append(solar)
            electricity_all = electricity_all.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()
        else:
            # 2021-12-01之後用FEM數據 故只有綠證
            # 如WOK、WKS 非FEM，則須加上太陽能
            check_electricity_type = pd.read_sql(
                f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()
                temp= renewable_energy[renewable_energy['category2'] == 'solar_energy']
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy[renewable_energy['category2'] == 'solar_energy']
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy['category'] = solar_energy['category2']
                solar_energy = cal_bo_site(solar_energy.drop('category2', axis=1),0)
                electricity_all = electricity.append(solar_energy.drop('category', axis=1))
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity

        # 碳排
        carbon_emission = pd.read_sql(
            f"""SELECT bo,site,plant,category as category2,amount,ytm_amount FROM staging.carbon_emission_group WHERE  period_start ='{period_start}'""", con=db)
        carbon_base = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.carbon_emission_group WHERE  period_start = '2016-12-01'""", con=db)

        # 碳排抵扣綠證目標
        green_energy_forecast = carbon_emission.copy()
        carbon_coef = pd.read_sql(
            f"""SELECT site,amount FROM staging."cfg_carbon_coef"  WHERE  year = '{period_year}'""", con=db)

        green_energy_forecast = green_energy_forecast.merge(
            carbon_base, on=['bo', 'site', 'plant'], how='left')

        green_energy_forecast = green_energy_forecast.merge(
            carbon_coef, on='site', how='left')

        # 計算目標 - (2021碳排-2016碳排*0.79)*1000/碳排放係數
        # get year carbon_target
        carbon_target = pd.read_sql(
            f"""SELECT target_desc FROM staging.cfg_target  WHERE bo = 'ALL' AND site = 'ALL' AND category = '碳排放量' AND year = '{period_year}'""", con=db)

        carbon_target['target_desc'] = carbon_target['target_desc'].str.replace(
            "下降 ", "")
        carbon_target['target_desc'] = carbon_target['target_desc'].str.replace(
            " %", "")
        carbon_target = (100-float(carbon_target['target_desc'][0]))/100

        green_energy_forecast['ytm_amount'] = (
            green_energy_forecast['ytm_amount_x'] - green_energy_forecast['ytm_amount_y']*carbon_target) * 1000 / green_energy_forecast['amount']

        green_energy_forecast['amount'] = green_energy_forecast['ytm_amount']
        green_energy_forecast['category'] = "green_energy_forecast"
        green_energy_forecast = green_energy_forecast[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]
        green_energy_forecast = green_energy_forecast.fillna(0)

        # carbon_emission total
        if carbon_emission.shape[0] > 0:
            carbon_emission_total = carbon_emission.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()
        else:
            carbon_emission_total = carbon_emission

        # carbon_coef
        carbon_coef = pd.read_sql(
            f"""SELECT site as "plant",amount FROM staging."cfg_carbon_coef"  WHERE  year = '{period_year}'""", con=db)

        plant_mapping = pd.read_sql(
            'SELECT bo,site,plant_name AS "plant" FROM raw.plant_mapping', con=db)
        carbon_coef = carbon_coef.merge(plant_mapping, on='plant', how='left')
        # map to bo and plant
        carbon_coef_bo_all = carbon_coef.copy()
        # carbon_coef_bo_all = carbon_coef_bo_all[carbon_coef_bo_all['bo'] != 'Others']
        carbon_coef_bo_all['bo'] = 'ALL'
        carbon_coef = carbon_coef.append(carbon_coef_bo_all)

        carbon_coef_bo_allothers = carbon_coef.copy()
        carbon_coef_bo_allothers = carbon_coef_bo_allothers[carbon_coef_bo_allothers['bo'] != 'ALL']
        carbon_coef_bo_allothers['bo'] = 'ALL+新邊界'
        carbon_coef = carbon_coef.append(carbon_coef_bo_allothers)

        carbon_coef_site = carbon_coef.copy()
        carbon_coef_site['plant'] = "ALL"
        carbon_coef_site = carbon_coef_site.drop_duplicates()

        carbon_coef = carbon_coef.append(
            carbon_coef_site).reset_index(drop=True)
        carbon_coef['ytm_amount'] = carbon_coef['amount']

        # 碳排放用電
        # 2022/1後為FEM，包含太陽能發電，須額外扣除
        if period_start <= '2021-12-01':
            green_energy = renewable_energy[renewable_energy['category2']
                                            == 'green_energy']
            green_energy = green_energy.fillna(0)
            electricity = electricity.merge(
                green_energy, on=['bo', 'site', 'plant'], how='left')
        else:
            # 扣除綠證
            green_energy = renewable_energy[renewable_energy['category2']  == 'green_energy']
            green_energy = green_energy.fillna(0)
            if green_energy.shape[0] > 0:
                green_energy['amount'] = green_energy['amount'] * -1
                green_energy['ytm_amount'] = green_energy['ytm_amount'] * -1
                green_energy = green_energy[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
                electricity = electricity.append(green_energy).reset_index(drop=True)

            # 扣除綠電
            green_electricity = renewable_energy[renewable_energy['category2']  == 'green_electricity']
            green_electricity = green_electricity.fillna(0)
            if green_electricity.shape[0] > 0:
                green_electricity['amount'] = green_electricity['amount'] * -1
                green_electricity['ytm_amount'] = green_electricity['ytm_amount'] * -1
                green_electricity = green_electricity[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
                electricity = electricity.append(green_electricity).reset_index(drop=True)

            # 需額外扣除WZS太陽能
            solar_energy = renewable_energy[renewable_energy['category2'] == 'solar_energy']
            solar_energy = solar_energy.fillna(0)
            solar_energy = solar_energy[solar_energy['site'] == 'WZS']
            #重新計算bo,site
            solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
            solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
            solar_energy = solar_energy.drop(['bo','site'],axis = 1)
            solar_energy = solar_energy.drop_duplicates()
            solar_energy['period_start'] = period_start
            solar_energy['category'] = solar_energy['category2']
            solar_energy = cal_bo_site(solar_energy.drop('category2', axis=1),0)
            if solar_energy.shape[0] > 0:
                solar_energy['amount'] = solar_energy['amount'] * -1
                solar_energy['ytm_amount'] = solar_energy['ytm_amount'] * -1
                solar_energy = solar_energy[['bo', 'site', 'plant', 'amount', 'ytm_amount']]
                electricity = electricity.append( solar_energy).reset_index(drop=True)

            electricity = electricity.groupby( ['bo', 'site', 'plant']).sum().reset_index()

        #replac renewable_energy category2 green_electricity to green_elect_buy for BE API
        renewable_energy['category2'].replace('green_electricity','green_elect_buy',inplace=True)
        # assign category
        electricity['category1'] = 'electricity'
        electricity_all['category1'] = 'electricity'
        renewable_energy['category1'] = 'electricity'
        green_energy_forecast['category1'] = 'electricity'

        electricity['category2'] = 'carbon_emission'
        electricity_all['category2'] = 'total'
        green_energy_forecast['category2'] = 'green_energy_forecast'

        carbon_emission['category1'] = 'carbon_emission'
        carbon_emission_total['category1'] = 'carbon_emission'
        carbon_coef['category1'] = 'carbon_emission'
        carbon_base['category1'] = 'carbon_emission'

        carbon_emission_total['category2'] = 'total'
        carbon_coef['category2'] = 'carbon_emission_coef'
        carbon_base['category2'] = 'base'

        carbon_emission_overview = electricity.append(electricity_all).append(renewable_energy).append(green_energy_forecast).append(
            carbon_emission).append(carbon_emission_total).append(carbon_coef).append(carbon_base).reset_index(drop=True)

        carbon_emission_overview['period_start'] = period_start
        carbon_emission_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        carbon_emission_overview = carbon_emission_overview[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'category1', 'category2', 'period_start', 'last_update_time']]

        #map site category
        carbon_emission_overview = map_site_category(carbon_emission_overview)

        # remove WMX and WCZ
        # carbon_emission_overview = carbon_emission_overview[~carbon_emission_overview['site'].isin(['WMX','WCZ'])]

        return db_operate(
            table_name,
            f"DELETE FROM app.carbon_emission_overview WHERE  period_start ='{period_start}'",
            carbon_emission_overview,
        )

    # 可再生能源
    if table_name == 'renewable_energy_overview':
        # 用電
        electricity = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE  period_start ='{period_start}'""", con=db)
        renewable_energy = pd.read_sql(
            f"""SELECT bo,site,plant,category,amount,ytm_amount FROM staging.renewable_energy WHERE  period_start = '{period_start}'""", con=db)

        #rename 直購綠電
        renewable_energy.loc[renewable_energy['category'] == 'green_electricity', 'category'] = 'green_elect_buy'

        if period_start <= '2021-12-01':
            # 加上太陽能
            solar = renewable_energy[renewable_energy['category']
                                     == 'solar_energy']
            electricity_all = electricity.append(
                solar.drop('category', axis=1))
            electricity_all = electricity_all.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()
        else:
            # 2021-12-01之後用FEM數據 故只有綠證
            # 如WOK、WKS 非FEM，則須加上太陽能
            check_electricity_type = pd.read_sql(f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()
                temp= renewable_energy[renewable_energy['category'] == 'solar_energy']
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy[renewable_energy['category'] == 'solar_energy']
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy = cal_bo_site(solar_energy,1)
                electricity_all = electricity.append(solar_energy.drop(['category','period_start'], axis=1))
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()

        # 可再生能源占比
        renewable_energy_all = renewable_energy.groupby(
            ['bo', 'site', 'plant']).sum().reset_index()

        renewable_energy_percent = renewable_energy_all.merge(
            electricity_all, on=['bo', 'site', 'plant'], how='left')

        renewable_energy_percent['amount'] = renewable_energy_percent['amount_x'] / \
            renewable_energy_percent['amount_y']
        renewable_energy_percent['ytm_amount'] = renewable_energy_percent['ytm_amount_x'] / \
            renewable_energy_percent['ytm_amount_y']
        renewable_energy_percent = renewable_energy_percent[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 再生能源綠證目標
        # get year target
        target = pd.read_sql(
            f"""SELECT DISTINCT amount FROM staging.cfg_target WHERE  category = '可再生能源' AND year ='{period_year}'""", con=db)

        solar_and_green_elect = renewable_energy[renewable_energy['category'].isin(['solar_energy','green_elect_buy'])]
        solar_and_green_elect = solar_and_green_elect.groupby(['bo', 'site', 'plant']).sum().reset_index()

        green_energy_forecast = electricity_all.merge(
            solar_and_green_elect, on=['bo', 'site', 'plant'], how='left')

        green_energy_forecast['amount_y'] = green_energy_forecast['amount_y'].fillna(0)
        green_energy_forecast['ytm_amount_y'] = green_energy_forecast['ytm_amount_y'].fillna(0)

        green_energy_forecast['amount'] = (
            green_energy_forecast['amount_x'] * target.iloc[0, 0]) - green_energy_forecast['amount_y']
        green_energy_forecast['ytm_amount'] = (
            green_energy_forecast['ytm_amount_x'] * target.iloc[0, 0]) - green_energy_forecast['ytm_amount_y']
        green_energy_forecast = green_energy_forecast[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]
        # 如已達標則補0
        green_energy_forecast['amount'] = green_energy_forecast['amount'].apply(
            lambda x: 0 if x < 0 else x)
        green_energy_forecast['ytm_amount'] = green_energy_forecast['ytm_amount'].apply(
            lambda x: 0 if x < 0 else x)

        electricity_all['category'] = 'electricity'
        renewable_energy_percent['category'] = 'renewable_energy_percent'
        green_energy_forecast['category'] = 'green_energy_forecast'

        renewable_energy_overview = electricity_all.append(renewable_energy).append(
            renewable_energy_percent).append(green_energy_forecast).reset_index(drop=True)
        renewable_energy_overview['period_start'] = period_start
        renewable_energy_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        renewable_energy_overview = renewable_energy_overview[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'category', 'period_start', 'last_update_time']]

        #map site category
        renewable_energy_overview = map_site_category(renewable_energy_overview)

        return db_operate(
            table_name,
            f"DELETE FROM app.renewable_energy_overview WHERE  period_start ='{period_start}'",
            renewable_energy_overview,
        )

    # 用電
    if table_name == 'electricity_overview':
        # 用電
        electricity = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE  period_start ='{period_start}'""", con=db)
        renewable_energy = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.renewable_energy WHERE category = 'solar_energy' AND period_start = '{period_start}'""", con=db)

        if period_start <= '2021-12-01':
            # 加上太陽能&綠證
            electricity_all = electricity.append(renewable_energy)
            electricity_all = electricity_all.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()

        else:
            # 2021-12-01之後用FEM數據 故只有綠證
            # 如WOK、WKS 非FEM，則須加上太陽能
            check_electricity_type = pd.read_sql(
                f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()

                temp= renewable_energy.copy()
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy.copy()
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy = cal_bo_site(solar_energy,0)
                electricity_all = electricity.append(solar_energy)
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity

        # 營業額 revenue (十億台幣)
        revenue = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.revenue WHERE period_start ='{period_start}'""", con=db)

        #出貨量 (片)
        invoice_qty = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.invoice_qty WHERE period_start ='{period_start}'""", con=db)
        #ASP (千台幣/片)
        ASP = revenue.merge(
            invoice_qty, on=['bo', 'site', 'plant'], how='left')
        ASP['amount'] = (ASP['amount_x'] * 1000000) / (ASP['amount_y'])
        ASP['ytm_amount'] = (ASP['ytm_amount_x'] *
                             1000000) / (ASP['ytm_amount_y'])
        ASP = ASP[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 十億營業額用電
        electricity_all_copy = electricity_all.copy()
        electricity_all_copy['amount'] = electricity_all_copy['amount'] / 1000
        electricity_all_copy['ytm_amount'] = electricity_all_copy['ytm_amount'] / 1000

        electricity_intensity = electricity_all_copy.merge(
            revenue, on=['bo', 'site', 'plant'], how='left')
        electricity_intensity['amount'] = electricity_intensity['amount_x'] / \
            electricity_intensity['amount_y']
        electricity_intensity['ytm_amount'] = electricity_intensity['ytm_amount_x'] / \
            electricity_intensity['ytm_amount_y']
        electricity_intensity = electricity_intensity[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        #ASP還原用電強度 = 用電量/(前一年ASP_YTM * 當月出貨量YTM)
        last_year_ASP = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM app.electricity_overview WHERE category = 'ASP' AND period_start ='{last_year_period_start}'""", con=db)
        electricity_recovery = electricity_all_copy.merge(
            last_year_ASP, on=['bo', 'site', 'plant'], how='left')
        electricity_recovery = electricity_recovery.merge(
            invoice_qty, on=['bo', 'site', 'plant'], how='left')

        electricity_recovery['amount'] = electricity_recovery['amount_x'] / (
            electricity_recovery['amount_y'] * electricity_recovery['amount'] / 1000000)
        electricity_recovery['ytm_amount'] = electricity_recovery['ytm_amount_x'] / (
            electricity_recovery['ytm_amount_y'] * electricity_recovery['ytm_amount'] / 1000000)
        electricity_recovery = electricity_recovery[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 單位轉換
        # 出貨量 片->千片
        invoice_qty['amount'] = invoice_qty['amount'] / 1000
        invoice_qty['ytm_amount'] = invoice_qty['ytm_amount'] / 1000

        electricity_all['category'] = 'electricity'
        revenue['category'] = 'revenue'
        ASP['category'] = 'ASP'
        invoice_qty['category'] = 'invoice_qty'
        electricity_intensity['category'] = 'electricity_intensity'
        electricity_recovery['category'] = 'electricity_recovery'

        electricity_overview = electricity_all.append(revenue).append(ASP).append(invoice_qty).append(
            electricity_intensity).append(electricity_recovery).reset_index(drop=True)

        electricity_overview['period_start'] = period_start
        electricity_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        electricity_overview = electricity_overview[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'category', 'period_start', 'last_update_time']]

        #map site category
        electricity_overview = map_site_category(electricity_overview)

        return db_operate(
            table_name,
            f"DELETE FROM app.electricity_overview WHERE  period_start ='{period_start}'",
            electricity_overview,
        )

    # 用水
    if table_name == 'water_overview':
        # 水
        water = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.water WHERE  period_start ='{period_start}'""", con=db)

        # 廠區用水 category = 'payroll'
        water_factory = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.water_detail WHERE period_start ='{period_start}' and category = 'factory'""", con=db)

        # 宿舍用水 category = 'living'
        water_dorm = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.water_detail WHERE period_start ='{period_start}' and category = 'living'""", con=db)

        # 營業額 revenue (十億台幣)
        revenue = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.revenue WHERE period_start ='{period_start}'""", con=db)

        # 計薪人力 payrollcnt (人)
        manpower = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.payrollcnt WHERE period_start ='{period_start}'""", con=db)

        # 廠區人力 (=計薪人力)
        factorycnt = manpower.copy()

        # 宿舍人力 livingcnt (人)
        livingcnt = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.livingcnt WHERE period_start ='{period_start}'""", con=db)

        # 廠區人均用水
        water_factory_cnt = water_factory.merge(
            factorycnt, on=['bo', 'site', 'plant'], how='left')
        water_factory_cnt['amount'] = water_factory_cnt['amount_x'] / \
            water_factory_cnt['amount_y']
        water_factory_cnt['ytm_amount'] = water_factory_cnt['ytm_amount_x'] / \
            water_factory_cnt['ytm_amount_y']
        water_factory_cnt = water_factory_cnt[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 宿舍人均用水
        water_living = water_dorm.merge(
            livingcnt, on=['bo', 'site', 'plant'], how='left')
        water_living['amount'] = water_living['amount_x'] / \
            water_living['amount_y']
        water_living['ytm_amount'] = water_living['ytm_amount_x'] / \
            water_living['ytm_amount_y']
        water_living = water_living[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 十億營業額用水
        water_copy = water.copy()
        water_copy['amount'] = water_copy['amount'] / 1000
        water_copy['ytm_amount'] = water_copy['ytm_amount'] / 1000
        water_intensity = water_copy.merge(
            revenue, on=['bo', 'site', 'plant'], how='left')
        water_intensity['amount'] = water_intensity['amount_x'] / \
            water_intensity['amount_y']
        water_intensity['ytm_amount'] = water_intensity['ytm_amount_x'] / \
            water_intensity['ytm_amount_y']
        water_intensity = water_intensity[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 人均用水
        water_manpower = water.merge(
            manpower, on=['bo', 'site', 'plant'], how='left')
        water_manpower['amount'] = water_manpower['amount_x'] / \
            water_manpower['amount_y']
        water_manpower['ytm_amount'] = water_manpower['ytm_amount_x'] / \
            water_manpower['ytm_amount_y']
        water_manpower = water_manpower[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        #出貨量 (片)
        invoice_qty = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.invoice_qty WHERE period_start ='{period_start}'""", con=db)
        #ASP (千台幣/片)
        ASP = revenue.merge(
            invoice_qty, on=['bo', 'site', 'plant'], how='left')
        ASP['amount'] = (ASP['amount_x'] * 1000000) / (ASP['amount_y'])
        ASP['ytm_amount'] = (ASP['ytm_amount_x'] *
                             1000000) / (ASP['ytm_amount_y'])
        ASP = ASP[['bo', 'site', 'plant', 'amount', 'ytm_amount']]

        #ASP還原用水強度 = 用水量/(前一年ASP_YTM * 當月出貨量YTM)
        last_year_ASP = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM app.water_overview WHERE category = 'ASP' AND period_start ='{last_year_period_start}'""", con=db)
        water_recovery = water_copy.merge(
            last_year_ASP, on=['bo', 'site', 'plant'], how='left')
        water_recovery = water_recovery.merge(
            invoice_qty, on=['bo', 'site', 'plant'], how='left')

        water_recovery['amount'] = water_recovery['amount_x'] / \
            (water_recovery['amount_y'] * water_recovery['amount'] / 1000000)
        water_recovery['ytm_amount'] = water_recovery['ytm_amount_x'] / \
            (water_recovery['ytm_amount_y'] *
             water_recovery['ytm_amount'] / 1000000)
        water_recovery = water_recovery[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        # 單位轉換
        # 出貨量 片->千片
        invoice_qty['amount'] = invoice_qty['amount'] / 1000
        invoice_qty['ytm_amount'] = invoice_qty['ytm_amount'] / 1000

        water['category'] = 'water'
        revenue['category'] = 'revenue'
        ASP['category'] = 'ASP'
        invoice_qty['category'] = 'invoice_qty'
        water_factory['category'] = 'factory'
        water_dorm['category'] = 'dorm'
        manpower['category'] = 'manpower'
        factorycnt['category'] = 'factorycnt'
        livingcnt['category'] = 'livingcnt'

        water_intensity['category'] = 'water_intensity'
        water_recovery['category'] = 'water_recovery'
        water_manpower['category'] = 'water_manpower'
        water_factory_cnt['category'] = 'water_factory_cnt'
        water_living['category'] = 'water_living'

        water_overview = water.append(revenue).append(ASP).append(invoice_qty).append(manpower).append(factorycnt).append(livingcnt).append(water_factory).append(
            water_dorm).append(water_intensity).append(water_manpower).append(water_recovery).append(water_factory_cnt).append(water_living).reset_index(drop=True)

        water_overview['period_start'] = period_start
        water_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        water_overview = water_overview[['bo', 'site', 'plant', 'amount',
                                        'ytm_amount', 'category', 'period_start', 'last_update_time']]

        #map site category
        water_overview = map_site_category(water_overview)

        return db_operate(
            table_name,
            f"DELETE FROM app.water_overview WHERE  period_start ='{period_start}'",
            water_overview,
        )

    # 單臺用電
    if table_name == 'electricity_unit_overview':
        # 用電
        electricity = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE  period_start ='{period_start}'""", con=db)
        renewable_energy = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.renewable_energy WHERE category = 'solar_energy' AND period_start = '{period_start}'""", con=db)

        if period_start <= '2021-12-01':
            # 加上太陽能
            electricity_all = electricity.append(renewable_energy)
            electricity_all = electricity_all.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()
        else:
            # 2021-12-01之後用FEM數據 故只有綠證
            # 如WOK、WKS 非FEM，則須加上太陽能
            check_electricity_type = pd.read_sql(f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()

                temp= renewable_energy.copy()
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy.copy()
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy = cal_bo_site(solar_energy,0)
                electricity_all = electricity.append(solar_energy)
                electricity_all = electricity_all.groupby(['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity

        #生產量 (片)
        pord_qty = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.production_qty WHERE period_start ='{period_start}'""", con=db)

        #單臺用電 (度)
        electricity_per_unit = electricity_all.merge(
            pord_qty, on=['bo', 'site', 'plant'], how='left')
        electricity_per_unit['amount'] = electricity_per_unit['amount_x'] / \
            electricity_per_unit['amount_y']
        electricity_per_unit['ytm_amount'] = electricity_per_unit['ytm_amount_x'] / \
            electricity_per_unit['ytm_amount_y']
        electricity_per_unit = electricity_per_unit[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount']]

        electricity_all['category'] = 'electricity'
        pord_qty['category'] = 'production_qty'
        electricity_per_unit['category'] = 'electricity_per_unit'

        electricity_unit_overview = electricity_all.append(
            pord_qty).append(electricity_per_unit).reset_index(drop=True)
        electricity_unit_overview['period_start'] = period_start
        electricity_unit_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        electricity_unit_overview = electricity_unit_overview[[
            'bo', 'site', 'plant', 'amount', 'ytm_amount', 'category', 'period_start', 'last_update_time']]

        #map site category
        electricity_unit_overview = map_site_category(electricity_unit_overview)

        return db_operate(
            table_name,
            f"DELETE FROM app.electricity_unit_overview WHERE  period_start ='{period_start}'",
            electricity_unit_overview,
        )

    # 脫碳目標
    if table_name == 'decarbon_target':
        electricity = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.electricity WHERE period_start ='{period_start}'""", con=db)

        renewable_energy = pd.read_sql(
            f"""SELECT bo,site,plant,category,amount,ytm_amount FROM staging.renewable_energy WHERE period_start ='{period_start}'""", con=db)

        carbon_emission = pd.read_sql(
            f"""SELECT bo,site,plant,category as category2,amount,ytm_amount FROM staging.carbon_emission_group WHERE  period_start ='{period_start}'""", con=db)

        carbon_base = pd.read_sql(
            f"""SELECT bo,site,plant,amount,ytm_amount FROM staging.carbon_emission_group WHERE  period_start = '2016-12-01'""", con=db)

        # 碳排抵扣綠證目標
        green_energy_forecast = carbon_emission.copy()

        plant_mapping = pd.read_sql(
            'SELECT bo,site,plant_name AS "plant" FROM raw.plant_mapping', con=db)

        # 碳排放用電
        # 2022/1後為FEM，包含太陽能發電，須額外扣除
        if period_start <= '2021-12-01':

            solar = renewable_energy[renewable_energy['category']
                                     == 'solar_energy']

            green_energy = renewable_energy[renewable_energy['category2']
                                            == 'green_energy']
            green_energy = green_energy.fillna(0)

        #     electricity = electricity.merge(green_energy, on=['bo', 'site', 'plant'], how='left')

            electricity_all = electricity.append(
                solar.drop('category', axis=1))
            electricity_all = electricity_all.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()

        else:
            renewable_energy_all = renewable_energy.groupby(
                ['bo', 'site', 'plant']).sum().reset_index()

            check_electricity_type = pd.read_sql(f"""SELECT plant FROM raw.electricity_total WHERE type != 'FEM' AND plant IN ('WOK','WKS-1','WKS-5','WKS-6','WHC') AND period_start ='{period_start}'""", con=db)
            if check_electricity_type.shape[0] > 0:
                plant_list = check_electricity_type['plant'].unique()
                temp = renewable_energy[renewable_energy['category'] == 'solar_energy']
                temp = temp[temp['plant'].isin(plant_list)].reset_index(drop=True)
                site_list = temp['site'].unique()
                solar_energy = renewable_energy[renewable_energy['category'] == 'solar_energy']
                solar_energy = solar_energy[solar_energy['site'].isin(site_list)].reset_index(drop=True)
                solar_energy = solar_energy.drop_duplicates()
                #重新計算bo,site
                solar_energy = solar_energy[solar_energy['plant'] != 'ALL']
                solar_energy = solar_energy[solar_energy['bo'] != 'ALL']
                solar_energy = solar_energy.drop(['bo','site'],axis = 1)
                solar_energy = solar_energy.drop_duplicates()
                solar_energy['period_start'] = period_start
                solar_energy = cal_bo_site(solar_energy,1)
                electricity_all = electricity.append(solar_energy.drop(['category','period_start'], axis=1))
                electricity_all = electricity_all.groupby(
                    ['bo', 'site', 'plant']).sum().reset_index()
            else:
                electricity_all = electricity
        #     electricity = electricity.merge(renewable_energy_all, on=['bo', 'site', 'plant'], how='left')

        # 可再生能源占比
        renewable_energy_all = renewable_energy.groupby(
            ['bo', 'site', 'plant']).sum().reset_index()
        non_renewable_electricity = electricity_all.merge(
            renewable_energy_all, on=['bo', 'site', 'plant'], how='left')
        non_renewable_electricity['amount'] = non_renewable_electricity['amount_x'] - \
            non_renewable_electricity['amount_y']
        non_renewable_electricity['ytm_amount'] = non_renewable_electricity['ytm_amount_x'] - \
            non_renewable_electricity['ytm_amount_y']
        non_renewable_electricity['category'] = 'nonrenewable_energy'
        non_renewable_electricity = non_renewable_electricity[[
            'bo', 'site', 'plant', 'category', 'amount', 'ytm_amount']]

        # electricity['category'] = 'nonrenewable_energy'
        energy_percent = non_renewable_electricity.append(
            renewable_energy).reset_index(drop=True)
        energy_percent = energy_percent.merge(
            electricity_all, on=['bo', 'site', 'plant'], how='left')
        energy_percent = energy_percent.fillna(0)
        energy_percent['amount_x'] = energy_percent['amount_x'] / \
            energy_percent['amount_y']
        energy_percent['ytm_amount_x'] = energy_percent['ytm_amount_x'] / \
            energy_percent['ytm_amount_y']
        energy_percent = energy_percent.rename(
            columns={'category_x': 'category', 'amount_x': 'amount', 'ytm_amount_x': 'ytm_amount'})
        energy_percent = energy_percent[[
            'bo', 'site', 'plant', 'category', 'amount', 'ytm_amount']]

        # energy_percent = energy_percent[energy_percent['category']
        #                                 != 'nonrenewable_energy']
        # energy_percent['category'] = 'solar_energy'

        energy_percent['category'] = energy_percent['category'] + "_percent"

        energy_percent_total = energy_percent[energy_percent['category']
                                              == 'nonrenewable_energy_percent']
        energy_percent_total['category'] = energy_percent_total['category'].replace(
            'nonrenewable_energy_percent', 'Total佔比')
        energy_percent_total['amount'] = 1 - energy_percent_total['amount']
        energy_percent_total['ytm_amount'] = 1 - \
            energy_percent_total['ytm_amount']
        energy_percent = energy_percent.append(energy_percent_total)

        energy_percent = energy_percent.loc[(
            energy_percent['category'] != 'nonrenewable_energy_percent')]

        energy_percent.rename(columns={'category': 'category2'}, inplace=True)

        energy_percent['amount'] = energy_percent['amount']*100
        energy_percent['ytm_amount'] = energy_percent['ytm_amount']*100

        # energy_percent_total = energy_percent.copy()
        # energy_percent['category'] = energy_percent['category'] + "_percent"

        electricity_all['amount'] = electricity_all['amount'] / 10**8
        electricity_all['ytm_amount'] = electricity_all['ytm_amount'] / 10**8

        # assing item
        electricity_all['item'] = '總電量'
        carbon_emission['item'] = '碳排放'
        energy_percent['item'] = '可再生能源'
        # energy_percent_total['item'] = '可再生能源'

        # assign category1
        # electricity['category1'] = 'electricity'
        # carbon_emission_total['category1'] = 'carbon_emission'
        # carbon_coef['category1'] = 'carbon_emission'
        # renewable_energy['category1'] = 'electricity'
        # green_energy_forecast['category1'] = 'electricity'
        # carbon_base['category1'] = 'carbon_emission'
        electricity_all['category1'] = '該年耗電'
        carbon_emission['category1'] = '噸 CO2e'
        energy_percent['category1'] = '當年度'
        # energy_percent_total['category1'] = '當年度'

        # assign category2
        # electricity['category2'] = 'carbon_emission'
        # carbon_emission_total['category2'] = 'total'
        # carbon_coef['category2'] = 'carbon_emission_coef'
        # green_energy_forecast['category2'] = 'green_energy_forecast'
        # carbon_base['category2'] = 'base'
        electricity_all['category2'] = '每年增長 5%'
        # energy_percent_total['category2'] = energy_percent_total['category2'].replace(
        #     'solar_energy', 'Total佔比')
        energy_percent['category2'] = energy_percent['category2'].replace(
            'solar_energy_percent', '自建太陽能')

        energy_percent['category2'] = energy_percent['category2'].replace(
            'green_energy_percent', '購買綠證')

        carbon_emission['category2'] = carbon_emission['category2'].replace(
            'scope1', 'Scope1')
        carbon_emission['category2'] = carbon_emission['category2'].replace(
            'scope2', 'Scope2')

        # assign unit
        electricity_all['unit'] = '億度'
        carbon_emission['unit'] = '噸'
        carbon_emission['unit'] = '噸'
        # energy_percent_total['unit'] = '%'
        energy_percent['unit'] = '%'

        decarbon_overview = electricity_all.append(
            carbon_emission).append(energy_percent).reset_index(drop=True)
        decarbon_overview['period_start'] = period_start

        current_day = dt.now().day

        if stage == 'development':  # DEV - 10號前抓2個月
            checkpoint = 10
        else:  # QAS、PRD - 15號前抓2個月
            checkpoint = 12

        if dt.now().month == 1:
            decarbon_overview['year'] = dt.now().year - 1

        elif (dt.now().month == 2) & (current_day < checkpoint):
            decarbon_overview['year'] = dt.now().year - 1

        else:
            decarbon_overview['year'] = dt.now().year

        decarbon_overview['status'] = 'actual'
        decarbon_overview['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        decarbon_overview = decarbon_overview[['bo', 'site', 'plant', 'amount', 'ytm_amount', 'item',
                                               'category1', 'category2', 'unit', 'period_start', 'year', 'status', 'last_update_time']]

        return db_operate(
            table_name,
            f"DELETE FROM app.decarbon_target WHERE period_start ='{period_start}' and status = 'actual'",
            decarbon_overview,
        )

    # 廢棄物
    if table_name == 'waste_overview':
        # 廢棄物
        waste = pd.read_sql(
            f"""SELECT bo,site,plant,category1,category2,amount,ytm_amount FROM staging.waste WHERE  period_start ='{period_start}'""", con=db)
        if is_data_exist(waste.shape[0]):
            waste_operate(waste, table_name, period_start,
                          db, last_year_period_start)
        else:
            return False


# def useful_datetime(i):

#     period_start = (date(dt.now().year, dt.now().month, 1) -
#                     relativedelta(months=i)).strftime("%Y-%m-%d")
#     last_year_period_start = (date(
#         dt.now().year-1, dt.now().month, 1) - relativedelta(months=i)).strftime("%Y-%m-%d")
#     period_start1 = (date(dt.now().year, dt.now().month, 1) -
#                      relativedelta(months=i)).strftime("%Y%m%d")
#     period = (date(dt.now().year-1, dt.now().month, 1) -
#               relativedelta(months=i)).strftime("%Y-%m")
#     period_year = (date(dt.now().year, dt.now().month, 1) -
#                    relativedelta(months=i)).strftime("%Y")

#     return period_start, last_year_period_start, period_start1, period, period_year


def useful_datetime(i):

    period_start = (date(2022, 1, 1) + relativedelta(months=i-1)).strftime("%Y-%m-%d")
    last_year_period_start = (date(2021, 1, 1) + relativedelta(months=i-1)).strftime("%Y-%m-%d")
    period_start1 = (date(2022, 1, 1) + relativedelta(months=i-1)).strftime("%Y%m%d")
    period = (date(2021, 1, 1) + relativedelta(months=i-1)).strftime("%Y-%m")
    period_year = (date(2022, 1, 1) + relativedelta(months=i-1)).strftime("%Y")

    return period_start, last_year_period_start, period_start1, period, period_year


def staging_to_app(table_name, stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    table_name = str(table_name)

    current_month = dt.now().month
    current_day = dt.now().day
    if stage == 'development':  # DEV - 10號更新上個月
        checkpoint = 10
    else:  # PRD - 15號更新上個月
        checkpoint = 12

    try:

        if current_day < checkpoint:

            start_date = dt(2022, 1, 1)
            end_date = dt(dt.now().year, dt.now().month -2, 1)

            current_date = start_date
            while current_date <= end_date:


                i = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month) + 1

                period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(i)
                data_import_app(table_name, period_start, last_year_period_start, period_start1, period, period_year, db, stage)
                current_date += relativedelta(months=1)
                # print("current_day < checkpoint:",period_start, last_year_period_start, period_start1, period, period_year)




        else:


            start_date = dt(2022, 1, 1)
            end_date = dt(dt.now().year, dt.now().month -1, 1)

            current_date = start_date
            while current_date <= end_date:


                i = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month) + 1

                period_start, last_year_period_start, period_start1, period, period_year = useful_datetime(i)
                data_import_app(table_name, period_start, last_year_period_start, period_start1, period, period_year, db, stage)
                current_date += relativedelta(months=1)
                # print("current_day < checkpoint:",period_start, last_year_period_start, period_start1, period, period_year)

        return True

    except:

        return False
