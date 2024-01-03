import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from models import engine

def carbon_emission_etl(stage):
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    current_day = dt.now().day

    if stage == 'development':  # DEV - 10號前抓2個月
        checkpoint = 10
    else:  # PRD - 15號前抓2個月
        checkpoint = 12

        # 起始年份
    start_year = 2022
    # 结束年份（不包括）
    end_year = dt.now().year+1

    for year in range(start_year, end_year):

        period_year = year

        period_year_start = date(year, 1, 1).strftime("%Y-%m-%d")

        if year == end_year - 1:

            if dt.now().month == 1 :

                period_year_end = date(year, 12, 1).strftime("%Y-%m-%d")

            else:

                period_year_end = date(year, date.today().month-1, 1).strftime("%Y-%m-%d")

        else:

            period_year_end = date(year , 12, 1).strftime("%Y-%m-%d")


        WCZ_heater = pd.read_sql(f"""SELECT plant, amount, period_start FROM raw.csr_kpi_data where category = 'heater' and  period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con = db)

        electricity = pd.read_sql(f"SELECT plant,amount,period_start FROM raw.electricity_total WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}' ",con = db)
        if electricity.shape[0] > 0:
            #2022/1後為FEM，包含太陽能發電，計算碳排須額外扣除
            renewable_energy = pd.read_sql(f"""SELECT plant,amount,period_start FROM raw.renewable_energy WHERE  category1 = '綠色能源' AND category2 = '光伏' AND period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)
            if renewable_energy.shape[0] > 0:
                if period_year_start >= '2022-01-01':
                    check_electricity_type = pd.read_sql(f"""SELECT plant,period_start,type FROM raw.electricity_total WHERE period_start >='{period_year_start}' and period_start <='{period_year_end}'""", con=db)
                    #資料為原FEM
                    plant_fem = check_electricity_type[check_electricity_type['type'] == 'FEM']
                    if plant_fem.shape[0] > 0:
                        plant_fem = plant_fem.merge(renewable_energy,on = ['plant','period_start'])
                        if plant_fem.shape[0] > 0:
                            plant_fem['amount'] = plant_fem['amount'] * -1
                            plant_fem = plant_fem.drop('type',axis=1)
                            electricity = electricity.append(plant_fem).reset_index(drop = True)
                    #資料為非FEM - WZS須扣除太陽能
                    plant_non_fem = check_electricity_type[check_electricity_type['type'] != 'FEM']
                    plant_non_fem = plant_non_fem[plant_non_fem['plant'].isin(['WZS-1','WZS-3','WZS-6','WZS-8'])]
                    if plant_non_fem.shape[0] > 0:
                        plant_non_fem = plant_non_fem.merge(renewable_energy,on = ['plant','period_start'])
                        if plant_non_fem.shape[0] > 0:
                            plant_non_fem['amount'] = plant_non_fem['amount'] * -1
                            plant_non_fem = plant_non_fem.drop('type',axis=1)
                            electricity = electricity.append(plant_non_fem).reset_index(drop = True)

            #年底須扣除綠證重新計算碳排
            green_energy =  pd.read_sql(f"""SELECT plant,amount,period_start FROM raw.renewable_energy WHERE category1 = '綠色能源' AND category2 = '綠證' AND period_start >='{period_year_start}' and period_start <='{period_year_end}' """,con = db)
            if green_energy.shape[0] > 0:
                green_energy['amount'] = green_energy['amount'] * -1
                electricity = electricity.append(green_energy).reset_index(drop = True)

            #扣除直購綠電重新計算碳排
            green_electricity =  pd.read_sql(f"""SELECT plant,amount,period_start FROM raw.renewable_energy WHERE category1 = '綠色能源' AND category2 = '綠電' AND period_start >='{period_year_start}' and period_start <='{period_year_end}' """,con = db)
            if green_electricity.shape[0] > 0:
                green_electricity['amount'] = green_electricity['amount'] * -1
                electricity = electricity.append(green_electricity).reset_index(drop = True)

            #get carbon coef
            carbon_coef =  pd.read_sql(f"""SELECT site AS "plant",amount FROM staging.cfg_carbon_coef WHERE year = '{period_year}' """,con = db)

            if carbon_coef.shape[0] > 0:
                carbon_emission = electricity.merge(carbon_coef,on = 'plant',how = 'left')
                carbon_emission = carbon_emission.dropna()

                #計算碳排
                carbon_emission['amount'] = carbon_emission['amount_x'] * carbon_emission['amount_y'] / 1000
                carbon_emission = carbon_emission.groupby(['plant','period_start']).sum().reset_index()
                carbon_emission = carbon_emission[['plant','period_start','amount']]

                # 計算碳排 : WCZ heater加總
                carbon_emission = carbon_emission.append(WCZ_heater)
                carbon_emission = carbon_emission.groupby(['plant','period_start']).sum().reset_index()
                carbon_emission = carbon_emission[['plant','period_start','amount']]

                carbon_emission['category'] = 'scope2'
                carbon_emission['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                conn = db.connect()
                conn.execute(f"DELETE FROM staging.carbon_emission WHERE category = 'scope2' AND period_start >='{period_year_start}' and period_start <='{period_year_end}'")
                carbon_emission.to_sql('carbon_emission', conn, index=False,if_exists='append', schema='staging', chunksize=10000)
                conn.close()
