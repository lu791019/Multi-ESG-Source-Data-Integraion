import pandas as pd
import numpy as np
import calendar
import requests
import json
import http.client
from base64 import b64encode
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
from models import engine, engine_source
from dateutil.relativedelta import relativedelta

from services.mail_service import MailService

connect_eco_string = engine.get_connect_string()
db_eco = create_engine(connect_eco_string, echo=True)

connect_source_string = engine_source.get_connect_string_wzsplt()
db_source = create_engine(connect_source_string, echo=True)


def fem_ratio_cal(site, consumtype, start_time, end_time):

    FEM_elect = pd.read_sql(
        f"""SELECT plant AS "plant_code", datadate, power FROM raw.wks_mfg_fem_dailypower where site in ('{site}') and datadate >= '{start_time}' and  datadate<= '{end_time}' and consumetype = '{consumtype}'""", db_eco)
    plant_map = pd.read_sql(
        f"""SELECT DISTINCT site,plant_name AS "plant",plant_code FROM raw.plant_mapping where site in ('{site}')""", con=db_eco)

    df = FEM_elect.merge(plant_map, on='plant_code', how='left').dropna()
    df['power'] = df.groupby(['plant'])['power'].transform('sum')
    df.drop(['datadate'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
    df['ratio'] = df['power'].div(df['power'].sum())

    return df


def update_raw(df_target, target_table, period_start, plant):

    #     connect_eco_string = get_connect_string()
    #     db_eco = create_engine(connect_eco_string, echo=True)

    conn = db_eco.connect()
    conn.execute(
        f"""delete from raw.{target_table} where plant in {plant} and period_start >= '{period_start}'""")

    df_target.to_sql(target_table, con=db_eco, schema='raw',
                     if_exists='append', index=False, chunksize=1000)
    conn.close()


def update_raw_renewable(df_target, target_table, period_start, plant):

    #     connect_eco_string = get_connect_string()
    #     db_eco = create_engine(connect_eco_string, echo=True)

    conn = db_eco.connect()
    conn.execute(
        f"""delete from raw.{target_table} where plant in {plant} and category1 ='綠色能源' and category2 ='光伏' and period_start = '{period_start}'""")

    df_target.to_sql(target_table, con=db_eco, schema='raw',
                     if_exists='append', index=False, chunksize=1000)
    conn.close()

def solar_cal(df, site,type_):
    solar_site = df[(df['site'] == str(site)) &  (df['type'] == str(type_))]

    if type_ == 'site':
        solar_site.drop(['type', 'area', 'datetime', 'target'], axis=1, inplace=True)
    elif type_ == 'area':
        solar_site.drop(['type', 'site', 'datetime', 'target'], axis=1, inplace=True)

    solar_site['actual'] = solar_site.groupby([str(type_)])['actual'].transform('sum')

    solar_site.drop_duplicates(inplace=True)
    solar_site.rename(columns={str(type_): 'plant'}, inplace=True)

    return solar_site

def solar_cal_daily(df, site,type_):
    solar_site = df[(df['site'] == str(site)) &  (df['type'] == str(type_))]

    if type_ == 'site':
        solar_site.drop(['type', 'area'], axis=1, inplace=True)
    elif type_ == 'area':
        solar_site.drop(['type', 'site'], axis=1, inplace=True)

    solar_site = solar_site.groupby([str(type_),'datetime']).sum().reset_index()

    solar_site.drop_duplicates(inplace=True)
    solar_site.rename(columns={str(type_): 'plant'}, inplace=True)

    return solar_site

def solar_melt(df):

    df_daily = pd.melt(df, id_vars=['datetime', 'plant'], var_name='category', value_name='amount')
    df_daily = df_daily[['datetime', 'plant', 'amount', 'category']].reset_index(drop=True)

    return df_daily


def process_solar_site(solar, site, type_):
    df = solar_cal_daily(solar, site, type_)
    df = solar_melt(df)
    return df

def solar_month(type_,site,start_time):

    re_json = requests.get(f"https://pps-api.wzs-arm-prd-02.k8s.wistron.com/power-generation-month/getPowerGenerationForWigps/{type_}/{site}",verify=False).json()
    period_start = pd.Series(re_json['datetime'],name = 'period_start')
    amount = pd.Series(re_json['actual'],name = 'amount')
    df =pd.concat([amount,period_start],axis=1)
    df['period_start'] = df['period_start'].apply(lambda x: dt.strptime(x, "%Y-%m").strftime("%Y-%m-%d"))
    df = df[df['period_start'] <= start_time]
    df['plant'] = site

    return df


def source_to_raw(source_type, target_table, period_start,stage):

    plant_dict = {'P1': 'WZS-1', 'P3': 'WZS-3', 'P6': 'WZS-6', 'P8': 'WZS-8'}

    area_dict = {'TB2': 'WZS-1', 'OB1': 'WZS-6',
                 'TB3': 'WZS-3', 'TB5': 'WZS-3'}

    if target_table == 'waste':

        """
        itme : waste
        plant : WZS
        source data : DB
        """
        category_dict = {'一般廢棄物': '一般廢棄物(焚化&掩埋)', '廚余廢棄物': '一般廢棄物(廚餘)'}

        plant = ('WZS-1', 'WZS-3', 'WZS-6', 'WZS-8')

        dict_waste = {'unit': '噸', 'type': source_type}

        connect_waste_string = engine_source.get_connect_string_wzsplt()
        db_source = create_engine(connect_waste_string, echo=True)

        try:
            df = pd.read_sql(
                f"""SELECT plant,garbageType AS category,netWeight AS amount,year,month FROM gws.vw_gws_eco_ssot where year >=2022 and month >=1 """, db_source)

            df['period_start'] = df['year'].astype(
                str) + '-' + df['month'].astype(str)

            dateFormatter = "%Y-%m"
            S_date = []

            for i in df['period_start']:
                S_date.append(dt.strptime(i, dateFormatter))

            df['period_start'] = S_date
            df.drop(columns=['year', 'month'], inplace=True)

            df = df.replace({'category': category_dict, 'plant': plant_dict})

            for i in dict_waste:
                df[str(i)] = dict_waste[i]

            df['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            if df.size!=0:
                update_raw(df, target_table, period_start, plant)

            return True

        except Exception as e:
            error = str(e)
            print(error)

            return False

    elif target_table == 'renewable_energy':

        """
        item : renewable_energy
        plant : WZS, WKS, WOK, WVN, WHC, WMI-2
        source data : API

        # site to plant 方法:
            # WKS 依照FEM用電量比例分配
            # WZS 依照area對應: TB2:P1, OB1:P6, TB3/TB5:P3
        """

        plant = ('WZS-1', 'WZS-3', 'WZS-6', 'WZS-8', 'WKS-1', 'WKS-5', 'WKS-6', 'WOK' , 'WVN', 'WHC', 'WMI-2')

        if dt.now().month == 1:
            start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year-1, 12, 31).strftime("%Y-%m-%d")

        else:
            start_time = date(dt.now().year, dt.now().month -
                              1, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year, dt.now().month-1,
                            calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        dict_solar = {'category1': '綠色能源', 'category2': '光伏', 'unit': '度', 'type': source_type}

        try:

            df_WZS1 = solar_month('area','TB2',start_time)
            df_WZS1['plant'] = 'WZS-1'

            df_WZS6 = solar_month('area','OB1',start_time)
            df_WZS6['plant'] = 'WZS-6'


            df_TB3 = solar_month('area','TB3',start_time)
            df_TB3['plant'] = 'WZS-3'
            df_TB5 = solar_month('area','TB5',start_time)
            df_TB5['plant'] = 'WZS-3'

            df_WZS3 = df_TB3.append(df_TB5)
            df_WZS3 = df_WZS3.groupby(['plant','period_start']).sum().reset_index()


            df_WVN = solar_month('site','WVN',start_time)
            df_WOK = solar_month('site','WOK',start_time)

            df_WNH = solar_month('site','WHC',start_time)
            df_WNH = df_WNH.replace({'plant': {'WHC': 'WNH'}})

            df_WMI = solar_month('site','WMI',start_time)


            df_WKS = solar_month('site','WKS',start_time)
            df_WKS.rename(columns = {'plant':'site','amount':'power'}, inplace = True)

            fem_ratio = pd.read_sql(f"""SELECT ratio, plant, period_start FROM raw.fem_ratio""", db_eco)
            fem_ratio['site'] = 'WKS'

            df_WKS['period_start'] = df_WKS['period_start'].astype(str)
            fem_ratio['period_start'] = fem_ratio['period_start'].astype(str)

            df_WKS = pd.merge(df_WKS, fem_ratio, on=['period_start','site'],how='left')
            df_WKS['amount'] = df_WKS['power'] * df_WKS['ratio']
            df_WKS = df_WKS[['period_start','plant','amount']]

            solar = df_WKS.append(df_WZS1).append(df_WZS3).append(df_WZS6).append(df_WVN).append(df_WOK).append(df_WNH).append(df_WMI)

            for i in dict_solar:
                solar[str(i)] = dict_solar[i]

            solar['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            datetime = solar['period_start'].min()

            if solar.size !=0:
                conn = db_eco.connect()
                conn.execute(f"""delete from raw.{target_table} where category1 ='綠色能源' and category2 ='光伏' and period_start >= '{datetime}'""")

                solar.to_sql(target_table, con=db_eco, schema='raw',
                                if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True



        except Exception as e:
            error = str(e)
            # print(error)
            # mail = MailService('[failed][{}] compute roi cron job report'.format(stage))
            # mail.send('failed: {}'.format(error))
            return error
            # return False

    elif target_table == 'solar_daily':

        plant = ('WZS-1', 'WZS-3', 'WZS-6', 'WZS-8', 'WKS-1', 'WKS-5', 'WKS-6', 'WOK' , 'WVN', 'WHC', 'WMI-2')

        area_dict = {'TB2': 'WZS-1', 'OB1': 'WZS-6', 'TB3': 'WZS-3', 'TB5': 'WZS-3'}

        if dt.now().month == 1:
            start_time = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
            end_time = (date(dt.now().year, dt.now().month, 1) - relativedelta(days=1)).strftime("%Y-%m-%d")

        else:
            start_time = date(dt.now().year, dt.now().month - 1, 1).strftime("%Y-%m-%d")
            end_time = date(dt.now().year, dt.now().month-1,  calendar.mdays[dt.now().month-1]).strftime("%Y-%m-%d")

        dict_solar = {'category1': '綠色能源', 'category2': '光伏',
                      'unit': '度', 'period_start': start_time, 'type': source_type}

        try:

            re_json = requests.get(f"https://pps-api.wzs-arm-prd-02.k8s.wistron.com/power-generation-month/getPowerGenerationDay?startime={start_time}&endtime={end_time}",verify=False).json()

            solar = pd.json_normalize(re_json)

            #WKS
            solar_WKS = solar[(solar['site'] == 'WKS') &  (solar['type'] == 'site')]

            df = fem_ratio_cal('WKS', '用電量', start_time, end_time)

            solar_WKS = (solar_WKS.merge(df, on='site', how='left')[['datetime', 'plant', 'ratio', 'target', 'actual']])

            solar_WKS_daily = solar_WKS.melt(id_vars=['datetime', 'plant', 'ratio'], var_name='category', value_name='amount').assign(amount=lambda x: x['amount'] * x['ratio'])[['datetime', 'plant', 'amount', 'category']].reset_index(drop=True)


            #WZS
            solar_WZS = solar[(solar['site'] == 'WZS') & (solar['type'] == 'area')]

            solar_WZS = (solar_WZS.assign(plant=solar_WZS['area'].map(area_dict))[['datetime', 'plant', 'target', 'actual']].pivot_table(index=['datetime', 'plant'], values=['target', 'actual'], aggfunc='sum').reset_index())

            solar_WZS_daily = solar_melt(solar_WZS)

            #WOK
            solar_WOK_daily = process_solar_site(solar, 'WOK', 'site')

            #WVN
            solar_WVN_daily = process_solar_site(solar, 'WVN', 'site')

            #WHC
            solar_WHC_daily = process_solar_site(solar, 'WHC', 'site')

            #WMI-2
            solar_WMI2_daily = process_solar_site(solar, 'WMI', 'area')

            solar_WMI2_daily.replace({'WMI2': 'WMI-2'},inplace = True)


            #total
            solar_daily = solar_WKS_daily.append(solar_WZS_daily).append(solar_WOK_daily).append(solar_WVN_daily).append(solar_WHC_daily).append(solar_WMI2_daily).reset_index(drop=True)

            solar_daily['last_update_time'] = dt.strptime( dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")


            if solar_daily.size !=0:
                conn = db_eco.connect()
                conn.execute(
                    f"""delete from raw.solar_energy where datetime >= '{start_time}' and datetime <= '{end_time}'""")

                solar_daily.to_sql('solar_energy', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)

                conn.close()

        except Exception as e:
            error = str(e)
            print(error)

            return False

    elif target_table == 'solar_climate_daily':

        try:
            with open('./jsonfiles/climate_info.json', 'r') as f:

                climate = json.load(f)

            #Daily
            x = 0
            now_time = dt.now()
            start_time = int((now_time - timedelta(days=x+1)).timestamp()) * 1000
            end_time = int((now_time - timedelta(days=x)).timestamp()) * 1000

            api_index = 'bms_photovoltaic_emi_*'

            conn = http.client.HTTPSConnection("zsarm-opensearchp.wistron.com", 9200)
            headers = {
                'Authorization': "Basic {}".format(
                            b64encode(bytes(f"{'bms_photovoltaic'}:{'c7$1dKLqh'}", "utf-8")).decode("ascii")
                    ),
                'Content-Type': 'application/json'
            }

            # 將 start_time 和 end_time 動態加入json
            climate['query']['bool']['filter'][0]['bool']['must'][0]['range']['log_dt']['from'] = start_time
            climate['query']['bool']['filter'][0]['bool']['must'][0]['range']['log_dt']['to'] = end_time

            # conn.request("POST", "/"+api_index+"/_search", json.dumps(climate), headers,verify=False)
            # res = conn.getresponse()
            # data = res.read()

            url = "https://zsarm-opensearchp.wistron.com:9200/"+api_index+"/_search"
            response = requests.request("POST", url, headers=headers, data=json.dumps(climate), verify=False)

            # dataset_json = json.loads(data)

            dataset_json = response.json()
            final_result = []
            for item in dataset_json['hits']['hits']:
                final_result.append(item['_source'])

            df = pd.DataFrame(final_result)

            df['log_dt'] = df['log_dt'].astype(float)
            df['log_dt'] = df['log_dt'].apply(lambda x: dt.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
            df.rename(columns = {'log_dt':'log_datetime'}, inplace = True)

            df = df[['site','area','temperatures','radiant_line','wind_speed','wind_direction','log_datetime','station']]
            df['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            if df.size !=0:
                conn = db_eco.connect()
                df.to_sql('solar_climate_daily', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close()

            return True

        except Exception as e:

            error = str(e)
            # print(error)
            mail = MailService('[failed][{}] solar climat cron job report'.format(stage))
            mail.send('failed: {}'.format(error))
            return error
            # return False


def wzs_api_process(df, unit, plant_dict):

    df['period_start'] = pd.to_datetime(df['period'], format='%Y%m')

    df1 = pd.melt(df, id_vars=['period', 'type', 'period_start'], value_vars=[
                  'p1', 'p3', 'p6', 'p8'], var_name='plant', value_name='amount')

    df1 = df1.replace({'plant': plant_dict})

    df1['unit'] = str(unit)
    df1['type'] = 'wzs_api'
    df1['last_update_time'] = dt.strptime(
        dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    df1.drop(columns=['period'], inplace=True)
    return df1


def wzs_api_etl(item, unit, etl_start):
    # 從2022-11-01開始用以ETL同步WZS用電用水
    plant_dict = {'p1': 'WZS-1', 'p3': 'WZS-3', 'p6': 'WZS-6', 'p8': 'WZS-8'}

    plant = ('WZS-1', 'WZS-3', 'WZS-6', 'WZS-8')

    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=1)).strftime("%Y-%m-%d")

    period_start1 = (date(dt.now().year, dt.now().month, 1) -
                     relativedelta(months=1)).strftime("%Y%m")

    if dt.now().month == 1:

        current_year = dt.now().year - 1

    else:

        current_year = dt.now().year

    try:
        get_data = requests.get(
            f"http://fams-api.wzs-arm-prd-02.k8s.wistron.com/data-board/getOutputAndTurnoverByTime?key=year&time={current_year}")

        list_of_dicts = get_data.json()

        if list_of_dicts['data']:

            if item == 'electricity_total':

                raw_table = 'electricity_total'
                WZS_elect = pd.json_normalize(list_of_dicts['data']['power'])
                df_elect = wzs_api_process(WZS_elect, unit, plant_dict)
                df_elect = df_elect[df_elect['period_start'] >= etl_start]

                if df_elect.size != 0:
                    update_raw(df_elect, raw_table, etl_start, plant)

            if item == 'water':

                raw_table = 'water'
                WZS_water = pd.json_normalize(list_of_dicts['data']['water'])
                df_water = wzs_api_process(WZS_water, unit, plant_dict)
                df_water = df_water[df_water['period_start'] >= etl_start]

                if df_water.size != 0:
                    update_raw(df_water, raw_table, etl_start, plant)
        else:
            pass

        return True

    except Exception as e:
        error = str(e)
        print(error)

        return False


def carbon_coef_etl():
    plant_mapping = pd.read_sql(f"""SELECT DISTINCT site,plant_name AS "plant" FROM raw.plant_mapping  WHERE boundary = true""", con=db_eco)


    for year in range(dt.now().year-1, dt.now().year+1):

        if year ==2022:

            plant_replace = {'WMYP1': 'WMY-1', 'WMYP2': 'WMY-2', 'WIHK1': 'WIHK-1', 'WIHK2': 'WIHK-2', 'WMIP1': 'WMI-1', 'WMIP2': 'WMI-2'}

            df_raw_carbon = pd.read_sql(f"""SELECT "year", site, amount FROM raw.carbon_coef where site not in ('WKS','WZS') and year ='{year}'""", con=db_eco)
            df_raw_carbon = df_raw_carbon.replace({'site': plant_replace})
            df_raw_carbon.drop_duplicates(inplace = True)

            carbon_WKS = pd.read_sql(f"""SELECT "year", site, amount FROM raw.carbon_coef where site in ('WKS') and year ='{year}'""", con=db_eco)
            carbon_WKS = carbon_WKS[carbon_WKS['site'] == 'WKS'].merge(plant_mapping, on='site', how='left')
            carbon_WKS['site'] = carbon_WKS['plant']

            carbon_WZS = pd.read_sql(f"""SELECT "year", site, amount FROM raw.carbon_coef where site in ('WZS') and year ='{year}'""", con=db_eco)
            carbon_WZS = carbon_WZS[carbon_WZS['site'] == 'WZS'].merge(plant_mapping, on='site', how='left')
            carbon_WZS['site'] = carbon_WZS['plant']

            df_raw_carbon = df_raw_carbon.append(carbon_WZS).append(carbon_WKS).reset_index(drop=True)

            df_raw_carbon = df_raw_carbon[['year','site','amount']]
            df_raw_carbon = df_raw_carbon[df_raw_carbon['amount'] != 0]
            df_raw_carbon['last_update_time'] = dt.strptime( dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            site_list = df_raw_carbon['site'].unique()
            site_list = "','".join(site_list)

            if df_raw_carbon.size != 0:
                conn = db_eco.connect()
                conn.execute(f"DELETE FROM staging.cfg_carbon_coef WHERE site IN ('{site_list}') AND year ='{year}'")
                df_raw_carbon.to_sql('cfg_carbon_coef', conn, index=False, if_exists='append', schema='staging', chunksize=10000)
                conn.close()

        else:

            plant_replace = {'WMYP1': 'WMY-1', 'WMYP2': 'WMY-2', 'WIHK1': 'WIHK-1', 'WIHK2': 'WIHK-2', 'WMIP1': 'WMI', 'WMIP2': 'WMI', 'WVN': 'WVN-1', 'WCD': 'WCD-1'}

            df_raw_carbon = pd.read_sql(f"""SELECT "year", site, amount FROM raw.carbon_coef where site not in ('WKS','WZS') and year ='{year}'""", con=db_eco)
            df_raw_carbon = df_raw_carbon.replace({'site': plant_replace})
            df_raw_carbon.drop_duplicates(inplace = True)

            carbon_WKS = pd.read_sql(f"""SELECT "year", site, amount FROM raw.carbon_coef where site in ('WKS') and year ='{year}'""", con=db_eco)
            carbon_WKS = carbon_WKS[carbon_WKS['site'] == 'WKS'].merge(plant_mapping, on='site', how='left')
            carbon_WKS['site'] = carbon_WKS['plant']

            carbon_WZS = pd.read_sql(f"""SELECT "year", site, amount FROM raw.carbon_coef where site in ('WZS') and year ='{year}'""", con=db_eco)
            carbon_WZS = carbon_WZS[carbon_WZS['site'] == 'WZS'].merge(plant_mapping, on='site', how='left')
            carbon_WZS['site'] = carbon_WZS['plant']

            df_raw_carbon = df_raw_carbon.append(carbon_WZS).append(carbon_WKS).reset_index(drop=True)

            df_raw_carbon = df_raw_carbon[['year','site','amount']]
            df_raw_carbon = df_raw_carbon[df_raw_carbon['amount'] != 0]
            df_raw_carbon['last_update_time'] = dt.strptime( dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            site_list = df_raw_carbon['site'].unique()
            site_list = "','".join(site_list)

            if df_raw_carbon.size != 0:
                conn = db_eco.connect()
                conn.execute(f"DELETE FROM staging.cfg_carbon_coef WHERE site IN ('{site_list}') AND year ='{year}'")
                df_raw_carbon.to_sql('cfg_carbon_coef', conn, index=False, if_exists='append', schema='staging', chunksize=10000)
                conn.close()






