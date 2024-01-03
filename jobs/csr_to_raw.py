import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from models import engine, engine_source

# import logging

# LOGGING_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
# DATE_FORMAT = '%Y%m%d %H:%M:%S'

# FORMAT = '%(asctime)s %(levelname)s: %(message)s'
# logging.basicConfig(level=logging.DEBUG,
#                     filename='./logs/csr2raw.log', filemode='w', format=FORMAT)


connect_csr_string = engine_source.get_connect_string_csr()
connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)
db_csr = create_engine(connect_csr_string, echo=True)


def update_csr_data(df_target, target_table):

    conn = db_eco.connect()
    conn.execute(f"""TRUNCATE TABLE raw.{target_table}""")

    df_target.to_sql(target_table, con=db_eco, schema='raw',
                     if_exists='append', index=False, chunksize=1000)
    conn.close()


def insert_csr_data(df_target, target_table):

    conn = db_eco.connect()
    conn.execute(
        f"""delete from raw.{target_table} where period_start >= '2022-01-01'""")

    df_target.to_sql(target_table, con=db_eco, schema='raw',
                     if_exists='append', index=False, chunksize=1000)
    conn.close()

def import_csr_kpi_data(kpiname):


    if kpiname =='heater':

        try:

            df = pd.read_sql(f"""select * from CSSR.dbo.View_KpiDetail_EcoSsot where indicators = '溫室氣體排放量_外購能源' and KpiYear>= 2018 and SiteName = 'WCZ' """, db_csr)

            IndicatorM = []
            for i in df['KpiMonth'].str.split("M"):
                IndicatorM.append(i[-1])

            df['KpiMonth'] = IndicatorM

            df['period_start'] = df['KpiYear'].astype(str) + '-' + df['KpiMonth'].astype(str)

            dateFormatter = "%Y-%m"

            S_date = []
            for i in df['period_start']:
                S_date.append(dt.strptime(i, dateFormatter))

            df['period_start'] = S_date

            df.columns = df.columns.map(lambda x: x.lower())

            df = df[['sitename', 'mvalue', 'period_start']]

            df['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            df['category'] = 'heater'

            df.rename(columns={'sitename': 'plant','mvalue': 'amount'}, inplace=True)

            conn = db_eco.connect()
            conn.execute(f"""TRUNCATE TABLE raw.csr_kpi_data""")

            df.to_sql('csr_kpi_data', con=db_eco, schema='raw',
                            if_exists='append', index=False, chunksize=1000)
            conn.close()

        except:

            return False


def import_csr_data(indicatorid):
    """
    # indicatorid
        [1] = csr_electricity_indicator
        [2] = csr_water_indicator
        [22, 23, 24, 25, 50] = csr_waste_indicator
        [3, 19, 90] = csr_unrenewable_indicator
        [130] = csr_renewable_indicator
    """
    try:

        df = pd.read_sql(
            f"""select * from CSSR.dbo.View_IndicatorDetail_EcoSsot""", db_csr)

        IndicatorM = []
        for i in df['IndicatorMonth'].str.split("M"):
            IndicatorM.append(i[-1])

        df['IndicatorM'] = IndicatorM

        df['period_start'] = df['IndicatorYear'].astype(
            str) + '-' + df['IndicatorM'].astype(str)

        dateFormatter = "%Y-%m"
        S_date = []
        for i in df['period_start']:
            S_date.append(dt.strptime(i, dateFormatter))

        df['period_start'] = S_date

        df.columns = df.columns.map(lambda x: x.lower())

        df['indicatorvalue'] = df['indicatorvalue'].replace('', 0)
        df['indicatorvalue'] = df['indicatorvalue'].astype(float)
        df['indicatorm'] = df['indicatorm'].astype(int)

        df.drop(columns=['verificationdate',
                'verificationthirdparty'], inplace=True)

        df_target = pd.DataFrame()

        for i in indicatorid:
            df_target = pd.concat([df_target, df[df['indicatorid'] == i]])

        df_target['etl_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        # if indicatorid == [1]:

        #     target_table = 'csr_electricity_indicator'

        if indicatorid == [131]:

            target_table = 'csr_electricity_indicator'

        elif indicatorid == [2]:

            target_table = 'csr_water_indicator'

        elif indicatorid == [22, 23, 24, 25, 50, 67, 68, 69, 85, 91, 189]:

            target_table = 'csr_waste_indicator'

        elif indicatorid == [3, 19, 90]:

            target_table = 'csr_unrenewable_indicator'

        elif indicatorid == [130]:

            target_table = 'csr_renewable_indicator'

        if df_target.size != 0:

            update_csr_data(df_target, target_table)

            # print('size not 0, row counts:', df_target.shape[0],
            #       file=open('./logs/csr2raw_'+str(target_table)+'.txt', 'w'))

        else:
            # print('size is 0, row counts:', df_target.shape[0],
            #       file=open('./logs/csr2raw_nodata.txt', 'w'))

            pass

        return True

    except:
        # logging.error("Catch an exception.", exc_info=True)
        # print('error:', e, file=open('./logs/csr2raw_error.txt', 'w'))
        return False


def import_csr_scope1():

    plant_dict = {'WMYP1': 'WMY-1', 'WMYP2': 'WMY-2',
                  'WIHK1': 'WIHK-1', 'WIHK2': 'WIHK-2'}

    try:
        # df = pd.read_sql(
        #     f"""select * from CSSR.dbo.View_KpiDetail_EcoSsot where indicators = '溫室氣體排放量(範疇1)' and KpiYear= 2021 and SiteName not in ('WZS','WKS')""", db_csr)

        df = pd.read_sql(
            f"""select * from CSSR.dbo.View_KpiDetail_EcoSsot where indicators = '範疇1/類別1的直接溫室氣體排放' and KpiYear>= 2021 """, db_csr)

        IndicatorM = []
        for i in df['KpiMonth'].str.split("M"):
            IndicatorM.append(i[-1])

        df['KpiMonth'] = IndicatorM

        df['period_start'] = df['KpiYear'].astype(
            str) + '-' + df['KpiMonth'].astype(str)

        dateFormatter = "%Y-%m"
        S_date = []
        for i in df['period_start']:
            S_date.append(dt.strptime(i, dateFormatter))

        df['period_start'] = S_date

        df.columns = df.columns.map(lambda x: x.lower())

        df_scope1 = df[['sitename', 'mvalue', 'period_start']]

        df_scope1['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        df_scope1['category'] = 'scope1'

        df_scope1.rename(columns={'sitename': 'plant',
                                  'mvalue': 'amount'}, inplace=True)
        # 針對其他廠調整命名
        df_scope1 = df_scope1.replace({'plant': plant_dict})

        # df_scope1 = df_scope1[~df_scope1['plant'].isin(['WIHK-1', 'WIHK-2'])]

        df_scope1_staging = df_scope1[~df_scope1['plant'].isin(['WZS', 'WKS'])]

        conn = db_eco.connect()
        conn.execute(
            f"""DELETE FROM staging.carbon_emission where period_start >= '2021-01-01' and category = 'scope1'""")

        df_scope1_staging.to_sql('carbon_emission', con=db_eco, schema='staging',
                                 if_exists='append', index=False, chunksize=1000)
        conn.close()

        df_scope1_raw = df_scope1[df_scope1['plant'].isin(['WZS', 'WKS'])]

        conn = db_eco.connect()
        conn.execute(
            f"""DELETE FROM raw.carbon_emission where period_start >= '2021-01-01' and category = 'scope1'""")

        df_scope1_raw.to_sql('carbon_emission', con=db_eco, schema='raw',
                             if_exists='append', index=False, chunksize=1000)
        conn.close()

    except:
        return False


'''
target_table = 'raw.csr_kpidetail'
indicoatrs =('總自來水取水量(宿舍)','總自來水取水量(工廠)','總用電量(宿舍)','總用電量(工廠)') # kpi_id = ('1','6','101','103','133','119','112','113')
site = ('WKS')
'''


def csr_detail_import(target_table, indicoatrs=('總自來水取水量(宿舍)', '總自來水取水量(工廠)', '總用電量(宿舍)', '總用電量(工廠)')):

    csr_cate_replace = {'總用電量(工廠)': '廠區', '總用電量(宿舍)': '宿舍',
                        '總自來水取水量(工廠)': '廠區', '總自來水取水量(宿舍)': '宿舍', 'KWH': '度', 'M3': '立方米'}

    detail_dict = {'subcategoryname': 'item', 'indicators': 'category',
                   'sitename': 'site', 'mvalue': 'amount', 'kvalue': 'ytm_amount'}

    try:
        df = pd.read_sql(
            f"""select * from CSSR.dbo.View_KpiDetail_EcoSsot where indicators in {indicoatrs} and KpiYear >= 2022""", db_csr)

        IndicatorM = []
        for i in df['KpiMonth'].str.split("M"):
            IndicatorM.append(i[-1])

        df['KpiMonth'] = IndicatorM

        df['period_start'] = df['KpiYear'].astype(
            str) + '-' + df['KpiMonth'].astype(str)

        dateFormatter = "%Y-%m"

        S_date = []
        for i in df['period_start']:
            S_date.append(dt.strptime(i, dateFormatter))

        df['period_start'] = S_date

        df.columns = df.columns.map(lambda x: x.lower())

        df = df.replace({'indicators': csr_cate_replace,
                        'unit': csr_cate_replace})

        df_detail = df[['subcategoryname', 'indicators', 'unit',
                        'sitename', 'mvalue', 'kvalue', 'period_start']]

        df_detail.rename(columns=detail_dict, inplace=True)

        df_detail['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        if df_detail.size != 0:

            insert_csr_data(df_detail, target_table)

        return True

    except Exception:

        return False


def import_carbon_coef(target_table):

    try:
        df = pd.read_sql(
            f"""select ParameterYear AS "year",SiteName AS "site",ParameterValue AS "amount",UpdateDate AS "csr_update_time" from CSSR.dbo.View_ParameterDetail_EcoSsot where CategoryName = '排放係數(Emission Factor)_CO2' and ParameterName = '電力係數(依區域電網公告)'""", db_csr)

        df['last_import_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        if df.size != 0:
            update_csr_data(df, target_table)

        return True

    except:

        return False

def csr_office_data(raw_dataname,plant_name,year):

    df = pd.read_sql(f"""SELECT EvidenceY, EvidenceM, RawDataValue as amount FROM CSSR.dbo.View_RawDataDetail_EcoSsot
        where RawDataName = '{raw_dataname}' and EvidenceY >= {year}""", db_csr)

    df['plant'] = str(plant_name)

    return df



def csr_office_data_import(raw_table):

    try:

        if dt.now().month == 1:

            last_year = dt.now().year - 2

        else:

            last_year = dt.now().year - 1

        if raw_table =='electricity_office':

            df_WKH = csr_office_data('外購電力_高雄','WKH',last_year)

            df_WTN = csr_office_data('外購電力_台南','WTN',last_year)

            df_WLT = csr_office_data('外購電力_龍潭','WLT',last_year)

            df_WHC = csr_office_data('外購電力_WHC','WHC',last_year)

            df_office = df_WKH.append(df_WTN).append(df_WLT).append(df_WHC)

            df_office['type'] = 'CSR'
            df_office['unit'] = '度'
            df_office['period_start'] = df_office['EvidenceY'] + '-' + df_office['EvidenceM'] + '-01'
            df_office = df_office[['plant', 'period_start', 'amount','type','unit']]


            period_start_value = df_office['period_start']
            plant_value = df_office['plant']


            if len(period_start_value) == 0 or len(plant_value) == 0:

                pass

            elif df_office.size ==0:

                pass

            else:

                delete_query = f"""DELETE FROM raw.electricity_office WHERE plant IN {tuple(plant_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_office.to_sql('electricity_office', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close()

        if raw_table =='water_office':

            df_WKH = csr_office_data('自來水取水量_高雄','WKH',last_year)

            df_WTN = csr_office_data('自來水取水量_台南致遠樓','WTN',last_year)

            df_WLT = csr_office_data('自來水取水量_龍潭','WLT',last_year)

            df_WHC = csr_office_data('自來水取水量_WHC','WHC',last_year)

            df_office = df_WKH.append(df_WTN).append(df_WLT).append(df_WHC)

            df_office['type'] = 'CSR'
            df_office['unit'] = '立方米'
            df_office['period_start'] = df_office['EvidenceY'] + '-' + df_office['EvidenceM'] + '-01'
            df_office = df_office[['plant', 'period_start', 'amount','type','unit']]


            period_start_value = df_office['period_start']
            plant_value = df_office['plant']


            if len(period_start_value) == 0 or len(plant_value) == 0:

                pass

            elif df_office.size ==0:

                pass

            else:

                delete_query = f"""DELETE FROM raw.water_office WHERE plant IN {tuple(plant_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

                conn = db_eco.connect()
                conn.execute(delete_query)

                df_office.to_sql('water_office', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
                conn.close()


        return True

    except:

        return False


def office2raw(raw_table):

    if raw_table =='electricity_total':

        df_office = pd.read_sql(f"""SELECT plant, amount, unit, period_start,"type" FROM raw.electricity_office""", db_eco)

        period_start_value = df_office['period_start']
        plant_value = df_office['plant']

        if len(period_start_value) == 0 or len(plant_value) == 0:

            pass

        elif df_office.size ==0:

            pass

        else:

            delete_query = f"""DELETE FROM raw.electricity_total WHERE plant IN {tuple(plant_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

            conn = db_eco.connect()
            conn.execute(delete_query)

            df_office.to_sql('electricity_total', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
            conn.close()

    if raw_table =='water':

        df_office = pd.read_sql(f"""SELECT plant, amount, unit, period_start,"type" FROM raw.water_office""", db_eco)

        period_start_value = df_office['period_start']
        plant_value = df_office['plant']

        if len(period_start_value) == 0 or len(plant_value) == 0:

            pass

        elif df_office.size ==0:

            pass

        else:

            delete_query = f"""DELETE FROM raw.water WHERE plant IN {tuple(plant_value)} AND period_start IN {tuple(str(date) for date in period_start_value)}"""

            conn = db_eco.connect()
            conn.execute(delete_query)

            df_office.to_sql('water', con=db_eco, schema='raw', if_exists='append', index=False, chunksize=1000)
            conn.close()