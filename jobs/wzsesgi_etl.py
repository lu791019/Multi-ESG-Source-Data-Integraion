import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
from models import engine, engine_source
import calendar
from dateutil.relativedelta import relativedelta


connect_eco_string = engine.get_connect_string()
connect_csr_string = engine_source.get_connect_string_csr()


db_eco = create_engine(connect_eco_string, echo=True)
db_csr = create_engine(connect_csr_string, echo=True)



def update_esgi_data(df_target,schema, target_table):

    conn = db_eco.connect()
    conn.execute(f"""TRUNCATE TABLE {schema}.{target_table}""")

    df_target.to_sql(target_table, con=db_eco, schema=str(schema), if_exists='append', index=False, chunksize=1000)
    conn.close()


def category_group(df,data_name):

    df_target = pd.DataFrame()

    for i in data_name:
        df_target = pd.concat([df_target, df[df['data_name'] == i]])

    return df_target


def insert_col(df,unit):
    df['unit'] = str(unit)
    df['type'] = 'wzs_esgi'

    return df

def preprocess_df(df):

    plant_dict = {'LCM-1':'WOK','LCM-2':'WTZ','WIH-1':'WIH'}

    df = df.replace({'plant': plant_dict})

    df['plant'] = df['plant'].replace(plant_dict)

    df['amount'] = df['amount'].replace('NA', np.nan)

    df['amount'] = df['amount'].astype(float)

    df['amount'].fillna(0, inplace=True)

    return df

def esgi_import():

    plant_dict = {'LCM-1':'WOK','LCM-2':'WTZ','WIH-1':'WIH'}
    value_change = {'NA': pd.NA}

    df = pd.read_sql(f"""SELECT data_name, plant, period_start,  data_value as amount FROM raw.wzs_esgi_environment_indicator_item where plant not in ('WZS','WKS','WCD')""", db_eco)

    df_elect_esgi = pd.read_sql(f"""SELECT data_name, plant, period_start,  data_value as amount FROM raw.wzs_esgi_environment_indicator_item
    where plant not in ('WZS','WKS','WCD') and performance_goalsid = 4""", db_eco)

    df_water_esgi = pd.read_sql(f"""SELECT data_name, plant, period_start,  data_value as amount FROM raw.wzs_esgi_environment_indicator_item
    where plant not in ('WZS','WKS','WCD') and performance_goalsid = 5""", db_eco)


    if df.size !=0:

        df = preprocess_df(df)

        df_elect_esgi = preprocess_df(df_elect_esgi)

        df_elect_esgi['amount'] = df_elect_esgi['amount'] * 1000

        df_water_esgi = preprocess_df(df_water_esgi)

        df_water_esgi['amount'] = df_water_esgi['amount'] * 1000

        df_elect = category_group(df_elect_esgi,['總用電度數'])

        df_water = category_group(df_water_esgi,['總取水量'])

        df_carbon = category_group(df,['範疇1/類別1的直接溫室氣體排放','範疇2/類別2的間接溫室氣體排放'])

        df_renew = category_group(df,['綠電電量','購買綠證電量','自建自用電量'])

    if df_elect.size !=0:

        df_elect = insert_col(df_elect,'度')

        df_elect = df_elect[[ 'plant', 'period_start', 'amount', 'unit', 'type']]

        df_elect.drop_duplicates(inplace=True)

        update_esgi_data(df_elect,'raw', 'electricity_total_wzsesgi')

    if df_water.size !=0:

        df_water = insert_col(df_water,'立方米')

        df_water = df_water[[ 'plant', 'period_start', 'amount', 'unit', 'type']]

        df_water.drop_duplicates(inplace=True)

        update_esgi_data(df_water,'raw', 'water_wzsesgi')

    if df_renew.size !=0:

        df_renew = insert_col(df_renew,'度')

        df_renew['category1'] = '綠色能源'

        df_renew.rename(columns={'data_name': 'category2'}, inplace=True)
        df_renew['category2'] = df_renew['category2'].replace({'綠電電量':'綠電','購買綠證電量':'綠證','自建自用電量':'光伏'})

        df_renew = df_renew[[ 'category1', 'category2', 'plant', 'period_start', 'amount', 'unit', 'type']]

        df_renew.drop_duplicates(inplace=True)

        update_esgi_data(df_renew,'raw', 'renewable_energy_wzsesgi')

    if df_carbon.size !=0:
        df_carbon.rename(columns={'data_name': 'category'}, inplace=True)

        df_carbon['category'] = df_carbon['category'].replace({'範疇1/類別1的直接溫室氣體排放':'scope1','範疇2/類別2的間接溫室氣體排放':'scope2'})

        df_carbon = df_carbon[[ 'category', 'plant', 'period_start', 'amount']]

        df_carbon.drop_duplicates(inplace=True)

        update_esgi_data(df_carbon,'staging', 'carbon_emission_wzsesgi')


def esgi_replace(schema, raw, esgi, category_columns=None):
    start_date = dt(2023, 1, 1)
    end_date = dt(dt.now().year, dt.now().month, 1)

    period_start = start_date

    while period_start <= end_date:

        if category_columns is None:
            select_query = f"""SELECT plant, amount, unit, period_start,"type" FROM {schema}.{esgi} WHERE period_start = '{period_start}'"""

        elif category_columns==['category']:
            category_columns_str = ', '.join(category_columns)
            select_query = f"""SELECT plant, amount, {category_columns_str}, period_start FROM {schema}.{esgi} WHERE period_start = '{period_start}'"""

        else:
            category_columns_str = ', '.join(category_columns)
            select_query = f"""SELECT plant, amount, {category_columns_str}, unit, period_start,"type" FROM {schema}.{esgi} WHERE period_start = '{period_start}'"""

        esgi_table = pd.read_sql(select_query, con=db_eco)
        esgi_table['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        esgi_table['period_start'] = pd.to_datetime(esgi_table['period_start'])

        period_start_value = esgi_table['period_start']
        plant_value = esgi_table['plant']

        if category_columns is not None:
            category_values = [esgi_table[col] for col in category_columns]
        else:
            category_values = []

        if len(period_start_value) == 0 or len(plant_value) == 0:
            pass

        elif esgi_table.size == 0:
            pass

        else:
            delete_query = f"""DELETE FROM {schema}.{raw} WHERE plant IN {tuple(plant_value)}"""

            if category_columns is not None:
                delete_query += f" AND {' AND '.join(f'{col} IN {tuple(value)}' for col, value in zip(category_columns, category_values))}"

            delete_query += f" AND period_start IN {tuple(str(date) for date in period_start_value)}"

            conn = db_eco.connect()
            conn.execute(delete_query)

            esgi_table.to_sql(str(raw), con=db_eco, schema=str(schema), if_exists='append', index=False, chunksize=1000)
            conn.close()

        period_start += relativedelta(months=1)

