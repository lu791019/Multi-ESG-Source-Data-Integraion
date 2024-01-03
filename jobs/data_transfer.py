import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
from jobs import etl_sql
import calendar

from models import engine, engine_source

connect_csr_string = engine_source.get_connect_string_csr()
connect_eco_string = engine.get_connect_string()

db_eco = create_engine(connect_eco_string, echo=True)
db_csr = create_engine(connect_csr_string, echo=True)


def copy_carbon_coef(current_date):

    try:
        # 設定每年的12/20執行一次
        if current_date.month == 12 and current_date.day == 20:

            etl_sql.run_sql_file('./sqls/carbon_coef_copy.sql')

        return True

    except:

        return False


def reset_saving_tech(current_date):

    try:

        if current_date.day == 1:

            current = date(dt.now().year, dt.now().month,
                           1).strftime("%Y-%m-%d")

            df = pd.read_sql(f"""SELECT id, datetime, electricity_type, modified_method, expected_benefits, pic, computational_logic, remark, plant, bo, by_copy, saving_id, "year", item, investing_amount, saving_amount, "type", "month", is_edited
                FROM app.saving_tech_overview""", db_eco)

            df['day'] = 1
            df['period_date'] = pd.to_datetime(df[['year', 'month', 'day']])

            df['expire_year'] = np.where(
                df['month'] == 12, df['year']+2, df['year']+1)
            df['expire_month'] = np.where(df['month'] == 12, 1, df['month']+1)
            df['expire_date'] = pd.to_datetime(
                df.expire_year*10000 + df.expire_month*100 + df.day, format='%Y%m%d')
            df['is_edited'] = np.where(
                df['expire_date'] < current, True, False)

            df = df[['id', 'datetime', 'electricity_type', 'modified_method', 'expected_benefits', 'pic', 'computational_logic', 'remark',
                     'plant',  'bo', 'by_copy', 'saving_id', 'year', 'item', 'investing_amount', 'saving_amount', 'type', 'month', 'is_edited']]

            conn = db_eco.connect()
            conn.execute(f"TRUNCATE TABLE app.saving_tech_overview")
            df.to_sql('saving_tech_overview', con=db_eco, schema='app',
                      if_exists='append', index=False, chunksize=1000)
            conn.close()

        else:

            pass

        return True

    except:

        return False


def copy_target_green():

    current_date = dt.now()

    try:

        df_target = pd.read_sql(
            f"""SELECT bo, site, plant, "year", category, base_year, target_desc, amount, unit, last_update_time FROM staging.cfg_target""", db_eco)
        df_green = pd.read_sql(
            f"""SELECT period_start, total_amount, region, buy_amount, price, currency, "comment" FROM staging.green_energy""", db_eco)
        df_green_site = pd.read_sql(
            f"""select "year", site, plant, amount FROM staging.green_energy_site""", db_eco)

        df_green_plant = pd.read_sql(
            f"""SELECT plant, category1, category2, amount, unit, period_start FROM raw.renewable_energy where category1 ='綠色能源' and category2 ='綠證' and period_start = '2021-12-01' """, db_eco)

        if current_date.year >= df_target['year'].max():

            df_target_current = df_target[df_target['year']
                                          == df_target['year'].max()]
            df_target_current.reset_index(drop=True, inplace=True)
            df_target_current['year'] = df_target['year'].max()+1
            df_target_current['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            if df_target_current.size != 0:

                df_target_current.to_sql(
                    'cfg_target', con=db_eco, schema='staging', if_exists='append', index=False, chunksize=1000)

        if current_date.year >= df_green['period_start'].max().year:

            df_green_current = df_green[df_green['period_start']
                                        == df_green['period_start'].max()]
            df_green_current.reset_index(drop=True, inplace=True)
            df_green_current['period_start'] = date(
                df_green['period_start'].max().year+1, 12, 1).strftime("%Y-%m-%d")
            df_green_current['comment'] = '注意: 此為系統偵測後自動複製添加,請修改內容'

            if df_green_current.size != 0:

                df_green_current.to_sql(
                    'green_energy', con=db_eco, schema='staging', if_exists='append', index=False, chunksize=1000)

        if current_date.year >= df_green_site['year'].max():

            df_green_site_current = df_green_site[df_green_site['year']
                                                  == df_green_site['year'].max()]
            df_green_site_current.reset_index(drop=True, inplace=True)
            df_green_site_current['year'] = df_green_site['year'].max()+1

            if df_green_site_current.size != 0:

                df_green_site_current.to_sql(
                    'green_energy_site', con=db_eco, schema='staging', if_exists='append', index=False, chunksize=1000)

        if current_date.year >= df_green_plant['period_start'].max().year:

            period_start = date(dt.now().year+1, 12, 1).strftime("%Y-%m-%d")

            df_green_plant['period_start'] = period_start
            df_green_plant['amount'] = 0
            df_green_plant['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            df_green_plant['type'] = 'System Copy'
            if df_green_plant.size != 0:
                df_green_plant.to_sql('renewable_energy', con=db_eco, schema='raw',
                                      if_exists='append', index=False, chunksize=1000)

        return True

    except:

        return False


def main():
    copy_target_green()
