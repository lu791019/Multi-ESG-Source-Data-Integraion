import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
import re

from models import engine

connect_string = engine.get_connect_string()

# 防呆機制 : 去除col中非中文字元的文字或空格
def foolproof_col(text):
    col_name = re.sub('[^\u4e00-\u9fff]+', '', text)
    return col_name

# 定義一個函數，輸入為字符串，輸出為轉換後的float值
def foolproof_num(input_str):
    # 嘗試將輸入轉換為float
    try:
        # 如果轉換成功，則直接返回轉換後的值
        return float(input_str)
    except ValueError:
        # 如果轉換失敗，則進入except分支
        # 將輸入中的非數字字符移除
        input_str = ''.join(c for c in input_str if c.isdigit() or c == '.')
        # 再次嘗試將輸入轉換為float
        try:
            # 如果轉換成功，則返回轉換後的值
            return float(input_str)
        except ValueError:
            # 如果轉換仍然失敗，則返回 0 (當輸入值出現兩個小數點時，就會到這一行)
            return np.nan


def excel_to_raw(filepath):
    if 'waste' in filepath:
        df = pd.read_excel(filepath, header=0)
        return handle_waste(df)
    if 'EBG' in filepath:
        df = pd.read_excel(filepath, header=0, sheet_name='WZS-1')
        handle_EBG(df, plant='WZS-1')
        df = pd.read_excel(filepath, header=0, sheet_name='WIH')
        handle_EBG(df, plant='WIH')
    if '能源' in filepath:
        handle_energy_report(filepath)
    if 'revenue' in filepath:
        df = pd.read_excel(filepath, header=0)
        return handle_WMX_revenue(df)


def handle_waste(df):
    '''
    @param df pd.Dataframe
    @return boolean
    '''

    db = create_engine(connect_string, echo=True)
    # get period_start

    # excel 格式錯誤時 period_start 會噴錯，故增加例外處理
    try:
        period_start = df.columns[0].split(
            ".")[0] + "-" + df.columns[0].split(".")[1] + "-01"

        # 只取 rule table的部分
        waste_template = df.iloc[1: len(df), :]
        # reset column name
        waste_template.columns = df.iloc[0, :].tolist()
        # reset index
        waste_template = waste_template.reset_index(drop=True)

        waste = pd.DataFrame(
            columns=[
                "plant",
                "category",
                "amount"
            ]
        )

        for i in range(0, len(waste_template)):
            temp = pd.DataFrame(waste_template.iloc[i, :]).reset_index()
            temp.columns = ['plant', 'amount']
            temp['category'] = temp.iloc[0, 1]
            temp = temp.iloc[1:, ]
            waste = waste.append(temp)

        waste = waste.reset_index(drop = True)
        waste['amount'] = waste['amount'].fillna(0)
        for i in range(0,len(waste)):
            if type(waste['amount'][i]) == str :
                waste['amount'][i] = 0

        waste['period_start'] = period_start
        waste['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        waste['unit'] = '噸'
        waste['type'] = 'excel'

        plant = "','".join(waste['plant'].unique())
        conn = db.connect()
        conn.execute(
            f"DELETE FROM raw.waste WHERE plant IN ('{plant}')  AND period_start ='{period_start}'")
        waste.to_sql('waste', conn, index=False,
                     if_exists='append', schema='raw', chunksize=10000)
        conn.close()

        return True
    except:
        return False


def handle_EBG(df, plant):
    '''
    @param df pd.Dataframe
    @return boolean
    '''
    # import revenue and invoice_qty in WZS-1 and WIH
    db = create_engine(connect_string, echo=True)

    try:
        if dt.now().month == 1:
            period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        else:
            period_start = date(
                dt.now().year, dt.now().month-1, 1).strftime("%Y-%m-%d")

        month = int(period_start[5:7])

        # get revenue
        revenue = pd.DataFrame([[plant, df.iloc[0, month+2]]],
                               columns=[
            "plant",
            "amount"
        ]
        )

        revenue['period_start'] = period_start
        revenue['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        # 單位轉換 M -> 十億
        revenue['amount'] = revenue['amount'] / 1000
        revenue['unit'] = '十億台幣'

        # get invoice_qty
        shipment = pd.DataFrame([[plant, df.iloc[1, month+2]]],
                                columns=[
            "plant",
            "amount"
        ]
        )

        shipment['period_start'] = period_start
        shipment['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        # 單位轉換 K -> 台
        shipment['amount'] = shipment['amount'] * 1000

        conn = db.connect()
        conn.execute(
            f"DELETE FROM raw.ebg_revenue WHERE plant = '{plant}' AND period_start = '{period_start}'")
        revenue.to_sql('ebg_revenue', conn, index=False,
                       if_exists='append', schema='raw', chunksize=10000)

        conn.execute(
            f"DELETE FROM raw.ebg_invoice_qty WHERE plant = '{plant}' AND period_start = '{period_start}'")
        shipment.to_sql('ebg_invoice_qty', conn, index=False,
                        if_exists='append', schema='raw', chunksize=10000)
        conn.close()

        return True
    except:
        return False


def handle_energy_report(filepath):
    # import energy report
    db = create_engine(connect_string, echo=True)


    try:
        conn = db.connect()
        plant_list = ['WKS-1', 'WKS-5', 'WKS-P6', 'WOK', 'WTZ', 'WCD', 'WCQ', 'WZS-1',
                      'WZS-3', 'WZS-6', 'WZS-8', 'WIH', 'WMX', 'WCZ']

        energy_all = pd.DataFrame(
            columns=["plant", "category", "amount", "period_start"]
        )
        renewable_energy = pd.DataFrame(
            columns=["plant", "category", "amount", "period_start"]
        )

        if dt.now().month == 1:
            year = str(dt.now().year-1)
        else:
            year = str(dt.now().year)

        for plant in plant_list:
            energy = pd.read_excel(filepath, sheet_name=plant, header=0)

            if year == "2020":
                energy_current = energy.iloc[21:42, 0:16]
                energy_current.columns = energy.iloc[20, 0:16].tolist()
            elif year == "2021":
                energy_current = energy.iloc[44:65, 0:16]
                energy_current.columns = energy.iloc[43, 0:16].tolist()
            elif year == "2022":
                energy_current = energy.iloc[67:88, 0:16]
                energy_current.columns = energy.iloc[43, 0:16].tolist()

            energy_current = energy_current.reset_index(drop=True)
            energy_current = pd.concat(
                [energy_current.iloc[:, 0:2], energy_current.iloc[:, 4:]], axis=1)

            # 營業額(十億)
            revenue = energy_current.iloc[0, 2:].transpose().reset_index()
            revenue.columns = ['period_start', 'amount']

            revenue['period_start'] = year + "-" + \
                revenue['period_start'] + "-1"

            revenue['period_start'] = revenue['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            revenue['plant'] = plant
            revenue['category'] = 'revenue'

            # 生產量
            prod_qty = energy_current.iloc[1, 2:].transpose().reset_index()
            prod_qty.columns = ['period_start', 'amount']

            prod_qty['period_start'] = year + "-" + \
                prod_qty['period_start'] + "-1"

            prod_qty['period_start'] = prod_qty['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            prod_qty['plant'] = plant
            prod_qty['category'] = 'prod_qty'

            # 出貨量
            invoice_qty = energy_current.iloc[2, 2:].transpose().reset_index()
            invoice_qty.columns = ['period_start', 'amount']

            invoice_qty['period_start'] = year + "-" + \
                invoice_qty['period_start'] + "-1"

            invoice_qty['period_start'] = invoice_qty['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            invoice_qty['plant'] = plant
            invoice_qty['category'] = 'invoice_qty'

            # 用電量(度)
            electricity = energy_current.iloc[4, 2:].transpose().reset_index()
            electricity.columns = ['period_start', 'amount']

            electricity['period_start'] = year + "-" + \
                electricity['period_start'] + "-1"

            electricity['period_start'] = electricity['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            electricity['plant'] = plant
            electricity['category'] = 'electricity'

            # 用水量(立方米)
            water = energy_current.iloc[5, 2:].transpose().reset_index()
            water.columns = ['period_start', 'amount']

            water['period_start'] = year + "-" + water['period_start'] + "-1"

            water['period_start'] = water['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            water['plant'] = plant
            water['category'] = 'water'

            # 碳排放(噸)
            carbon_emission = energy_current.iloc[6, 2:].transpose(
            ).reset_index()
            carbon_emission.columns = ['period_start', 'amount']

            carbon_emission['period_start'] = year + "-" + \
                carbon_emission['period_start'] + "-1"

            carbon_emission['period_start'] = carbon_emission['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            carbon_emission['plant'] = plant
            carbon_emission['category'] = 'carbon_emission'

            # 可再生能源 - 綠色能源 - 太陽能
            solar_energy = energy_current.iloc[12,
                                               2:].transpose().reset_index()
            solar_energy.columns = ['period_start', 'amount']

            solar_energy['period_start'] = year + "-" + \
                solar_energy['period_start'] + "-1"

            solar_energy['period_start'] = solar_energy['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            solar_energy['plant'] = plant
            solar_energy['category'] = 'solar_energy'

            # 可再生能源 - 綠色能源 - 太陽能
            green_energy = energy_current.iloc[13,
                                               2:].transpose().reset_index()
            green_energy.columns = ['period_start', 'amount']

            green_energy['period_start'] = year + "-" + \
                green_energy['period_start'] + "-1"

            green_energy['period_start'] = green_energy['period_start'].apply(
                lambda x: dt.strptime(x, "%Y-%b-%d"))
            green_energy['plant'] = plant
            green_energy['category'] = 'green_energy'

            # 可再生能源 - 電網
        #     power_system = energy_2020.iloc[13:16,2:].transpose().reset_index()
        #     power_system = power_system.fillna(0)
        #     power_system[13] = power_system[13] + power_system[14] + power_system[15]
        #     power_system = power_system.drop([14,15],axis = 1)
        #     power_system.columns = ['period_start','amount']

        #     power_system['period_start'] = year + power_system['period_start'] + ",1"

        #     power_system['period_start'] = power_system['period_start'].apply(lambda x : datetime.datetime.strptime(x, "%Y,%b,%d"))
        #     power_system['plant'] = plant
        #     power_system['category'] = 'power_system'

            energy_all = energy_all.append(revenue).append(prod_qty).append(
                invoice_qty).append(electricity).append(water).append(carbon_emission)
            renewable_energy = renewable_energy.append(
                solar_energy).append(green_energy)

            # get max period_start
            period_start = energy_all['period_start'][energy_all['amount'] > 0].max(
            )

            # filter period_start
            energy_all = energy_all[energy_all['period_start']
                                    == period_start].reset_index(drop=True)
            renewable_energy = renewable_energy[renewable_energy['period_start'] == period_start].reset_index(
                drop=True)

            # rename plant
            energy_all.loc[energy_all['plant'] == 'WKS-P6', 'plant'] = 'WKS-6'
            renewable_energy.loc[renewable_energy['plant']
                                 == 'WKS-P6', 'plant'] = 'WKS-6'

        # write to raw table
        if energy_all[energy_all['amount'] > 0].shape[0] > 0:
            # 用電
            electricity = energy_all[energy_all['category'] == 'electricity']
            electricity['unit'] = '度'
            electricity = electricity.drop('category', axis=1)
            # 扣除太陽能
            solar_energy = renewable_energy
            solar_energy = solar_energy[solar_energy['category']
                                        == 'solar_energy']

            electricity = electricity.merge(
                solar_energy, on=['plant', 'period_start'], how='left')
            electricity['amount_y'] = electricity['amount_y'].fillna(0)
            electricity['amount'] = electricity['amount_x'] - \
                electricity['amount_y']
            electricity = electricity.drop(
                ['amount_x', 'amount_y', 'category'], axis=1)
            electricity['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            electricity

            conn.execute(
                f"DELETE FROM raw.electricity_total WHERE period_start = '{period_start}' AND plant not in ('WNH','WHC')")
            electricity.to_sql('electricity_total', conn, index=False,
                               if_exists='append', schema='raw', chunksize=10000)

            # 太陽能
            solar_energy = renewable_energy

            solar_energy.loc[solar_energy['category']
                             == 'green_energy', 'category2'] = '綠證'
            solar_energy.loc[solar_energy['category']
                             == 'solar_energy', 'category2'] = '光伏'
            solar_energy['category1'] = '綠色能源'
            solar_energy = solar_energy.drop('category', axis=1)
            solar_energy['unit'] = '度'
            solar_energy['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            solar_energy = solar_energy[solar_energy['category2'] == '光伏']
            solar_energy

            conn.execute(
                f"DELETE FROM raw.renewable_energy WHERE category2 = '光伏' AND period_start = '{period_start}'")
            solar_energy.to_sql('renewable_energy', conn, index=False,
                                if_exists='append', schema='raw', chunksize=10000)

            # 碳排
            carbon_emission = energy_all[energy_all['category']
                                         == 'carbon_emission']

            carbon_emission['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            carbon_emission = carbon_emission.drop('category', axis=1)
            carbon_emission['category'] = 'scope2'
            carbon_emission

            conn.execute(
                f"DELETE FROM staging.carbon_emission WHERE category = 'scope2' AND period_start = '{period_start}'  AND plant not in ('WNH','WHC')")
            carbon_emission.to_sql('carbon_emission', conn, index=False,
                                   if_exists='append', schema='staging', chunksize=10000)

            # 用水
            water = energy_all[energy_all['category'] == 'water']
            water['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            water = water.drop('category', axis=1)
            water['unit'] = '立方米'
            water

            conn.execute(
                f"DELETE FROM raw.water WHERE period_start = '{period_start}' AND plant not in ('WNH','WHC')")
            water.to_sql('water', conn, index=False,
                         if_exists='append', schema='raw', chunksize=10000)

            # 約當產量
            production_qty = energy_all[energy_all['category'] == 'prod_qty']

            production_qty = production_qty.drop('category', axis=1)
            production_qty['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            production_qty

            conn.execute(
                f"DELETE FROM raw.production_qty WHERE period_start = '{period_start}'")
            production_qty.to_sql('production_qty', conn, index=False,
                                  if_exists='append', schema='raw', chunksize=10000)

            # 出貨
            invoice_qty = energy_all[energy_all['category'] == 'invoice_qty']
            invoice_qty = invoice_qty.drop('category', axis=1)
            invoice_qty['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            invoice_qty

            conn.execute(
                f"DELETE FROM raw.invoice_qty WHERE period_start = '{period_start}'")
            invoice_qty.to_sql('invoice_qty', conn, index=False,
                               if_exists='append', schema='raw', chunksize=10000)

            # 營收
            revenue = energy_all[energy_all['category'] == 'revenue']
            revenue = revenue.drop('category', axis=1)
            revenue['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            revenue['unit'] = '十億台幣'
            revenue

            conn.execute(
                f"DELETE FROM raw.revenue WHERE period_start = '{period_start}'")
            revenue.to_sql('revenue', conn, index=False,
                           if_exists='append', schema='raw', chunksize=10000)

        conn.close()

        return True
    except:
        return False


def update_rpt2raw(df_target, raw_table, plant):

    try:
        try:
            period_start = dt.strptime(
                df_target.loc[df_target['period_start'].notna(),'period_start'].min().strftime("%Y-%m-%d"), "%Y-%m-%d")
            period_end = dt.strptime(
                df_target.loc[df_target['period_start'].notna(),'period_start'].max().strftime("%Y-%m-%d"), "%Y-%m-%d")
        except:
            period_start = dt.strptime(
                df_target['period_start'].min().strftime("%Y-%m-%d"), "%Y-%m-%d")
            period_end = dt.strptime(
                df_target['period_start'].max().strftime("%Y-%m-%d"), "%Y-%m-%d")

        db = create_engine(connect_string, echo=True)
        conn = db.connect()
        conn.execute(
            f"""Delete From raw.{raw_table} where plant in ('{plant}') and period_start >= '{period_start}' and period_start <= '{period_end}'""")
        df_target.to_sql(str(raw_table), conn, index=False,
                        if_exists='append', schema='raw', chunksize=10000)
        conn.close()

    except:
        print("df_target['period_start']",df_target['period_start'])


def new_rpt_import(filepath):

    try:

        dates = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        file = pd.ExcelFile(filepath)

        if dt.now().month == 1:
            year = str(dt.now().year-1)
        else:
            year = str(dt.now().year)

        for sheets in file.sheet_names:

            if sheets not in ('WMX','WCZ'):

                rpt = pd.read_excel(str(filepath), sheet_name=str(
                    sheets), usecols="A:O", header=0)
                rpt['時間'] = rpt['時間'].fillna(method='ffill').astype(int)

                rpt = rpt[rpt['時間'] == int(year)]

                energy = pd.DataFrame()
                for idx, row in rpt.iterrows():

                    sub_df = row[dates].to_frame('amount')
                    sub_df['item'] = row['項目']
                    sub_df['unit'] = row['單位']
                    sub_df['period_start'] = row['時間']
                    sub_df['period_start'] = sub_df['period_start'].astype(
                        str) + '-' + sub_df.index
                    energy = energy.append(sub_df)

                energy.reset_index(drop=True, inplace=True)
                dateFormatter = "%Y-%b"
                S_date = []
                for i in energy['period_start']:
                    S_date.append(dt.strptime(i, dateFormatter))

                energy['item'] = energy['item'].map(lambda x:foolproof_col(x))
                energy['period_start'] = S_date
                energy['plant'] = str(sheets)
                energy['last_update_time'] = dt.strptime(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                energy['type'] = '月報表'

                energy['amount'] = energy['amount'].map(lambda x:foolproof_num(x))
                energy = energy[energy['amount'].notna()]

                plant = str(sheets)
                # revenue
                revenue = energy[['plant', 'amount', 'unit', 'period_start',
                                    'last_update_time', 'type']][energy['item'] == '營業額']
                revenue['unit'] = '十億台幣'

                if revenue.size !=0:

                    update_rpt2raw(revenue, 'revenue', plant)

                # invoice_qty
                invoice_qty = energy[['plant', 'amount', 'period_start',
                                        'last_update_time', 'type']][energy['item'] == '出貨量']
                if invoice_qty.size !=0:

                    update_rpt2raw(invoice_qty, 'invoice_qty', plant)

                # electricity_total
                electricity_total = energy[['plant', 'amount', 'unit',
                                            'period_start', 'last_update_time', 'type']][energy['item'] == '電']
                electricity_total['unit'] = '度'

                if electricity_total.size !=0:

                    update_rpt2raw(electricity_total, 'electricity_total', plant)

                # water
                water = energy[['plant', 'amount', 'unit', 'period_start',
                                'last_update_time', 'type']][energy['item'] == '水']
                water['unit'] = '立方米'

                if water.size !=0:

                    update_rpt2raw(water, 'water', plant)

                # production_qty
                prod = ['PCBA生產量', 'FA生產量']
                prod = energy[energy['item'].isin(prod)]
                prod['amount'] = prod.groupby(['plant', 'period_start', 'last_update_time', 'type'])[
                    'amount'].transform('sum')
                prod.drop(['item', 'unit'], axis=1, inplace=True)
                prod.drop_duplicates(inplace=True)

                if prod.size !=0:

                    update_rpt2raw(prod, 'production_qty', plant)

        return True

    except:
        return False

def handle_WMX_revenue(df):
    # import revenue WMX - WYHQ
    db = create_engine(connect_string, echo=True)

    try:
        period_start = df['period_start'][0]

        df['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        conn = db.connect()
        conn.execute(f"DELETE FROM raw.revenue_wmx WHERE plant = 'WMX' AND period_start = '{period_start}'")
        df.to_sql('revenue_wmx', conn, index=False,if_exists='append', schema='raw', chunksize=10000)


        # revenue_WMX = pd.read_sql(f"SELECT period_start,plant,amount FROM raw.revenue_wmx WHERE  period_start = '{period_start}' ", con=db)
        # revenue = pd.read_sql( f"SELECT plant,amount,period_start,type FROM raw.revenue WHERE plant = 'WMX' AND period_start = '{period_start}' ", con=db)
        # if revenue.shape[0] > 0:
        #     data_type = revenue['type'][0]
        #     revenue = revenue[['plant','amount','period_start']]
        #     revenue = revenue.append(revenue_WMX)
        #     revenue = revenue.groupby(['period_start', 'plant']).sum().reset_index()
        # else:
        #     revenue = revenue_WMX
        #     data_type = 'user'

        # revenue['unit'] = '十億台幣'
        # revenue['type'] = data_type
        # revenue['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        # conn.execute( f"DELETE FROM raw.revenue WHERE plant = 'WMX' AND period_start = '{period_start}'")
        # revenue.to_sql('revenue', db, index=False, if_exists='append', schema='raw', chunksize=10000)

        conn.close()

        return True
    except:
        return False
