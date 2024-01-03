import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pandasql import sqldf
from sqlalchemy import create_engine
from models import engine
import logging
# import warnings
# warnings.filterwarnings("ignore")

###########################
##### --- Function --- ####
###########################
## plant代碼轉換
def plant_transf(plant_code, plant_mapping):
    return plant_mapping.get(plant_code, plant_code)
# def plant_transf(plant_code,mapping):
#     try:
#         plant = mapping[mapping['plant_code']==plant_code]['plant_name'].reset_index(drop=True)[0]
#     except:
#         plant = plant_code
#     return plant

## site-plant代碼轉換
def site_plant_transf(x):
    site = x['site']
    plant = x['plant']
    
    if (site=='WKS') & (plant == 'P1'):
        plant = 'WKS-1'
        
    elif (site=='WKS') & (plant == 'P5'):
        plant = 'WKS-5'
        
    elif (site=='WKS') & (plant == 'P6'):
        plant = 'WKS-6'
        
    elif (site=='WOK'):
        plant = 'WOK' 
        
    elif (site=='WTZ'):
        plant = 'WTZ' 
        
    elif (site=='WZS') & (plant == 'P1'):
        plant = 'WZS-1'
        
    elif (site=='WZS') & (plant == 'P3'):
        plant = 'WZS-3'
    
    elif (site=='WZS') & (plant == 'P6'):
        plant = 'WZS-6'
    
    elif (site=='WZS') & (plant == 'P8'):
        plant = 'WZS-8'
        
    elif (site=='XTRKS'):
        plant = 'XTRKS' 
    
    else:
        plant = np.nan
    return plant

def bo_trans(plant,plant_mapping):
    if plant in plant_mapping[plant_mapping.bo=='WSD'].plant_name.unique():
        plant = 'WSD'
    elif plant in plant_mapping[plant_mapping.bo=='WT'].plant_name.unique():
        plant = 'WT'
    elif plant in plant_mapping[plant_mapping.bo=='Others'].plant_name.unique():
        plant = 'Others'
    elif plant in plant_mapping[plant_mapping.bo=='Corp'].plant_name.unique():
        plant = 'Corp'
    else:
        plant=''
    return plant

## 用電細項重新命名
def detail_to_chinese(x):
    if x == 'AIRCONDITIONER':
        x = '空調用電'
        
    elif x == 'BASIC':
        x = '基礎用電'
    
    elif x == 'FOUNDATION':
        x = '基礎用電'
        
    elif x == 'AIRCOMPRESSOR':
        x = '空壓用電'
        
    elif x == 'MIS':
        x = 'MIS用電'
        
    elif x == 'LIGHTFIXTURE':
        x = '照明插座'
        
    elif x == 'OFFLINE':
        x = '線下用電'
        
    elif x == 'CANTEEN':
        x = '餐廳用電'

    elif x == 'PRODUCTION':
        x = '生產用電'
        
    elif x == 'CLEANSHED':
        x = '無塵室用電'
    
    elif x == 'QE':
        x = '生產用電'
        
    elif x == 'LINE':
        x = '線體用電'
        
    elif x == 'VENTILATOR':
        x = '真空排風'
    return x

## 抓取氣象站，氣溫資料
def get_city_weather(city_number):
    # 抓取氣象站，氣溫資料
    get_weather_api = requests.get(f"https://worldweather.wmo.int/tc/json/{city_number}_tc.xml").json()
    last_month_weather = pd.DataFrame(get_weather_api['city']['climate']['climateMonth'])

    # 固定讀取上個月的資訊
    last_month_weather['year'] = (datetime.now()-timedelta(days=30)).year
    last_month_weather = last_month_weather[last_month_weather['month']==(datetime.now()-timedelta(days=30)).month]
    last_month_weather[['maxTemp','minTemp']] = last_month_weather[['maxTemp','minTemp']].astype(float)
    # 若 meanTemp 是空值則取最大/最小溫度平均
    last_month_weather.meanTemp = last_month_weather.meanTemp.fillna((last_month_weather.maxTemp*0.7+last_month_weather.minTemp*0.3))
    
    if city_number == 547:
        last_month_weather['plant'] = 'WCD-1'
    elif city_number == 349:
        last_month_weather['plant'] = 'WCQ'
    elif city_number == 241: # 中山氣溫以廣州取代
        last_month_weather['plant'] = 'WZS-1'
        tmp_ = last_month_weather.copy()
        tmp_['plant'] = 'WZS-3'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'WZS-6'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'WZS-8'
        last_month_weather = last_month_weather.append(tmp_)
    elif city_number == 240: # 崑山、緯視晶氣溫以上海取代
        last_month_weather['plant'] = 'WOK'
        tmp_ = last_month_weather.copy()
        tmp_['plant'] = 'WKS-1'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'WKS-5'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'WKS-6'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'XTRKS'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'WGKS'
        last_month_weather = last_month_weather.append(tmp_)
    elif city_number == 350: # 泰州氣溫用南京取代
        last_month_weather['plant'] = 'WTZ'
    elif city_number == 1838:
        last_month_weather['plant'] = 'KOE'
    elif city_number == 308: # 越南 河內市
        last_month_weather['plant'] = 'WVN'
    elif city_number == 354: # 新竹氣溫用台北取代
        last_month_weather['plant'] = 'WIH'
        tmp_ = last_month_weather.copy()
        tmp_['plant'] = 'WIHK-1'
        last_month_weather = last_month_weather.append(tmp_)
        tmp_['plant'] = 'WIHK-2'
        last_month_weather = last_month_weather.append(tmp_)
    elif city_number == 693: # 捷克 布爾諾
        last_month_weather['plant'] = 'WCZ'
    elif city_number == 1278: # 墨西哥 華雷斯
        last_month_weather['plant'] = 'WMX'
    elif city_number == 88: # 馬來西亞 必打靈查亞 (WMY-2) ; WMY-1 在巴生港(但API中與其較近的站點為必打靈查亞)
        last_month_weather['plant'] = 'WMY-1'
        tmp_ = last_month_weather.copy()
        tmp_['plant'] = 'WMY-2'
        last_month_weather = last_month_weather.append(tmp_)
    elif city_number == 523: # 印度 班加羅爾
        last_month_weather['plant'] = 'WMI-1'
        tmp_ = last_month_weather.copy()
        tmp_['plant'] = 'WMI-2'
        last_month_weather = last_month_weather.append(tmp_)
    
    return last_month_weather[['year','month','plant','meanTemp']]
    

## 移除無時間資訊的資料
def drop_na_time(x):
    try:
        return datetime.strptime(x,'%Y-%m-%d %H:%M')
    except:
        return np.nan

## PCBA/FA產品判斷
# King Tang : WOK 產品開始到結束可能需要2~3天不等，因為中間需要產品靜置等等，因此開線主要是看 CGL段的開線數！
# Tony Wen : WOK 需要將產品數轉換成開線數(CGL段每產出16400個產品就算一條線)
def process_type(df):
    try:
        process_ = df['process']
    except:
        process_ = df['mfgtype']
        
    if (process_ == 'FA_small') | (process_ == 'FA_big') | (process_ == 'FA') | (process_ == 'CGL'):
        df['mfgtype'] = 'FA'
    elif (process_ == 'PCBA') | (process_ == 'PCB') | (process_ == 'JI'):
        df['mfgtype'] = 'PCBA'
    else:
        df['mfgtype'] = process_
    return df

def baseline_data_update():

    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)

   
    sql_last_mon = str(datetime.now()-timedelta(days=63))[0:10]
    last_month_year = (datetime.now()-timedelta(days=30)).year
    last_month  = (datetime.now()-timedelta(days=30)).month
    plant_mapping = pd.read_sql('SELECT * FROM raw.plant_mapping',con=conn)
    # WZS用電分類用P1~P8表示，須新增規則。(同一個建物下分成很多廠區，因此比較複雜)
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WZS','WZS-1','P1',datetime.now()]
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WZS','WZS-3','P3',datetime.now()]
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WZS','WZS-6','P6',datetime.now()]
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WSD','WZS','WZS-8','P8',datetime.now()]
    # WCD 後來要分 WCD-1 & WCD-2，原 WCD 的為修改過後的 WCD-1
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WCD','WCD-1','WCD',datetime.now()]

    # 將 mapping 資料表的 plant_code 設為 index
    mapping = plant_mapping.set_index('plant_code')
    # 建立 plant_code 到 plant_name 的 mapping
    plant_mapping_ = dict(zip(mapping.index, mapping['plant_name']))

    # try:
    ###########################
    ##### --- 用電細項 --- #####
    ###########################
    # - 用電細項資料 - #
    wks_mfg_fem_dailypower = pd.read_sql(
        f"""
            SELECT * 
            FROM raw.wks_mfg_fem_dailypower
            WHERE consumetype NOT IN ('用電量','用水量','用氣量') 
                AND datadate > '{sql_last_mon}'
        """, con=conn)
    # 防呆機制 : consumetype 類別全部轉大寫，並去掉類別名稱後面的空格(某些廠類別後面有空白)
    wks_mfg_fem_dailypower['consumetype'] = wks_mfg_fem_dailypower['consumetype'].str.replace(" ", "").str.upper()
    # 轉成中文類別，方便理解各項代表的意義(因為各廠分類略有不同)
    wks_mfg_fem_dailypower['consumetype'] = wks_mfg_fem_dailypower['consumetype'].map(lambda x: detail_to_chinese(x))
    # plant代碼與名稱對照
    wks_mfg_fem_dailypower['plant'] = wks_mfg_fem_dailypower['plant'].map(lambda plant : plant_transf(plant,plant_mapping_))

    ele_groupby = wks_mfg_fem_dailypower.groupby(['plant','consumetype','datadate']).agg({'power':'sum'}).reset_index()
    # --- 計算各項用電 --- #
    production_electricity = \
        ele_groupby[ele_groupby.consumetype.isin(['線下用電','線體用電','真空排風','生產用電','無塵室用電'])]\
            .groupby(['plant','datadate']).agg({'power':'sum'}).reset_index().rename(columns={'power':'生產用電'})

    ap_electricity = \
        ele_groupby[ele_groupby.consumetype.isin(['空壓用電'])]\
            .groupby(['plant','datadate']).agg({'power':'sum'}).reset_index().rename(columns={'power':'空壓用電'})

    ac_electricity = \
        ele_groupby[ele_groupby.consumetype.isin(['空調用電'])]\
            .groupby(['plant','datadate']).agg({'power':'sum'}).reset_index().rename(columns={'power':'空調用電'})

    base_electricity = \
        ele_groupby[ele_groupby.consumetype.isin(['基礎用電','照明插座','MIS用電','餐廳用電'])]\
            .groupby(['plant','datadate']).agg({'power':'sum'}).reset_index().rename(columns={'power':'基礎用電'})

    # --- merge 各項 table --- #
    ele_detail_table = \
        base_electricity.merge(ap_electricity,left_on=['plant','datadate'],right_on=['plant','datadate'],how='inner')\
                        .merge(ac_electricity,left_on=['plant','datadate'],right_on=['plant','datadate'],how='inner')\
                        .merge(production_electricity,left_on=['plant','datadate'],right_on=['plant','datadate'],how='inner')

    ele_detail_table['year'] = ele_detail_table.datadate.str.slice(0,4)
    ele_detail_table['month'] = ele_detail_table.datadate.str.slice(5,7)
    ele_detail_table['day'] = ele_detail_table.datadate.str.slice(8,10)

    fill_baseline_table = \
        ele_detail_table\
            .groupby(['plant','year','month'])\
            .agg({'基礎用電':'sum','空壓用電':'sum','空調用電':'sum','生產用電':'sum'})\
            .reset_index()

    # 更改成 DB 上使用的名稱
    fill_baseline_table = \
        fill_baseline_table.rename(columns={
            '空調用電':'ac_electricity',
            '生產用電':'production_electricity',
            '空壓用電':'ap_electricity',
            '基礎用電':'base_electricity'
        })
    fill_baseline_table['factory_electricity'] = fill_baseline_table['ac_electricity'] + fill_baseline_table['production_electricity'] + fill_baseline_table['ap_electricity'] + fill_baseline_table['base_electricity']

    ###########################
    ##### --- 外氣溫度 --- #####
    ###########################
    ## - 地區代碼 - ##
    # "成都";"547"
    # "重慶";"349"
    # "蘇州";"1857" (崑山市)(緯視晶右邊附近)(資料不完整，可能是近期才設的站點)
    # "廣州";"241" (中山)
    # "上海";"240" (崑山、緯視晶)
    # "南京";"350" (泰州)
    # "台北";"354"
    # "高雄";"1838"
    # "布爾諾";"693" (捷克)
    # "華雷斯";"1278" (墨西哥)
    # "河內";"308" (越南)
    # https://worldweather.wmo.int/tc/json/full_city_list.txt 地址編號一覽表

    last_month_temp = pd.DataFrame()
    
    for city_number in [547,349,241,240,350,308,354,693,1278,88,523]:
        last_month_temp = last_month_temp.append(get_city_weather(city_number))

    last_month_temp = last_month_temp.reset_index(drop = True)

    fill_baseline_table['year'] = fill_baseline_table['year'].astype(int)
    fill_baseline_table['month'] = fill_baseline_table['month'].astype(int)

    sum_table = last_month_temp.merge(fill_baseline_table,left_on=['year','month','plant'],right_on=['year','month','plant'],how='outer')
    sum_table = sum_table.rename(columns={'meanTemp':'average_temperature'})

    sum_table['datetime'] = sum_table['year'].astype(str)+'-'+sum_table['month'].astype(str).str.zfill(2)
    sum_table['datetime'] = sum_table['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    sum_table['bo'] = sum_table['plant'].map(lambda x : bo_trans(x,plant_mapping))

    ###########################
    ##### --- 人力 --- #########
    ###########################
    ## WW 全球人力
    employeeinfo_count = pd.read_sql(
        f"""
            SELECT * 
            FROM raw.employeeinfo_count
            WHERE period_start >= '{sql_last_mon}'
        """,con=conn)

    employeeinfo_count['year'] = employeeinfo_count.period_start.astype(str).str.slice(0,4).astype(int)
    employeeinfo_count['month'] = employeeinfo_count.period_start.astype(str).str.slice(5,7).astype(int)
    employeeinfo_count['day'] = employeeinfo_count.period_start.astype(str).str.slice(8,10).astype(int)
    
    last_month_manpower = employeeinfo_count[(employeeinfo_count.year==last_month_year)&(employeeinfo_count.month==last_month)]
    last_month_manpower = last_month_manpower.groupby(['plant','year','month']).agg({'dl':'mean','idl':'mean'}).reset_index()
    last_month_manpower['member_counts'] = last_month_manpower['dl'] + last_month_manpower['idl']
    last_month_manpower = last_month_manpower[last_month_manpower.plant.isin(plant_mapping.plant_name.unique())]
    last_month_manpower = last_month_manpower[['plant', 'year', 'month', 'member_counts']]

    sum_table2 = sum_table.merge(last_month_manpower,left_on=['year','month','plant'],right_on=['year','month','plant'],how='outer')
    sum_table2['datetime'] = sum_table2['year'].astype(str)+'-'+sum_table2['month'].astype(str).str.zfill(2)
    sum_table2['datetime'] = sum_table2['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    sum_table2['bo'] = sum_table2['plant'].map(lambda x : bo_trans(x,plant_mapping))
    sum_table2 = sum_table2[sum_table2.month==last_month]
    
    # # WSD 打卡時數
    # wsd_clockevent = pd.read_sql(
    #     """
    #         SELECT bu, site, plant, emplid, baseline_date, f_punch_date, f_punch_time, l_punch_date, l_punch_time, duration 
    #         FROM raw.ww_hcm_tl_clockevent_g_wsd
    #     """,con=conn)
        
    # wsd_clockevent['year'] = wsd_clockevent.baseline_date.str.slice(0,4)
    # wsd_clockevent['month'] = wsd_clockevent.baseline_date.str.slice(5,7)
    # wsd_clockevent['day'] = wsd_clockevent.baseline_date.str.slice(8,10)

    # # 合併打卡日期與時間，並將合併後的結果轉乘時間格式(為了要計算上班時數，若當日請假打卡為空值的將直接跳過該筆資料)
    # wsd_clockevent['f_time'] = wsd_clockevent['f_punch_date']+' '+wsd_clockevent['f_punch_time']
    # wsd_clockevent['l_time'] = wsd_clockevent['l_punch_date']+' '+wsd_clockevent['l_punch_time']
    # wsd_clockevent.loc[~wsd_clockevent['f_time'].isna(),'f_time'] = wsd_clockevent.loc[~wsd_clockevent['f_time'].isna(),'f_time'].map(lambda x:drop_na_time(x))
    # wsd_clockevent.loc[~wsd_clockevent['l_time'].isna(),'l_time'] = wsd_clockevent.loc[~wsd_clockevent['l_time'].isna(),'l_time'].map(lambda x:drop_na_time(x))

    # # 發現表格中的上班時數(duration)似乎有計算問題？ 因此自己計算一個上班時間
    # # 刷出-刷進時間 = 上班時數(d_time) 並轉換成小時為單位 
    # wsd_clockevent.loc[~wsd_clockevent['l_time'].isna(),'d_time'] = wsd_clockevent.loc[~wsd_clockevent['l_time'].isna(),'l_time'] - wsd_clockevent.loc[~wsd_clockevent['f_time'].isna(),'f_time']
    # wsd_clockevent.d_time = wsd_clockevent.d_time / np.timedelta64(1, 'h')

    # #刷出-刷進時間 != 上班時間(duration)的欄位
    # # wsd_clockevent.loc[abs(wsd_clockevent.d_time - wsd_clockevent.duration)>0.6,:]

    # wsd_clockevent['year'] = wsd_clockevent['year'].astype(int)
    # wsd_clockevent['month'] = wsd_clockevent['month'].astype(int)

    # WSD_man_hours = \
    #     sqldf(f"""
    #         SELECT bu,site,plant,year,month, SUM(d_time) man_hours
    #         FROM wsd_clockevent
    #         WHERE site IN ('WKS','WOK','WTZ','WZS','XTRKS') 
    #             AND year = {last_month_year}
    #             AND month = {last_month}
    #             AND d_time > 0
    #             AND plant IN ('P1','P5','P6','P8','LC','XK')
    #         GROUP BY bu,site,plant,year,month
    #     """)

    # # plant代碼與名稱對照
    # WSD_man_hours['plant'] = WSD_man_hours.apply(lambda x : site_plant_transf(x),1)
    # WSD_man_hours = WSD_man_hours.dropna()[['plant','year','month','man_hours']]

    # sum_table2 = sum_table.merge(WSD_man_hours,left_on=['year','month','plant'],right_on=['year','month','plant'],how='outer')
    # sum_table2 = sum_table2.rename(columns={'man_hours':'member_counts'})


    # sum_table2['datetime'] = sum_table2['year'].astype(str)+'-'+sum_table2['month'].astype(str).str.zfill(2)
    # sum_table2['datetime'] = sum_table2['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    # sum_table2['bo'] = sum_table2['plant'].map(lambda x : bo_trans(x,plant_mapping))
    # sum_table2 = sum_table2[sum_table2.month==last_month]

    ###########################
    ##### --- 營業額 --- #######
    ###########################

    # 營業額
    revenue = pd.read_sql('SELECT * FROM staging.revenue',con=conn)
    revenue = revenue[['bo','site','plant','amount','period_start']] # 單位 : 十億台幣
    revenue = revenue[~(revenue.bo=='ALL')]
    revenue = revenue[~(revenue.bo=='ALL+新邊界')]

    revenue['datetime'] = revenue['period_start'].map(lambda x : datetime.strftime(x,'%Y-%m')).astype(str).str.slice(0,10)

    revenue['year'] = revenue.datetime.str.slice(0,4)
    revenue['month'] = revenue.datetime.str.slice(5,7)

    revenue['year'] = revenue['year'].astype(int)
    revenue['month'] = revenue['month'].astype(int)
    revenue = revenue[(revenue.year==last_month_year)&(revenue.month==last_month)]
    revenue = revenue[['plant','year','month','amount']]
    revenue = \
        revenue.loc[revenue.plant.isin(['WCD-1', 'WCQ', 'WCZ', 'WIH', 'WKS-1', 'WKS-5', 'WMX','WOK', 'WTZ',
                                        'WZS-1', 'WZS-3', 'WZS-6', 'WZS-8', 'WKS-6', 'KOE']),:]


    sum_table3 = sum_table2.merge(revenue,left_on=['year','month','plant'],right_on=['year','month','plant'],how='outer')
    sum_table3 = sum_table3.rename(columns={'amount':'revenue'})


    sum_table3['datetime'] = sum_table3['year'].astype(str)+'-'+sum_table3['month'].astype(str).str.zfill(2)
    sum_table3['datetime'] = sum_table3['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    sum_table3['bo'] = sum_table3['plant'].map(lambda x : bo_trans(x,plant_mapping))
    sum_table3 = sum_table3[sum_table3.month==last_month]

    ###########################
    ##### --- 開線數 --- #######
    ###########################
    # WTZ規則還需要再確認 (討論窗口 Wei Z Zhang)

    # --- 開線數 --- #
    wks_accs_ps_air_schedule =  pd.read_sql(
        f"""
            SELECT * 
            FROM raw.wks_accs_ps_air_schedule
            WHERE time > '{datetime.now()-timedelta(days=40)}'
        """,con=conn)
    # F330 為 WKS-3 已經賣給立訊，因此直接移除
    wks_accs_ps_air_schedule = wks_accs_ps_air_schedule.loc[wks_accs_ps_air_schedule.plant!='F330',['time', 'site', 'building', 'plant','process', 'line', 'count']]

    # plant代碼與名稱對照
    wks_accs_ps_air_schedule['plant'] = wks_accs_ps_air_schedule['plant'].map(lambda plant : plant_transf(plant,plant_mapping_))
    # WMY 目前只有一廠代碼為 P1 為了避免其他廠未來也有 P1，暫時更改成 WMY，後續有增廠或是有正是代碼 (Fxxx)再進行更改。
    wks_accs_ps_air_schedule.loc[(wks_accs_ps_air_schedule['site']=='WMY')&(wks_accs_ps_air_schedule['plant']=='P1'),'plant']='WMY'

    wks_accs_ps_air_schedule['year'] = wks_accs_ps_air_schedule.time.str.slice(0,4)
    wks_accs_ps_air_schedule['month'] = wks_accs_ps_air_schedule.time.str.slice(5,7)
    wks_accs_ps_air_schedule['day'] = wks_accs_ps_air_schedule.time.str.slice(8,10)
    wks_accs_ps_air_schedule['hour'] = wks_accs_ps_air_schedule.time.str.slice(11,13)
    # WOK 比較特殊沒有 hour 是一天一次的資料
    wks_accs_ps_air_schedule.loc[wks_accs_ps_air_schedule['hour']=='','hour'] = '00'

    # 2019~2020的資料暫不需要
    wks_accs_ps_air_schedule = wks_accs_ps_air_schedule[~wks_accs_ps_air_schedule.year.isin(['2019','2020','2021'])]

    lines_count = wks_accs_ps_air_schedule.apply(lambda x : process_type(x),1)
    # 透過 Tony Wen 提供的換算方式 將 CGL 產量除上 16400 轉換成線數
    lines_count.loc[(lines_count.site=='WOK')&(lines_count.mfgtype=='FA'),'count'] = \
        lines_count.loc[(lines_count.site=='WOK')&(lines_count.mfgtype=='FA'),'count']/16400

    WSD_lines = \
        lines_count.groupby(['plant','mfgtype','year','month'])\
            .agg({'count':'mean'})\
            .reset_index()
    # # WTZ 目前邏輯有問題 先移除！！！！！！！！！！！！！！
    # WSD_lines_tmp = WSD_lines[WSD_lines.plant!='WTZ']
    # WSD_lines_tmp = WSD_lines_tmp[(WSD_lines_tmp.mfgtype=='FA')|(WSD_lines_tmp.mfgtype=='PCBA')]
    WSD_lines = WSD_lines[WSD_lines.mfgtype.isin(['FA', 'PCBA'])]
    WSD_lines = pd.pivot_table(WSD_lines, values = 'count', columns = ['mfgtype'],index = ['plant','year','month'])\
        .reset_index()\
        .rename_axis(None, axis=1)\
        .rename(columns={
            'FA':'fa_lines',
            'PCBA':'pcba_lines'})\
        .fillna(0)

    WSD_lines['year'] = WSD_lines['year'].astype(int)
    WSD_lines['month'] = WSD_lines['month'].astype(int)

    sum_table4= sum_table3.merge(WSD_lines,left_on=['year','month','plant'],right_on=['year','month','plant'],how='outer')
    sum_table4 = sum_table4.rename(columns={'amount':'revenue'})


    sum_table4['datetime'] = sum_table4['year'].astype(str)+'-'+sum_table4['month'].astype(str).str.zfill(2)
    sum_table4['datetime'] = sum_table4['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    sum_table4['bo'] = sum_table4['plant'].map(lambda x : bo_trans(x,plant_mapping))
    sum_table4 = sum_table4[sum_table4.month==last_month]

    ###########################
    ##### --- 約當產量 --- #####
    ###########################

    # 約當產量
    as_PCBA_FA_output = pd.read_sql(
        f"""
            SELECT * 
            FROM raw.wks_mfg_dpm_upphndetail
            WHERE period > '{datetime.now()-timedelta(days=40)}'
        """,con=conn)
    as_PCBA_FA_output = as_PCBA_FA_output.loc[as_PCBA_FA_output.periodtype=='daily',['period','periodtype','site','plant','mfgtype','output']]
    as_PCBA_FA_output = as_PCBA_FA_output[as_PCBA_FA_output.period>'20211231']

    as_PCBA_FA_output['period'] = as_PCBA_FA_output.period.map(lambda x: datetime.strptime(x,'%Y%m%d')).astype(str).str.slice(0,10)
    as_PCBA_FA_output['year'] = as_PCBA_FA_output.period.str.slice(0,4)
    as_PCBA_FA_output['month'] = as_PCBA_FA_output.period.str.slice(5,7)
    as_PCBA_FA_output['day'] = as_PCBA_FA_output.period.str.slice(8,10)

    as_PCBA_FA_output['plant'] = as_PCBA_FA_output['plant'].map(lambda plant : plant_transf(plant,plant_mapping_))
    as_PCBA_FA_output = as_PCBA_FA_output.apply(lambda x : process_type(x),1)

    as_output = \
        as_PCBA_FA_output.groupby(['plant','mfgtype','year','month'])\
            .agg({'output':'sum'})\
            .reset_index()

    as_output = \
        pd.pivot_table(as_output, values = 'output', columns = ['mfgtype'],index = ['plant','year','month'])\
            .reset_index()\
            .rename_axis(None, axis=1)\
            .rename(columns={
                'FA':'fa_qty',
                'PCBA':'pcba_qty'
            })[['plant','year','month','fa_qty','pcba_qty']]

    as_output['year'] = as_output['year'].astype(int)
    as_output['month'] = as_output['month'].astype(int)

    sum_table5= sum_table4.merge(as_output,left_on=['year','month','plant'],right_on=['year','month','plant'],how='outer')

    sum_table5['datetime'] = sum_table5['year'].astype(str)+'-'+sum_table5['month'].astype(str).str.zfill(2)
    sum_table5['datetime'] = sum_table5['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    sum_table5['bo'] = sum_table5['plant'].map(lambda x : bo_trans(x,plant_mapping))
    sum_table5 = sum_table5[sum_table5.month==last_month]
    sum_table5['last_update_time'] = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    ############################################################################################################################
    # --- connect DB --- #
    df = pd.read_sql('SELECT * FROM app.baseline_data_overview',con=conn)
    df['year'] = df['datetime'].map(lambda x: x.year)
    df['month'] = df['datetime'].map(lambda x: x.month)

    # 將年,月,廠相同的資料填入新的資訊
    df_ = \
        sqldf("""
            SELECT df.datetime, df.dorm_electricity, df.plant,  df.bo,  df.year,  df.month,
                    s.member_counts, s.revenue, s.pcba_lines, s.fa_lines,s.pcba_qty, s.fa_qty,s.average_temperature,
                    s.base_electricity, s.ap_electricity, s.ac_electricity, s.production_electricity,s.factory_electricity, s.last_update_time
            FROM sum_table5 s JOIN df ON (s.year = df.year AND s.month = df.month AND s.plant = df.plant)
        """)

    # 若資料庫已有其他基線數據則將含有新的資料覆蓋過去；若無則甚麼事也不做
    if df_.shape[0] != 0:
        output = df[(df['datetime'].astype(str)!=df_['datetime'][0])].append(df_).reset_index(drop=True)
        output = output.drop_duplicates().reset_index(drop = True)
        del output['year']
        del output['month']
        
        conn.execute(f'TRUNCATE TABLE app.baseline_data_overview')
        output.to_sql(name='baseline_data_overview',schema='app',if_exists='append',index=False,con=conn)

    else:
        output = df.append(sum_table5).reset_index(drop=True)
        output = output.drop_duplicates().reset_index(drop = True)
        del output['year']
        del output['month']
        
        conn.execute(f'TRUNCATE TABLE app.baseline_data_overview')
        output.to_sql(name='baseline_data_overview',schema='app',if_exists='append',index=False,con=conn)
    
    return True
        
    # except:
    #     # logging.basicConfig(level=logging.INFO)
    #     # logger = logging.getLogger(__name__)
    #     # logger.info('\n'+'='*127) 
    #     logging.exception('Catch an exception.', exc_info=True)


    #     return False









