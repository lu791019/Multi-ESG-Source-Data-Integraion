import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pandasql import sqldf
from sqlalchemy import create_engine
from models import engine
import warnings
warnings.filterwarnings("ignore")

# function setting
def plant_transf(plant_code,mapping):
    try:
        plant = mapping[mapping['plant_code']==plant_code]['plant_name'].reset_index(drop=True)[0]
    except:
        plant = plant_code
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
# "布爾諾";"693" (捷克)
# "華雷斯";"1278" (墨西哥)
# "河內";"308" (越南)
# https://worldweather.wmo.int/tc/json/full_city_list.txt 地址編號一覽表
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
        last_month_weather['plant'] = 'WCD'
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


# main

def catch_weather_info():

    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)

    # 對應需要維護的plant
    plant_mapping = pd.read_sql('SELECT * FROM raw.plant_mapping',con=conn)
    plant_mapping.sort_values(['bo','site'])

    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WZS','WZS-1','P1',datetime.now()]
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WZS','WZS-3','P3',datetime.now()]
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WT','WZS','WZS-6','P6',datetime.now()]
    plant_mapping.loc[len(plant_mapping),['bo','site', 'plant_name','plant_code','last_update_time']]=['WSD','WZS','WZS-8','P8',datetime.now()]

    # get weather api
    last_month_temp = pd.DataFrame()
    for city_number in [547,349,241,240,350,308,354,693,1278,88,523]:
        last_month_temp = last_month_temp.append(get_city_weather(city_number))

    last_month_temp = last_month_temp.reset_index(drop = True)

    last_month_temp = last_month_temp.rename(columns={'meanTemp':'average_temperature'})
    last_month_temp['datetime'] = last_month_temp['year'].astype(str)+'-'+last_month_temp['month'].astype(str).str.zfill(2)
    last_month_temp['datetime'] = last_month_temp['datetime'].map(lambda x : datetime.strptime(x,'%Y-%m')).astype(str).str.slice(0,10)
    last_month_temp['bo'] = last_month_temp['plant'].map(lambda x : bo_trans(x,plant_mapping))

    last_month_temp['last_update_time'] = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    last_month_temp = last_month_temp[['datetime', 'average_temperature', 'plant', 'bo', 'last_update_time' ]]

    # upload data into db
    df = pd.read_sql('SELECT * FROM app.catch_weather_info',con=conn)
    output = df[(df['datetime'].astype(str)!=last_month_temp['datetime'][0])].append(last_month_temp).reset_index(drop=True)
    conn.execute(f'TRUNCATE TABLE app.catch_weather_info;')
    output.to_sql(name='catch_weather_info',schema='app',if_exists='append',index=False,con=conn)

    return True


