#!/usr/bin/python
import time
import os
import json
# from azureml.core import Workspace, Dataset
# from azureml.core.authentication import ServicePrincipalAuthentication
# from inference_schema.schema_decorators import input_schema, output_schema
# from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
# from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
# from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
import psycopg2
import pandas as pd
from datetime import datetime
import math
import numpy as np
import pickle
from sqlalchemy import create_engine
# import xgboost as xgb
import urllib.request
import json
import requests
import ssl
from services.mail_service import MailService
from models import engine

def baseline_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)

    # close the communication with the PostgreSQL
    # cur.close()

    data_result_history = data_result[data_result.columns[np.where([x.find('id')==-1 for x in data_result.columns])]]
    data_result_history.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                                   'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                                   '營業額（十億NTD）','外氣平均溫度（℃）','plant','site','last_update_time']
    return data_result, data_result_history

def predict_baseline_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)

    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null',np.nan)
    for x in dataset.columns:
        if x in ['datetime','plant','bo','日期','site']:
            dataset[x] = dataset[x].astype('string')
        elif x in ['year','month']:
            dataset[x] = dataset[x].astype('int')
        else:
            dataset[x] = dataset[x].astype('float')
    return dataset

def data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

def model_api_caller(data, url, api_key):
    payload = json.dumps(data)
    headers = {
    'Authorization': 'Bearer ' + api_key,
    'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, data=payload, verify=False)
        result = response.text
        # print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))
    return result

def data_uploader(data, db_name, table_name):
    # Truncate table
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name,conn,index= False, if_exists = 'append',schema=db_name, chunksize = 10000)
    return 0

def real_power_computer(power_real_data):
    # modify column names
    power_real_data = power_real_data[power_real_data.columns[np.where([x.find('id')==-1 for x in power_real_data.columns])]]
    power_real_data.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                               'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                               '營業額（十億NTD）','外氣平均溫度（℃）','plant','site','last_update_time']
    power_ten_month_total = power_real_data.loc[[power_real_data['日期'].astype(str)[x][5:7]<'11' for x in range(power_real_data.shape[0])],:].reset_index(drop=True)
    power_ten_month_total['year'] = [power_ten_month_total['日期'].astype(str)[x][0:4] for x in range(power_ten_month_total.shape[0])]
    power_ten_month_total = power_ten_month_total.groupby(['plant','site','year'], group_keys=True).agg({'工廠用電（kwh）':'sum','宿舍用電（kwh）':'sum','日期':'size'}).reset_index().rename(columns={'日期':'month_count'})
    # Compute total power
    power_ten_month_total['ten_month_real'] = power_ten_month_total['工廠用電（kwh）']+12*power_ten_month_total['宿舍用電（kwh）']/power_ten_month_total['month_count']
    power_ten_month_total_final = power_ten_month_total.loc[power_ten_month_total.month_count==10,:]
    return power_ten_month_total_final

def prediction_rec_computer(power_ten_month_total_final, power_predict_data):
    # filter specific month data
    power_predict_data_two_month = power_predict_data.loc[(power_predict_data.month>='11') & (~power_predict_data.predict_electricity.isna()),:]
    power_predict_data_two_month = power_predict_data_two_month.groupby(['plant','year'], group_keys=True).agg({'predict_electricity':'sum'}).reset_index().rename(columns={'predict_electricity':'two_month_predict'})
    power_green_total = pd.merge(power_predict_data_two_month,power_ten_month_total_final,on=['plant','year'],how='left')
    power_green_total['rec_year']=power_green_total.two_month_predict+power_green_total.ten_month_real
    power_green_total = power_green_total[['plant','year','rec_year']]
    power_green_total['month'] = '11'
    power_predict_data_new = pd.merge(power_predict_data.copy(deep=True),power_green_total,on=['plant','year','month'],how='left')
    power_predict_data_new['rec']=np.where(power_predict_data_new.rec.isna(),power_predict_data_new.rec_year, power_predict_data_new.rec)
    power_predict_data_new = power_predict_data_new.drop(columns={'rec_year'})
    return power_predict_data_new

def power_baseline_main_fn(stage):
    print('power_baseline_main_fn start')
    if datetime.now().strftime('%Y-%m-%d')[8:10]=='10':
        try:
            ############### Generate Inputs #####################
            print('Upload predict power baseline is start!')
            connect_string = engine.get_connect_string()
            conn = create_engine(connect_string, echo=True)
            url0 = 'http://10.30.80.134:80/api/v1/service/wtz-ele-baseline-prd/score'
            api_key0 = '2WYJmOhDZE4BFvUeCr1YoHsTbEf3bmSb'
            # Connect to DB to download data
            print('Download power baseline from DB.')
            # conn = psycopg2.connect(host=host0, port=port0, database=database0, 
            #                         user=user0, password=password0)
            load_baseline_data_query = "SELECT * FROM app.baseline_data_overview;"
            load_predict_data_query = "SELECT * FROM app.predict_baseline_data;"
            data_result, data_result_history = baseline_data_loader(conn, load_baseline_data_query)
            data_result = pd.merge(data_result,
                       data_result.loc[(~data_result.ac_electricity.isna()) & (~data_result.revenue.isna()) & (~data_result.pcba_qty.isna()),:].groupby(['plant','bo']).agg({'datetime':'max'}).reset_index().rename(columns={'datetime':'datetime_max'}),
                       on=['plant','bo'], how='left')
            # Adjust plant name
            data_result.plant = np.where(data_result.plant=='WCQ','WCQ-1',data_result.plant)
            data_result_history.plant = np.where(data_result_history.plant=='WCQ','WCQ-1',data_result_history.plant)
            # data_result = data_result.loc[((data_result['plant'].isin(['WKS-5','WKS-6B'])) & (data_result['datetime'].astype(str)>'2020-09-01')) | (~data_result['plant'].isin(['WKS-5','WKS-6B'])),:].reset_index(drop=True)
            # data_result = data_result.loc[((data_result['plant'].isin(['WIH','WCQ-1'])) & (data_result['datetime'].astype(str)>'2020-12-01') & (data_result['datetime'].astype(str)<='2022-04-01')) | ((data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ'])) & (data_result['datetime'].astype(str)>'2020-09-01') & (data_result['datetime'].astype(str)<='2022-05-01')) | ((~data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ','WIH','WCQ-1'])) & (data_result['datetime'].astype(str)<='2022-04-01')),:].reset_index(drop=True)
            data_result = data_result.loc[((data_result['plant'].isin(['WIH','WCQ-1'])) & (data_result['datetime'].astype(str)>'2020-12-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ'])) & (data_result['datetime'].astype(str)>'2020-09-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((~data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ','WIH','WCQ-1'])) & (data_result['datetime']<=data_result['datetime_max'])),:].reset_index(drop=True)
            # data_result['datetime'] = data_result['datetime'].astype('string')
            data_result = data_result.astype('string')
            # data_result_history['日期'] = data_result_history['日期'].astype('string')
            data_result_history = data_result_history.astype('string')
            data_result = data_result.drop(columns={'last_update_time','datetime_max'})
            data_result_history = data_result_history.drop(columns={'last_update_time'})
            print('Download power baseline from DB is end.')
            print('Download predict power baseline from DB.')
            connect_string = engine.get_connect_string()
            conn = create_engine(connect_string, echo=True)
            predict_data_result = predict_baseline_data_loader(conn, load_predict_data_query)
            predict_data_result.plant = np.where(predict_data_result.plant=='WCQ','WCQ-1',predict_data_result.plant)
            # predict_data_result['datetime'] = predict_data_result['datetime'].astype('string')
            predict_data_result = predict_data_result.astype('string')
            # data_result['json'] = data_result.to_json(orient='records', lines=True).splitlines()
            print('Download predict power baseline from DB is end.')
            # Generate Input json by batch
            print('Send predict power baseline to API by batch.')
            plant_batch = [['WTZ','WOK'],['WZS-1','WZS-3'],['WZS-6','WZS-8'],['WCD','WKS-5'],['WKS-6','WCQ-1'],['WIH']]
            data_upload_final = pd.DataFrame({})
            for x in plant_batch:
                print(x)
                data_result_json =  json.loads(data_result.fillna('null').loc[data_result['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))
                data_result_history_json = json.loads(data_result_history.fillna('null').loc[data_result_history['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))
                predict_data_result_json = json.loads(predict_data_result.fillna('null').loc[predict_data_result['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))

                input_json = {
                    "data_result": data_result_json,
                    "data_result_history":data_result_history_json,
                    "predict_data_result":predict_data_result_json
                }
                ############### Call API #####################
                outputs = model_api_caller(input_json, url0, api_key0)
                # outputs
                # input_json
                # print(inputs)
                request = json.loads(outputs)
                data_upload_final_sub = data_type_checker(request['data_upload_final'])
                data_upload_final = data_upload_final.append(data_upload_final_sub).reset_index(drop=True)
            # Adjust plant name
            data_upload_final.plant = np.where(data_upload_final.plant=='WCQ-1','WCQ',data_upload_final.plant)
            print('Get predict power baseline from API.')
            ############### Upload dataset #####################
            # data_uploader(data_upload_final,'app','predict_history')
            print('Upload predict power baseline is successful!')

            ############### Compute energy rec #####################
            # Compute all year rec prediction
            print('Compute green rec is start!')
            power_real_data_query = "SELECT * FROM app.baseline_data_overview;"
            power_predict_data_query = "SELECT * FROM app.predict_history;"
            print('Load green rec is start!')
            connect_string = engine.get_connect_string()
            conn = create_engine(connect_string, echo=True)
            power_real_data = data_loader(conn, power_real_data_query)
            # power_predict_data = data_loader(conn, power_predict_data_query)
            data_upload_final.year = data_upload_final.year.astype('str')
            data_upload_final.month = data_upload_final.month.astype('str')
            power_predict_data = data_upload_final
            print('Load green rec is end!')
            # Compute real 10 months and prediction 2 months
            print('Compute rec is start!')
            power_ten_month_total_final = real_power_computer(power_real_data)
            power_predict_data_new = prediction_rec_computer(power_ten_month_total_final, power_predict_data)
            print('Compute rec is end!')
            ############### Upload dataset #####################
            data_uploader(power_predict_data_new,'app','predict_history')
            print('Upload predict rec energy is successful!')
            return 0
        except Exception as e:
            error = str(e)
            mail = MailService('[failed][{}] baseline data cron job report'.format(stage))
            mail.send('failed: {}'.format(error))
            return error
    else:
        return 0
    print('power_baseline_main_fn end')
