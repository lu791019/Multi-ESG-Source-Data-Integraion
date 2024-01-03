import argparse
import json
import numpy as np
import os
import pandas as pd
import psycopg2
import urllib.request
import requests
from datetime import datetime
from azureml.core.model import InferenceConfig
from azureml.core.webservice import AksWebservice, Webservice
from azureml.core.compute import AksCompute
from azureml.core.environment import Environment
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import Workspace, Run, Dataset, Model as azmodel
from sqlalchemy import create_engine
# from models import engine

run  = Run.get_context()
ws   = run.experiment.workspace
def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--power_type_seq", type=str)
    parser.add_argument("--best_model", type=str)
    parser.add_argument("--connect_string", type=str)
    args = parser.parse_args()
    return args

def extract_plantname(dataset):
    plant_name = Dataset.get_by_name(workspace=ws, name=dataset).download()
    if len(plant_name)>0:
        plant_name = [x for x in plant_name if dataset.replace('-','_') in x]
    tmp_open = open(plant_name[0], 'r')
    tmp = tmp_open.read()
    tmp_open.close()
    tmp = json.loads(tmp)
    print('type of plant names: ',type(tmp),tmp)
    return tmp

def baseline_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    if sql == "SELECT * FROM app.baseline_data_overview;":
        data_result = Dataset.get_by_name(workspace=ws, name='ecossot-dataset')
        data_result = data_result.to_pandas_dataframe().drop_duplicates().reset_index(drop=True)
        data_result['datetime'] = pd.to_datetime(data_result['datetime'])
        data_result['pcba_qty'] = data_result['pcba_qty'].astype('float')
        data_result['fa_qty'] = data_result['fa_qty'].astype('float')
        data_result['pcba_lines'] = data_result['pcba_lines'].astype('float')
        data_result['fa_lines'] = data_result['fa_lines'].astype('float')
        data_result['revenue'] = data_result['revenue'].astype('float')
        data_result['average_temperature'] = data_result['average_temperature'].astype('float')
        data_result['member_counts'] = data_result['member_counts'].astype('float')
        data_result['factory_electricity'] = data_result['factory_electricity'].astype('float')
        data_result['ac_electricity'] = data_result['ac_electricity'].astype('float')
        data_result['ap_electricity'] = data_result['ap_electricity'].astype('float')
        data_result['production_electricity'] = data_result['production_electricity'].astype('float')
        data_result['base_electricity'] = data_result['base_electricity'].astype('float')
        data_result['dorm_electricity'] = data_result['dorm_electricity'].astype('float')
        data_result = data_result.loc[(~data_result.factory_electricity.isna()),:].reset_index(drop=True)
    else:
        data_result = pd.read_sql(sql,con=conn)

    # close the communication with the PostgreSQL
    # cur.close()

    data_result_history = data_result[data_result.columns[np.where([x.find('id')==-1 for x in data_result.columns])]]
    data_result_history.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                                   'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                                   '營業額（十億NTD）','外氣平均溫度（℃）','plant','site','last_update_time']
    return data_result, data_result_history

def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null',np.nan)
    for x in dataset.columns:
        if x in ['datetime','plant','bo','日期','site','power_type','predict_type']:
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
    if sql == "SELECT * FROM app.baseline_data_overview;":
        data_result = Dataset.get_by_name(workspace=ws, name='ecossot-dataset')
        data_result = data_result.to_pandas_dataframe().drop_duplicates().reset_index(drop=True)
        data_result['datetime'] = pd.to_datetime(data_result['datetime'])
        data_result['pcba_qty'] = data_result['pcba_qty'].astype('float')
        data_result['fa_qty'] = data_result['fa_qty'].astype('float')
        data_result['pcba_lines'] = data_result['pcba_lines'].astype('float')
        data_result['fa_lines'] = data_result['fa_lines'].astype('float')
        data_result['revenue'] = data_result['revenue'].astype('float')
        data_result['average_temperature'] = data_result['average_temperature'].astype('float')
        data_result['member_counts'] = data_result['member_counts'].astype('float')
        data_result['factory_electricity'] = data_result['factory_electricity'].astype('float')
        data_result['ac_electricity'] = data_result['ac_electricity'].astype('float')
        data_result['ap_electricity'] = data_result['ap_electricity'].astype('float')
        data_result['production_electricity'] = data_result['production_electricity'].astype('float')
        data_result['base_electricity'] = data_result['base_electricity'].astype('float')
        data_result['dorm_electricity'] = data_result['dorm_electricity'].astype('float')
        data_result = data_result.loc[(~data_result.factory_electricity.isna()),:].reset_index(drop=True)
    else:
        data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

def model_api_caller(data, url, api_key):
    payload = json.dumps(data)
    # print(payload)
    # payload = json.loads(data)
    headers = {
    'Authorization': 'Bearer ' + api_key,
    'Content-Type': 'application/json'
    # 'Content-Type': 'text/plain'
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
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
    # connect_string = engine.get_connect_string()
    args = parse_args()
    conn = create_engine(args.connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    # connect_string = engine.get_connect_string()
    conn = create_engine(args.connect_string, echo=True)
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
    print(power_green_total.two_month_predict)
    print(power_green_total.ten_month_real)
    power_green_total['rec_year']=power_green_total.two_month_predict.astype(float)+power_green_total.ten_month_real.astype(float)
    power_green_total = power_green_total[['plant','year','rec_year']]
    power_green_total['month'] = '11'
    power_predict_data_new = pd.merge(power_predict_data.copy(deep=True),power_green_total,on=['plant','year','month'],how='left')
    power_predict_data_new['rec']=np.where(power_predict_data_new.rec.isna(),power_predict_data_new.rec_year, power_predict_data_new.rec)
    power_predict_data_new = power_predict_data_new.drop(columns={'rec_year'})
    return power_predict_data_new

def prediction_main():
    ############### Generate Inputs #####################
    args = parse_args()
    def_blob_store = ws.get_default_datastore()
    # stage = 'dev'
    # host0 = "10.37.37.211"
    # if stage=='dev':
    #     port0 = "15063"
    # elif stage=='qas':
    #     port0 = "15118"
    # else:
    #     port0 = "15068"
    # database0 = "postgres"
    # user0 = ""
    # password0 = ""
    
    print("Found workspace {} at location {}".format(ws.name, ws.location))

    # Connect to DB to download data
    # conn = psycopg2.connect(host=host0, port=port0, database=database0, 
    #                         user=user0, password=password0)
    # connect_string = engine.get_connect_string()
    conn = create_engine(args.connect_string, echo=True)
    load_baseline_data_query = "SELECT * FROM app.baseline_data_overview;"
    load_predict_data_query = "SELECT * FROM app.predict_baseline_data;"
    data_result, data_result_history = baseline_data_loader(conn, load_baseline_data_query)
    data_result = pd.merge(data_result,
                           data_result.loc[(~data_result.ac_electricity.isna()) & (~data_result.revenue.isna()) & (~data_result.pcba_qty.isna()),:].groupby(['plant','bo']).agg({'datetime':'max'}).reset_index().rename(columns={'datetime':'datetime_max'}),
                           on=['plant','bo'], how='left')
    # Adjust plant name
    data_result = data_result.loc[((data_result['plant'].isin(['WIH','WCQ-1'])) & (data_result['datetime'].astype(str)>'2022-02-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ'])) & (data_result['datetime'].astype(str)>'2022-02-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((~data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ','WIH','WCQ-1'])) & (data_result['datetime'].astype(str)>'2022-02-01') & (data_result['datetime']<=data_result['datetime_max'])),:].reset_index(drop=True)

    data_result['predict_type'] = np.where(args.power_type_seq=='工廠用電（kwh）','predict','baseline')
    data_result = data_result.astype('string')
    data_result_history = data_result_history.astype('string')
    data_result = data_result.drop(columns={'last_update_time','datetime_max'})
    data_result_history = data_result_history.drop(columns={'last_update_time'})
    predict_data_result = data_loader(conn, load_predict_data_query)
    predict_data_result['predict_type'] = np.where(args.power_type_seq=='工廠用電（kwh）','predict','baseline')
    predict_data_result.plant = np.where(predict_data_result.plant=='WCQ-1','WCQ',predict_data_result.plant)
    predict_data_result = predict_data_result.astype('string')
    predict_data_result = predict_data_result.loc[~(((predict_data_result.plant=='WZS-3') | (predict_data_result.plant=='WTZ')) & (predict_data_result.datetime>'2022-09-01')),:].reset_index(drop=True) # fix me
    # power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
    #                           np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
    #                                   np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
    #                                            np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    
    data_upload_final_retrain = pd.DataFrame({})
    print(args.power_type_seq)
    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac-electricity',
                                np.where(args.power_type_seq=='空壓用電（kwh）','ap-electricity',
                                    np.where(args.power_type_seq=='生產用電（kwh）','production-electricity',
                                        np.where(args.power_type_seq=='基礎用電（kwh）','base-electricity','predict-electricity')))))
    if power_predict_name=='predict-electricity':
        plant_name_list = ['WCQ','WIH','WKS-5','WKS-6','WZS-1','WZS-3','WZS-6','WZS-8','WCD','WTZ','WOK']
    else:
        plant_name_list = extract_plantname(f"{power_predict_name}_retrain_plant_list".replace('_','-'))
    # Load noretrain data
    print(f"{power_predict_name}_noretrain_data".replace('_','-'))
    data_upload_final_noretrain = extract_plantname(f"{power_predict_name}_noretrain_data".replace('_','-'))
    data_upload_final_noretrain = pd.DataFrame.from_dict(data_upload_final_noretrain)
    print(data_upload_final_noretrain)
    
    power_type = args.power_type_seq
    power_type_name = str(np.where(power_type=='空調用電（kwh）','ac',
                              np.where(power_type=='空壓用電（kwh）','ap',
                                      np.where(power_type=='生產用電（kwh）','prod',
                                               np.where(power_type=='基礎用電（kwh）','base','pred')))))
    # plant_name_list = ['WCD']
    if len(plant_name_list)>0:
        for x in plant_name_list:
            # for power_type in args.power_type_seq:
            print(x+'+'+str(power_type))
            service = Webservice(workspace=ws, name=x.lower()+"-"+power_type_name+"-elec-api") # fix me
            scoring_uri = service.scoring_uri
            key, _ = service.get_keys()
            print(scoring_uri)
            print(key)
            data_result['power_type'] = str(power_type)
            predict_data_result['power_type'] = str(power_type)
            data_result_json =  json.loads(data_result.fillna('null').loc[data_result['plant'].isin([x]),:].reset_index(drop=True).to_json(orient="records"))
            data_result_history_json = json.loads(data_result_history.fillna('null').loc[data_result_history['plant'].isin([x]),:].reset_index(drop=True).to_json(orient="records"))
            predict_data_result_json = json.loads(predict_data_result.fillna('null').loc[predict_data_result['plant'].isin([x]),:].reset_index(drop=True).to_json(orient="records"))
            # best_performance_json = json.loads(best_performance.fillna('null').loc[best_performance['Plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))

            input_json = {
                "data_result": data_result_json,
                "data_result_history":data_result_history_json,
                "predict_data_result":predict_data_result_json
            }
            ############### Call API #####################
            outputs = model_api_caller(input_json, scoring_uri, key)
            print(outputs)
            request = json.loads(outputs)
            data_upload_final_sub = data_type_checker(request['data_upload_final'])
            if x=='WKS-6':
                data_upload_final_sub['bo'] = 'WSD' 
            print(data_upload_final_sub)
            data_upload_final_retrain = data_upload_final_retrain.append(data_upload_final_sub).reset_index(drop=True)
        print('Retrain')
        print(data_upload_final_retrain)
        print('NoRetrain')
        print(data_upload_final_noretrain)
        if power_type_name=='pred':
            data_upload_final = data_upload_final_retrain
        else:
            data_upload_final = data_upload_final_retrain.append(data_upload_final_noretrain).reset_index(drop=True)
    else:
        data_upload_final = data_upload_final_noretrain
    
    if power_type!='工廠用電（kwh）':
        # 註冊 test prediction table 
        print('write test data')
        with open(f"{power_type_name}_test_prediction.txt", "w") as savedf:
            json.dump(data_upload_final.to_dict(), savedf)
        data_path = os.path.join("test-prediction",f"{datetime.now().strftime('%Y-%m-%d')}")
        def_blob_store.upload_files(files=[f"{power_type_name}_test_prediction.txt"], target_path=data_path, overwrite=True)
        registered_data = Dataset.File.from_files(def_blob_store.path(data_path,f"{power_type_name}_test_prediction.txt"),validate=False)\
                                            .register(ws, (f"{power_type_name}-test-prediction").replace('_','-'), create_new_version=True)
        
    else:
        #load best table
        for power_type_sub in ['ac','ap','prod','base']:
            best_performance = extract_plantname(f"{power_type_sub}_test_prediction".replace('_','-'))
            best_performance = pd.DataFrame.from_dict(best_performance)
            best_performance['month']=[x[5:7] for x in best_performance.datetime]
            best_performance['year']=[x[0:4] for x in best_performance.datetime]
            if power_type_sub=='ac':
                data_upload_final = pd.merge(data_upload_final[['plant','bo','year','month','datetime']].append(best_performance[['plant','bo','year','month','datetime']]).drop_duplicates().reset_index(drop=True),
                                             data_upload_final,on=['plant','bo','year','month','datetime'],how='left').reset_index(drop=True)
            data_upload_final = pd.merge(data_upload_final,best_performance,on=['plant','bo','year','month','datetime'],how='left')
        # data_uploader(user0,password0,host0,port0,database0,data_upload_final,'app','predict_history')
        print(data_upload_final)
        print('Upload predict power baseline is successful!')

        ############### Compute energy rec #####################
        # Compute all year rec prediction
        print('Compute green rec is start!')
        power_real_data_query = "SELECT * FROM app.baseline_data_overview;"
        power_predict_data_query = "SELECT * FROM app.predict_history;"
        print('Load green rec is start!')
        power_real_data = data_loader(conn, power_real_data_query)
        # power_predict_data = data_loader(conn, power_predict_data_query)
        data_upload_final.year = data_upload_final.year.astype('str')
        data_upload_final.month = data_upload_final.month.astype('str')
        data_upload_final.predict_electricity = data_upload_final.predict_electricity.astype(float)
        power_predict_data = data_upload_final
        print('Load green rec is end!')
        # Compute real 10 months and prediction 2 months
        print('Compute rec is start!')
        power_ten_month_total_final = real_power_computer(power_real_data)
        power_predict_data_new = prediction_rec_computer(power_ten_month_total_final, power_predict_data)
        print('Compute rec is end!')
        
        with open(f"{power_type_name}_test_prediction.txt", "w") as savedf:
            json.dump(power_predict_data_new.to_dict(), savedf)
        data_path = os.path.join("test-prediction",f"{datetime.now().strftime('%Y-%m-%d')}")
        def_blob_store.upload_files(files=[f"{power_type_name}_test_prediction.txt"], target_path=data_path, overwrite=True)
        registered_data = Dataset.File.from_files(def_blob_store.path(data_path,f"{power_type_name}_test_prediction.txt"),validate=False)\
                                            .register(ws, (f"{power_type_name}-test-prediction").replace('_','-'), create_new_version=True)
        
        ############### Upload dataset #####################
        # data_uploader(power_predict_data_new,'app','predict_history')
        print('Upload predict rec energy is successful!')
    
if __name__ == "__main__":
    print('Predicting!')
    prediction_main()
