import argparse
import json
import numpy as np
import os
import psycopg2
import pandas as pd
import pickle
from azureml.core import Workspace, Experiment, Dataset,Run
from datetime import datetime
from os.path import join
from sqlalchemy import create_engine
# from models import engine

run  = Run.get_context()
ws   = run.experiment.workspace

def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--test_input", type=str)
    parser.add_argument("--power_type_seq", type=str)
    parser.add_argument("--connect_string", type=str)
    args = parser.parse_args()
    return args

def baseline_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    # data_result = pd.read_sql(sql,con=conn)
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
    # cur.close()
    return data_result

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
    data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

def data_uploader(user0, password0, host0, port0, 
                  database0, data, db_name, table_name):
    # Truncate table
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name,conn,index= False, if_exists = 'append',schema=db_name, chunksize = 10000)
    return 0


def upload_main():
    """
    需要上傳測試資料與上一期模型最佳表現的csv & 模型檔案
    """
    def_blob_store = ws.get_default_datastore() 
    args = parse_args()
    
    
#     data = pd.DataFrame({'dev_prd':['dev']})
#     if data["dev_prd"][0]=='dev':
#         host0 = "10.37.37.211"
#         port0 = "15063"
#     elif data["dev_prd"][0]=='qas':
#         host0 = "10.37.37.211" 
#         port0 = "15118"
#     else:
#         host0 = "10.37.37.211" 
#         port0 = "15068"

#     database0 = "postgres"
#     user0 = ""
#     password0 = ""

#     conn = psycopg2.connect(host=host0, port=port0, database=database0, 
#                             user=user0, password=password0)
    # connect_string = engine.get_connect_string()
    conn = create_engine(args.connect_string, echo=True)
    load_baseline_data_query = "SELECT * FROM app.baseline_data_overview;"
    load_predict_data_query = "SELECT * FROM app.predict_baseline_data;"
    data_result, data_result_history = baseline_data_loader(conn, load_baseline_data_query)
    data_result = pd.merge(data_result,
                           data_result.loc[((~data_result.ac_electricity.isna()) & (~data_result.revenue.isna()) & (~data_result.pcba_qty.isna())) & (data_result.datetime.astype(str)<'2023-03-01'),:].groupby(['plant','bo']).agg({'datetime':'max'}).reset_index().rename(columns={'datetime':'datetime_max'}),
                           on=['plant','bo'], how='left')
    print(data_result)
    data_result = data_result.loc[((data_result['plant'].isin(['WIH','WCQ'])) & (data_result['datetime'].astype(str)>'2020-12-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ'])) & (data_result['datetime'].astype(str)>'2020-09-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((~data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ','WIH','WCQ'])) & (data_result['datetime']<=data_result['datetime_max'])),:].reset_index(drop=True)
    
    data_result['predict_type'] = 'baseline'
    data_result = data_result.astype('string')
    data_result_history = data_result_history.astype('string')
    data_result = data_result.drop(columns={'last_update_time','datetime_max'})
    data_result_history = data_result_history.drop(columns={'last_update_time'})
    predict_data_result = predict_baseline_data_loader(conn, load_predict_data_query)
    predict_data_result['predict_type'] = 'predict'
    predict_data_result = predict_data_result.astype('string')
    
    
    for plant in ['WCQ','WIH','WKS-5','WKS-6','WZS-1','WZS-3','WZS-6','WZS-8','WCD','WTZ','WOK']: #fix me:要填入所有廠區
        print(plant)
        x = [plant]
        y = args.power_type_seq
        data_result['power_type'] = y
        predict_data_result['power_type'] = y
        data_result_json =  json.loads(data_result.fillna('null').loc[data_result['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))
        data_result_history_json = json.loads(data_result_history.fillna('null').loc[data_result_history['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))
        predict_data_result_json = json.loads(predict_data_result.fillna('null').loc[predict_data_result['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))

        # note: 先跑過 baseline pipeline後，input_json就不用再attach best_performance_table
        input_json = {
            "data_result": data_result_json,
            "data_result_history":data_result_history_json,
            "predict_data_result":predict_data_result_json
        }
        print('input_json:\n',input_json)
        power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                      np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                              np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                                       np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
        os.makedirs(args.test_input, exist_ok=True)
        with open(args.test_input+'/'+ f"{plant}_{power_predict_name}_inputs.txt", "w", encoding='utf-8') as savedf:
            json.dump(input_json, savedf,ensure_ascii=False)
                
    
if __name__ == "__main__":
    print('Uploading!')
    upload_main()
                                                    

