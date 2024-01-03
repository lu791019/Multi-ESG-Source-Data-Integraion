import argparse
import json
import numpy as np
import os
import pandas as pd
import pickle
import psycopg2
import re
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import Workspace, Experiment, Dataset,Run
from datetime import datetime
from sklearn.svm import SVR
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
    parser.add_argument("--raw_data", type=str)
    parser.add_argument("--connect_string", type=str)
    args = parser.parse_args()
    return args

def extract_plantname(dataset):
    plant_name = Dataset.get_by_name(workspace=ws, name=dataset).download()
    tmp_open = open(plant_name[0], 'r')
    tmp = tmp_open.read()
    tmp_open.close()
    tmp = json.loads(tmp)
    print('type of plant names: ',type(tmp),tmp)
    return tmp

def db_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

# Load Data
def data_loader(plant, conn, power_raw_data_query, power_test_data_query):
    # Load train data
    # power_raw_data = db_data_loader(conn, power_raw_data_query)
    
    dataset = Dataset.get_by_name(workspace=ws, name='ecossot-dataset')
    dataset = dataset.to_pandas_dataframe().drop_duplicates().reset_index(drop=True)
    dataset['datetime'] = pd.to_datetime(dataset['datetime'])
    dataset['pcba_qty'] = dataset['pcba_qty'].astype('float')
    dataset['fa_qty'] = dataset['fa_qty'].astype('float')
    dataset['pcba_lines'] = dataset['pcba_lines'].astype('float')
    dataset['fa_lines'] = dataset['fa_lines'].astype('float')
    dataset['revenue'] = dataset['revenue'].astype('float')
    dataset['average_temperature'] = dataset['average_temperature'].astype('float')
    dataset['member_counts'] = dataset['member_counts'].astype('float')
    dataset['factory_electricity'] = dataset['factory_electricity'].astype('float')
    dataset['ac_electricity'] = dataset['ac_electricity'].astype('float')
    dataset['ap_electricity'] = dataset['ap_electricity'].astype('float')
    dataset['production_electricity'] = dataset['production_electricity'].astype('float')
    dataset['base_electricity'] = dataset['base_electricity'].astype('float')
    dataset['dorm_electricity'] = dataset['dorm_electricity'].astype('float')
    dataset = dataset.loc[(~dataset.factory_electricity.isna()),:].reset_index(drop=True)
    
    if plant=='WCQ':
        power_raw_data = dataset.loc[(dataset.plant.isin([plant,plant+'-1','WKS-1'])),:].reset_index(drop=True)
    else:
        power_raw_data = dataset.loc[(dataset.plant.isin([plant])),:].reset_index(drop=True)
  
    power_raw_data.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                              'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                              '營業額（十億NTD）','外氣平均溫度（℃）','plant','bo','last_update_time']
    power_raw_data = power_raw_data.drop(['last_update_time'],axis=1)# fix me
    power_raw_data = power_raw_data.loc[power_raw_data['工廠用電（kwh）']!=0]
    power_raw_data = power_raw_data.sort_values(by='日期').reset_index(drop=True)
    if plant=='WOK':
        start_time = str(pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - pd.Timedelta(days=60))[0:10] # fix me 
    else:
        start_time = str(pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - pd.Timedelta(days=60))[0:10] # fix me
    if (plant=='WZS-1') or (plant=='WZS-3') or (plant=='WZS-6') or (plant=='WZS-8') or (plant=='WCD') or (plant=='WCQ') or (plant=='WIH'):
        power_raw_data = power_raw_data.drop(columns=['PCBA平均開線數（條）','FA平均開線數量（條）'])
    if (plant=='WIH') or (plant=='WCQ'):
        power_raw_data = power_raw_data.loc[(power_raw_data['日期'].astype('string')>='2021-01-01') & (power_raw_data['日期'].astype('string')<=start_time)]
    elif (plant=='WKS-5') or (plant=='WKS-6') or (plant=='WTZ') or (plant=='WOK'):
        power_raw_data = power_raw_data.loc[(power_raw_data['日期'].astype('string')>='2020-12-01') & (power_raw_data['日期'].astype('string')<=start_time)]
    else:
        power_raw_data = power_raw_data.loc[(power_raw_data['日期'].astype('string')>='2020-12-01') & (power_raw_data['日期'].astype('string')<=start_time)]
    power_raw_data.columns = [re.sub("綫","線", x) for x in power_raw_data.columns]
    
    # Load test data
    if (plant=='WCQ'):
        power_test_data_query = "SELECT * FROM app.predict_baseline_data where plant in ('"+plant+"','"+plant+"-1');"
    else:
        power_test_data_query = "SELECT * FROM app.predict_baseline_data where plant in ('"+plant+"');"
    power_test_data = db_data_loader(conn, power_test_data_query)
    power_test_data['plant'] = plant
    power_test_data.columns = ['日期','PCBA產量（pcs)','FA產量（pcs)','PCBA平均開線數（條）','FA平均開線數量（條）',
                               '營業額（十億NTD）','外氣平均溫度（℃）','plant','bo','人數（人）']
    power_test_data = power_test_data.loc[(power_test_data['日期'].astype('string')>='2021-11-01') & (power_test_data['日期'].astype('string')<='2022-09-01')] # fix me
    print('data_loader power_test_data:\n',power_test_data)
    return power_raw_data, power_test_data

# Remove NA and add new features
def data_processor(power_raw_data, power_test_data):
    # Remove NA rows
    # test_colnames = power_raw_data.columns[np.where([x.find('kwh')==-1 for x in power_raw_data.columns])]
    # power_test_data.columns = test_colnames[1:len(test_colnames)].insert(len(test_colnames),test_colnames[0])
    # power_test_data = power_test_data.loc[-power_test_data['PCBA產量（pcs)'].isna(),:]
    power_test_data = power_test_data.reset_index(drop=True)
    power_test_data.columns = [re.sub("綫","線", x) for x in power_test_data.columns]
    # Remove NA columns
    str_column_raw = np.where(power_raw_data.applymap(type).eq(str).any())[0]
    na_column_raw = np.where(power_raw_data.apply(lambda col: 1 if col.isnull().all() else 0, axis=0))[0]
    na_column_raw = np.append(na_column_raw,str_column_raw)
    power_raw_data = power_raw_data.drop(power_raw_data.columns[na_column_raw],axis=1)
    str_column_test = np.where(power_test_data.applymap(type).eq(str).any())[0]
    na_column_test = np.where(power_test_data.apply(lambda col: 1 if col.isnull().all() else 0, axis=0))[0]
    na_column_test = np.append(na_column_test,str_column_test)
    power_test_data = power_test_data.drop(power_test_data.columns[na_column_test],axis=1)
    print(power_test_data)
    # Add new features
    print(power_raw_data)
    # print(power_raw_data['外氣平均溫度（℃）'])
    temp_cutoff = power_raw_data['外氣平均溫度（℃）'].iloc[np.argmax(np.abs(np.diff(power_raw_data['空調用電（kwh）'],n=2)))+2]
    power_raw_data['temp_cutoff'] = temp_cutoff #fix me
    power_raw_data['temp_indicate'] = np.where(power_raw_data['外氣平均溫度（℃）']<=temp_cutoff,1,0)
    power_raw_data['日期'] = power_raw_data['日期'].astype('string')
    power_test_data['temp_indicate'] = np.where(power_test_data['外氣平均溫度（℃）']<=temp_cutoff,1,0)
    power_test_data['日期'] = power_test_data['日期'].astype('string')
    return power_raw_data, power_test_data, temp_cutoff

def query_main():
    def_blob_store = ws.get_default_datastore() 
    # host0 = '10.37.37.211'
    # database0 = "postgres"
    # user0 = ""
    # password0 = ""
    # port0 = "15063"
    # conn = psycopg2.connect(host=host0, 
    #                         port=port0, 
    #                         database=database0, 
    #                         user=user0,
    #                         password=password0)
    
    args = parse_args()
    # connect_string = engine.get_connect_string()
    conn = create_engine(args.connect_string, echo=True)
    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                  np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                          np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                                   np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    
    plant_name_list = extract_plantname(f"{power_predict_name}_retrain_plant_list".replace('_','-'))
    
    for plant in plant_name_list: 
        print(plant+'+'+power_predict_name)
        if (plant=='WKS-6B') or (plant=='WKS-6') or (plant=='WIH'):
            train_prop = 0.85
        else:
            train_prop = 0.8
        print('train_prop= ',train_prop)

        # Start querying
        if plant=='WCQ':
            print('select WCQ data')
            power_raw_data_query = "SELECT * FROM app.baseline_data_overview where plant in ('"+plant+"','"+plant+"-1','WKS-1');"
            power_test_data_query = "SELECT * FROM app.predict_baseline_data where plant in ('"+plant+"','"+plant+"-1','WKS-1');"
        else:
            print('select non WCQ data')
            power_raw_data_query = "SELECT * FROM app.baseline_data_overview where plant='"+plant+"';"
            power_test_data_query = "SELECT * FROM app.predict_baseline_data where plant='"+plant+"';"
        # Load raw data
        power_raw_data, power_test_data = data_loader(plant, conn, power_raw_data_query, power_test_data_query)
        if plant=='WCQ':
            print('go to WCQ')
            power_raw_data_sub1 = power_raw_data.loc[power_raw_data['日期'].astype('string')<='2021-09-01',:].groupby(['日期','bo'], group_keys=True).agg({'工廠用電（kwh）':'sum','空調用電（kwh）':'mean','空壓用電（kwh）':'sum','生產用電（kwh）':'sum','基礎用電（kwh）':'sum','宿舍用電（kwh）':'sum','PCBA產量（pcs)':'sum','FA產量（pcs)':'sum','人數（人）':'sum','營業額（十億NTD）':'sum','外氣平均溫度（℃）':'mean'}).reset_index() #'PCBA平均開線數（條）':'sum','FA平均開線數量（條）':'sum'
            power_raw_data_sub1['plant'] = plant
            power_raw_data_sub1['宿舍用電（kwh）'] = power_raw_data.loc[power_raw_data['日期'].astype('string')<='2021-09-01','宿舍用電（kwh）']
            power_raw_data_sub2 = power_raw_data.loc[(power_raw_data['日期'].astype('string')>'2021-09-01') & (power_raw_data['plant']==plant),:]
            power_raw_data_sub1 = power_raw_data_sub1.loc[:,power_raw_data.columns].append(power_raw_data_sub2)
            power_raw_data = power_raw_data_sub1

        # Data processor
        power_raw_data, power_test_data, temp_cutoff = data_processor(power_raw_data, power_test_data)
        
        os.makedirs(args.raw_data, exist_ok=True)
        power_raw_data.to_csv(f"{args.raw_data}/{plant}_{power_predict_name}_power_raw_data.csv",index=False,encoding='utf-8-sig')
        power_test_data.to_csv(f"{args.raw_data}/{plant}_{power_predict_name}_power_test_data.csv",index=False,encoding='utf-8-sig')
    

if __name__ == "__main__":
    query_main()

                                                    
