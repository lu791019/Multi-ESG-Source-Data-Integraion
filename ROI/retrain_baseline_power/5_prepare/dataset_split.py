import argparse
import datetime
import json
import numpy as np
import os
import pandas as pd
import pickle
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import Workspace, Experiment, Dataset,Run
from datetime import datetime as from_datetime
from sklearn.svm import SVR

run  = Run.get_context()
ws   = run.experiment.workspace
def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--power_type_seq", type=str)
    parser.add_argument("--raw_data", type=str)
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

# Generate train, valid, test data
def data_spliter(power_raw_data, power_test_data, power_type,train_prop):
    # Generate train, valid, test data
    col_unique, col_counts = np.unique(np.array(power_raw_data.columns.tolist()+power_test_data.columns.tolist()),return_counts=True)
    forcast_name = col_unique[np.where((col_counts==2) & (col_unique!='日期'))]
    feature_name = np.append(forcast_name,[power_type]+['temp_cutoff']) 
    train_data = power_raw_data.loc[:,feature_name]
    train_data['lag_1'] = np.append(np.array(-1),np.array(train_data[power_type][:-1]))
  
    if power_type=='空壓用電（kwh）':
        train_data['lag_1_air_condition'] = np.append(np.array(-1),np.array(power_raw_data['空調用電（kwh）'][:-1]))
    elif power_type=='生產用電（kwh）':
        # train_data['lag_1_air_condition'] = np.append(np.array(-1),np.array(power_raw_data['空調用電（kwh）'][:-1]))
        train_data['lag_1_air_compress'] = np.append(np.array(-1),np.array(power_raw_data['空壓用電（kwh）'][:-1]))
    train_data = train_data.loc[train_data.lag_1>0,:]
    valid_data = train_data.tail(train_data.shape[0]-round(train_data.shape[0]*train_prop)).reset_index(drop=True)
    train_data = train_data.head(round(train_data.shape[0]*train_prop)).reset_index(drop=True)
    test_data = power_test_data.loc[:,forcast_name]
 
    lag_1_time = [(datetime.datetime.strptime(x,'%Y-%m-%d') + pd.DateOffset(months=-1)).strftime('%Y-%m-%d') for x in power_test_data['日期']]
    test_data['lag_1'] = np.repeat(power_raw_data.loc[power_raw_data['日期'].isin(lag_1_time),:][power_type],2).reset_index(drop=True)
    if power_type=='空壓用電（kwh）':
        test_data['lag_1_air_condition'] = np.repeat(power_raw_data.loc[power_raw_data['日期'].isin(lag_1_time),:]['空調用電（kwh）'],2).reset_index(drop=True)
    elif power_type=='生產用電（kwh）':
        # test_data['lag_1_air_condition'] = np.repeat(power_raw_data.loc[power_raw_data['日期'].isin(lag_1_time),:]['空調用電（kwh）'],2).reset_index(drop=True)
        test_data['lag_1_air_compress'] = np.repeat(power_raw_data.loc[power_raw_data['日期'].isin(lag_1_time),:]['空壓用電（kwh）'],2).reset_index(drop=True)
    # Generate interaction term
    train_data_interact = train_data.iloc[:,np.where(train_data.columns!=power_type)[0]]
    for x in train_data_interact.columns:
        for y in train_data_interact.columns:
            if x!=y and x!= 'temp_cutoff' and y!= 'temp_cutoff': 
                train_data[x+'_'+y] = train_data[x]*train_data[y]
                valid_data[x+'_'+y] = valid_data[x]*valid_data[y]
                test_data[x+'_'+y] = test_data[x]*test_data[y]
    return train_data, valid_data, test_data, train_data_interact

def split_main():
    def_blob_store = ws.get_default_datastore() 
    args = parse_args()  
    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                  np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                          np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                                   np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    plant_name_list = extract_plantname(f"{power_predict_name}_retrain_plant_list".replace('_','-'))
    
    for plant in plant_name_list:
        power_raw_data = pd.read_csv(f"{args.raw_data}/{plant}_{power_predict_name}_power_raw_data.csv",encoding='utf-8-sig')
        power_test_data = pd.read_csv(f"{args.raw_data}/{plant}_{power_predict_name}_power_test_data.csv",encoding='utf-8-sig')
        if (plant=='WKS-6B') or (plant=='WIH'):
            train_prop = 0.85
        else:
            train_prop = 0.8

        train_data, valid_data, test_data, train_data_interact = data_spliter(power_raw_data, power_test_data, args.power_type_seq,train_prop)
        train_data.to_csv(f'{plant}_{power_predict_name}_train_data.csv',index=False,encoding='utf-8-sig')
        valid_data.to_csv(f'{plant}_{power_predict_name}_valid_data.csv',index=False,encoding='utf-8-sig')
        test_data.to_csv(f'{plant}_{power_predict_name}_test_data.csv',index=False,encoding='utf-8-sig')
        train_data_interact.to_csv(f'{plant}_{power_predict_name}_train_data_interact.csv',index=False,encoding='utf-8-sig')
        
        for data_type in ['train','valid','test','train_data_interact']:
            
            print('This is ',data_type)
            ## Upload data from local file #fix me args.plant->plant
            data_path = os.path.join(f"eco-{data_type.replace('_','-')}",f"{from_datetime.now().strftime('%Y-%m-%d')}",plant)# e.g. eco-train/2022-05-30/WIH
            raw_data_name = f"{plant}_{power_predict_name}_{data_type}_data.csv" if data_type != 'train_data_interact' else f"{plant}_{power_predict_name}_train_data_interact.csv"
            register_data_name = (f"{power_predict_name}-{plant}-{data_type}").replace('_','-')

            ## Upload data to blob
            def_blob_store.upload_files(files=[raw_data_name], target_path=data_path, overwrite=True)

            ## Register data in Dataset
            new_data = Dataset.Tabular.from_delimited_files(def_blob_store.path(os.path.join(data_path,raw_data_name)))\
                                              .register(ws, register_data_name,create_new_version=True)
            
            print(f"Data Registered - {plant} - {data_type}")
    

if __name__ == "__main__":
    split_main()

                                                    

