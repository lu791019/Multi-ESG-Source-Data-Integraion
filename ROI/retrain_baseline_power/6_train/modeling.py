import argparse
import logging
import numpy as np
import pickle
import pandas as pd
import re
import statsmodels.api as sm
import time
import xgboost as xgb

from azureml.core import Dataset, Run
from datetime import datetime
from numpy import arange
from opencensus.ext.azure.log_exporter import AzureLogHandler
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso, LassoCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler
from trainmodels import TrainModels
# # ---------------------------------------------------------------

print('-'*100)
parent_run  = Run.get_context().parent
run  = Run.get_context()
ws   = run.experiment.workspace
print('Modeling Run: ',run.id)
def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--k", type=int, default=2 )
    parser.add_argument("--adjust_month", type=int, default=6)
    parser.add_argument("--power_type_seq", type=str)
    parser.add_argument("--plant_name", type=str)
    parser.add_argument("--models", type=str)
    parser.add_argument("--best_model", type=str)

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
    
    
def modeling_main():
    args = parse_args() 
    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                    np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                        np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                            np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))

    plant_name_list = extract_plantname(f"{power_predict_name}_retrain_plant_list".replace('_','-')) #fix me
    # plant_name_list = ['WCD']
    for plant in plant_name_list:
        # import raw data from Data
        raw_train = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-train").to_pandas_dataframe()
        raw_valid = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-valid").to_pandas_dataframe()
        raw_test  = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-test").to_pandas_dataframe()
        raw_train_data_interact = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-train-data-interact").to_pandas_dataframe()
        os.makedirs(args.models, exist_ok=True)
        os.makedirs(args.best_model, exist_ok=True)

        temp_cutoff_value = raw_train.temp_cutoff.unique()[0]
        raw_train = raw_train.drop('temp_cutoff',axis = 1)
        raw_valid = raw_valid.drop('temp_cutoff',axis = 1)
        raw_train_data_interact = raw_train_data_interact.drop('temp_cutoff',axis = 1)
        plant_dummy_value = (sorted(plant_name_list).index(plant))+1 # fix me

        training = TrainModels(plant = plant,
                               plant_dummy_value = plant_dummy_value, 
                               model_path = args.models,
                               k = args.k,
                               best_model = args.best_model,
                               power_type = args.power_type_seq,                                         
                               adjust_month = args.adjust_month,
                               temp_cutoff = temp_cutoff_value,
                               train_data = raw_train,
                               valid_data = raw_valid,
                               test_data = raw_test,
                               train_data_interact = raw_train_data_interact)
        best_model, performance_table, performance_table_best = training.model_building()
        
        performance_table.Valid_MAPE = round(performance_table.Valid_MAPE,4)
        performance_table_best.Valid_MAPE = round(performance_table_best.Valid_MAPE,4)
        run.log_table(f"{power_predict_name} {plant} Prediction Performance", (performance_table.to_dict('list')))
        run.log_table(f"{power_predict_name} {plant} Best Prediction Performance", (performance_table_best.to_dict('list')))
        run.log_table(f"{power_predict_name} {plant} Valid MAPE by Model", (performance_table[["DummyVar_Model","Valid_MAPE"]].to_dict('list')))
        
        # 註冊 best perf table 
        data_path = os.path.join(f"best-perf",f"{datetime.now().strftime('%Y-%m-%d')}")# e.g. best-perf/2022-05-30
        raw_data_name = f"{plant}_{power_predict_name}_best_prediction_perf.csv" 
        register_data_name = (f"{power_predict_name}-{plant}-best-perf").replace('_','-')
        def_blob_store = ws.get_default_datastore() 
        
        ## Upload data to blob
        performance_table_best.to_csv(raw_data_name,index=False)
        def_blob_store.upload_files(files=[raw_data_name], target_path=data_path, overwrite=True)

        ## Register data in Dataset
        new_data = Dataset.Tabular.from_delimited_files(def_blob_store.path(os.path.join(data_path,raw_data_name)))\
                                          .register(ws, register_data_name,create_new_version=True)


    return 

if __name__ == '__main__':
    print('Modeling!')
    modeling_main()

