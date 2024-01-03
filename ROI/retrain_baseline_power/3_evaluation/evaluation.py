import argparse
import numpy as np
import pandas as pd
import pickle
import re
import statsmodels.api as sm
import time
import xgboost as xgb

from azureml.core import Dataset, Run, Model as azmodel
from datetime import datetime
from numpy import arange
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso, LassoCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler
from scoremodule import ScoreModule
from opencensus.ext.azure.log_exporter import AzureLogHandler
import logging
# # ---------------------------------------------------------------

run  = Run.get_context()
ws   = run.experiment.workspace
print('Modeling Run: ',run.id)

def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--test_input", type=str)
    parser.add_argument("--power_type_seq", type=str)
    args = parser.parse_args()
    return args

   
    
def evaluation_main():
    args = parse_args()
    def_blob_store = ws.get_default_datastore()        
    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                  np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                          np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                                   np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    # 設定 Logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(AzureLogHandler(connection_string='InstrumentationKey=98fd46f9-7e7b-4f4b-87d5-5271ce9bcd85'))
    logger.warning(f"Sent {power_predict_name} to Application Insights")
    
    retrain_plant_list = [] 
    noretrain_data = pd.DataFrame({})
    for plant in ['WIH','WCQ','WKS-5','WKS-6','WZS-1','WZS-3','WZS-6','WZS-8','WCD','WTZ','WOK']: #fix me: 放入所有工廠
        
        with open(args.test_input+'/'+ f"{plant}_{power_predict_name}_inputs.txt", 'r') as opendf:
            test_input = json.load(opendf)
        
        print(f"{plant} test_input as shown below and its type is: ",type(test_input))
        print(test_input)
        # 計算每個廠的預測結果是否達標
        prediction = ScoreModule(inputs = test_input,
                                 workspace = ws) 
        testing_result_json = prediction.runScore()
        print(testing_result_json)
        data_upload_final = pd.DataFrame.from_dict(testing_result_json['data_upload_final'])
        validate_performance = pd.DataFrame.from_dict(testing_result_json['validate_performance'])
        print(validate_performance)
        #load best table
        best_performance = Dataset.get_by_name(workspace=ws, name=(f"{power_predict_name}-{plant}-best-perf").replace('_','-')).to_pandas_dataframe()
        print(best_performance.Valid_MAPE_best.values)
        if validate_performance.mape.values.astype(float) < best_performance.Valid_MAPE_best.values: 
            print(f"Keep the original model of {plant} in {args.power_type_seq} prediction.")
            noretrain_data = noretrain_data.append(data_upload_final).reset_index(drop=True)
        else:
            retrain_plant_list.append(plant)
        
        # 設定Logger要監控的參數
        register_model_name = f"{plant}_{power_predict_name}_baseline" # note: 根據註冊的模型名稱調用
        model = azmodel(ws, register_model_name)
        custom_dimensions = {
            "step_id": run.id,
            "step_name": run.name,
            "experiment_name": run.experiment.name,
            "run_type": "evaluation",
            "power_type":f"{power_predict_name}",
            "stage":"prd",
            "plant": plant,
            "validation_mape": float(validate_performance.mape),
            "model_name": model.name,
            "model_version": model.version
        }
        logger.info(f"Add metric of {plant}", extra= {"custom_dimensions":custom_dimensions})
        
    print(f"plant:{retrain_plant_list} need to be retrained.") 
    
    if noretrain_data.shape[0]>0: #代表有部分工廠不需要retrain 
        print("noretrain_data: ",noretrain_data)
        # 將不要retrain的預測結果註冊到 dataset
        with open(f"{power_predict_name}_noretrain_data.txt", "w") as savedf:
            json.dump(noretrain_data.to_dict(), savedf)
        data_path = os.path.join("noretrain-data",f"{datetime.now().strftime('%Y-%m-%d')}")
        def_blob_store.upload_files(files=[f"{power_predict_name}_noretrain_data.txt"], target_path=data_path, overwrite=True)
        registered_data = Dataset.File.from_files(def_blob_store.path(data_path,f"{power_predict_name}_noretrain_data.txt"),validate=False)\
                                            .register(ws, (f"{power_predict_name}-noretrain-data").replace('_','-'), create_new_version=True)
    
    if len(retrain_plant_list)>0: #代表有部分工廠需要retrain 
        print("retrain_plant_list: ",retrain_plant_list)
        # 將要retrain的工廠清單註冊到 dataset
        with open(f"{power_predict_name}_retrain_plant_list.txt", "w") as savedf:
            json.dump(retrain_plant_list, savedf)
        data_path = os.path.join("retrain-plant",f"{datetime.now().strftime('%Y-%m-%d')}")
        def_blob_store.upload_files(files=[f"{power_predict_name}_retrain_plant_list.txt"], target_path=data_path, overwrite=True)
        registered_plant_list = Dataset.File.from_files(def_blob_store.path(data_path,f"{power_predict_name}_retrain_plant_list.txt"),validate=False)\
                                            .register(ws, (f"{power_predict_name}-retrain-plant-list").replace('_','-'), create_new_version=True)
        
    else:
        raise ValueError("Keep all original models.")
        
    
    
if __name__ == '__main__':
    print('Evaluation!')
    evaluation_main()
