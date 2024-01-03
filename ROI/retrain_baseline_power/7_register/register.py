import argparse
import json
import logging
import numpy as np
import os
import pandas as pd
import pickle
import pickle
import sys
from azureml.core import Dataset, Run
from azureml.core.model import Model as azmodel
from datetime import datetime
from opencensus.ext.azure.log_exporter import AzureLogHandler
# # ---------------------------------------------------------------

run  = Run.get_context().parent
ws   = run.experiment.workspace
print('Register Run: ',run.id)
def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--power_type_seq", type=str)
    parser.add_argument("--best_model", type=str)
    parser.add_argument("--git_id", required=True)
    parser.add_argument("--git_path", required=True)
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

def register_main():
    args = parse_args()
    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                  np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                          np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                                   np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    # 設定 Logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(AzureLogHandler(connection_string='InstrumentationKey=98fd46f9-7e7b-4f4b-87d5-5271ce9bcd85'))
    logger.warning(f"Sent {power_predict_name} to Application Insights")
    plant_name_list = extract_plantname(f"{power_predict_name}-retrain-plant-list".replace('_','-')) #fix me
    # plant_name_list = ['WCD']
    # 須確認是否能讀到
    for plant in plant_name_list: 
        metrics = pd.read_csv(f"{args.best_model}/{plant}_{power_predict_name}_best_prediction_perf.csv")
        raw_train = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-train")
        raw_valid = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-valid")
        raw_test  = Dataset.get_by_name(workspace=ws, name=f"{power_predict_name.replace('_','-')}-{plant}-test")
        print(metrics)
        best = azmodel.register(
                        workspace=ws,
                        model_name=f"{plant}_{power_predict_name}_baseline",
                        model_path=args.best_model+'/'+ f"{plant}_{power_predict_name}_bestmodel.pickle", 
                        tags={"project_name":"ECO_SSOT",
                              "project_code":"ep202202.075",
                              "department":"VD2600",
                              "pic":"zack_li@wistron.com",
                              "project_type":"dt",
                              "ai_field":"tabular",
                              "language":"python",
                              "frame_work":"scikitlearn, xgboost",
                              "ml_type":"Regression",
                              "algorithm":"Linear Regression, Lasso, Random Forest, SVM, XGBoost, Ensemble",
                              "gpu":"False",
                              "system":"False",
                              "code":"None",
                              "pipeline_id":run.id,
                              "git_id":args.git_id,
                              "git_path":args.git_path,
                              "business_domain":"r&d",
                              "business_unit":"dt",
                              "architecture":"amd64(intel)",
                              "external_model_version":"none",
                              "dataset": f"{power_predict_name}-{plant}-train:{raw_train.version},\
                                           {power_predict_name}-{plant}-valid:{raw_valid.version},\
                                           {power_predict_name}-{plant}-test:{raw_test.version}"},
                        properties={f"MAPE of {power_predict_name}": round(metrics['Valid_MAPE_best'][0],4),
                                    f"MAPE of {power_predict_name}_bottomline":0.2,
                                    f"MAPE of {power_predict_name}_target":0.15 },
                        description="透過時間序列方法預測工廠各類用電量，包含空調、空壓、生產以及基礎用電量"
        )
        # 設定Logger要監控的參數
        custom_dimensions = {
            "step_id": run.id,
            "step_name": run.name,
            "experiment_name": run.experiment.name,
            "run_type": "register",
            "power_type":f"{power_predict_name}",
            "stage":"prd",
            "plant": plant,
            "valid_mape": float(metrics['Valid_MAPE_best'][0]),
            "model_name": best.name,
            "model_version": best.version
        }
        logger.info(f"Add metric of {plant}", extra= {"custom_dimensions":custom_dimensions})
    return

if __name__ == '__main__':
    print('Registering!')
    register_main()

