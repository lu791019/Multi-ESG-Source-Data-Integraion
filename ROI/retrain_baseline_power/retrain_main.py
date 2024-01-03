import numpy as np
import time
import argparse
from datetime import datetime
from ROI.retrain_baseline_power.retrain_baseline_pl import RetrainElecBaselinePipeline
# from models import engine
import time

parser = argparse.ArgumentParser()
parser.add_argument("-aks", required=True)
parser.add_argument("-git_id", required=True)
parser.add_argument("-git_path", required=True)
parser.add_argument("-connect_string", required=True)
args = parser.parse_args([
    '-aks','1',
    '-git_id','None',
    '-git_path','None',
    '-connect_string', 'abc'
])

def retrain_main_fun():
    # run_list = []
    print('retrain_main_fun start')
    if (datetime.now().strftime('%Y-%m-%d')[8:10]=='10') | (datetime.now().strftime('%Y-%m-%d')[8:10]=='16'):
        try:
            print('pipeline generate!')
            connect_string = engine.get_connect_string()
            for power in ['空調用電（kwh）','生產用電（kwh）','基礎用電（kwh）','工廠用電（kwh）']:#fix me  '空壓用電（kwh）','空調用電（kwh）','生產用電（kwh）','基礎用電（kwh）','工廠用電（kwh）'
                if power=='工廠用電（kwh）':
                    time.sleep(180)
                power_predict_name = str(np.where(power=='空調用電（kwh）','ac-ele',
                                                  np.where(power=='空壓用電（kwh）','ap-ele',
                                                          np.where(power=='生產用電（kwh）','prod-ele',
                                                                   np.where(power=='基礎用電（kwh）','base-ele','pred-ele')))))
                print(power)
                eco_pipeline = RetrainElecBaselinePipeline(job = "eco-retrain-pipeline",
                                                           display = f"{power_predict_name}-{datetime.now().strftime('%Y-%m-%d')}",
                                                           compute_target = "vm-mcc",
                                                           env = "eco-env",
                                                           power_target = power,
                                                           GIT_ID = args.git_id,
                                                           GIT_PATH = args.git_path,
                                                           connect_string = args.connect_string
                                                          )
                run_result = eco_pipeline.trigger_pipeline()
                # run_list.append({f"{power}":run_result})
                print(f"{power} Finish !")
            return 0
        except Exception as e:
            error = str(e)
            return error
    else:
        return 0
    print('retrain_main_fun end')
    
# if __name__=='__main__':
#     print('Start !')
#     retrain_main_fun()

