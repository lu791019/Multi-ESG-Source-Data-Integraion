import argparse
import numpy as np
import os
import pandas as pd
from azureml.core.model import InferenceConfig
from azureml.core.webservice import AksWebservice, Webservice
from azureml.core.compute import AksCompute
from azureml.core.environment import Environment
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import Workspace, Run,Dataset, Model as azmodel


run  = Run.get_context()
ws   = run.experiment.workspace
def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--power_type_seq", type=str)
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

def deploy_main():
    args = parse_args()
    def_blob_store = ws.get_default_datastore() 
    env = Environment.get(workspace=ws, name="eco-env")

    power_predict_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_electricity',
                                  np.where(args.power_type_seq=='空壓用電（kwh）','ap_electricity',
                                          np.where(args.power_type_seq=='生產用電（kwh）','production_electricity',
                                                   np.where(args.power_type_seq=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    power_service_name = str(np.where(args.power_type_seq=='空調用電（kwh）','ac_elec',
                                  np.where(args.power_type_seq=='空壓用電（kwh）','ap_elec',
                                          np.where(args.power_type_seq=='生產用電（kwh）','prod_elec',
                                                   np.where(args.power_type_seq=='基礎用電（kwh）','base_elec','pred_elec')))))
    
    plant_name_list = extract_plantname(f"{power_predict_name}_retrain_plant_list".replace('_','-')) #fix me
    # plant_name_list = ['WCD']
    for plant in plant_name_list:
        model = azmodel(ws, f"{plant}_{power_predict_name}_baseline")
        service_name = (f"{plant.lower()}_{power_service_name}_api").replace('_','-')
        aks_name = "dev-mic"
        print("deploy path", os.getcwd())

        inference_config = InferenceConfig(
            entry_script="score_baseline.py",
            environment=env,
            source_directory='./',
        )
        aci_config = AksWebservice.deploy_configuration(enable_app_insights=True,
                                                        collect_model_data=True,
                                                        cpu_cores=0.1, 
                                                        memory_gb=0.5,
                                                        auth_enabled=True,
                                                        description = "透過營收、生產量、天氣溫度與人力等營運資訊，評估各廠區5大用電類型每月用電基準值")
        deployment_target = AksCompute(workspace=ws, name=aks_name)

        service = azmodel.deploy(
            workspace=ws,
            name=service_name,
            models=[model],
            inference_config=inference_config,
            deployment_config=aci_config,
            deployment_target=deployment_target,
            overwrite=True
        )
        service.wait_for_deployment(show_output=True)
    return
    
if __name__ == "__main__":
    print('Deploying!')
    deploy_main()
