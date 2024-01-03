import json
import os
from datetime import datetime
from azureml.core import Workspace, Environment, Dataset, Experiment, Datastore
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core.compute import AmlCompute
from azureml.core.runconfig import RunConfiguration
from azureml.pipeline.core import Pipeline, PipelineData, StepSequence
from azureml.pipeline.core.graph import PipelineParameter
from azureml.pipeline.steps import PythonScriptStep
from azureml.data.datapath import DataPath

# Connect to Workspcae
svc_pr = ServicePrincipalAuthentication(
    tenant_id = os.getenv("AZURE_TENANT_ID"),
    service_principal_id = os.getenv("AZURE_CLIENT_ID"),
    service_principal_password = os.getenv("AZURE_CLIENT_SECRET")
)

config = {
    "subscription_id": "a3a07cdf-d288-4ef3-ad5d-2a3fcc94fd0e",
    "resource_group": "ea-tooling-mlops",
    "workspace_name": "ecossot_ml_workspace"
}

ws = Workspace(
    **config,
    auth=svc_pr
)
print("Found workspace {} at location {}".format(ws.name, ws.location))


        
class RetrainElecBaselinePipeline:
    def __init__(self,
                 job = "eco-retrain-pipeline",
                 display = f"{datetime.now().strftime('%Y-%m-%d')}",
                 compute_target = "vm-mcc",
                 env = "eco-env",
                 power_target = "空調用電（kwh）",
                 GIT_ID="",
                 GIT_PATH="",
                 connect_string=""
                ):
        self.job = job
        self.display = display
        self.compute_target = compute_target
        self.env = env
        self.power_target = power_target
        self.git_id = GIT_ID
        self.git_path = GIT_PATH
        self.connect_string = connect_string
        
    def trigger_pipeline(self):
  
        # Get resources
        def_blob_store = ws.get_default_datastore() 
        aml_compute = AmlCompute(workspace=ws, name=self.compute_target)
        env = Environment.get(workspace=ws, name=self.env) 
        aml_run_config = RunConfiguration()
        aml_run_config.target = aml_compute
        aml_run_config.environment = env
        # Setup pipeline parameters
        power_type_seq = PipelineParameter(name="power_type_seq",default_value=self.power_target)
        k = PipelineParameter(name="k",default_value=2)
        adjust_month = PipelineParameter(name="adjust_month",default_value=6)
        models = PipelineData(name="models", datastore=def_blob_store)#.as_dataset()
        tfdv_initial = PipelineParameter(name="tfdv_initial",default_value=1) #note: 0 = not initial / 1 = initial
        git_id = PipelineParameter(name="git_id", default_value=self.git_id) 
        git_path = PipelineParameter(name="git_path", default_value=self.git_path)
        connect_string = PipelineParameter(name="connect_string", default_value=self.connect_string)
        #中繼資料
        best_model = PipelineData(name="best_model", datastore=def_blob_store).as_dataset()
        test_input = PipelineData(name="test_input", datastore=def_blob_store).as_dataset()
        raw_data = PipelineData(name="raw_data", datastore=def_blob_store).as_dataset()
        # Setup pipeine steps
        data_validation = PythonScriptStep(
                source_directory='./ROI/retrain_baseline_power/1_data_validation',
                name="data_validation",
                script_name="data_validation.py",
                compute_target=aml_compute,
                runconfig=aml_run_config,
                arguments=["--tfdv_initial",tfdv_initial,
                           "--connect_string",connect_string],
                allow_reuse=False
                        )
        
        upload_test = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/2_test',
                            name="upload_test_data",
                            script_name="upload_testdata.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--test_input",test_input,
                                       "--power_type_seq", power_type_seq,
                                       "--connect_string",connect_string],
                            outputs=[test_input],
                            allow_reuse=False
                        )
      
        evaluation = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/3_evaluation',
                            name="evaluation",
                            script_name="evaluation.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--test_input",test_input,
                                       "--power_type_seq", power_type_seq],
                            inputs=[test_input],
                            allow_reuse=False 
                        )

        database_query = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/4_query',
                            name="database_query",
                            script_name="database_query.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--power_type_seq", power_type_seq,
                                       "--raw_data",raw_data,
                                       "--connect_string",connect_string],
                            outputs=[raw_data],
                            allow_reuse=False 
                        )
        
        dataset_split = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/5_prepare',
                            name="dataset_split",
                            script_name="dataset_split.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--power_type_seq", power_type_seq,
                                    "--raw_data",raw_data],
                            inputs=[raw_data],
                            allow_reuse=False 
                        )

        modeling = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/6_train',
                            name="modeling",
                            script_name="modeling.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--k", k,
                                    "--adjust_month", adjust_month,
                                    "--power_type_seq", power_type_seq,
                                    "--models", models,
                                    "--best_model", best_model],
                            outputs=[best_model,models],
                            allow_reuse=False
                    )

        register = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/7_register',
                            name="register_best_model",
                            script_name="register.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--best_model", best_model,
                                       "--power_type_seq", power_type_seq,
                                       "--git_id", git_id,
                                       "--git_path", git_path],
                            inputs=[best_model],
                            allow_reuse=False
                    )

        deploy = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/8_deploy',
                            name="deploy_best_model",
                            script_name="deploy.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--power_type_seq", power_type_seq],
                            allow_reuse=False
                    )
        prediction = PythonScriptStep(
                            source_directory='./ROI/retrain_baseline_power/9_prediction',
                            name="prediction_baseline_data",
                            script_name="prediction.py",
                            compute_target=aml_compute,
                            runconfig=aml_run_config,
                            arguments=["--best_model", best_model,
                                       "--power_type_seq", power_type_seq,
                                       "--connect_string",connect_string],
                            allow_reuse=False
                    )
        step_sequence_sub = StepSequence(steps=[data_validation,
                                                upload_test,
                                                evaluation,
                                                database_query,
                                                dataset_split,
                                                modeling,
                                                register,
                                                deploy,
                                                prediction])
        pipeline = Pipeline(workspace=ws, steps=[step_sequence_sub])

        Job = Experiment(ws, self.job)
        run = Job.submit(pipeline)
        run.display_name = self.display
        # run.wait_for_completion(show_output=True)
        # print('ID\n\t',[child.id for child in run.get_children()])
        return run
    
    # for TFDV
    # def downloads(self,parent_run,local_path = None):
    #     childrenID = [child.id for child in parent_run.get_children()]
    #     model_run = ws.get_run(childrenID[2])#fix me：
    #     download_list = [filepath for filepath in model_run.get_file_names() if "anomaly" in filepath]

    #     #Download perf. files
    #     for filepath in download_list:
    #         model_run.download_file(filepath, output_file_path = local_path) 
    #     print("Finish downloading. ")
    #     return 
