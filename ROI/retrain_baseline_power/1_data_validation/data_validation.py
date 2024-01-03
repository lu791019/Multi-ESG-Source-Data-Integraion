import argparse
import datetime
import json
import numpy as np
import os
import pandas as pd
import pickle
import psycopg2 
import requests 
import tensorflow_data_validation as tfdv 
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import Workspace, Experiment, Dataset,Run
from datetime import datetime as from_datetime
from eco_tfdv import EcoTFDV
from sqlalchemy import create_engine
# from models import engine


run  = Run.get_context()
ws   = run.experiment.workspace
def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--tfdv_initial", type=int)
    parser.add_argument("--connect_string", type=str)
    args = parser.parse_args()
    return args
def data_loader(status):
    # PRD
    # host = '10.37.37.211'
    # port = '15063'
    # Database = 'postgres'
    # Username = ''
    # password = ''
    # conn = create_engine(f"postgresql+psycopg2://{Username}:{password}@{host}:{port}/{Database}")
    # conn = psycopg2.connect(host=host, 
    #                         port=port, 
    #                         database=Database, 
    #                         user=Username,
    #                         password=password)
    # connect_string = engine.get_connect_string()
    args = parse_args()
    conn = create_engine(args.connect_string, echo=True)
    print(' *** connect to prd *** ')
    
    if status == 1:
        print(' *** data loader - initial status *** ')
        # overview_query = f"SELECT *\
        #                      FROM (\
        #                       SELECT *,\
        #                         ROW_NUMBER() OVER (PARTITION BY plant ORDER BY plant,datetime) AS sn\
        #                       FROM app.baseline_data_overview\
        #                       WHERE datetime >= '2022-01-01' and plant in ('WKS-5','WKS-6','WOK','WTZ','WZS-1','WZS-3','WZS-6','WZS-8','WIH','WCQ','WCD')\
        #                      ) AS R WHERE R.sn=1"
        dataset = Dataset.get_by_name(workspace=ws, name='ecossot-dataset')
        dataset = dataset.to_pandas_dataframe().drop_duplicates().reset_index(drop=True)
        dataset['datetime'] = pd.to_datetime(dataset['datetime'])
        dataset['datetime'] = [str(x)[0:10] for x in dataset['datetime']]
        # dataset = dataset.loc[(dataset.datetime=='2023-01-01'),:].reset_index(drop=True)
        dataset = pd.merge(dataset,
                           dataset.loc[(~dataset.ac_electricity.isna()) & (~dataset.revenue.isna()) & (~dataset.pcba_qty.isna()),:].groupby(['plant','bo']).agg({'datetime':'max'}).reset_index().rename(columns={'datetime':'datetime_max'}),
                           on=['plant','bo'], how='left')
        dataset = dataset.loc[dataset['datetime']==dataset['datetime_max'],:].reset_index(drop=True)
        
        
        baseline_query = f"SELECT *\
                             FROM (\
                              SELECT *,\
                                ROW_NUMBER() OVER (PARTITION BY plant ORDER BY plant,datetime) AS sn\
                              FROM app.predict_baseline_data\
                              WHERE datetime >= '2022-01-01'\
                             ) AS R WHERE R.sn=1"
        
    else:
        print(' *** data loader - evaluative status *** ')
        # overview_query = f"SELECT *\
        #                      FROM (\
        #                       SELECT *,\
        #                         ROW_NUMBER() OVER (PARTITION BY plant ORDER BY plant,datetime DESC) AS sn\
        #                       FROM app.baseline_data_overview\
        #                       WHERE datetime > '2022-01-01' and plant in ('WKS-5','WKS-6','WOK','WTZ','WZS-1','WZS-3','WZS-6','WZS-8','WIH','WCQ','WCD')\
        #                      ) AS R WHERE R.sn=1"
        
        dataset = Dataset.get_by_name(workspace=ws, name='ecossot-dataset')
        dataset = dataset.to_pandas_dataframe().drop_duplicates().reset_index(drop=True)
        dataset['datetime'] = pd.to_datetime(dataset['datetime'])
        dataset['datetime'] = [str(x)[0:10] for x in dataset['datetime']]
        # dataset = dataset.loc[(dataset.datetime=='2023-01-01'),:].reset_index(drop=True)
        dataset = pd.merge(dataset,
                           dataset.loc[(~dataset.ac_electricity.isna()) & (~dataset.revenue.isna()) & (~dataset.pcba_qty.isna()),:].groupby(['plant','bo']).agg({'datetime':'max'}).reset_index().rename(columns={'datetime':'datetime_max'}),
                           on=['plant','bo'], how='left')
        dataset = dataset.loc[dataset['datetime']==dataset['datetime_max'],:].reset_index(drop=True)
        
        baseline_query = f"SELECT *\
                             FROM (\
                              SELECT *,\
                                ROW_NUMBER() OVER (PARTITION BY plant ORDER BY plant,datetime DESC) AS sn\
                              FROM app.predict_baseline_data\
                              WHERE datetime > '2022-01-01'\
                             ) AS R WHERE R.sn=1"
        
    # overview_ptable_query = f"SELECT DISTINCT datetime,plant FROM ({overview_query}) AS pt1"
    baseline_ptable_query = f"SELECT DISTINCT datetime,plant FROM ({baseline_query}) AS pt2"
    
    
    # overview_data = pd.read_sql(overview_query,con=conn)
    overview_data = dataset
    baseline_data = pd.read_sql(baseline_query,con=conn)

    # overview_ptable = pd.read_sql(overview_ptable_query,con=conn)
    overview_ptable = dataset.loc[:,['datetime','plant']].drop_duplicates().reset_index(drop=True)
    baseline_ptable = pd.read_sql(baseline_ptable_query,con=conn)
    
    return overview_data, baseline_data, overview_ptable, baseline_ptable

def pack_messages(section_ls,fact_content,dvtype):
    package = \
            {
                "activityTitle": f"<span style=color:green;font-size:20px>{plant}</span>",
                "activitySubtitle": f"Anomaly Info - {current_date} ver." if dvtype == 'anomaly' else f"Drift Info - {current_date} ver.",
                "facts": fact_content,
                "markdown": True
            }
    section_ls.append(package)
    return section_ls

def send_messages(section_ls, card_title):
    webhook_url = "https://wistron.webhook.office.com/webhookb2/ee554281-994a-432c-8915-094e19dbbd1c@de0795e0-d7c0-4eeb-b9bb-bc94d8980d3b/IncomingWebhook/67e4f4de66d3455784813a367b81d30f/06311596-22ce-4d57-af5b-b99434623017"
    # DEV ENV:"https://wistron.webhook.office.com/webhookb2/5d77c802-8ccf-4a4d-9238-ff967bcd32f1@de0795e0-d7c0-4eeb-b9bb-bc94d8980d3b/IncomingWebhook/19de8a62937e428a9f48f2aa43650bc9/06311596-22ce-4d57-af5b-b99434623017"
    headers = {"Content-Type": "application/json"}
    text = \
        {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": "Send the messages",
        "title":card_title.capitalize().replace('_',' '),
        "sections": section_ls
    }
    resp = requests.post(webhook_url, data=json.dumps(text), headers=headers)
    print(f"Sending status of {plant}: {resp.status_code}")
    return 

def go_dv(data, plant_table, status, data_src):
    def_blob_store = ws.get_default_datastore()
    anomaly_sec_ls = []
    drift_sec_ls = []
    global plant, current_date
        
    if status is True:
        for plant in plant_table.plant:
            print(plant)
            subdata = data.query(f"plant == '{plant}' ").drop(['datetime'],axis = 1)
            print(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0])
            print(isinstance(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0], str))
            if isinstance(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0], str):
                current_date = plant_table.loc[(plant_table.plant== plant),'datetime'].values[0].replace('-','')
            else:
                current_date = plant_table.loc[(plant_table.plant== plant),'datetime'].values[0].strftime('%Y-%m-%d').replace('-','')
            # print(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0])
            # print(type(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0]))
            # current_date = plant_table.loc[(plant_table.plant== plant),'datetime'].values[0].replace('-','')
            print(f"*** 初始 {plant} {data_src} {current_date} 資料僅進行schema註冊，而不會執行資料驗證 ***")
            
            ecotfdv = EcoTFDV(DATA = subdata, INITIAL = status)
            train_stats, schema = ecotfdv.Train_stats()
            tfdv.write_stats_text(train_stats,f"{plant}_{data_src}_{current_date}_trainstats.txt")
            tfdv.write_schema_text(schema,f"{plant}_{data_src}_{current_date}_trainschema.txt")

            for data_type in ['trainstats','trainschema']:
            ## Upload data from local file 
                data_path = os.path.join(f"eco-validation",f"{from_datetime.now().strftime('%Y-%m-%d')}")# e.g. eco-validation/2022-05-30/WIH
                raw_data_name = f"{plant}_{data_src}_{current_date}_{data_type}.txt"
                register_data_name = (f"{data_type}-{data_src}-{plant}").replace('_','-')

                ## Upload data to blob
                def_blob_store.upload_files(files=[raw_data_name], target_path=data_path, overwrite=True)

                ## Register data in Dataset
                new_data = Dataset.File\
                                    .from_files(def_blob_store.path(os.path.join(data_path,raw_data_name)),validate=False)\
                                      .register(ws, register_data_name,create_new_version=True,
                                                description=f"Plant_name : {plant} Data_source : {data_src} on {current_date}.")
                                                            

    else:
        for plant in plant_table.plant:
            print(plant)
            subdata = data.query(f"plant == '{plant}' ").drop(['datetime'],axis = 1)
            if isinstance(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0], str):
                current_date = plant_table.loc[(plant_table.plant== plant),'datetime'].values[0].replace('-','')
            else:
                current_date = plant_table.loc[(plant_table.plant== plant),'datetime'].values[0].strftime('%Y-%m-%d').replace('-','')
            # print(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0])
            # print(type(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0]))
            # current_date = str(plant_table.loc[(plant_table.plant== plant),'datetime'].values[0]).replace('-','')
            print(f"*** 即將針對 {plant} {data_src} {current_date} 進行資料驗證 ***")
            
            ecotfdv = EcoTFDV(DATA = subdata, INITIAL = status)
            eval_stats = ecotfdv.Eval_stats()
            schema_path = Dataset.get_by_name(ws, name=(f"trainschema-{data_src}-{plant}").replace('_','-')).download()
            trainstats_path = Dataset.get_by_name(ws, name=(f"trainstats-{data_src}-{plant}").replace('_','-')).download()
            schema = tfdv.load_schema_text(input_path = schema_path[0])
            trainstats = tfdv.load_stats_text(input_path = trainstats_path[0])
            # print(schema)

            # 異常檢測 資料品質 & 資料偏移
            feature_ls = ['pcba_qty','fa_qty','pcba_lines','fa_lines','revenue','average_temperature']

            anomaly,atxt = ecotfdv.Check_anomalies(eval_stats,schema)
            drift,dtxt = ecotfdv.Check_eval_drift(trainstats, eval_stats, schema, feature_ls)

            pack_messages(section_ls = anomaly_sec_ls, fact_content = atxt,dvtype = 'anomaly')
            pack_messages(section_ls = drift_sec_ls, fact_content = dtxt,dvtype = 'drift')

            # save anomalies text into outputs/
            tfdv.write_anomalies_text( anomalies = anomaly, output_path = f"outputs/{plant}_{data_src}_{current_date}_anomaly.txt")
            tfdv.write_anomalies_text( anomalies = drift, output_path = f"outputs/{plant}_{data_src}_{current_date}_drift.txt")
            
        #將異常訊息送至 teams頻道
        print(f'{data_src} anomaly_sec_ls:\n',type(anomaly_sec_ls),anomaly_sec_ls)
        print(f'{data_src} drift_sec_ls:\n',type(drift_sec_ls),drift_sec_ls)
        send_messages(section_ls = anomaly_sec_ls, card_title = data_src)
        send_messages(section_ls = drift_sec_ls, card_title = data_src)
              
    
    return


def validation_main():
 
    args = parse_args()  
    overview_data, baseline_data, overview_ptable, baseline_ptable = data_loader(args.tfdv_initial)
    initial_status = False if args.tfdv_initial == 0 else True
    go_dv(overview_data, overview_ptable, initial_status,'overview_data')
    go_dv(baseline_data, baseline_ptable, initial_status,'baseline_data')
    
    return
        

if __name__ == "__main__":
    validation_main()


                                                    
