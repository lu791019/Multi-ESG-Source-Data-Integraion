#!/usr/bin/python
import datetime
import math
import numpy as np
from numpy import arange
import pandas as pd
import psycopg2
# import pickle
import re
import random
# from sklearn.linear_model import LinearRegression
# from sklearn.metrics import r2_score
from sqlalchemy import create_engine
import urllib.request
import json
import requests
import ssl
import os
from services.mail_service import MailService
from models import engine
import time
# import statsmodels.api as sm

def db_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

def data_processer(accs_data, air_compress_data, maintain_info_data):
    # Process accs_data
    accs_data = accs_data.drop_duplicates().reset_index(drop=True)
    accs_data.power = np.where((accs_data.building=='TB2') & (accs_data.id.isin(['AC'+str(x) for x in range(17)])),75,
                               np.where((accs_data.building=='TB2') & (accs_data.id.isin(['AC17','AC18'])),130,
                                        np.where(((accs_data.building=='TB2') & (accs_data.id.isin(['AC12','AC15']))) | ((accs_data.building=='C') & (accs_data.id.isin(['9#','10#']))),110,
                                                 np.where(((accs_data.building=='F2') & (accs_data.id.isin(['1#','2#']))),250,
                                                          np.where(((accs_data.building=='F2') & (accs_data.id.isin(['3#','4#']))),132,
                                                                   np.where((accs_data.building=='TB5'),75,accs_data.power))))))
    # accs_data = accs_data.loc[(~accs_data.id.isin(['AC12','AC15'])),:]
    accs_data = (accs_data.loc[~((accs_data.id.isin(['9#'])) & (accs_data.building=='Fab12')),:]).reset_index(drop=True) # fix me
    accs_data = pd.merge(accs_data,
                         accs_data.groupby(['id','building','periodend']).agg({'energy':'max','press':'max'}).reset_index(). \
                         rename(columns={'energy':'energy_max','press':'press_max'}),
                         on=['id','building','periodend'],how='left')
    accs_data = (accs_data.loc[(accs_data.energy==accs_data.energy_max) & (accs_data.press==accs_data.press_max),:]).drop(columns=['energy_max','press_max']).reset_index(drop=True)
    accs_data['periodend'] = pd.to_datetime(accs_data.periodend,unit='ms',origin='1970-01-01') + pd.Timedelta(hours=8) # - pd.Timedelta(hours=6)
    accs_data['eer_real'] = accs_data.flow/accs_data.power
    # Summary accs_data
    accs_data['energy_check'] = np.where(accs_data.energy!=0,1,0)
    # Machine altenative runtime computation
    accs_data_summary = accs_data.groupby(['building','id']).agg({'energy_check':'sum','periodend':'size','power':'mean'}).reset_index(). \
        rename(columns={'energy_check':'energy_check_sum','periodend':'total_cnt'})
    accs_data_summary = accs_data_summary.iloc[np.where([x.find('#10')==-1 for x in accs_data_summary.id])[0],:]
    accs_data_summary['run_rate']=accs_data_summary.energy_check_sum/accs_data_summary.total_cnt
    accs_data_summary['plant']=np.where(accs_data_summary.building=='C','WKS',
                                        np.where(accs_data_summary.building=='F2','WTZ',
                                                 np.where(accs_data_summary.building=='Fab12','WOK',
                                                           np.where(accs_data_summary.building=='KD','WCQ',
                                                                    np.where(accs_data_summary.building=='F1','WCD','WZS')))))
    accs_data_summary = accs_data_summary[['building','id','run_rate']].rename(columns={'id':'compress_id'})
    # Process for old_info_data
    air_compress_data['eer_r'] = air_compress_data.flow_r*60/air_compress_data.power_r
    # Process for maintain info data
    maintain_info_data = maintain_info_data.loc[maintain_info_data.last_maintain_hour!=-1,:]
    # maintain_info_data_sub['last_maintain_hour'] = maintain_info_data_sub.next_maintain_hour - maintain_info_data_sub.maintain_hour/maintain_info_data_sub.run_rate
    maintain_info_data['last_maintain_time_previous'] = np.where(maintain_info_data.building=='Fab12',
                                                                 maintain_info_data.last_maintain_time - pd.Timedelta(days=14),
                                                                 maintain_info_data.last_maintain_time - pd.Timedelta(days=35))
    maintain_info_data['last_maintain_time_previous_all'] = maintain_info_data.last_maintain_time - pd.Timedelta(days=180)
    maintain_info_data['last_maintain_time_after'] = maintain_info_data.last_maintain_time + pd.Timedelta(days=20)
    maintain_info_data = pd.merge(maintain_info_data,
                                  air_compress_data[['plant','building','uid','id','eer_r','power_r','oil_type','press_type']].rename(columns={'uid':'machine_id','id':'compress_id'}),
                                  on = ['plant','building','machine_id'], how='left')
    maintain_info_data = pd.merge(maintain_info_data.drop(columns=['run_rate']),
                                  accs_data_summary,
                                  on = ['building','compress_id'], how='left')
    maintain_info_data = maintain_info_data.loc[~maintain_info_data.run_rate.isna(),:]
    maintain_info_data_detail = pd.merge(accs_data.rename(columns={'id':'compress_id'}), 
                                         maintain_info_data, 
                                         on = ['compress_id','building'], how='left')
    return maintain_info_data, maintain_info_data_detail

def q3(x): 
    return x.quantile(0.9)
def maintain_eer_improved_computer(maintain_info_data, maintain_info_data_detail, energy_cost):
    maintain_info_data_pervious = maintain_info_data_detail.loc[(maintain_info_data_detail.periodend>=maintain_info_data_detail.last_maintain_time_previous) & 
                                                                (maintain_info_data_detail.periodend<maintain_info_data_detail.last_maintain_time) & 
                                                                (maintain_info_data_detail.eer_real>0) & (maintain_info_data_detail.eer_real<15),:]
    maintain_info_data_pervious_summary = maintain_info_data_pervious.groupby(maintain_info_data.columns.values.tolist()).agg({'eer_real':'mean'}).reset_index(). \
                                                                      rename(columns={'eer_real':'previous_eer_median'})
    maintain_info_data_after = maintain_info_data_detail.loc[(maintain_info_data_detail.periodend<=maintain_info_data_detail.last_maintain_time_after) & 
                                                             (maintain_info_data_detail.periodend>maintain_info_data_detail.last_maintain_time) & 
                                                             (maintain_info_data_detail.eer>0) & (maintain_info_data_detail.eer<15),:]
    maintain_info_data_after_summary = maintain_info_data_after.groupby(maintain_info_data.columns.values.tolist()).agg({'eer':q3}).reset_index(). \
                                                                rename(columns={'eer':'after_eer_median'})
    maintain_info_data_summary = pd.merge(maintain_info_data_pervious_summary,
                                          maintain_info_data_after_summary,
                                          on=maintain_info_data.columns.values.tolist(),how='inner')
    maintain_info_data_summary['eer_diff'] = maintain_info_data_summary.after_eer_median-maintain_info_data_summary.previous_eer_median
    maintain_info_data_summary = maintain_info_data_summary.loc[maintain_info_data_summary.eer_diff>0,:].reset_index(drop=True)
    maintain_info_data_summary['maintain_revenue'] = maintain_info_data_summary.power_r*energy_cost*maintain_info_data_summary.eer_diff*maintain_info_data_summary.maintain_hour/maintain_info_data_summary.previous_eer_median
    return maintain_info_data_summary

def maintain_dynamic_recommender(maintain_type,maintain_hour,plant,power,maintain_cost,run_rate,last_maintain_time,test_data2_sub):
    
    # test_data2_sub['previous_hour']=(test_data2_sub.runtime_max//500+1)*500
    test_data2_sub['eer_rate']=test_data2_sub.eer_delta/test_data2_sub.eer_predict
    previous_maintain_info_all = pd.DataFrame({})
    measure_range = np.where(maintain_hour>5000,1000,500)
    for hour_index in np.unique((test_data2_sub.runtime_max//measure_range+1)*measure_range):
        test_data2_sub_summary = test_data2_sub.loc[test_data2_sub.runtime_max<hour_index,:].groupby(['compress_id','building']).agg({'eer_rate':'mean'}).reset_index()
        test_data2_sub_summary['maintain_type'] = maintain_type
        test_data2_sub_summary['maintain_hour'] = maintain_hour
        test_data2_sub_summary['plant'] = plant
        test_data2_sub_summary['previous_hour'] = hour_index
        test_data2_sub_summary['revenue_cost'] = test_data2_sub_summary.eer_rate*hour_index*0.8*power
        test_data2_sub_summary['repaired_roi'] = maintain_cost*(1+hour_index/(maintain_hour-hour_index))/test_data2_sub_summary.revenue_cost*measure_range
        test_data2_sub_summary['addition_cost'] = maintain_cost*(1+hour_index/(maintain_hour-hour_index)-1)
        test_data2_sub_summary['revenue_diff'] = test_data2_sub_summary.revenue_cost-test_data2_sub_summary.addition_cost
        previous_maintain_info_all = (previous_maintain_info_all.append(test_data2_sub_summary)).reset_index(drop=True)
    previous_maintain_info_all = previous_maintain_info_all.rename(columns={'compress_id':'machine_id'})
    previous_maintain_info_summary = previous_maintain_info_all.groupby(['plant','building','machine_id','maintain_type','maintain_hour']).agg({'revenue_diff':'max'}).reset_index(). \
                                                                rename(columns={'revenue_diff':'revenue_diff_max'})
    previous_maintain_info_summary['best_previous_hour'] = previous_maintain_info_all.previous_hour[np.where(previous_maintain_info_all.revenue_diff==max(previous_maintain_info_all.revenue_diff))[0][0]]
    previous_maintain_info_summary['best_previous_hour'] = np.where(previous_maintain_info_summary.revenue_diff_max<=0,
                                                                    0,
                                                                    previous_maintain_info_summary.best_previous_hour)
    if (previous_maintain_info_summary.best_previous_hour[0])==0:
        previous_maintain_info_summary['addition_cost'] = 0
        previous_maintain_info_summary['revenue_diff_max'] = 0
    else:
        previous_maintain_info_summary['addition_cost'] = int(previous_maintain_info_all.loc[previous_maintain_info_all.previous_hour==previous_maintain_info_summary.best_previous_hour[0],'addition_cost'])

    now_pass_hour = (pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) -pd.to_datetime(last_maintain_time)).days*24
    now_pass_hour = now_pass_hour*np.where(run_rate>1,1,run_rate)
    previous_maintain_info_summary['now_pass_hour'] = previous_maintain_info_summary.maintain_hour-previous_maintain_info_summary.best_previous_hour-now_pass_hour
    previous_maintain_info_summary['revenue_rate'] = previous_maintain_info_summary.revenue_diff_max/maintain_cost
    previous_maintain_info_summary['best_maintain_time'] = np.where(previous_maintain_info_summary.now_pass_hour<=0,pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d')),
                                                                   pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d')) + pd.Timedelta(days=int(previous_maintain_info_summary.now_pass_hour/run_rate/24)))

    previous_maintain_info_all['check_roi'] = previous_maintain_info_all.repaired_roi-previous_maintain_info_all.previous_hour
    previous_maintain_info_all_summary_gmin = pd.merge(previous_maintain_info_all[['plant','building','machine_id','maintain_type','previous_hour','repaired_roi','check_roi']],
                                                       previous_maintain_info_all.groupby(['plant','building','machine_id','maintain_type']).agg({'check_roi':gmin}).reset_index(),
                                                       on=['plant','building','machine_id','maintain_type','check_roi'],how='inner').rename(columns={'previous_hour':'previous_hour_gmin','check_roi':'check_roi_gmin','repaired_roi':'repaired_roi_gmin'})
    previous_maintain_info_all_summary_lmax = pd.merge(previous_maintain_info_all[['plant','building','machine_id','maintain_type','previous_hour','repaired_roi','check_roi']],
                                                       previous_maintain_info_all.groupby(['plant','building','machine_id','maintain_type']).agg({'check_roi':lmax}).reset_index(),
                                                       on=['plant','building','machine_id','maintain_type','check_roi'],how='inner').rename(columns={'previous_hour':'previous_hour_lmax','check_roi':'check_roi_lmax','repaired_roi':'repaired_roi_lmax'})
    previous_maintain_info_all_summary = pd.merge(previous_maintain_info_all_summary_lmax,previous_maintain_info_all_summary_gmin,
                                                  on=['plant','building','machine_id','maintain_type'],how='left')
    # compute maintain match ROI
    a1=previous_maintain_info_all_summary.previous_hour_gmin-previous_maintain_info_all_summary.previous_hour_lmax
    a2=previous_maintain_info_all_summary.repaired_roi_gmin-previous_maintain_info_all_summary.repaired_roi_lmax
    previous_maintain_info_all_summary['match_roi']=(a1*previous_maintain_info_all_summary.repaired_roi_gmin-previous_maintain_info_all_summary.previous_hour_gmin*a2)/(a1-a2)
    previous_maintain_info_all_summary['match_roi']=np.where(previous_maintain_info_all_summary.match_roi.isna(),0,previous_maintain_info_all_summary.match_roi)
    previous_maintain_info_all = pd.merge(previous_maintain_info_all,previous_maintain_info_all_summary[['plant','building','machine_id','maintain_type','match_roi']],
                                          on=['plant','building','machine_id','maintain_type'],how='left')
    previous_maintain_info_all = previous_maintain_info_all.drop(columns={'check_roi','maintain_hour'})

    # fix me
    if (previous_maintain_info_all.loc[previous_maintain_info_all.maintain_type=='小保養',:]).shape[0]!=0:
        previous_maintain_info_all = previous_maintain_info_all.loc[previous_maintain_info_all.maintain_type=='小保養',:].reset_index(drop=True)
        previous_maintain_info_summary = previous_maintain_info_summary.loc[previous_maintain_info_summary.maintain_type=='小保養',:].reset_index(drop=True)
    else:
        previous_maintain_info_all.maintain_type='小保養'
        previous_maintain_info_summary.maintain_type='小保養'
    return previous_maintain_info_all, previous_maintain_info_summary

def data_uploader(data, db_name, table_name):
    # Truncate table
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name,conn,index= False, if_exists = 'append',schema=db_name, chunksize = 10000)
    return 0

def model_api_caller(data, url, api_key):
    payload = json.dumps(data)
    headers = {
    'Authorization': 'Bearer ' + api_key,
    'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, data=payload, verify=False)
        result = response.text
        # print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))
    return result

def gmin(x): 
    return np.where(len(x[x>0])==0,[-1],[np.min(x[x>0])])[0]
    # return np.min(x[x>0])
def lmax(x): 
    return np.where(len(x[x<0])==0,[-1],[np.max(x[x<0])])[0]

def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null',np.nan)
    for x in dataset.columns:
        if x in ['id','building','plant','compress_id','machine_id','maintain_type','oil_type','press_type','eer_type']:
            dataset[x] = dataset[x].astype('string')
        elif x in ['periodend']:
            dataset[x] = [datetime.datetime.strptime(y,'%Y-%m-%d %H:%M:%S') if len(y)>11 else datetime.datetime.strptime(str(y+' 00:00:00'),'%Y-%m-%d %H:%M:%S') for y in dataset[x]]
        elif x in ['last_maintain_time','next_maintain_time','best_maintain_time','last_maintain_time_previous', 'last_maintain_time_previous_all','last_maintain_time_after']:
            dataset[x] = [datetime.datetime.strptime(y,'%Y-%m-%d') for y in dataset[x]]
        elif x in ['runtime','energy_check']:
            dataset[x] = dataset[x].astype('int')
        else:
            dataset[x] = dataset[x].astype('float')
    return dataset

def maintain_previous_after_data_generator(maintain_info_data_detail):
    maintain_info_data_detail['runtime'] = np.where(maintain_info_data_detail.runtime>0,1,0)
    maintain_info_data_pervious_all = maintain_info_data_detail.loc[(maintain_info_data_detail.periodend>=maintain_info_data_detail.last_maintain_time_previous_all) & 
                                                                    (maintain_info_data_detail.periodend<maintain_info_data_detail.last_maintain_time) & 
                                                                    (maintain_info_data_detail.eer_real>0) & (maintain_info_data_detail.eer_real<15),:]
    # maintain_info_data_pervious_all = maintain_info_data_pervious_all.loc[(maintain_info_data_pervious_all.compress_id=='11#') & (maintain_info_data_pervious_all.last_maintain_time.astype(str)=='2022-01-18'),:]
    maintain_info_data_pervious_all = pd.merge(maintain_info_data_pervious_all,
                                               maintain_info_data_pervious_all.groupby(['building','compress_id','maintain_type','last_maintain_time']).agg({'periodend':'max'}).rename(columns={'periodend':'runtime_max'}).reset_index(),
                                               on=['building','compress_id','maintain_type','last_maintain_time'],how='left').reset_index(drop=True)
    maintain_info_data_pervious_all['runtime_max'] = np.floor((maintain_info_data_pervious_all.periodend-maintain_info_data_pervious_all.runtime_max)/ pd.Timedelta('1 hour'))+maintain_info_data_pervious_all.maintain_hour
    maintain_info_data_pervious_all_list = maintain_info_data_pervious_all[['plant','compress_id','building','last_maintain_time','maintain_cost','maintain_hour','power','maintain_type','run_rate']].drop_duplicates().reset_index(drop=True)

    # Generate after last maintain data
    maintain_info_data_after_all = maintain_info_data_detail.loc[(maintain_info_data_detail.periodend>=maintain_info_data_detail.last_maintain_time) & 
                                                                 (maintain_info_data_detail.eer>0) & (maintain_info_data_detail.eer<15),:]
    maintain_info_data_after_all = pd.merge(maintain_info_data_after_all,
                                               maintain_info_data_after_all.groupby(['building','compress_id','maintain_type','last_maintain_time']).agg({'periodend':'min'}).rename(columns={'periodend':'runtime_max'}).reset_index(),
                                               on=['building','compress_id','maintain_type','last_maintain_time'],how='left').reset_index(drop=True)
    runtime_adjustment = np.floor((maintain_info_data_after_all.runtime_max - pd.to_datetime(maintain_info_data_after_all.last_maintain_time))/ pd.Timedelta('1 hour')*maintain_info_data_after_all.run_rate)
    maintain_info_data_after_all['runtime_max'] = maintain_info_data_after_all.groupby(['compress_id','building','last_maintain_time'])['runtime'].cumsum()+runtime_adjustment
    return maintain_info_data_pervious_all, maintain_info_data_pervious_all_list, maintain_info_data_after_all

def maintain_data_daily_merger(data):
    merger_column = ['per', 'energy', 'flow', 'eer', 'press', 'temperature', 'runtime', 'eer_real', 'energy_check', 'runtime_max']
    common_column = [x for x in data.columns[~data.columns.isin(merger_column)]]
    data['periodend'] = [str(x)[0:11]+'00:00:00' for x in data.periodend]
    data = data.groupby(common_column).agg({'per':'median', 'energy':'mean', 'flow':'mean', 'eer':'median', 'press':'mean', 
                                            'temperature':'mean', 'runtime':'max', 'eer_real':'median', 'energy_check':'sum', 
                                            'runtime_max':'max'}).reset_index()
    return data

def air_compress_maintain_main_fn(stage):
    print('air_compress_maintain_main_fn start')
    try:
        ############### Generate Inputs #####################
        connect_string = engine.get_connect_string()
        conn = create_engine(connect_string, echo=True)
        # data = pd.DataFrame({'dev_prd':[env]})
        # if data["dev_prd"][0]=='dev':
        #     host0 = "active.eco-ssot-devdev.service.paas.wistron.com"
        #     port0 = "15063"
        # elif data["dev_prd"][0]=='qas':
        #     host0 = "active.eco-ssot-qasdev.service.paas.wistron.com"
        #     port0 = "15118"
        # else:
        #     host0 = "active.eco-ssot-prdprd.service.paas.wistron.com"
        #     port0 = "15068"
        # database0 = "postgres"
        # user0 = ""
        # password0 = ""
        url0 = 'http://10.30.80.134:80/api/v1/service/air-compress-maintain-prd/score'
        api_key0 = 'QrbUVYVGd7sZ313RCZtfRXydtXTw85Ae'
        # year_runtime = 7200
        energy_cost = 0.8
        carbon_efficient = 0.8
        # train_start_time = '2021-09-30 00:00:00'
        building_batch = ['C','F2','Fab12','TB2','TB1','TB5']
        previous_maintain_info_all = pd.DataFrame({})
        previous_maintain_info_summary_all = pd.DataFrame({})
        maintain_info_data_eer_final_all = pd.DataFrame({})
        for x in building_batch:
            # x = 'C'
            # Connect to DB to download data
            # conn = psycopg2.connect(host=host0, port=port0, database=database0, 
            #                         user=user0, password=password0)
            days_period = int(np.where(x=='Fab12',720,np.where(x=='C',540,480))) # fix me
            start_time = str(pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) - pd.Timedelta(days=days_period))
            end_time = str(pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            start_int = str(int(datetime.datetime.timestamp(pd.to_datetime(start_time))*1000))
            end_int = str(int(datetime.datetime.timestamp(pd.to_datetime(end_time))*1000))

            # Connect to DB to download data
            # conn = psycopg2.connect(host=host0, port=port0, database=database0, 
            #                     user=user0, password=password0)
            load_accs_data_query = "SELECT distinct * FROM raw.accs_data where periodend between "+start_int+" and "+end_int+" and building='"+x+"';"
            # load_accs_data_query = "SELECT * FROM raw.accs_data where building='TB2';"
            # load_accs_data_query = "SELECT * FROM raw.accs_data;"
            load_old_info_query = "SELECT * FROM raw.old_machine_info;"
            load_new_info_query = "SELECT * FROM raw.new_machine_info;"
            load_maintain_info_query = "SELECT * FROM raw.old_machine_maintain_info;"

            # Load data from DB
            accs_data = db_data_loader(conn, load_accs_data_query)
            old_info_data = db_data_loader(conn, load_old_info_query)
            # new_info_data = db_data_loader(conn, load_new_info_query)
            maintain_info_data = db_data_loader(conn, load_maintain_info_query)
            print(accs_data.shape)

            print('load data is success')
            # Process data
            maintain_info_data, maintain_info_data_detail = data_processer(accs_data, old_info_data, maintain_info_data)
            print('process data is success')

            # Compute year run hour
            maintain_runtime_info = maintain_info_data_detail[['compress_id','building','periodend','eer','press','temperature','runtime']].drop_duplicates()
            maintain_runtime_info['runtime'] = np.where(maintain_runtime_info.runtime>=1,1,0)
            maintain_runtime_info_summary = maintain_runtime_info.groupby(['compress_id','building']).agg({'runtime':'sum'}).rename(columns={'runtime':'year_runtime'}).reset_index()
            year_day = (max(maintain_runtime_info.periodend)-min(maintain_runtime_info.periodend))/ pd.Timedelta('1 day')
            maintain_runtime_info_summary['year_runtime'] = round(maintain_runtime_info_summary.year_runtime/year_day*365,0)
            print('compute run hour data is success')

            # Generate before/after last maintain data and list
            maintain_info_data_pervious_all, maintain_info_data_pervious_all_list, maintain_info_data_after_all = maintain_previous_after_data_generator(maintain_info_data_detail)
            maintain_info_data_pervious_all_list = maintain_info_data_pervious_all_list.loc[maintain_info_data_pervious_all_list.maintain_type=='小保養',:].reset_index(drop=True) # fix me
            print('generate before or after maintain data is success')

            # Filter eer and baseline before last maintain time
            maintain_info_data_eer_final = maintain_info_data_pervious_all.loc[maintain_info_data_pervious_all.maintain_type=='小保養',:] # fix me
            maintain_info_data_eer_final = maintain_info_data_eer_final[['compress_id','building','periodend','eer_real','last_maintain_time']].loc[maintain_info_data_pervious_all.periodend>=maintain_info_data_pervious_all.last_maintain_time-pd.Timedelta(days=30),:].reset_index(drop=True) # fix me
            maintain_info_data_eer_final['eer_predict']=-1
            maintain_info_data_eer_final['eer_type']='previous'
            # maintain_info_data_eer_final['last_maintain_time']= test_data2.last_maintain_time[0]

            previous_maintain_info_machine = pd.DataFrame({})
            previous_maintain_info_summary_machine = pd.DataFrame({})
            # Compute main recommend result process
            print('compute maintain recommend result is start')
            for row_index in range(maintain_info_data_pervious_all_list.shape[0]):
                # Main process
                time.sleep(30)
                # model_path = 'D:/ESG/空壓機資料/'
                # Specific building and id
                building = maintain_info_data_pervious_all_list.building[row_index]
                compress_id = maintain_info_data_pervious_all_list.compress_id[row_index]
                print(building+'_'+compress_id)
                last_maintain_time = maintain_info_data_pervious_all_list.last_maintain_time[row_index]
                maintain_hour = maintain_info_data_pervious_all_list.maintain_hour[row_index]
                power = maintain_info_data_pervious_all_list.power[row_index]
                maintain_cost = maintain_info_data_pervious_all_list.maintain_cost[row_index]
                maintain_type = maintain_info_data_pervious_all_list.maintain_type[row_index]
                plant = maintain_info_data_pervious_all_list.plant[row_index]
                run_rate = maintain_info_data_pervious_all_list.run_rate[row_index]
                hour_shift = 10

                # Get baseline eer before now
                maintain_info_data_pervious_all_sub = maintain_info_data_pervious_all.loc[(maintain_info_data_pervious_all.building==building) & (maintain_info_data_pervious_all.compress_id==compress_id) & (maintain_info_data_pervious_all.maintain_hour==maintain_hour),:].reset_index(drop=True)
                maintain_info_data_after_all_sub = maintain_info_data_after_all.loc[(maintain_info_data_after_all.building==building) & (maintain_info_data_after_all.compress_id==compress_id) & (maintain_info_data_after_all.last_maintain_time==last_maintain_time) & (maintain_info_data_after_all.maintain_hour==maintain_hour) & (maintain_info_data_after_all.runtime_max<=maintain_hour),:].reset_index(drop=True) # fix me
                # Merge to daily data
                maintain_info_data_pervious_all_sub_merge = maintain_data_daily_merger(maintain_info_data_pervious_all_sub)
                maintain_info_data_after_all_sub_merge = maintain_data_daily_merger(maintain_info_data_after_all_sub)
                # Select nearest 60 days data
                if maintain_info_data_pervious_all_sub_merge.shape[0]<=5:
                    maintain_info_data_pervious_all_sub = maintain_info_data_pervious_all_sub.tail(60)
                else:
                    maintain_info_data_pervious_all_sub = maintain_info_data_pervious_all_sub_merge.tail(60)
                if maintain_info_data_after_all_sub_merge.shape[0]<=5:
                    maintain_info_data_after_all_sub = maintain_info_data_after_all_sub.head(60)
                else:
                    maintain_info_data_after_all_sub = maintain_info_data_after_all_sub_merge.head(60)
                print(maintain_info_data_after_all_sub.shape)

                # Round float columns value 
                for float_col in maintain_info_data_pervious_all_sub.columns[maintain_info_data_pervious_all_sub.dtypes=='float64']:
                    maintain_info_data_pervious_all_sub[float_col] = round(maintain_info_data_pervious_all_sub[float_col],1)
                maintain_info_data_pervious_all_sub = maintain_info_data_pervious_all_sub.drop(columns={'next_maintain_time', 'next_maintain_hour','last_maintain_time_previous', 'last_maintain_time_previous_all','last_maintain_time_after','oil_type','press_type'})
                for float_col in maintain_info_data_after_all_sub.columns[maintain_info_data_after_all_sub.dtypes=='float64']:
                    maintain_info_data_after_all_sub[float_col] = round(maintain_info_data_after_all_sub[float_col],1)
                maintain_info_data_after_all_sub = maintain_info_data_after_all_sub.drop(columns={'next_maintain_time', 'next_maintain_hour','last_maintain_time_previous', 'last_maintain_time_previous_all','last_maintain_time_after','oil_type','press_type'})

                # Forecast Main process
                maintain_info_data_pervious_all_sub = maintain_info_data_pervious_all_sub.astype('string')
                maintain_info_data_pervious_all_json =  json.loads(maintain_info_data_pervious_all_sub.fillna('null').reset_index(drop=True).to_json(orient="records"))
                maintain_info_data_after_all_sub = maintain_info_data_after_all_sub.astype('string')
                maintain_info_data_after_all_json =  json.loads(maintain_info_data_after_all_sub.fillna('null').reset_index(drop=True).to_json(orient="records"))
                input_json = {
                    "maintain_info_data_pervious_all":maintain_info_data_pervious_all_json,
                    "maintain_info_data_after_all":maintain_info_data_after_all_json,
                    "maintain_type":maintain_type,
                    "maintain_hour":str(maintain_hour),
                    "plant":plant,
                    "power": str(power),
                    "maintain_cost":str(maintain_cost),
                    "last_maintain_time":str(last_maintain_time),
                    "building":building,
                    "compress_id":compress_id,
                    "run_rate":run_rate,
                    "hour_shift":hour_shift
                }
                print('Tranform data to API input is end.')
                ############### Call API #####################
                print('Send air compress data to API.')
                outputs = model_api_caller(input_json, url0, api_key0)
                print('Get predict roi data from API.')
                ##############################################
                print('Tranform data to dataframe.')
                # outputs
                # input_json
                # print(inputs)
                outputs = outputs.replace("'",'\"')
                request = json.loads(outputs)
                test_data2_sub_forecast_final = data_type_checker(request['test_data2_sub_forecast_final'])
                previous_maintain_info_all_one = data_type_checker(request['previous_maintain_info_all'])
                previous_maintain_info_summary_one = data_type_checker(request['previous_maintain_info_summary'])
                maintain_info_data_eer_final = maintain_info_data_eer_final.append(test_data2_sub_forecast_final).reset_index(drop=True)
                previous_maintain_info_machine = previous_maintain_info_machine.append(previous_maintain_info_all_one).reset_index(drop=True)
                previous_maintain_info_summary_machine = previous_maintain_info_summary_machine.append(previous_maintain_info_summary_one).reset_index(drop=True)

            print('compute maintain recommend result is end')
            print('maintain recommend data is success')

            # FIXME: failed: 'DataFrame' object has no attribute 'revenue_diff'
            if 'revenue_diff' not in previous_maintain_info_machine:
                continue

            previous_maintain_info_machine = previous_maintain_info_machine.loc[(previous_maintain_info_machine.revenue_diff!=0) & (previous_maintain_info_machine.repaired_roi!=0),:].reset_index(drop=True)
            previous_maintain_info_machine['upload_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            previous_maintain_info_summary_machine['upload_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            maintain_info_data_eer_final['upload_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            maintain_info_data_eer_final_all = maintain_info_data_eer_final_all.append(maintain_info_data_eer_final).reset_index(drop=True)
            previous_maintain_info_all = previous_maintain_info_all.append(previous_maintain_info_machine).reset_index(drop=True)
            previous_maintain_info_all = previous_maintain_info_all.loc[~(previous_maintain_info_all.repaired_roi.isin([-np.inf])) & ~(previous_maintain_info_all.repaired_roi.isin([np.inf])),:].reset_index(drop=True)
            # Compute year benefit
            previous_maintain_info_summary_machine = pd.merge(previous_maintain_info_summary_machine,
                                                              maintain_runtime_info_summary.rename(columns={'compress_id':'machine_id'}),
                                                              on=['building','machine_id'],how='left').reset_index(drop=True)
            previous_maintain_info_summary_machine['year_maintain_revenue'] = previous_maintain_info_summary_machine.revenue_diff_max* \
                                                                              previous_maintain_info_summary_machine.year_runtime/previous_maintain_info_summary_machine.maintain_hour
            previous_maintain_info_summary_machine['year_depower_usage'] = (previous_maintain_info_summary_machine.revenue_diff_max+previous_maintain_info_summary_machine.addition_cost)* \
                                                                              previous_maintain_info_summary_machine.year_runtime/previous_maintain_info_summary_machine.maintain_hour/energy_cost
            previous_maintain_info_summary_machine['year_decarbon_emission'] = (previous_maintain_info_summary_machine.revenue_diff_max+previous_maintain_info_summary_machine.addition_cost)* \
                                                                              previous_maintain_info_summary_machine.year_runtime/previous_maintain_info_summary_machine.maintain_hour*carbon_efficient/energy_cost/1000
            previous_maintain_info_summary_all = previous_maintain_info_summary_all.append(previous_maintain_info_summary_machine).reset_index(drop=True)
        # Modify compress id to machine id
        previous_maintain_info_summary_all = pd.merge(previous_maintain_info_summary_all.rename(columns = {'machine_id':'compress_id'}),
                                                    old_info_data[['building','uid','id']].rename(columns = {'id':'compress_id','uid':'machine_id'}),
                                                    on=['building','compress_id'],how='left')
        previous_maintain_info_summary_all = previous_maintain_info_summary_all.drop(columns={'compress_id'})
        previous_maintain_info_all = pd.merge(previous_maintain_info_all.rename(columns = {'machine_id':'compress_id'}),
                                              old_info_data[['building','uid','id']].rename(columns = {'id':'compress_id','uid':'machine_id'}),
                                              on=['building','compress_id'],how='left')
        previous_maintain_info_all = previous_maintain_info_all.drop(columns={'compress_id'})
        maintain_info_data_eer_final_all['plant']=np.where(maintain_info_data_eer_final_all.building=='C','WKS',
                                                            np.where(maintain_info_data_eer_final_all.building=='F2','WTZ',
                                                                     np.where(maintain_info_data_eer_final_all.building=='Fab12','WOK',
                                                                               np.where(maintain_info_data_eer_final_all.building=='KD','WCQ',
                                                                                        np.where(maintain_info_data_eer_final_all.building=='F1','WCD','WZS')))))
        maintain_info_data_eer_final_all['eer_real'] = np.where(maintain_info_data_eer_final_all.eer_real>=15,15,maintain_info_data_eer_final_all.eer_real)
        maintain_info_data_eer_final_all['eer_predict'] = np.where(maintain_info_data_eer_final_all.eer_predict>=15,15,maintain_info_data_eer_final_all.eer_predict)
        maintain_info_data_eer_final_all['periodend'] = [str(x)[0:10]+' 00:00:00' for x in maintain_info_data_eer_final_all.periodend]
        maintain_info_data_eer_final_all['eer_type'] = maintain_info_data_eer_final_all.eer_type.astype('string')
        maintain_info_data_eer_final_all = maintain_info_data_eer_final_all.groupby(['building','compress_id','periodend','last_maintain_time','eer_type','upload_time','plant']).agg({'eer_real':'mean','eer_predict':'mean'}).reset_index()
        maintain_info_data_eer_final_all = pd.merge(maintain_info_data_eer_final_all,
                                                    old_info_data[['building','uid','id']].rename(columns = {'id':'compress_id','uid':'machine_id'}),
                                                    on=['building','compress_id'],how='left')
        maintain_info_data_eer_final_all = maintain_info_data_eer_final_all.drop(columns={'compress_id'})
        maintain_info_data_eer_final_all = maintain_info_data_eer_final_all.drop_duplicates().reset_index(drop=True)
        ############### Upload dataset #####################
        # Upload data to DB
        data_uploader(previous_maintain_info_all,'app','air_compress_maintain_info')
        data_uploader(previous_maintain_info_summary_all,'app','air_compress_maintain_recommend_info')
        data_uploader(maintain_info_data_eer_final_all,'app','air_compress_maintain_dynamic_info')
        print('upload data is success')
        return 0
    except Exception as e:
        error = str(e)
        mail = MailService('[failed][{}] generate maintain info cron job report'.format(stage))
        if error=='Expecting value: line 1 column 1 (char 0)':
            mail.send('failed: {}'.format('AAW API 目前有問題無法正常使用'))
        else:
            mail.send('failed: {}'.format(error))
        return error
    print('air_compress_maintain_main_fn end')