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
                                                           np.where(accs_data_summary.building=='KD','WCQ','WZS'))))
    accs_data_summary = accs_data_summary[['building','id','run_rate']].rename(columns={'id':'compress_id'})
    # Process for old_info_data
    air_compress_data['eer_r'] = air_compress_data.flow_r*60/air_compress_data.power_r
    # Process for maintain info data
    maintain_info_data = maintain_info_data.loc[maintain_info_data.last_maintain_hour!=-1,:]
    # maintain_info_data_sub['last_maintain_hour'] = maintain_info_data_sub.next_maintain_hour - maintain_info_data_sub.maintain_hour/maintain_info_data_sub.run_rate
    maintain_info_data['last_maintain_time_previous'] = np.where(maintain_info_data.building=='Fab12',
                                                                 maintain_info_data.last_maintain_time - pd.Timedelta(days=14),
                                                                 maintain_info_data.last_maintain_time - pd.Timedelta(days=35))
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

def maintain_recommender(maintain_info_data_summary):
    previous_maintain_info_all = pd.DataFrame({})
    previous_maintain_info_summary_all = pd.DataFrame({})
    for x in range(maintain_info_data_summary.shape[0]):
        maintain_info_data_summary_sub = maintain_info_data_summary.loc[x,:]
        period_index = maintain_info_data_summary_sub.maintain_hour/500
        # for y in range(int(period_index)):
        repaired_cost = np.array([maintain_info_data_summary_sub.maintain_revenue*(1-y/period_index) for y in range(int(period_index)+1)])
        repaired_roi = np.array([maintain_info_data_summary_sub.maintain_cost*(1/(1-y/period_index)-1) for y in range(int(period_index)+1)])/repaired_cost*500
        revenue_cost = np.cumsum(np.append([0],-1*np.diff(repaired_cost/2)))
        addition_cost = np.array([maintain_info_data_summary_sub.maintain_cost*(1/(1-y/period_index)-1) for y in range(int(period_index)+1)])
        revenue_diff = revenue_cost - addition_cost
        previous_maintain_info = pd.DataFrame({'plant':maintain_info_data_summary_sub.plant,
                                               'building':maintain_info_data_summary_sub.building,
                                               'machine_id':maintain_info_data_summary_sub.machine_id,
                                               'maintain_type':maintain_info_data_summary_sub.maintain_type,
                                               'maintain_hour':maintain_info_data_summary_sub.maintain_hour,
                                               'previous_hour':np.arange(0,int(period_index)+1,1)*500, 
                                               'addition_cost':addition_cost, 'revenue_cost':revenue_cost, 
                                               'revenue_diff':revenue_diff, 'repaired_roi':repaired_roi[::-1], 
                                               'eer_diff_seq':(np.arange(0,int(period_index)+1,1)/np.median(np.arange(0,int(period_index)+1,1)))*maintain_info_data_summary_sub.eer_diff,
                                               'revenue_diff_seq':(np.arange(0,int(period_index)+1,1)/np.median(np.arange(0,int(period_index)+1,1)))*max(revenue_diff)})
        previous_maintain_info_all = previous_maintain_info_all.append(previous_maintain_info).reset_index(drop=True)
        previous_maintain_info_summary = previous_maintain_info.groupby(['plant','building','machine_id','maintain_type','maintain_hour']).agg({'revenue_diff':'max'}).reset_index(). \
                                                                    rename(columns={'revenue_diff':'revenue_diff_max'})
        previous_maintain_info_summary['best_previous_hour'] = previous_maintain_info.previous_hour[np.where(previous_maintain_info.revenue_diff==max(revenue_diff))[0][0]]
        previous_maintain_info_summary['best_previous_hour'] = np.where(previous_maintain_info_summary.best_previous_hour<=0,
                                                                        0,
                                                                        previous_maintain_info_summary.best_previous_hour)
        now_pass_hour = (pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) -pd.to_datetime(maintain_info_data_summary_sub.last_maintain_time)).days*24
        now_pass_hour = now_pass_hour*np.where(maintain_info_data_summary_sub.run_rate>1,1,maintain_info_data_summary_sub.run_rate)
        previous_maintain_info_summary['now_pass_hour'] = previous_maintain_info_summary.maintain_hour-previous_maintain_info_summary.best_previous_hour-now_pass_hour
        previous_maintain_info_summary['revenue_rate'] = previous_maintain_info_summary.revenue_diff_max/maintain_info_data_summary_sub.maintain_cost
        previous_maintain_info_summary['best_maintain_time'] = np.where(previous_maintain_info_summary.now_pass_hour<=0,pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d')),
                                                                       pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d')) + pd.Timedelta(days=int(previous_maintain_info_summary.now_pass_hour/maintain_info_data_summary_sub.run_rate/24)))
        previous_maintain_info_summary_all = previous_maintain_info_summary_all.append(previous_maintain_info_summary).reset_index(drop=True)
    previous_maintain_info_all['check_roi'] = previous_maintain_info_all.repaired_roi-previous_maintain_info_all.previous_hour
    previous_maintain_info_all_summary_gmin = pd.merge(previous_maintain_info_all[['plant','building','machine_id','maintain_type','previous_hour','repaired_roi','check_roi']],
                                                       previous_maintain_info_all.groupby(['plant','building','machine_id','maintain_type']).agg({'check_roi':gmin}).reset_index(),
                                                       on=['plant','building','machine_id','maintain_type','check_roi'],how='inner').rename(columns={'previous_hour':'previous_hour_gmin','check_roi':'check_roi_gmin','repaired_roi':'repaired_roi_gmin'})
    previous_maintain_info_all_summary_lmax = pd.merge(previous_maintain_info_all[['plant','building','machine_id','maintain_type','previous_hour','repaired_roi','check_roi']],
                                                       previous_maintain_info_all.groupby(['plant','building','machine_id','maintain_type']).agg({'check_roi':lmax}).reset_index(),
                                                       on=['plant','building','machine_id','maintain_type','check_roi'],how='inner').rename(columns={'previous_hour':'previous_hour_lmax','check_roi':'check_roi_lmax','repaired_roi':'repaired_roi_lmax'})
    previous_maintain_info_all_summary = pd.merge(previous_maintain_info_all_summary_lmax,previous_maintain_info_all_summary_gmin,
                                                  on=['plant','building','machine_id','maintain_type'],how='left')
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
        previous_maintain_info_summary_all = previous_maintain_info_summary_all.loc[previous_maintain_info_summary_all.maintain_type=='小保養',:].reset_index(drop=True)
    else:
        previous_maintain_info_all.maintain_type='小保養'
        previous_maintain_info_summary_all.maintain_type='小保養'
    return previous_maintain_info_all, previous_maintain_info_summary_all

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
        response = requests.post(url, headers=headers, data=payload)
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
        # url0 = 'https://aaw-endpoint.wistron.com/prd/api/v1/service/air-compress-roi-prd/score'
        # api_key0 = 'Yl76cvmkLXIof7iqq3i71ezL1wXmEcZt'
        # year_runtime = 7200
        energy_cost = 0.8
        # train_start_time = '2021-09-30 00:00:00'
        building_batch = ['C','F2','Fab12','TB2','TB1','TB5']
        previous_maintain_info_all = pd.DataFrame({})
        previous_maintain_info_summary_all = pd.DataFrame({})
        for x in building_batch:
            print(x)
            days_period = int(np.where(x=='Fab12',720,300)) # fix me
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

            print('load data is success')
            # Process data
            maintain_info_data, maintain_info_data_detail = data_processer(accs_data, old_info_data, maintain_info_data)
            print('process data is success')
            # Compute maintain improved eer
            maintain_info_data_summary = maintain_eer_improved_computer(maintain_info_data, maintain_info_data_detail, energy_cost)
            print('compute maintain eer difference is success')
            # Maintain recommendation
            previous_maintain_info_sub, previous_maintain_info_summary_sub = maintain_recommender(maintain_info_data_summary)
            print('maintain recommend data is success')
            previous_maintain_info_sub = previous_maintain_info_sub.loc[(previous_maintain_info_sub.revenue_diff!=0) & (previous_maintain_info_sub.repaired_roi!=0),:].reset_index(drop=True)
            previous_maintain_info_sub['upload_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            previous_maintain_info_summary_sub['upload_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            previous_maintain_info_all = previous_maintain_info_all.append(previous_maintain_info_sub).reset_index(drop=True)
            previous_maintain_info_summary_all = previous_maintain_info_summary_all.append(previous_maintain_info_summary_sub).reset_index(drop=True)

        ############### Upload dataset #####################
        # Upload data to DB
        data_uploader(previous_maintain_info_all,'app','air_compress_maintain_info')
        data_uploader(previous_maintain_info_summary_all,'app','air_compress_maintain_recommend_info')
        print('upload data is success')
        return 0
    except Exception as e:
        error = str(e)
        return error
    print('air_compress_maintain_main_fn end')