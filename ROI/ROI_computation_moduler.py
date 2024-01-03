#!/usr/bin/python
import datetime
import math
import numpy as np
from numpy import arange
import pandas as pd
import psycopg2
import pickle
import re
import random
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sqlalchemy import create_engine
import urllib.request
import json
import requests
import ssl
from services.mail_service import MailService
from models import engine
# import statsmodels.api as sm


def db_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql, con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result


def lm_model_predictor(train_data, test_data, target_name, sig_feature, inter_index, path):
    # train_model_lm = sm.OLS(train_data[target_name], train_data[sig_feature]).fit()
    model_file_name = path + \
        train_data.building[0]+'_' + \
        re.sub('#|AC', '', train_data.id[0])+'_linear_model.pickle'
    with open(model_file_name, 'rb') as f:
        train_model_lm = pickle.load(f)
    # train_model_lm = LinearRegression(fit_intercept=inter_index).fit(train_data[sig_feature],train_data[target_name])
    # train_model.summary()
    # print(train_data[sig_feature])
    # print(test_data[sig_feature])
    train_lm = train_model_lm.predict(train_data[sig_feature])
    test_lm = train_model_lm.predict(test_data[sig_feature])
    # rsq_lm = train_model_lm.rsquared_adj
    rsq_lm = r2_score(train_data[target_name], train_lm)
    train_mape = np.mean(np.abs(train_lm-train_data[target_name])/np.where(
        train_data[target_name] == 0, 0.1, train_data[target_name]))
    test_mape = np.mean(np.abs(test_lm-test_data[target_name])/np.where(
        test_data[target_name] == 0, 0.1, test_data[target_name]))
    return train_model_lm, train_lm, test_lm, rsq_lm, train_mape, test_mape


def data_loader(accs_data, air_compress_data, new_air_compress_data, start_time, end_time):
    # Load accs data
    # accs_data = pd.read_csv(accs_path)
    accs_data = accs_data.drop_duplicates().reset_index(drop=True)
    accs_data.power = np.where((accs_data.building == 'TB2') & (accs_data.id.isin(['AC'+str(x) for x in range(17)])), 75,
                               np.where((accs_data.building == 'TB2') & (accs_data.id.isin(['AC17', 'AC18'])), 130,
                                        np.where(((accs_data.building == 'TB2') & (accs_data.id.isin(['AC12', 'AC15']))) | ((accs_data.building == 'C') & (accs_data.id.isin(['9#', '10#']))), 110,
                                                 np.where(((accs_data.building == 'F2') & (accs_data.id.isin(['1#', '2#']))), 250,
                                                          np.where(((accs_data.building == 'F2') & (accs_data.id.isin(['3#', '4#']))), 132,
                                                                   np.where((accs_data.building == 'TB5'), 75, accs_data.power))))))
    accs_data = accs_data.loc[(~accs_data.id.isin(['AC12', 'AC15'])), :]
    accs_data = pd.merge(accs_data,
                         accs_data.groupby(['id', 'building', 'periodend']).agg({'energy': 'max', 'press': 'max'}).reset_index().
                         rename(
                             columns={'energy': 'energy_max', 'press': 'press_max'}),
                         on=['id', 'building', 'periodend'], how='left')
    accs_data = (accs_data.loc[(accs_data.energy == accs_data.energy_max) & (
        accs_data.press == accs_data.press_max), :]).drop(columns=['energy_max', 'press_max']).reset_index(drop=True)
    accs_data['periodend'] = pd.to_datetime(
        accs_data.periodend, unit='ms', origin='1970-01-01') + pd.Timedelta(hours=8)  # - pd.Timedelta(hours=6)
    accs_data = accs_data.loc[((accs_data.building != 'F1') & (accs_data.building != 'TB3') & (accs_data.building != 'TB5') & (accs_data.flow != 0)) | ((accs_data.building == 'F1') & (accs_data.eer >= 2) & (accs_data.eer <= 15) & (accs_data.press > 0) & (accs_data.temperature > 0) & (accs_data.runtime <= 1))
                              | ((accs_data.building == 'TB3') & (accs_data.eer >= 2) & (accs_data.eer <= 15) & (accs_data.press > 0) & (accs_data.temperature > 0) & (accs_data.runtime <= 1))
                              | ((accs_data.building == 'TB5') & (accs_data.eer >= 2) & (accs_data.eer <= 15) & (accs_data.press > 0) & (accs_data.temperature > 0) & (accs_data.runtime <= 1)), :].drop_duplicates().reset_index(drop=True)  # fix me

    accs_data['energy_check'] = np.where(accs_data.energy != 0, 1, 0)
    # Machine altenative runtime computation
    accs_data_summary = accs_data.groupby(['building', 'id']).agg({'energy_check': 'sum', 'periodend': 'size', 'power': 'mean'}).reset_index(). \
        rename(columns={'energy_check': 'energy_check_sum',
               'periodend': 'total_cnt'})
    accs_data_summary = accs_data_summary.iloc[np.where(
        [x.find('#10') == -1 for x in accs_data_summary.id])[0], :]
    accs_data_summary['runtime_predict'] = round(
        7200*accs_data_summary.energy_check_sum/accs_data_summary.total_cnt, 0)
    accs_data_summary['plant'] = np.where(accs_data_summary.building == 'C', 'WKS',
                                          np.where(accs_data_summary.building == 'F2', 'WTZ',
                                                   np.where(accs_data_summary.building == 'Fab12', 'WOK',
                                                            np.where(accs_data_summary.building == 'KD', 'WCQ',
                                                                     np.where(accs_data_summary.building == 'F1', 'WCD',
                                                                              np.where(accs_data_summary.building == 'P1', 'WMY-1', 'WZS'))))))
    accs_data_summary_near = accs_data.loc[(accs_data.periodend >= start_time) & (accs_data.periodend < end_time), :].groupby(['building', 'id']).agg({'energy_check': 'sum'}).reset_index(). \
        rename(columns={'energy_check': 'energy_check_near'})
    accs_data_summary = pd.merge(accs_data_summary,
                                 accs_data_summary_near,
                                 on=['building', 'id'], how='left')
    accs_data_summary = accs_data_summary.loc[accs_data_summary.energy_check_near > 15, :].reset_index(
        drop=True)

    # Choose relative machine
    accs_data_summary_alt = accs_data_summary[['building', 'id', 'power', 'runtime_predict', 'total_cnt']].copy().rename(
        columns={'id': 'id_alt', 'power': 'power_alt', 'runtime_predict': 'runtime_predict_alt', 'total_cnt': 'total_cnt_alt'})
    # 500
    accs_data_summary_alt = accs_data_summary_alt.loc[accs_data_summary_alt.total_cnt_alt > 20, :]
    accs_data_summary_alt = pd.merge(accs_data_summary,
                                     accs_data_summary_alt,
                                     on=['building'], how='left')
    # accs_data_summary_alt = accs_data_summary_alt.assign(runtime_predict_max = lambda df: df.groupby(['building']).runtime_predict.transform('max'))
    # accs_data_summary_alt_more = accs_data_summary_alt.loc[(accs_data_summary_alt.id!=accs_data_summary_alt.id_alt) & (accs_data_summary_alt.power==accs_data_summary_alt.power_alt)  & ((accs_data_summary_alt.runtime_predict<=accs_data_summary_alt.runtime_predict_alt) | ((accs_data_summary_alt.runtime_predict-2000<=accs_data_summary_alt.runtime_predict_alt) & (accs_data_summary_alt.runtime_predict==accs_data_summary_alt.runtime_predict_max))),:].reset_index(drop=True)
    accs_data_summary_alt = (accs_data_summary_alt.loc[(accs_data_summary_alt.id != accs_data_summary_alt.id_alt) & (
        accs_data_summary_alt.power == accs_data_summary_alt.power_alt), :]).reset_index(drop=True)
    accs_data_summary_alt = accs_data_summary_alt.assign(runtime_predict_max=lambda df: df.groupby(
        ['building', 'id']).runtime_predict_alt.transform('max'))
    accs_data_summary_alt_more = accs_data_summary_alt.loc[(accs_data_summary_alt.id != accs_data_summary_alt.id_alt) & (
        accs_data_summary_alt.power == accs_data_summary_alt.power_alt) & (accs_data_summary_alt.runtime_predict_alt == accs_data_summary_alt.runtime_predict_max), :].reset_index(drop=True)

    # Choose min run time
    accs_data_summary_alt_more = accs_data_summary_alt_more.assign(
        runtime_predict_alt_min=lambda df: df.groupby(['building', 'id']).runtime_predict_alt.transform('min'))
    accs_data_summary_alt_more = accs_data_summary_alt_more.loc[accs_data_summary_alt_more.runtime_predict_alt ==
                                                                accs_data_summary_alt_more.runtime_predict_alt_min, :].reset_index(drop=True)
    accs_data_summary_alt_more['runtime_rate'] = accs_data_summary_alt_more.runtime_predict/(
        accs_data_summary_alt_more.runtime_predict+accs_data_summary_alt_more.runtime_predict_alt)
    accs_data_summary_alt_more['total_cnt_alt'] = accs_data_summary_alt_more['total_cnt_alt'].astype(
        'int')
    # Load air compress data
    # air_compress_data = pd.read_excel(air_compress_path)
    air_compress_data = air_compress_data.loc[(air_compress_data.id != -1) & (
        ~air_compress_data['power_r'].astype('int').isin([220])), :]  # 132
    # Load new air compress data
    # new_air_compress_data = pd.read_excel(new_air_compress_path)
    # Join new and old data
    air_compress_data = pd.merge(air_compress_data,
                                 new_air_compress_data,
                                 on=['oil_type'], how='left')  # 'power_r'
    air_compress_data = air_compress_data.loc[(air_compress_data.power_r_x-air_compress_data.power_r_y < 10) & (
        air_compress_data.power_r_x-air_compress_data.power_r_y >= 0), :].reset_index(drop=True)
    air_compress_data = air_compress_data.loc[((air_compress_data.flow_r_x-air_compress_data.flow_r_y < 5) & (
        air_compress_data.building == 'P1')) | ~(air_compress_data.building == 'P1'), :].reset_index(drop=True)
    air_compress_data['power_r'] = air_compress_data.power_r_x
    air_compress_data = air_compress_data.drop(
        columns={'power_r_x', 'power_r_y'})
    # air_compress_data['building']=np.where(air_compress_data['plant']=='WKS','C',
    #                                 np.where(air_compress_data['plant']=='WTZ','F2',
    #                                         np.where(air_compress_data['plant']=='WOK','Fab12','TB2')))
    return accs_data, accs_data_summary_alt_more, air_compress_data


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
    data.to_sql(table_name, conn, index=False, if_exists='append',
                schema=db_name, chunksize=10000)
    return 0


def model_api_caller(data, url, api_key):
    payload = json.dumps(data)
    headers = {
        'Authorization': 'Bearer ' + api_key,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers,
                                 data=payload, verify=False)
        result = response.text
        # print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))
    return result


def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null', np.nan)
    for x in dataset.columns:
        if x in ['id', 'building', 'plant', 'id_alt', 'uid', 'oil_type', 'press_type_x', 'brand', 'press_type_y', 'machine_code', 'other',
                 'compress_id', 'machine_id', 'machine_id_rec', 'run_type', 'compress_type', 'compress_id_alt', 'machine_id_alt']:
            dataset[x] = dataset[x].astype('string')
        elif x in ['periodend', 'datetime']:
            dataset[x] = [datetime.datetime.strptime(
                y, '%Y-%m-%d %H:%M:%S') for y in dataset[x]]
        elif x in ['energy_check', 'energy_check_sum', 'total_cnt', 'total_cnt_alt', 'year_r', 'power_r', 'cost']:
            dataset[x] = dataset[x].astype('float').astype('int')
        else:
            dataset[x] = dataset[x].astype('float')
    return dataset


def air_compress_roi_main_fn(stage):
    print('air_compress_roi_main_fn start')
    try:
        print('Upload data is start!')
        ############### Generate Inputs #####################
        connect_string = engine.get_connect_string()
        conn = create_engine(connect_string, echo=True)
        url0 = 'http://10.30.80.134:80/api/v1/service/air-compress-roi-prd/score'
        api_key0 = 'ZYV9aghwLxgYwsNJ9Cn2Hzgwif79ETe0'
        year_runtime = 7200
        energy_cost = 0.8
        train_start_time = '2021-09-30 00:00:00'
        start_time = str(pd.to_datetime(datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S')) - pd.Timedelta(days=240))
        end_time = str(pd.to_datetime(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        start_int = str(int(datetime.datetime.timestamp(
            pd.to_datetime(start_time))*1000))
        end_int = str(int(datetime.datetime.timestamp(
            pd.to_datetime(end_time))*1000))

        # Connect to DB to download data
        print('Download machine info from DB.')
        # conn = psycopg2.connect(host=host0, port=port0, database=database0,
        #                     user=user0, password=password0)
        load_accs_data_query = "SELECT distinct * FROM raw.accs_data where periodend between " + \
            start_int+" and "+end_int+";"
        load_old_info_query = "SELECT * FROM raw.old_machine_info;"
        load_new_info_query = "SELECT * FROM raw.new_machine_info;"
        # Load data from DB
        accs_data = db_data_loader(conn, load_accs_data_query)
        old_info_data = db_data_loader(conn, load_old_info_query)
        new_info_data = db_data_loader(conn, load_new_info_query)
        print('Download machine info from DB is end.')
        print('Tranform data to API input.')
        accs_data, accs_data_summary, air_compress_data = data_loader(
            accs_data, old_info_data, new_info_data, start_time, end_time)
        accs_data = accs_data.astype('string')
        accs_data_summary = accs_data_summary.astype('string')
        air_compress_data = air_compress_data.astype('string')
        # accs_data_json =  json.loads(accs_data.fillna('null').to_json(orient="records"))
        # accs_data_summary_json = json.loads(accs_data_summary.fillna('null').to_json(orient="records"))
        # air_compress_data_json = json.loads(air_compress_data.fillna('null').to_json(orient="records"))

        # building_batch = [['C'],['F2'],['Fab12'],['TB2'],['TB1'],['TB5'],['KD']]

        building_batch = [[x] for x in np.unique(accs_data_summary.loc[(
            accs_data_summary.power != '220.0'), :].building)]  # fix me

        # ignore invalid building
        skip_building = ['KD']
        building_batch = list(
            filter(lambda x: x[0] not in skip_building, building_batch))

        all_eer_roi = pd.DataFrame({})
        old_air_compress_rec_list = pd.DataFrame({})
        new_air_compress_rec_list = pd.DataFrame({})
        old_air_compress_year_cost = pd.DataFrame({})
        new_air_compress_year_cost = pd.DataFrame({})
        for x in building_batch:
            print(x)
            accs_data_json = json.loads(accs_data.fillna('null').loc[accs_data['building'].isin(
                x), :].reset_index(drop=True).to_json(orient="records"))
            accs_data_summary_json = json.loads(accs_data_summary.fillna(
                'null').loc[accs_data_summary['building'].isin(x), :].reset_index(drop=True).to_json(orient="records"))
            air_compress_data_json = json.loads(air_compress_data.fillna(
                'null').loc[air_compress_data['building'].isin(x), :].reset_index(drop=True).to_json(orient="records"))

            input_json = {
                "year_runtime": year_runtime,
                "energy_cost": energy_cost,
                "train_start_time": train_start_time,
                "start_time": start_time,
                "end_time": end_time,
                "accs_data": accs_data_json,
                "accs_data_summary": accs_data_summary_json,
                "air_compress_data": air_compress_data_json
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
            outputs = outputs.replace("'", '\"')
            request = json.loads(outputs)
            all_eer_roi_sub = data_type_checker(request['all_eer_roi'])
            old_air_compress_rec_list_sub = data_type_checker(
                request['old_air_compress_rec_list'])
            new_air_compress_rec_list_sub = data_type_checker(
                request['new_air_compress_rec_list'])
            old_air_compress_year_cost_sub = data_type_checker(
                request['old_air_compress_year_cost'])
            new_air_compress_year_cost_sub = data_type_checker(
                request['new_air_compress_year_cost'])
            all_eer_roi = all_eer_roi.append(
                all_eer_roi_sub).reset_index(drop=True)
            old_air_compress_rec_list = old_air_compress_rec_list.append(
                old_air_compress_rec_list_sub).reset_index(drop=True)
            new_air_compress_rec_list = new_air_compress_rec_list.append(
                new_air_compress_rec_list_sub).reset_index(drop=True)
            old_air_compress_year_cost = old_air_compress_year_cost.append(
                old_air_compress_year_cost_sub).reset_index(drop=True)
            new_air_compress_year_cost = new_air_compress_year_cost.append(
                new_air_compress_year_cost_sub).reset_index(drop=True)

        # input_json = {
        #     "year_runtime":year_runtime,
        #     "energy_cost":energy_cost,
        #     "train_start_time":train_start_time,
        #     "start_time":start_time,
        #     "end_time":end_time,
        #     "accs_data": accs_data_json,
        #     "accs_data_summary":accs_data_summary_json,
        #     "air_compress_data":air_compress_data_json
        # }
        # ############### Call API #####################
        # outputs = model_api_caller(input_json, url0, api_key0)
        # ##############################################
        # outputs = outputs.replace("'",'\"')
        # request = json.loads(outputs)
        # all_eer_roi = data_type_checker(request['all_eer_roi'])
        all_eer_roi.eer = np.where(all_eer_roi.eer > 15, 15, all_eer_roi.eer)
        all_eer_roi.roi = np.where(all_eer_roi.roi > 20, 20, all_eer_roi.roi)
        all_eer_roi = pd.merge(all_eer_roi, accs_data_summary[['building', 'id', 'runtime_rate', 'power']].rename(columns={'id': 'compress_id'}),
                               on=['building', 'compress_id'], how='left').reset_index(drop=True)
        all_eer_roi['power'] = all_eer_roi['power'].astype(float).astype(int)
        all_eer_roi['rank'] = all_eer_roi.groupby(['building', 'machine_id'])[
            'datetime'].rank(ascending=False, method='dense')
        # all_eer_roi = pd.merge(all_eer_roi,
        #                        all_eer_roi[['building','machine_id','compress_id','rank','eer']].rename(columns={'eer':'eer_alt','compress_id':'compress_id_alt','machine_id':'machine_id_alt'}),
        #                        on=['building','rank','compress_id_alt'],how='inner')
        # all_eer_roi = all_eer_roi.drop(columns=['rank','compress_id_alt'])
        all_eer_roi = pd.merge(all_eer_roi,
                               all_eer_roi[['machine_id', 'machine_id_alt', 'eer', 'eer_alt']].groupby(['machine_id', 'machine_id_alt']).agg({'eer': 'median', 'eer_alt': 'median'}).reset_index().
                               rename(
                                   columns={'eer': 'eer_median', 'eer_alt': 'eer_alt_median'}),
                               on=['machine_id', 'machine_id_alt'], how='left')
        all_eer_roi = all_eer_roi.drop(
            columns={'compress_id_alt', 'roi_alt', 'rank'})
        # old_air_compress_rec_list = data_type_checker(request['old_air_compress_rec_list'])
        # new_air_compress_rec_list = data_type_checker(request['new_air_compress_rec_list'])
        # old_air_compress_year_cost = data_type_checker(request['old_air_compress_year_cost'])
        # new_air_compress_year_cost = data_type_checker(request['new_air_compress_year_cost'])

        all_eer_roi['upload_time'] = datetime.datetime.now().strftime(
            '%Y-%m-%d')
        old_air_compress_rec_list['upload_time'] = datetime.datetime.now().strftime(
            '%Y-%m-%d')
        new_air_compress_rec_list = new_air_compress_rec_list.loc[new_air_compress_rec_list.predict_roi > 0, :].reset_index(
            drop=True)
        new_air_compress_rec_list['upload_time'] = datetime.datetime.now().strftime(
            '%Y-%m-%d')
        new_air_compress_rec_list = new_air_compress_rec_list.drop(
            columns={'energy_check_sum'})
        # Add maintain rule
        new_air_compress_rec_list['predict_per'] = new_air_compress_rec_list.predict_eer / \
            new_air_compress_rec_list.eer
        new_air_compress_rec_list['per_upper_bound'] = 8.7 / \
            new_air_compress_rec_list.eer
        new_air_compress_rec_list['per_lower_bound'] = 7.8 / \
            new_air_compress_rec_list.eer
        new_air_compress_rec_list['recommend_result'] = np.where((new_air_compress_rec_list.predict_per < new_air_compress_rec_list.per_lower_bound) & (new_air_compress_rec_list.predict_roi < 3), '屬於汰換',
                                                                 np.where((new_air_compress_rec_list.predict_per > new_air_compress_rec_list.per_upper_bound), '運行良好', '維護保養'))
        new_air_compress_rec_list = pd.merge(new_air_compress_rec_list,
                                             air_compress_data[['building', 'uid', 'brand', 'machine_code']].rename(
                                                 columns={'uid': 'machine_id'}),
                                             on=['building', 'machine_id'], how='left')
        old_air_compress_year_cost['upload_time'] = datetime.datetime.now().strftime(
            '%Y-%m-%d')
        new_air_compress_year_cost = new_air_compress_year_cost.loc[new_air_compress_year_cost.predict_roi > 0, :].reset_index(
            drop=True)
        new_air_compress_year_cost['upload_time'] = datetime.datetime.now().strftime(
            '%Y-%m-%d')
        new_air_compress_year_cost = new_air_compress_year_cost.drop(
            columns={'energy_check_sum'})
        print('Tranform data to dataframe is end.')
        ############### Upload dataset #####################
        print('Upload roi data is start.')
        # Upload data to DB
        data_uploader(all_eer_roi, 'app', 'air_compress_info')
        data_uploader(old_air_compress_rec_list, 'app',
                      'old_air_compress_rec_list')
        data_uploader(new_air_compress_rec_list, 'app',
                      'new_air_compress_rec_list')
        data_uploader(old_air_compress_year_cost, 'app',
                      'old_air_compress_rec_cost')
        data_uploader(new_air_compress_year_cost, 'app',
                      'new_air_compress_rec_cost')

        # Upload air compress history data
        print('Upload air compressor history data is start.')
        ac_history_new = new_air_compress_rec_list[['building', 'machine_id', 'predict_roi',
                                                    'plant', 'born_year', 'power', 'predict_eer', 'recommend_result', 'upload_time']]
        # Load data from DB
        load_ac_history_query = "SELECT * FROM app.air_compress_info_history;"
        ac_history = db_data_loader(conn, load_ac_history_query)
        if datetime.datetime.now().strftime('%Y-%m-%d') == (max(ac_history.upload_time) + pd.Timedelta(days=28)).strftime('%Y-%m-%d'):
            ac_history_max = ac_history.loc[ac_history.upload_time == max(
                ac_history.upload_time), :].drop(columns={'id'}).reset_index(drop=True)
            ac_history_info = ac_history_max.append(
                ac_history_new).reset_index(drop=True)
            data_uploader(ac_history_info, 'app', 'air_compress_info_history')
        # Upload backup data to DB
        ac_history_new = new_air_compress_rec_list[['building', 'machine_id', 'predict_roi',
                                                    'plant', 'born_year', 'power', 'predict_eer', 'recommend_result', 'upload_time']]
        ac_history_new.to_sql('air_compress_info_history_detail', conn,
                              index=False, if_exists='append', schema='app', chunksize=10000)
        conn.execute(f'DELETE FROM '+'app'+'.'+'air_compress_info_history_detail'+" where upload_time < '" +
                     (max(ac_history.upload_time) - pd.Timedelta(days=90)).strftime('%Y-%m-%d')+"';")
        print('Upload air compressor history data is end.')
        print('Upload roi data is finished!')
        return 0
    except Exception as e:
        error = str(e)
        print(error)
        # mail = MailService('[failed][{}] compute roi cron job report'.format(stage))
        # mail.send('failed: {}'.format(error))
        # return error
    print('air_compress_roi_main_fn end')
