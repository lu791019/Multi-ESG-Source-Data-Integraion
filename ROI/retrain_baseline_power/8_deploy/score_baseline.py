import json
import math
import numpy as np
import pickle
import pandas as pd
import os
import time
import xgboost as xgb
from azureml.core import Dataset,Workspace
from azureml.core.authentication import ServicePrincipalAuthentication
from datetime import datetime
from inference_schema.schema_decorators import input_schema, output_schema
from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType

def init():
    global model_path, ws
    model_path = os.getenv("AZUREML_MODEL_DIR")  
    
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
    
###---------------------------------預測API接口定義(可為檔案)---------------------------------###
# input_sample={"dev_prd":"dev"}
# output_sample=np.array([0])
# @input_schema('data',StandardPythonParameterType(input_sample))
# @output_schema(NumpyParameterType(output_sample))

def run(inputs):
  
    try:
    ###---------------------------------模型預測前處理---------------------------------###
        inputs  = inputs.replace("'",'\"')
        request = json.loads(inputs)
        data_result = data_type_checker(request['data_result'])
        data_result_history = data_type_checker(request['data_result_history'])
        predict_data_result = data_type_checker(request['predict_data_result']) 
        # best_model_list = data_type_checker(request['best_model_list']) 改成 Dataset.get_by_name        
        
        data_result_plant     = data_result.plant.unique()[0]
        data_result_powertype = data_result.power_type.unique()[0] 
        predict_data_result_plant     = predict_data_result.plant.unique()[0] 
        predict_data_result_powertype = predict_data_result.power_type.unique()[0] 
        input_predict_type = data_result.predict_type.unique()[0]
        power_predict_name = str(np.where(data_result_powertype=='空調用電（kwh）','ac_electricity',
                                          np.where(data_result_powertype=='空壓用電（kwh）','ap_electricity',
                                                  np.where(data_result_powertype=='生產用電（kwh）','production_electricity',
                                                           np.where(data_result_powertype=='基礎用電（kwh）','base_electricity','predict_electricity')))))
        
        best_model_list = Dataset.get_by_name(workspace=ws, name=(f"{power_predict_name}-{data_result_plant}-best-perf").replace('_','-')).to_pandas_dataframe()
        print(f"Going to predict {input_predict_type.upper()} DATA of {data_result_plant}.\n Load data is success")
   
        data_upload = pd.DataFrame({})
        if input_predict_type == 'baseline':
            # Process data
            data_result_sub, data_result_sub_temp = data_processor(data_result, data_result_history, 'baseline', data_result_powertype, best_model_list) 
            print(f"Going to predict {data_result_plant.upper()} DATA of {data_result_plant}.")
            # Model Prediction in baseline data
            for row_index in range(data_result_sub.shape[0]):
                plant = data_result_sub['plant'][row_index]
                powertype = data_result_sub['power_type'][row_index] #note: 因為模型是分廠別和power type建置，故調用時需要再多從資料中抓取power type參數
                # print(plant)
                # Model prediction
                data_result_part_upload = model_predictor(pd.DataFrame(data_result_sub.iloc[row_index,:]).T.reset_index(drop=True), 
                                                          data_result_sub_temp, model_path, plant, 'baseline', powertype)
                data_upload = data_upload.append(data_result_part_upload)
        elif input_predict_type == 'predict':
            predict_data_result_sub, predict_data_result_sub_temp = data_processor(predict_data_result, data_result_history, 'predict', predict_data_result_powertype, best_model_list) 
            # Model Prediction in predict baseline data
            for row_index in range(predict_data_result_sub.shape[0]):
                # Plant
                plant = predict_data_result_sub['plant'][row_index]
                powertype = predict_data_result_sub['power_type'][row_index] #note: 因為模型是分廠別和power type建置，故調用時需要再多從資料中抓取power type參數
                # print(plant)
                # Model prediction
                if predict_data_result_sub['日期'].astype(str)[row_index][5:7]=='11':
                    data_result_part_upload = model_predictor(pd.DataFrame(predict_data_result_sub.iloc[row_index:(row_index+2),:]).reset_index(drop=True), 
                                                              predict_data_result_sub_temp, model_path, plant, 'predict', powertype)
                    data_upload = data_upload.append(data_result_part_upload)
        print(f"data_upload is successful:\n {data_upload}")
        # Join baseline and predict data
        # data_upload_final = data_upload#[['datetime','year','month','plant','bo']].append(predict_data_upload[['datetime','year','month','plant','bo']]).drop_duplicates().reset_index(drop=True) 
        # data_upload_final = pd.merge(data_upload_final,predict_data_upload,on=['datetime','year','month','plant','bo'],how='left')
        # data_upload_final = pd.merge(data_upload_final,data_upload,on=['datetime','year','month','plant','bo'],how='left')
        if power_predict_name=='predict_electricity':
            data_upload_final = data_upload[[power_predict_name,'rec','plant','bo','year','month','datetime']]
        else:
            data_upload_final = data_upload[[power_predict_name,'plant','bo','year','month','datetime']]
        data_upload_final = data_upload_final.astype('string')
        data_upload_final_json =  json.loads(data_upload_final.fillna('null').to_json(orient="records"))
        output_json = {
            'data_upload_final':data_upload_final_json
        }
        print(f"output_json is successful:\n {output_json}")
    
    ###---------------------------------API傳回資訊結果---------------------------------###
        return output_json
    ###---------------------------------例外處理--------------------------------###
    except Exception as e:
        error = str(e)
        return error
    

###---------------------------------前後處理函數---------------------------------###
def bo(plant_):
    if plant_ in ['WOK','WTZ','WKS-5','WKS-6B','WKS-6','WZS-8']:
        return 'WSD'
    else:
        return 'WT'
def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null',np.nan)
    for x in dataset.columns:
        if x in ['datetime','plant','bo','日期','site','power_type','predict_type','Target','Plant','Model']:
            dataset[x] = dataset[x].astype('string')
        elif x in ['year','month','DummyVar_Plant']:
            dataset[x] = dataset[x].astype('int')
        else: 
            dataset[x] = dataset[x].astype('float')
    return dataset
def data_processor(data_result, data_result_history, predict_type, power_type, best_model_list):
    data_result_sub = data_result[data_result.columns[np.where([x.find('electricity')==-1 & x.find('id')==-1 for x in data_result.columns])]]
    max_date = max(data_result_sub.datetime)
    max2_date = max(data_result_sub.datetime[data_result_sub.datetime!=max_date])
    min_date = min(data_result_sub.datetime)
    min2_date = min(data_result_sub.datetime[data_result_sub.datetime!=min_date])
    if predict_type=='predict':
        min2_date = min_date
    data_result_sub = data_result_sub.loc[(data_result_sub.datetime <= max_date) & (data_result_sub.datetime >= min2_date),:].reset_index(drop=True)
    data_result_sub = data_result_sub.rename(columns = {'datetime':'日期','pcba_qty':'PCBA產量（pcs)','fa_qty':'FA產量（pcs)',
                                                        'pcba_lines':'PCBA平均開線數（條）','fa_lines':'FA平均開線數量（條）',
                                                        'revenue':'營業額（十億NTD）','average_temperature':'外氣平均溫度（℃）',
                                                        'member_counts':'人數（人）'},inplace=False)
    # Load bset model list
    plant_temp_cutoff = best_model_list[['Plant','Target','Temp_cutoff','Adjust_value','Model']].drop_duplicates()
    plant_temp_cutoff.columns = ['plant','Target','Temp_cutoff','Adjust_value','Model']
    # Temp indicate
    data_result_sub_temp = pd.merge(data_result_sub,plant_temp_cutoff, on=['plant'], how='left')
    data_result_sub_temp['temp_indicate'] = np.where(data_result_sub_temp['外氣平均溫度（℃）']<=data_result_sub_temp['Temp_cutoff'],1,0)
    # Lag 1 feature
    lag_1_time = np.unique([(datetime.strptime(x,'%Y-%m-%d') + pd.DateOffset(months=-1)).strftime('%Y-%m-%d') for x in data_result_sub_temp['日期']])
    data_result_sub_temp_lag = data_result_history.loc[data_result_history['日期'].astype(str).isin(lag_1_time),:]
    data_result_sub_temp_lag = data_result_sub_temp_lag[data_result_sub_temp_lag.columns[np.where([x.find('kwh')!=-1 or x.find('plant')!=-1 or x.find('日期')!=-1 for x in data_result_sub_temp_lag.columns])]].reset_index(drop=True)
    data_result_sub_temp_lag.columns = ['日期','工廠用電（kwh）_lag1','空調用電（kwh）_lag1','空壓用電（kwh）_lag1','生產用電（kwh）_lag1','基礎用電（kwh）_lag1','宿舍用電（kwh）_lag1','plant']
    data_result_sub_temp['日期']=[str(datetime.strptime(x,'%Y-%m-%d').strftime('%Y-%m-%d')) for x in data_result_sub_temp['日期']]
    data_result_sub_temp_lag['日期']=[(datetime.strptime(x,'%Y-%m-%d') + pd.DateOffset(months=1)).strftime('%Y-%m-%d') for x in data_result_sub_temp_lag['日期']]
    # Join temp and lag 1 feature
    data_result_sub_temp = pd.merge(data_result_sub_temp,data_result_sub_temp_lag,on=['日期','plant'],how='left')
    return data_result_sub, data_result_sub_temp
def model_predicting(best_model_name, data_colnames, data_result_sub, data_result_part, predict_model, predict_type):
    if best_model_name.find('lasso')!=-1 or best_model_name.find('rf')!=-1 or best_model_name.find('svm')!=-1:
        test_data = pd.DataFrame(data_result_part[predict_model.feature_names_in_].iloc[0,:]).T.reset_index(drop=True)
    elif best_model_name == 'stepwise_lm':
        test_data = pd.DataFrame(data_result_part[predict_model.pvalues.index.values].iloc[0,:]).T.reset_index(drop=True)
    else:
        test_data = xgb.DMatrix(pd.DataFrame(data_result_part[predict_model.feature_names].astype(float).iloc[0,:]).T.reset_index(drop=True))
    print(test_data)
    init_prdiction = predict_model.predict(test_data)
    if (predict_type=='predict') and (data_result_sub['日期'].astype(str)[0][5:7]=='11'):
        data_result_part['lag_1'] = [data_result_part['lag_1'][0],init_prdiction[0]]
        for x in data_colnames:
            for y in data_colnames:
                if x!=y:
                    data_result_part[x+'_'+y] = data_result_part[x]*data_result_part[y]
        if best_model_name.find('lasso')!=-1 or best_model_name.find('rf')!=-1 or best_model_name.find('svm')!=-1:
            test_data = data_result_part[predict_model.feature_names_in_]
        elif best_model_name == 'stepwise_lm':
            test_data = data_result_part[predict_model.pvalues.index.values]
        else:
            test_data = xgb.DMatrix(data_result_part[predict_model.feature_names].astype(float))
        final_prediction = predict_model.predict(test_data)
    else:
        final_prediction = init_prdiction
    return np.abs(final_prediction)
def model_predictor(data_result_sub, data_result_sub_temp, model_path, plant, predict_type, power_type):
    data_result_part_all = data_result_sub.loc[(data_result_sub.plant==plant),:].reset_index(drop=True).copy(deep=True)
    # Generate interaction features
    data_result_sub_temp_power = data_result_sub_temp.copy(deep=True)
    data_result_sub_temp_power['lag_1'] = data_result_sub_temp_power[power_type+'_lag1']
    # data_result_sub_temp_power = data_result_sub_temp_power.loc[~data_result_sub_temp_power['lag_1'].isna(),:]
    # print(data_result_sub_temp_power[['日期','plant','lag_1']].loc[data_result_sub_temp_power['plant']=='WKS-5',:])
    if power_type=='空壓用電（kwh）':
        data_result_sub_temp_power['lag_1_air_condition'] = data_result_sub_temp_power['空調用電（kwh）_lag1']
    elif power_type=='生產用電（kwh）':
        # data_result_sub_temp_power['lag_1_air_condition'] = data_result_sub_temp_power['空調用電（kwh）_lag1']
        data_result_sub_temp_power['lag_1_air_compress'] = data_result_sub_temp_power['空壓用電（kwh）_lag1']
    data_colnames = data_result_sub_temp_power.columns[np.where([(x.find('（')!=-1 & x.find('_lag1')==-1) | (x.find('temp_indicate')!=-1) | (x.find('lag_1')!=-1) for x in data_result_sub_temp_power.columns])]
    # print(data_colnames)
    for x in data_colnames:
        data_result_sub_temp_power[x] = np.where((data_result_sub_temp_power[x]=='') | (data_result_sub_temp_power[x].isna()) | (data_result_sub_temp_power[x]=='NaN'),math.nan,data_result_sub_temp_power[x]).astype(float)
    # Generate interaction term
    for x in data_colnames:
        for y in data_colnames:
            if x!=y:
                data_result_sub_temp_power[x+'_'+y] = data_result_sub_temp_power[x]*data_result_sub_temp_power[y]
    # Specific in plant and power type
    data_result_part = data_result_sub_temp_power.loc[(data_result_sub_temp_power.plant==plant) & (data_result_sub_temp_power.Target==power_type) & (data_result_sub_temp_power['日期'].astype(str).isin(data_result_part_all['日期'].astype(str))),:].reset_index(drop=True)
    best_model_name = data_result_part.Model[0]
    # print(best_model_name)
    # Load Model
    power_predict_name = str(np.where(power_type=='空調用電（kwh）','ac_electricity',
                                  np.where(power_type=='空壓用電（kwh）','ap_electricity',
                                          np.where(power_type=='生產用電（kwh）','production_electricity',
                                                   np.where(power_type=='基礎用電（kwh）','base_electricity','predict_electricity')))))
    model_name = f"{plant}_{power_predict_name}_bestmodel.pickle"
    model_file_name = os.path.join(model_path, model_name) #從 AZUREML_MODEL_DIR 撈進來
    with open(model_file_name, 'rb') as f:
        predict_model = pickle.load(f)
    # Model prediction
    if best_model_name!='ensemble':
        final_prediction = model_predicting(best_model_name, data_colnames, data_result_sub, data_result_part, predict_model, predict_type)
        data_result_part_all[str(power_predict_name)] = final_prediction[0:data_result_part_all.shape[0]]
    else:
        predict_result = pd.DataFrame({'p2':model_predicting('lasso', data_colnames, data_result_sub, data_result_part, predict_model['lasso'], predict_type)})
        predict_result['p3'] = model_predicting('rf', data_colnames, data_result_sub, data_result_part, predict_model['rf'], predict_type)
        predict_result['p4'] = model_predicting('svm', data_colnames, data_result_sub, data_result_part, predict_model['svm'], predict_type)
        # predict_result['p5'] = model_predicting('xgb', data_colnames, data_result_sub, data_result_part, predict_model['xgb'], predict_type)
        final_prediction = predict_model['ensemble'].predict(predict_result)
        data_result_part_all[str(power_predict_name)] = final_prediction[0:data_result_part_all.shape[0]]
    data_result_part_all['bo'] = bo(plant)
    data_result_part_all['year'] = [str(x)[0:4] for x in data_result_part_all['日期']]
    data_result_part_all['month'] = [str(x)[5:7] for x in data_result_part_all['日期']]
    data_result_part_all['datetime'] = [str(x) for x in data_result_part_all['日期']]
    # if ~data_result_part_all['month'].isin(['11','12'])[0]:
    #     data_result_part_all['predict_electricity'] = math.nan
    #     data_result_part_all['rec'] = math.nan
    if data_result_part_all['month'].isin(['11','12'])[0] and power_type=='工廠用電（kwh）':
        data_result_part_all['predict_electricity'] = data_result_part_all['predict_electricity']*(1+data_result_part.Adjust_value[0])
        data_result_part_all['rec'] = math.nan
        data_result_part_upload = data_result_part_all[['datetime','year','month','predict_electricity','rec','plant','bo']]
    else:
        # data_result_part_all['rec'] = math.nan
        data_result_part_upload = data_result_part_all[['datetime','year','month',power_predict_name,'plant','bo']]
    # data_result_part_upload = data_result_part_all[['year','month','predict_electricity','ac_electricity','ap_electricity','production_electricity','base_electricity','rec','plant','bo']]
    return data_result_part_upload
def real_power_computer(power_real_data):
    # modify column names
    power_real_data = power_real_data[power_real_data.columns[np.where([x.find('id')==-1 for x in power_real_data.columns])]]
    power_real_data.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                               'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                               '營業額（十億NTD）','外氣平均溫度（℃）','plant','site']
    power_ten_month_total = power_real_data.loc[[power_real_data['日期'].astype(str)[x][5:7]<'11' for x in range(power_real_data.shape[0])],:].reset_index(drop=True)
    power_ten_month_total['year'] = [power_ten_month_total['日期'].astype(str)[x][0:4] for x in range(power_ten_month_total.shape[0])]
    power_ten_month_total = power_ten_month_total.groupby(['plant','site','year'], group_keys=True).agg({'工廠用電（kwh）':'sum','宿舍用電（kwh）':'sum','日期':'size'}).reset_index().rename(columns={'日期':'month_count'})
    # Compute total power
    power_ten_month_total['ten_month_real'] = power_ten_month_total['工廠用電（kwh）']+12*power_ten_month_total['宿舍用電（kwh）']/power_ten_month_total['month_count']
    power_ten_month_total_final = power_ten_month_total.loc[power_ten_month_total.month_count==10,:]
    return power_ten_month_total_final
def prediction_rec_computer(power_ten_month_total_final, power_predict_data):
    # filter specific month data
    power_predict_data_two_month = power_predict_data.loc[(power_predict_data.month>='11') & (~power_predict_data.predict_electricity.isna()),:]
    power_predict_data_two_month = power_predict_data_two_month.groupby(['plant','year'], group_keys=True).agg({'predict_electricity':'sum'}).reset_index().rename(columns={'predict_electricity':'two_month_predict'})
    power_green_total = pd.merge(power_predict_data_two_month,power_ten_month_total_final,on=['plant','year'],how='left')
    power_green_total['rec_year']=power_green_total.two_month_predict+power_green_total.ten_month_real
    power_green_total = power_green_total[['plant','year','rec_year']]
    power_green_total['month'] = '11'
    power_predict_data_new = pd.merge(power_predict_data.copy(deep=True),power_green_total,on=['plant','year','month'],how='left')
    power_predict_data_new['rec']=np.where(power_predict_data_new.rec.isna(),power_predict_data_new.rec_year, power_predict_data_new.rec)
    power_predict_data_new = power_predict_data_new.drop(columns={'rec_year'})
    return power_predict_data_new
