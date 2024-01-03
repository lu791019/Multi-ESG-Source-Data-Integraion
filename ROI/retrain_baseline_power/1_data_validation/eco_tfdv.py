import tensorflow as tf
import tensorflow_data_validation as tfdv
import os
import pandas as pd

class EcoTFDV:
    
    def __init__(self, 
                 DATA,
                 INITIAL = True):
        
        self.initial = INITIAL
        
        if self.initial == True:
            self.TRAIN_DATA = DATA
        else:
            self.EVAL_DATA = DATA
    
    # 初始情況使用，僅用一次
    def Train_stats(self):
        train_stats = tfdv.generate_statistics_from_dataframe(dataframe = self.TRAIN_DATA)
        schema = tfdv.infer_schema(statistics=train_stats)
        
        return train_stats, schema
    
    # 非初始情況使用
    def Eval_stats(self):
        eval_stats = tfdv.generate_statistics_from_dataframe(dataframe = self.EVAL_DATA)
        
        return eval_stats
    
    # 非初始情況，新資料跟baseline做比較
    def Check_anomalies(self,EVAL_STATS,SCHEMA):
        anomalies = tfdv.validate_statistics(statistics=EVAL_STATS, schema=SCHEMA)
        # # Display anomalies
        # for k, v in anomalies.anomaly_info.items():
        #     print("Anomaly Info:",
        #           f"\n{'feature:':<8}{k}",
        #           f"\n{'description:':<8}{v.description}",
        #           f"\n{'*'*20}") 
        anomalies_text = pd.DataFrame()
        for k, v in anomalies.anomaly_info.items(): 
            tmp = pd.DataFrame({'name':k,'value':v.short_description},index = [0])
            anomalies_text = anomalies_text.append(tmp, ignore_index=True)
        anomalies_text = anomalies_text.to_dict(orient='records')
        
        return anomalies, anomalies_text
    
    def Check_eval_drift(self,TRAIN_STATS,EVAL_STATS,SCHEMA, FEATURES:list):
        for Object in FEATURES:
            tfdv.get_feature(SCHEMA, Object).drift_comparator.jensen_shannon_divergence.threshold = 0.01
        drift_anomalies = tfdv.validate_statistics(TRAIN_STATS, SCHEMA, previous_statistics=EVAL_STATS)
        # Display anomalies
        # for k, v in drift_anomalies.anomaly_info.items():
        #     print("Anomaly Info:",
        #           f"\n{'feature:':<8}{k}",
        #           f"\n{'description:':<8}{v.description}",
        #           f"\n{'*'*20}") 
        drift_text = pd.DataFrame()
        for k, v in drift_anomalies.anomaly_info.items(): 
            tmp = pd.DataFrame({'name':k,'value':v.short_description},index = [0])
            drift_text = drift_text.append(tmp, ignore_index=True)
        drift_text = drift_text.to_dict(orient='records')
        
        return drift_anomalies, drift_text
