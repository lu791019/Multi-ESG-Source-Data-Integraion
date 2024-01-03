import pandas as pd
import numpy as np
import re
import statsmodels.api as sm
import xgboost as xgb
import pickle
import argparse
import time

from sklearn.linear_model import LinearRegression
from numpy import arange
from sklearn.linear_model import Lasso, LassoCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler
from datetime import datetime

class TrainModels:
    def __init__(self, 
                 plant, plant_dummy_value, model_path, k, best_model, power_type, adjust_month, temp_cutoff, 
                 train_data, valid_data, test_data, train_data_interact):
       
        self.plant = plant
        self.plant_dummy_value = plant_dummy_value
        self.model_path = model_path
        self.k = k
        self.best_model = best_model
        # self.output_best_data = output_best_data
        self.power_type = power_type
        self.adjust_month = adjust_month
        self.temp_cutoff = temp_cutoff
        self.train_data = train_data
        self.valid_data = valid_data
        self.test_data = test_data
        self.train_data_interact = train_data_interact
        
        
    '''Related functions are shown below: 
    1. model_building
    2. _model_performance
        2-1. _MAPE_measurement
        2-2. _Adjust_measurement
        
    -  Model types:
        3-1. _stepwise_lm_model
            3-1-1. _backward_regression
        3-2. _lasso_model
        3-3. _rf_model
        3-4. _svm_model
        3-5. _xgb_model
        3-6. _ensemble_model
    '''
    
    '''Storages of outputs    
    In *model_path*: store whole model (.pickle)
    In *best_model*: store best_perf.csv and bestmodel.pickle
    In *outputs*: store whole perf .csv and best_perf.csv (for a requirement of downloading files)
    '''

    def model_building(self):
        
        # Pre LM model
        train_model = LinearRegression(fit_intercept=False, normalize=False).fit(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]], self.train_data[self.power_type])
        train_model_coef = train_model.coef_
        # Stepwise + LM
        train_model_stepwise_lm, train_stepwise_lm, valid_stepwise_lm, test_stepwise_lm, rsq_stepwise_lm, sig_feature1 = self._stepwise_lm_model(train_model_coef)
        # Lasso
        train_model_lasso, train_lasso, valid_lasso, test_lasso, rsq_lasso, sig_feature2 = self._lasso_model()
        # Random Forest
        train_model_rf, train_rf, valid_rf, test_rf, rsq_rf = self._rf_model(sig_feature1, sig_feature2)
        # SVM
        train_model_svm, train_svm, valid_svm, test_svm, rsq_svm = self._svm_model(sig_feature1, sig_feature2)
        # XGB
        train_model_xgb, train_xgb, valid_xgb, test_xgb = self._xgb_model(sig_feature1, sig_feature2)
        # Ensemble
        train_model_ensemble, train_ensemble, valid_ensemble, test_ensemble = \
            self._ensemble_model(train_lasso, train_rf, train_svm, train_xgb, 
                                 valid_lasso, valid_rf, valid_svm, valid_xgb,
                                 test_lasso, test_rf, test_svm, test_xgb)


        # Merge all model
        train_model_ensemble_all = {'lasso':train_model_lasso, 'rf':train_model_rf, 'svm':train_model_svm, 'xgb':train_model_xgb, 'ensemble':train_model_ensemble}

        # Model performance
        train_stepwise_lm_mape, valid_stepwise_lm_mape, valid_stepwise_lm_mape_adjust, test_stepwise_lm_adjust, adjust_stepwise_lm = \
            self._model_performance( train_stepwise_lm, valid_stepwise_lm, test_stepwise_lm)
        train_lasso_mape, valid_lasso_mape, valid_lasso_mape_adjust, test_lasso_adjust, adjust_lasso = \
            self._model_performance( train_lasso, valid_lasso, test_lasso)
        train_rf_mape, valid_rf_mape, valid_rf_mape_adjust, test_rf_adjust, adjust_rf = \
            self._model_performance( train_rf, valid_rf, test_rf)
        train_svm_mape, valid_svm_mape, valid_svm_mape_adjust, test_svm_adjust, adjust_svm = \
            self._model_performance( train_svm, valid_svm, test_svm)
        train_xgb_mape, valid_xgb_mape, valid_xgb_mape_adjust, test_xgb_adjust, adjust_xgb = \
            self._model_performance( train_xgb, valid_xgb, test_xgb)
        train_ensemble_mape, valid_ensemble_mape, valid_ensemble_mape_adjust, test_ensemble_adjust, adjust_ensemble = \
            self._model_performance( train_ensemble, valid_ensemble, test_ensemble)
        
        power_predict_name = np.where(self.power_type=='空調用電（kwh）','ac_electricity',
                                  np.where(self.power_type=='空壓用電（kwh）','ap_electricity',
                                          np.where(self.power_type=='生產用電（kwh）','production_electricity',
                                                   np.where(self.power_type=='基礎用電（kwh）','base_electricity','predict_electricity'))))
        # Merge all model
        train_model_all = {'stepwise_lm':train_model_stepwise_lm, 'lasso':train_model_lasso, 
                           'rf':train_model_rf, 'svm':train_model_svm, 
                           'xgb':train_model_xgb, 'ensemble':train_model_ensemble_all}
        # Save each model for next step
        for name,model in list(train_model_all.items()):
            with open(self.model_path+'/'+self.plant+f"_{power_predict_name}_"+name+'.pickle', 'wb') as f:
                pickle.dump(model, f)

        # Summary all performance
        if self.power_type != '工廠用電（kwh）':
            prediction_performance = pd.DataFrame({'Plant':np.repeat(self.plant,6),
                                                   'Target':np.repeat(self.power_type,6),
                                                   'Model':['stepwise_lm','lasso','rf','svm','xgb','ensemble'],
                                                   'Train_MAPE':[train_stepwise_lm_mape,train_lasso_mape,train_rf_mape,train_svm_mape,train_xgb_mape,train_ensemble_mape],
                                                   'Valid_MAPE':[valid_stepwise_lm_mape,valid_lasso_mape,valid_rf_mape,valid_svm_mape,valid_xgb_mape,valid_ensemble_mape],
                                                   'Valid_MAPE_Adjust':[valid_stepwise_lm_mape_adjust,valid_lasso_mape_adjust,valid_rf_mape_adjust,
                                                                        valid_svm_mape_adjust,valid_xgb_mape_adjust,valid_ensemble_mape_adjust],
                                                   'Adjust_value':[adjust_stepwise_lm,adjust_lasso,adjust_rf,adjust_svm,adjust_xgb,adjust_ensemble],
                                                   'Forcast_1':[test_stepwise_lm[0],test_lasso[0],test_rf[0],test_svm[0],test_xgb[0],test_ensemble[0]],
                                                   'Forcast_2':[test_stepwise_lm[1],test_lasso[1],test_rf[1],test_svm[0],test_xgb[1],test_ensemble[1]],
                                                   'Temp_cutoff':np.repeat(self.temp_cutoff,6)})
            # if power_type == '空壓用電（kwh）' and plant=='WZS-8':
            #     prediction_performance['Valid_MAPE_best'] = prediction_performance.loc[(prediction_performance.Model!='svm') & (prediction_performance.Model!='ensemble') & (prediction_performance.Model!='xgb'),:].groupby(['Plant','Target'], group_keys=False)['Valid_MAPE'].transform(lambda x: min(x))
            # else:
            prediction_performance['Valid_MAPE_best'] = prediction_performance.loc[(prediction_performance.Model!='svm') & (prediction_performance.Model!='xgb'),:]\
                                                                                .groupby(['Plant','Target'], group_keys=False)['Valid_MAPE'].transform(lambda x: min(x))
            prediction_performance_best = (prediction_performance.loc[prediction_performance.Valid_MAPE==prediction_performance.Valid_MAPE_best,:]).reset_index(drop=True)
        else:

            prediction_performance = pd.DataFrame({'Plant':np.repeat(self.plant,6),
                                                   'Target':np.repeat(self.power_type,6),
                                                   'Model':['stepwise_lm','lasso','rf','svm','xgb','ensemble'],
                                                   'Train_MAPE':[train_stepwise_lm_mape,train_lasso_mape,train_rf_mape,train_svm_mape,train_xgb_mape,train_ensemble_mape],
                                                   'Valid_MAPE':[valid_stepwise_lm_mape,valid_lasso_mape,valid_rf_mape,valid_svm_mape,valid_xgb_mape,valid_ensemble_mape],
                                                   'Valid_MAPE_Adjust':[valid_stepwise_lm_mape_adjust,valid_lasso_mape_adjust,valid_rf_mape_adjust,
                                                                        valid_svm_mape_adjust,valid_xgb_mape_adjust,valid_ensemble_mape_adjust],
                                                   'Adjust_value':[adjust_stepwise_lm,adjust_lasso,adjust_rf,adjust_svm,adjust_xgb,adjust_ensemble],
                                                   'Forcast_1':[test_stepwise_lm_adjust[0],test_lasso_adjust[0],test_rf_adjust[0],test_svm_adjust[0],test_xgb_adjust[0],test_ensemble_adjust[0]],
                                                   'Forcast_2':[test_stepwise_lm_adjust[1],test_lasso_adjust[1],test_rf_adjust[1],test_svm_adjust[0],test_xgb_adjust[1],test_ensemble_adjust[1]],
                                                   'Temp_cutoff':np.repeat(self.temp_cutoff,6)})
            prediction_performance['Valid_MAPE_best'] = prediction_performance.loc[(prediction_performance.Model!='svm') & (prediction_performance.Model!='xgb'),:]\
                                                                                .groupby(['Plant','Target'], group_keys=False)['Valid_MAPE_Adjust'].transform(lambda x: min(x))
            prediction_performance_best = (prediction_performance.loc[prediction_performance.Valid_MAPE_Adjust==prediction_performance.Valid_MAPE_best,:]).reset_index(drop=True)
        
        prediction_performance = prediction_performance.sort_values('Model').assign(DummyVar_Model = list(range(1, 7, 1)),#fix me:須依據模型數目做修正
                                                                                    DummyVar_Plant = self.plant_dummy_value)
        prediction_performance_best = prediction_performance_best.assign(DummyVar_Plant = self.plant_dummy_value)
        Model_best = prediction_performance_best.Model
        
        #欲下載至地端的檔案需存放於 outputs/
        model_file_name = f"{self.plant}_{str(power_predict_name)}_bestmodel.pickle" 
        best_performance_path1 = f"{self.best_model}/{self.plant}_{str(power_predict_name)}_best_prediction_perf.csv" 
        best_performance_path2 = f"outputs/{self.plant}_{str(power_predict_name)}_best_prediction_perf.csv" 
        # Here we only save the best model
        with open(self.best_model+'/'+ model_file_name, 'wb') as f:
            pickle.dump(train_model_all[Model_best[0]], f)
        
        prediction_performance_best.to_csv(best_performance_path1,index =False) 
        prediction_performance_best.to_csv(best_performance_path2,index =False) 
        
        performance_path = f"outputs/{self.plant}_{str(power_predict_name)}_prediction_perf.csv"
        prediction_performance.to_csv(performance_path,index =False)
        
        return train_model_all[Model_best[0]], prediction_performance, prediction_performance_best
            
            
    def _model_performance(self, train_predict, valid_predict, test_predict):
        # Model performance
        train_predict_mape = self._MAPE_measurement(self.train_data[self.power_type],train_predict)
        valid_predict_mape = self._MAPE_measurement(self.valid_data[self.power_type],valid_predict)
        adjust_predict = self._Adjust_measurement(self.train_data[self.power_type],train_predict,self.adjust_month)
        valid_predict_mape_adjust = self._MAPE_measurement(self.valid_data[self.power_type],valid_predict*(1+adjust_predict))
        test_predict_adjust = test_predict*(1+adjust_predict)
        return train_predict_mape, valid_predict_mape, valid_predict_mape_adjust, test_predict_adjust, adjust_predict
    # MAPE function
    def _MAPE_measurement(self,real_value,predict_value):
        return np.average(np.abs(real_value-predict_value)/real_value)
    # Adjustment function
    def _Adjust_measurement(self,real_value,predict_value, recent_month):
        return np.average((real_value[-len(real_value):-1*int(recent_month)]-predict_value[-len(real_value):-1*int(recent_month)])/predict_value[-len(real_value):-1*int(recent_month)])

    '''
    Model types 

    '''
    def _stepwise_lm_model(self,train_model_coef):
        # Stepwise + LM
        if self.train_data.shape[1]>self.train_data.shape[0]:
            sig_feature_pre = pd.DataFrame({'feature_name':self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]].columns}, 
                                           index=np.abs(train_model_coef).tolist()).sort_index(ascending=False)['feature_name'].iloc[0:round(self.train_data.shape[0]*0.9)]
            sig_feature1 = self._backward_regression(self.train_data.iloc[:,np.where(self.train_data.columns.isin(sig_feature_pre))[0]], 
                                                     self.train_data[self.power_type],threshold_in = 0.05,threshold_out = 0.01)
        else:
            sig_feature1 = self._backward_regression(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0][1:20]], 
                                                     self.train_data[self.power_type],threshold_in = 0.05,threshold_out = 0.01)
        train_model_stepwise_lm = sm.OLS(self.train_data[self.power_type], self.train_data[sig_feature1]).fit()
        # train_model.summary()
        train_stepwise_lm = train_model_stepwise_lm.predict(self.train_data[sig_feature1])
        valid_stepwise_lm = train_model_stepwise_lm.predict(self.valid_data[sig_feature1])
        if self.k == 2:
            self.test_data['lag_1'] = [self.valid_data[self.power_type].iloc[-1],train_model_stepwise_lm.predict(self.test_data[sig_feature1].head(1))[0]]
            # Generate interaction term
            for x in self.train_data_interact.columns:
                for y in self.train_data_interact.columns:
                    if x!=y:
                        self.test_data[x+'_'+y] = self.test_data[x]*self.test_data[y]
        test_stepwise_lm = train_model_stepwise_lm.predict(self.test_data[sig_feature1])
        rsq_stepwise_lm = train_model_stepwise_lm.rsquared_adj
        return train_model_stepwise_lm, train_stepwise_lm, valid_stepwise_lm, test_stepwise_lm, rsq_stepwise_lm, sig_feature1
    def _backward_regression(self,X, y,
                               initial_list=[], 
                               threshold_in=0.01, 
                               threshold_out = 0.05, 
                               verbose=True):
        included=list(X.columns)
        while True:
            changed=False
            # model = sm.OLS(y, sm.add_constant(pd.DataFrame(X[included]))).fit()
            model = sm.OLS(y, pd.DataFrame(X[included])).fit()
            # use all coefs except intercept
            pvalues = model.pvalues.iloc[1:]
            worst_pval = pvalues.max() # null if pvalues is empty
            print(worst_pval)
            if worst_pval > threshold_out:
                changed=True
                worst_feature = pvalues.idxmax()
                included.remove(worst_feature)
                if verbose:
                    print('Drop  with p-value '.format(worst_feature, worst_pval))
            if not changed:
                break
        return included

    def _lasso_model(self):
        # Shrinkage + LASSO
        lassocv = LassoCV(cv=10, fit_intercept=False, alphas = arange(0, 1, 0.01), random_state=100, max_iter=500000)
        train_model_lasso = lassocv.fit(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]], self.train_data[self.power_type])
        lasso = Lasso(fit_intercept=False, alpha = train_model_lasso.alpha_, random_state=100)
        train_model_lasso = lasso.fit(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]], self.train_data[self.power_type])
        sig_feature2 = self.train_data.columns[np.where(train_model_lasso.coef_<0.0001)]
        train_lasso = train_model_lasso.predict(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]])
        valid_lasso = train_model_lasso.predict(self.valid_data.iloc[:,np.where(self.valid_data.columns!=self.power_type)[0]])
        if self.k == 2:
            self.test_data['lag_1'] = [self.valid_data[self.power_type].iloc[-1],train_model_lasso.predict(self.test_data.iloc[:,np.where(self.test_data.columns!=self.power_type)[0]])[0]]
            # Generate interaction term
            for x in self.train_data_interact.columns:
                for y in self.train_data_interact.columns:
                    if x!=y:
                        self.test_data[x+'_'+y] = self.test_data[x]*self.test_data[y]
        test_lasso = train_model_lasso.predict(self.test_data.iloc[:,np.where(self.test_data.columns!=self.power_type)[0]])
        rsq_lasso = train_model_lasso.score(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]],self.train_data[self.power_type])
        return train_model_lasso, train_lasso, valid_lasso, test_lasso, rsq_lasso, sig_feature2

    def _rf_model(self,sig_feature1, sig_feature2):
        rf = RandomForestRegressor(n_estimators = 1000, random_state = 100)
        # Train the model on training data
        train_model_rf = rf.fit(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]], self.train_data[self.power_type])
        sig_feature = pd.DataFrame({'feature_name':self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]].columns},
                                   index=train_model_rf.feature_importances_.tolist()).sort_index(ascending=False)['feature_name'].iloc[0:max(len(sig_feature1),len(sig_feature2))]
        train_model_rf = rf.fit(self.train_data[sig_feature], self.train_data[self.power_type])
        train_rf = train_model_rf.predict(self.train_data[sig_feature])
        valid_rf = train_model_rf.predict(self.valid_data[sig_feature])
        if self.k == 2:
            self.test_data['lag_1'] = [self.valid_data[self.power_type].iloc[-1],train_model_rf.predict(self.test_data[sig_feature])[0]]
            # Generate interaction term
            for x in self.train_data_interact.columns:
                for y in self.train_data_interact.columns:
                    if x!=y:
                        self.test_data[x+'_'+y] = self.test_data[x]*self.test_data[y]
        test_rf = train_model_rf.predict(self.test_data[sig_feature])
        rsq_rf = train_model_rf.score(self.train_data[sig_feature], self.train_data[self.power_type])
        return train_model_rf, train_rf, valid_rf, test_rf, rsq_rf

    def _svm_model(self,sig_feature1, sig_feature2):
        svr = SVR(kernel='rbf', epsilon=0.00001, gamma=0.001)
        # X = StandardScaler().fit_transform(train_data.iloc[:,np.where(train_data.columns!=power_type)[0]])
        X = self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]]
        y = self.train_data[self.power_type]
        train_model_svm = svr.fit(X,y)
        feature_import = np.abs(np.dot(train_model_svm.dual_coef_,train_model_svm.support_vectors_))
        sig_feature = pd.DataFrame({'feature_name':self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]].columns}, 
                                   index=feature_import.tolist()).sort_index(ascending=False)['feature_name'][0:max(len(sig_feature1),len(sig_feature2))]
        train_model_svm = svr.fit(self.train_data[sig_feature], self.train_data[self.power_type])
        train_svm = train_model_svm.predict(self.train_data[sig_feature])
        valid_svm = train_model_svm.predict(self.valid_data[sig_feature])
        if self.k == 2:
            self.test_data['lag_1'] = [self.valid_data[self.power_type].iloc[-1],train_model_svm.predict(self.test_data[sig_feature])[0]]
            # Generate interaction term
            for x in self.train_data_interact.columns:
                for y in self.train_data_interact.columns[np.where(self.train_data_interact.columns==x)[0][0]+1:len(self.train_data_interact.columns)]:
                    if x!=y:
                        self.test_data[x+'_'+y] = self.test_data[x]*self.test_data[y]
        test_svm = train_model_svm.predict(self.test_data[sig_feature])
        rsq_svm = train_model_svm.score(self.train_data[sig_feature], self.train_data[self.power_type])
        return train_model_svm, train_svm, valid_svm, test_svm, rsq_svm

    def _xgb_model(self, sig_feature1, sig_feature2):
        dtrain = xgb.DMatrix(self.train_data.iloc[:,np.where(self.train_data.columns!=self.power_type)[0]], label=self.train_data[self.power_type])
        watchlist = [(dtrain, 'train')]
        param = {'max_depth': 6, 'learning_rate': 0.2}
        num_round = 200
        train_model_xgb = xgb.train(param, dtrain, num_round, watchlist)
        sig_feature = [x[0] for x in (sorted(train_model_xgb.get_score(importance_type='weight').items(),key=lambda x: x[1], reverse=True))][0:max(len(sig_feature1),len(sig_feature2))]
        dtrain = xgb.DMatrix(self.train_data[sig_feature], label=self.train_data[self.power_type])
        dvalid = xgb.DMatrix(self.valid_data[sig_feature], label=self.valid_data[self.power_type])
        dtest = xgb.DMatrix(self.test_data[sig_feature].astype(float))
        watchlist = [(dtrain, 'train')]
        train_model_xgb = xgb.train(param, dtrain, num_round, watchlist)
        train_xgb = train_model_xgb.predict(dtrain)
        valid_xgb = train_model_xgb.predict(dvalid)
        if self.k == 2:
            self.test_data['lag_1'] = [self.valid_data[self.power_type].iloc[-1],train_model_xgb.predict(dtest)[0]]
            # Generate interaction term
            for x in self.train_data_interact.columns:
                for y in self.train_data_interact.columns:
                    if x!=y:
                        self.test_data[x+'_'+y] = self.test_data[x]*self.test_data[y]
            dtest = xgb.DMatrix(self.test_data[sig_feature].astype(float))
        test_xgb = train_model_xgb.predict(dtest)
        return train_model_xgb, train_xgb, valid_xgb, test_xgb 

    def _ensemble_model(self,train_lasso, train_rf, train_svm, train_xgb, 
                        valid_lasso, valid_rf, valid_svm, valid_xgb, 
                        test_lasso, test_rf, test_svm, test_xgb):
        # Ensemble
        train_data_ensemble = pd.DataFrame({'p2':train_lasso,'p3':train_rf,'p4':train_svm,self.power_type:self.train_data[self.power_type]})
        valid_data_ensemble = pd.DataFrame({'p2':valid_lasso,'p3':valid_rf,'p4':valid_svm,self.power_type:self.valid_data[self.power_type]})
        test_data_ensemble = pd.DataFrame({'p2':test_lasso,'p3':test_rf,'p4':test_svm})
        train_model_ensemble = LinearRegression(fit_intercept=True, normalize=False).fit(train_data_ensemble.iloc[:,np.where(train_data_ensemble.columns!=self.power_type)[0]],
                                                                                         train_data_ensemble[self.power_type])
        train_ensemble = train_model_ensemble.predict(train_data_ensemble.iloc[:,np.where(train_data_ensemble.columns!=self.power_type)[0]])
        valid_ensemble = train_model_ensemble.predict(valid_data_ensemble.iloc[:,np.where(valid_data_ensemble.columns!=self.power_type)[0]])
        test_ensemble = train_model_ensemble.predict(test_data_ensemble.iloc[:,np.where(test_data_ensemble.columns!=self.power_type)[0]])
        return train_model_ensemble, train_ensemble, valid_ensemble, test_ensemble

