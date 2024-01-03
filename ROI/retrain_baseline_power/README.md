# Baseline-power Prediction Retrain Pipeline 用電基線預測

使用 retrain pipeline 之前，需使用 baseline-power pipeline 進行初始化 
> *i.e.讓雲端存有各廠的 best_performance_table 後才能執行 retrain 機制。*

# Change log
| Version | Release Date | Description |
| ------- | ------------ | ----------- | 
| v1.00   | 2022.07.13 | 1. 訓練資料先在地端執行完ETL和切分後，將檔案存放於 *test/{plant}/* 再執行pipeline |
| v2.00   | 2022.08.15 | 1. 資料串接調整為直接從DB query; 2. modeling stepes 會將每廠產出的best_performance_table進行註冊，以利後續使用; 3. 當需要做retrain時，會直接在同一隻pipeline進行 | 

# Installation
```bash
dependencies:
    - python 3.8.13
    - pip:
        - azureml==0.2.7
        - azure-core==1.22.1
        - azureml-pipeline==1.42.0
```

<!-- ## 1. Pipeline Flow -->
 <!-- ![image info](static/pipeline_flow.png) -->

<!-- ## 2. Pipeline function關係圖 -->
 <!-- ![image info](static/pipeline_related_function.png) -->
 
# retrainbaseline
## *class* **RetrainElecBaselinePipeline(self, params)**
- ### Parameters:
 1. *job* : `str`，欲提交的job其名稱，也可視為 pipeline family的名稱，當前預設為`eco-retrain-pipeline`。若每次提交相同名稱的job，則可以在同一job空間內查看數個display。
 2. *display* :`str`，可視為單一pipeline的名稱，當前預設為`當下時間 yyyy-mm-dd`。
 3. *compute_target* : `str`，欲調用的雲端計算引擎，當前預設為`vm-mcc`。
 4. *env* : `str`，進行模型訓練與endpoint部署的環境，當前預設為`eco-env`。
 5. *power_target* : `str`，預測目標，當前預設為`空調用電（kwh）`。

- ### Methods: 
  #### *trigger_pipeline(self)*
  > 使用trigger_pipeline提交pipeline實驗，已串接四個 `pipeline steps： upload_test -> evaluation -> database_query -> dataset_split -> modeling -> register_best_model -> deploy_best_model`。當沒有任何廠區需要做retrain時，pipeline 在evaluation完後會直接報錯跳出。
  + **Return**: pipeline run。

# 補充說明
- ## Input data for endpoint
  > data_result 以一個工廠的資料為單位，並且須包含 *power_type* 和 *predict_type* 欄位 ; predict_data_result 須包含 *power_type = '工廠用電（kwh）'* 欄位。
  - ### Column information:
    1. *power_type* : `str`，如：`'工廠用電（kwh）'`，`'空調用電（kwh）'`，`'空壓用電（kwh）'`，`'生產用電（kwh）'`，`'基礎用電（kwh）'`，~~`'宿舍用電（kwh）'`~~。
    2. *predict_type* : `str`，依據 power_types 給定 **baseline** 或 **predict** 。
   ![image info](../static/inputdata_sample.png)



