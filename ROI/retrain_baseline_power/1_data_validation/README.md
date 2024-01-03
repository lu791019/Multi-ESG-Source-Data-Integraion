# eco_tfdv
## *class* **EcoTFDV(self, data_path, initial_status)**
- ### Parameters:
 1. *data_path* : `str`，欲進行資料驗證的資料集路徑。
 2. *initial_status* :`boolean`，是否為第一次資料驗證。

- ### Methods:
  #### *Train_stats()*
  > 僅在該筆資料集第一次驗證時執行，計算訓練集 (aka baseline dataset) 的統計資訊。
  + **Return**: pipeline run。
  #### *Eval_stats()*
  > 計算資料集 (aka target dataset) 的統計資訊。
  #### *Check_anomalies(self,eval_stats_input,train_schema)*
  > 以 baseline dataset 為基準對 target dataseet進行異常驗證 
  + **Parameters**:
    1. *eval_stats_input*: `tensorflow_metadata.proto.v0.statistics_pb2.DatasetFeatureStatisticsList`，target dataset 的統計資訊。
    2. *train_schema*: `tensorflow_metadata.proto.v0.schema_pb2.Schema`，訓練集 (aka baseline dataset) 的schema。

# 補充
- ## 驗證結果視覺化呈現方式(須在地端執行)
  ```python
  import tensorflow_data_validation as tfdv

  train_stats = tfdv.generate_statistics_from_csv(data_location=TRAIN_DATA)
  tfdv.visualize_statistics(train_stats) 
  # 將 train_stats 視覺化呈現

  schema = tfdv.infer_schema(statistics=train_stats)
  tfdv.display_schema(schema=schema)
  # 將 schema 視覺化呈現

  # Check eval data for errors by validating the eval data stats using the previously inferred schema.
  anomalies = tfdv.validate_statistics(statistics=eval_stats, schema=schema)
  tfdv.display_anomalies(anomalies)
  ```
  - **`train_stats` 下載方式：**
    > 以 trainstats-{power_predict_name}-{plant} 名稱註冊於 *AzureML Data*。如：trainstats-production-electricity-WCQ 
    1. 到 *Datastore: workspaceblobstore* 針對目標檔案點選 *Download*，如下：
    ![image info](../../static/trainstats_datastore.png)
    2. 在地端使用 AzureML SDK 下載：
    ```python
    import tensorflow_data_validation as tfdv
    from azureml.core import Dataset

    # ws = 你的workspace
    cloud_path = Dataset.get_by_name(ws, name=(f"trainstats-{power_predict_name}-{plant}").replace('_','-')).download()
    train_stats = tfdv.load_stats_text(input_path = cloud_path[0])
    ```

  - **`schema` 下載方式：**
    > 以 trainschema-{power_predict_name}-{plant} 名稱註冊於 *AzureML Data*。如：trainschema-production-electricity-WCQ 
    1. 到 *Datastore: workspaceblobstore* 針對目標檔案點選 *Download*，同 `train_stats`。
    2. 在地端使用 AzureML SDK 下載：
    ```python
    import tensorflow_data_validation as tfdv
    from azureml.core import Dataset

    # ws = 你的workspace
    cloud_path = Dataset.get_by_name(ws, name=(f"trainschema-{power_predict_name}-{plant}").replace('_','-')).download()
    schema = tfdv.load_schema_text(input_path = cloud_path[0])
    ```

  - **`anomalies` 下載方式：**
    > 以 {plant}_{power_predict_name}_anomaly.txt 形式儲存於 *Experiment*中的 *outputs/*。
    1. 進到 *Experiment* 的 *outputs/* 針對目標檔案點選 *Download*，如下：
    ![image info](../../static/anomaly_outputs.png)
    2. 在地端使用 AzureML SDK 下載：
    ```python
    import tensorflow_data_validation as tfdv
    from azureml.core import Dataset

    # ws = 你的workspace
    run_id = '221df76e-7156-4837-a483-402ceaeead6a'
    # 即為 Name，於 steps 的 overview 中可以查看

    model_run = ws.get_run(run_id)
    download_list = [s for s in model_run.get_file_names() if "anomaly" in s]

    for filepath in download_list:
    model_run.download_file(filepath, output_file_path = None) 
    ```
