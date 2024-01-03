import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta
from sqlalchemy import *
import requests
from models import engine


def get_living_hc():
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)
    # 取得住宿人數
    version = (dt.now()-timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        get_data = requests.get(
            'http://tddweb105.wks.wistron.com.cn/DormAPI/API/Common/GetBedEmpCountGroupByPlant2')

        list_of_dicts = get_data.json()
        hc = pd.json_normalize(list_of_dicts['Group_By_PlantID'])

        hc = hc[hc['Site'] != 'CSD']
        hc['PlantID'] = hc['PlantID'].replace("P1", "WKS-1")
        hc['PlantID'] = hc['PlantID'].replace("P5", "WKS-5")
        hc['PlantID'] = hc['PlantID'].replace("P6", "WKS-6")
        hc['PlantID'] = hc['PlantID'].replace("Site", "Site_KS")
        hc['plant'] = hc['PlantID']
        hc['amount'] = hc['EmpCount']
        hc = hc[['plant', 'amount']]

        hc['version'] = version
        hc['last_update_time'] = dt.strptime(
            dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        conn = db.connect()
        conn.execute(f"DELETE FROM raw.living_hc WHERE  version = '{version}'")

        hc.to_sql('living_hc', db, index=False, if_exists='append',
                  schema='raw', chunksize=10000)
        conn.close()

        return True

    except Exception as e:
        error = str(e)
        print(error)

        return False
