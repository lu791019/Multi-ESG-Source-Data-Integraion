import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from models import engine

def site_plant_transf(x):
    site = x['site']
    plant = x['plant']
    
    if (site=='WKS') & (plant == 'P1'):
        plant = 'WKS-1'
        
    elif (site=='WKS') & (plant == 'P5'):
        plant = 'WKS-5'
        
    elif (site=='WKS') & (plant == 'P6'):
        plant = 'WKS-6'
        
    elif (site=='WOK'):
        plant = 'WOK' 

    elif (site=='WCD') & (plant == 'WCD'):
        plant = 'WCD-1' 
        
    elif (site=='WTZ'):
        plant = 'WTZ' 
        
    elif (site=='WZS') & (plant == 'P1'):
        plant = 'WZS-1'
        
    elif (site=='WZS') & (plant == 'P3'):
        plant = 'WZS-3'

    # 未來可能會併入 WZS-8 的人力
    elif (site=='WZS') & (plant == 'P5'):
        plant = 'WZS-5'
    
    elif (site=='WZS') & (plant == 'P6'):
        plant = 'WZS-6'
    
    elif (site=='WZSOPT') & (plant == 'P6'):
        plant = 'WZS-6'
    
    elif (site=='WZS') & (plant == 'P8'):
        plant = 'WZS-8'
    
    elif (site=='WCQ') & (plant == 'P1'):
        plant = 'WCQ'
        
    elif (site=='WCQ') & (plant == 'P2'):
        plant = 'WCQ'
    
    elif (site=='WMY') & (plant == 'P1'):
        plant = 'WMY-1'
        
    elif (site=='WMY') & (plant == 'P3'):
        plant = 'WMY-3'

    elif (site=='WIHK') & (plant == 'WIHK-1'):
        plant = 'WIHK-1'
    
    elif (site=='WIHK') & (plant == 'WIHK-2'):
        plant = 'WIHK-2'
        
    elif (site=='XTRKS'):
        plant = 'XTRKS' 
    
    else:
        plant = site
    return plant




def ww_manpower():

    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)

    plant_mapping = pd.read_sql('SELECT * FROM raw.plant_mapping',con=conn)

    try:
        ww_hcm_wa_employeeinfo_g1 = pd.read_sql("""
                SELECT bu,bg,site,plant,emplid,"location",company, jobtitle_descr,termination_dt,labor_type,job_family,last_updt_dt 
                FROM raw.ww_hcm_wa_employeeinfo_g1 
                WHERE batch_id = (
                    SELECT MAX(batch_id) 
                    FROM raw.ww_hcm_wa_employeeinfo_g1)
            """,con=conn)

        # 新竹湖口廠的人力被歸類在 WIH，需要用 location 區分。
        ww_hcm_wa_employeeinfo_g1.loc[(ww_hcm_wa_employeeinfo_g1['site']=='WIH') & (ww_hcm_wa_employeeinfo_g1['location']=='010'),['site','plant']] = ['WIHK','WIHK-1']
        ww_hcm_wa_employeeinfo_g1.loc[(ww_hcm_wa_employeeinfo_g1['site']=='WIH') & (ww_hcm_wa_employeeinfo_g1['location']=='032'),['site','plant']] = ['WIHK','WIHK-2']

        # 抓取我們需要的 site ('WZSOPT','WIHK'不在名單上需要額外加入，另外需要額外保存新邊界的人力資訊，以便未來需要)
        need_site = pd.Series(['WZSOPT','WIHK','WBR','XTRKS','WSMX','WMKS','WSKS','WGKS','WVN','WSCQ','WTX','WMY','WMI','WSCZ'])
        ww_hcm_wa_employeeinfo_g1 = ww_hcm_wa_employeeinfo_g1[ww_hcm_wa_employeeinfo_g1.site.isin(plant_mapping.site.append(need_site))]

        # termination_dt : 最後受薪日 (有值表示離職，會保存一個月的資料。因此選擇空值的來計算人力)
        ww_hcm_wa_employeeinfo_g1 = ww_hcm_wa_employeeinfo_g1[ww_hcm_wa_employeeinfo_g1.termination_dt.isnull()]

        # 對應 plant
        ww_hcm_wa_employeeinfo_g1['plant'] = ww_hcm_wa_employeeinfo_g1.apply(lambda x : site_plant_transf(x),1)

        # 修改更新日期格式
        ww_hcm_wa_employeeinfo_g1.last_updt_dt = ww_hcm_wa_employeeinfo_g1.last_updt_dt.astype(str).str.slice(0,10)


        # 第二個字是 1 : dl
        # 第二個字是 2,3 : idl
        ww_hcm_wa_employeeinfo_g1['dl_idl'] = ww_hcm_wa_employeeinfo_g1.labor_type.str.slice(1,2)
        ww_hcm_wa_employeeinfo_g1.loc[ww_hcm_wa_employeeinfo_g1['dl_idl']!='1','dl_idl'] = 'idl'
        ww_hcm_wa_employeeinfo_g1.loc[ww_hcm_wa_employeeinfo_g1['dl_idl']=='1','dl_idl'] = 'dl'

        employe_count = \
            ww_hcm_wa_employeeinfo_g1\
                .groupby(['site','plant','dl_idl','last_updt_dt'])\
                .agg({'emplid':'nunique'})\
                .reset_index()\
                .rename(columns={
                    'emplid':'count',
                    'last_updt_dt':'period_start'
                })

        psd = employe_count.period_start.unique()[0]

        employeeinfo_count = \
            pd.pivot_table(employe_count, values = 'count', columns = ['dl_idl'],index = ['site','plant','period_start'])\
                    .reset_index()\
                    .rename_axis(None, axis=1)\
                    .fillna(0)
        employeeinfo_count['last_update_time'] = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        # 避免測試時一天重複執行多次導致同一天資料被多次上傳，因此每次上傳皆會先移除當日的資訊
        conn.execute(
            f"""Delete From raw.employeeinfo_count where period_start = '{psd}' """)
        employeeinfo_count.to_sql(name='employeeinfo_count',schema='raw',if_exists='append',index=False,con=conn)

        return True
        
    except Exception as e:
        print(e)
        return False
