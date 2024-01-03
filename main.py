from jobs import data_check
from jobs import data_check_csr
from jobs import csr_to_raw
from jobs import etl_sql
from jobs import raw_to_staging
from jobs import data_transfer
from jobs import csr_replace
from jobs import elec_fem_import, water_fem_import
from datetime import datetime
from jobs.baseline_renew_fcn import baseline_data_update
from jobs.ww_manpower_fcn import ww_manpower
from jobs.source_check import data_import, data_import_OPM
from jobs.raw_to_staging import raw_to_staging
from jobs.raw_to_staging_fix import raw_to_staging_fix
from jobs.staging_to_app import staging_to_app
from jobs.linving_hc_import import get_living_hc
from jobs.source_to_raw import source_to_raw, wzs_api_etl, carbon_coef_etl
from jobs.plant_cal import site2plant_ratio_cal, csr_detail_integration
from jobs.wks_detail_import import csr_item_import
from jobs.headcount_etl import payrollcnt_etl, livingcnt_etl
from jobs.fix_data import fix_raw, fix_scope1, change_plant_process, current_csr_update
from jobs.catch_carbon_price import catch_carbon_price
from jobs.carbon_emission_etl import carbon_emission_etl
from jobs.catch_weather_info import catch_weather_info
from jobs.send_WMX_revenue import send_WMX_revenue
from jobs.wzsesgi_etl import esgi_import, esgi_replace
import os

from services.mail_service import MailService

from ROI.Data_prediction_moduling import power_baseline_main_fn
from ROI.ROI_computation_moduler import air_compress_roi_main_fn
from ROI.Maintain_moduler_update import air_compress_maintain_main_fn
from ROI.Air_compress_data_daily_uploader_wsd import data_uploader_wsd_main_fn
from ROI.Air_compress_data_daily_uploader_wt_update import data_uploader_wt_main_fn
from ROI.retrain_baseline_power.retrain_main import retrain_main_fun


def get_stage():
    return os.environ['STAGE'] if 'STAGE' in os.environ else 'development'


def main(stage):

    esgi_import()

    csr_to_raw.csr_office_data_import('electricity_office')
    csr_to_raw.csr_office_data_import('water_office')
    # csr indicator data to raw indicator table
    # 總用電量先改外購電
    csr_to_raw.import_csr_data([131])  # 用電
    csr_to_raw.import_csr_data([2])  # 用水
    csr_to_raw.import_csr_data(
        [22, 23, 24, 25, 50, 67, 68, 69, 85, 91, 189])  # 廢棄物
    csr_to_raw.import_csr_data([130])  # 再生能源1

    # 碳排scope1 to staging
    csr_to_raw.import_csr_scope1()

    # csr_KPIDdetail to raw
    csr_to_raw.csr_detail_import('csr_kpidetail')

    # import csr carbon coef csr data to raw
    csr_to_raw.import_carbon_coef('carbon_coef')

    # carbon coef etl raw tp staging
    carbon_coef_etl()

    # import csr WCZ heater
    csr_to_raw.import_csr_kpi_data('heater')

    # WZS系統串接waste資料 @start on : 2022-01
    source_to_raw('wzs_waste', 'waste', '2022-01-01', stage)

    # 全球太陽能系統串已API串接solar資料 @start on : 2022-05
    # source_to_raw('solar_sys', 'renewable_energy', '2022-05-01', stage)

    # solar_climate_daily import from 2023-04-28
    source_to_raw('solar_climate', 'solar_climate_daily', '2023-04-28', stage)

    # WZS 用水用電API PIC通知同月報表數據
    # wzs_api_etl('electricity_total', '度', '2023-01-01')
    # wzs_api_etl('water', '立方米', '2023-01-01')

    # csr indicator data (site to plant) to backstage(資料更新狀態)
    # 後台設定-資料更新狀態- CSR to 後台table(包含拆分)
    etl_sql.run_sql_file('./sqls/electricity_backstage_import.sql')
    etl_sql.run_sql_file('./sqls/water_backstage_import.sql')

    # 廢棄物 csr to raw
    etl_sql.run_sql_file('./sqls/waste_csr_to_raw.sql')

    # 用電 from FEM to raw (FEM用電已拆分)
    # elec_fem_import.elec_FEM_to_raw(stage)

    # 用水 from FEM to raw
    # water_fem_import.water_FEM_to_raw(stage)

    # csr data replace
    # csr_replace.to_raw('electricity_total', 'app.electricity_backstage_update')
    # csr_replace.to_raw('water', 'app.water_backstage_update')
    # csr data append
    # csr_replace.append_to_raw('electricity_total', 'app.electricity_backstage_update')
    # csr_replace.append_to_raw('water', 'app.water_backstage_update')

   # 住宿人數
    get_living_hc()
    # 計薪人數
    ww_manpower()
    # 計算 stie to plant 比例 by 宿舍和廠區人力
    site2plant_ratio_cal('WKS')

    # 計薪人力 from employee table ETL to raw
    payrollcnt_etl('WKS')
    payrollcnt_etl('WZS')
    payrollcnt_etl('other')
    # 宿舍人力 from living_hc table ETL to raw
    livingcnt_etl()

    # WKS,WOK區分廠區/宿舍用水 to raw
    '''
    csr_detail_integration guide line
    --------
    csr_detail_integration(item: Water/Electricity, raw table, app table, site)
    '''
    # csr_detail_integration('Water', 'water_detail',
    #                        'water', stage, 'WKS')
    # csr_detail_integration('Water', 'water_detail',
    #                        'water_backstage_update', stage, 'WOK')
    # csr_detail_integration('Water', 'water_detail',
    #                        'water_backstage_update', stage, 'WTZ')

    # csr_item_import('Electricity', 'electricity_total',
    #                 ('WKS-5', 'WKS-6'), stage)

    '''
    用電,用水 : 1. 以CSR replace 廠區&辦公區 ; 2. 以WZS_ESGI replace 廠區資料 ; 3. 以CSR replace 臺灣辦公區資料
    再生能源 : 綠電綠證 by API from documnets ,太陽能以 solar system  get 廠區&辦公區資料   ; 此三者以WZS_ESGI replace 廠區資料
    碳排 : 先針對覆蓋回來的用電, 再生能源 透過碳排係數計算出 廠區&辦公區資料 ; 再用WZS_ESGI覆蓋 廠區資料
    '''
    # current_csr_update('electricity_total')

    # current_csr_update('water')

    esgi_replace('raw','electricity_total','electricity_total_wzsesgi')

    esgi_replace('raw','water','water_wzsesgi')

    csr_to_raw.office2raw('electricity_total')

    csr_to_raw.office2raw('water')

    # 1個月仍無來源先補值(0)
    fix_raw(1, 'electricity_total')
    fix_raw(1, 'water')

    # 2個月仍無來源先補值(0)
    fix_raw(2, 'electricity_total')
    fix_raw(2, 'water')

    esgi_replace('raw','renewable_energy','renewable_energy_wzsesgi',category_columns=['category1', 'category2'])

    fix_raw(1, 'renewable_energy')
    fix_raw(2, 'renewable_energy')

    carbon_emission_etl(stage)

    esgi_replace('staging','carbon_emission','carbon_emission_wzsesgi',category_columns=['category'])

    # 廠區異動 2023後的資料 WCD to WCD-1、WVN to WVN-1、 WMI 不分廠
    change_plant_process('waste', '2022-01-01')
    change_plant_process('revenue', '2022-01-01')
    change_plant_process('renewable_energy', '2022-01-01')
    change_plant_process('invoice_qty', '2022-01-01')
    change_plant_process('production_qty', '2022-01-01')
    change_plant_process('water', '2022-01-01')
    change_plant_process('electricity_total', '2022-01-01')
    change_plant_process('payrollcnt', '2022-01-01')
    change_plant_process('livingcnt', '2022-01-01')
    change_plant_process('water_detail', '2022-01-01')

    # raw to staging
    raw_to_staging('revenue', stage)  # 營收
    raw_to_staging('invoice_qty', stage)  # 出貨量
    raw_to_staging('production_qty', stage)  # 生產量
    raw_to_staging('manpower', stage)  # 人力
    raw_to_staging('livingcnt', stage)  # 宿舍人數

    raw_to_staging_fix('waste', stage)  # 廢棄物
    raw_to_staging_fix('renewable_energy', stage)  # 可再生能源
    raw_to_staging_fix('water', stage)  # 用水
    raw_to_staging_fix('electricity_saving_tech', stage)  # 節能技改
    raw_to_staging_fix('electricity_saving_digital', stage)  # 節電數位化
    raw_to_staging_fix('electricity', stage)  # 用電
    raw_to_staging_fix('water_detail', stage)  # 宿舍和工廠的用水

    # 當月仍無來源先補值(0)
    # 1個月前的補0
    fix_scope1(1)
    # 2個月前的補0
    fix_scope1(2)

    change_plant_process('carbon_emission', '2022-01-01', 'staging')

    raw_to_staging_fix('carbon_emission', stage)  # 碳排

    # send_WMX_revenue
    # send_WMX_revenue(stage)

    # staging_to_app
    staging_to_app('reporting_summary', stage)  # 首頁
    staging_to_app('energy_overview', stage)  # 總覽比較
    staging_to_app('carbon_emission_overview', stage)  # 碳排放量
    staging_to_app('renewable_energy_overview', stage)  # 可再生能源
    staging_to_app('electricity_overview', stage)  # 用電
    staging_to_app('water_overview', stage)  # 用水
    staging_to_app('electricity_unit_overview', stage)  # 單臺用電
    staging_to_app('waste_overview', stage)  # 廢棄物
    staging_to_app('decarbon_target', stage)  # 脫碳目標

    # 每年12/20複製碳排係數表
    data_transfer.copy_carbon_coef(datetime.now())
    # 節能技改 每月RESET
    data_transfer.reset_saving_tech(datetime.now())

    # baseline power and air compress ETL
    data_uploader_wsd_main_fn()
    data_uploader_wt_main_fn()
    # power_baseline_main_fn(stage)
    retrain_main_fun()
    air_compress_roi_main_fn(stage)
    air_compress_maintain_main_fn(stage)

    # daily power and production
    raw_to_staging('daily', stage)

    source_to_raw('SolarSys', 'solar_daily', '2023-02-01', stage)

    # source status ETL

    data_import('waste', '廢棄物')
    data_import('water', '用水')
    data_import('electricity_total', '用電')
    data_import('revenue', '營業額')
    data_import('production_qty', '約當生產量')
    data_import('invoice_qty', '出貨量')
    data_import('renewable_energy', '可再生能源')
    data_import('payrollcnt', '人力')
    data_import_OPM('出貨量')
    data_import_OPM('營業額')
    baseline_data_update()
    catch_weather_info()
    catch_carbon_price()
    data_check.data_check(datetime.now().day)
    data_check_csr.data_check_csr()


if __name__ == '__main__':
    stage = get_stage()

    try:
        main(stage)
    except Exception as inst:
        mail = MailService('[failed][{}] etl cron job report'.format(stage))
        mail.send('failed: {}'.format(inst))
