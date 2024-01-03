from flask import Flask
from flask import request
from jobs import excel_to_raw

from jobs.raw_to_staging import raw_to_staging
from jobs.staging_to_app import staging_to_app

from jobs import etl_sql

from services.mail_service import MailService

import os
from datetime import datetime

app = Flask(__name__)


def get_stage():
    return os.environ['STAGE'] if 'STAGE' in os.environ else 'development'

@app.route("/excel_to_raw", methods=['POST'])
def route_excel_to_raw():
    if 'filename' not in request.json:
        return {'msg': 'please give filename'}

    stage = get_stage()

    try:
        filename = request.json['filename']

        excel_to_raw.excel_to_raw(os.path.dirname(
            __file__) + '/files/{}'.format(filename))

        raw_to_staging('revenue', stage),  # 營收
        raw_to_staging('renewable_energy', stage),  # 可再生能源
        raw_to_staging('invoice_qty', stage),  # 出貨量
        raw_to_staging('production_qty', stage),  # 生產量
        raw_to_staging('water', stage),  # 用水
        raw_to_staging('electricity_saving_tech', stage),  # 節能技改
        raw_to_staging('electricity_saving_digital', stage),  # 節電數位化
        raw_to_staging('electricity', stage),  # 用電
        raw_to_staging('carbon_emission', stage),  # 碳排
        raw_to_staging('waste', stage)
        staging_to_app('reporting_summary', stage),  # 首頁
        staging_to_app('energy_overview', stage),  # 總覽比較
        staging_to_app('carbon_emission_overview', stage),  # 碳排放量
        staging_to_app('renewable_energy_overview', stage),  # 可再生能源
        staging_to_app('electricity_overview', stage),  # 用電
        staging_to_app('water_overview', stage),  # 用水
        staging_to_app('electricity_unit_overview', stage)  # 單臺用電
        staging_to_app('waste_overview', stage)

        return {'msg': 'success'}
    except Exception as inst:
        print(inst)
        return {'msg': 'fail'}, 400


def executes_funcs(*funcs):
    for func in funcs:
        func()


def upload_base(filename, *funcs):
    try:
        excel_to_raw.excel_to_raw(os.path.dirname(
            __file__) + '/files/{}'.format(filename))

        executes_funcs(*funcs)
        return {'msg': 'success'}
    except Exception as inst:
        print(inst)
        return {'msg': 'fail'}, 400


def upload_newbase(filename, *funcs):
    try:
        excel_to_raw.new_rpt_import(os.path.dirname(
            __file__) + '/files/{}'.format(filename))

        executes_funcs(*funcs)
        return {'msg': 'success'}
    except Exception as inst:
        print(inst)
        return {'msg': 'fail'}, 400


@app.route("/upload_shipment_excel", methods=['POST'])
def route_upload_shipment_excel():
    if 'filename' not in request.json:
        return {'msg': 'please give filename'}

    stage = get_stage()

    filename = request.json['filename']
    print(filename)
    return upload_base(filename,
                       raw_to_staging('invoice_qty', stage),
                       staging_to_app('energy_overview', stage),
                       staging_to_app('electricity_overview', stage)
                       )


@app.route("/upload_energy_excel", methods=['POST'])
def route_upload_energy_excel():
    if 'filename' not in request.json:
        return {'msg': 'please give filename'}

    stage = get_stage()

    filename = request.json['filename']
    print(filename)
    return upload_base(filename,
                       raw_to_staging('revenue', stage),  # 營收
                       raw_to_staging('renewable_energy', stage),  # 可再生能源
                       raw_to_staging('invoice_qty', stage),  # 出貨量
                       raw_to_staging('production_qty', stage),  # 生產量
                       raw_to_staging('water', stage),  # 用水
                       raw_to_staging(
                           'electricity_saving_tech', stage),  # 節能技改
                       raw_to_staging(
                           'electricity_saving_digital', stage),  # 節電數位化
                       raw_to_staging('electricity', stage),  # 用電
                       raw_to_staging('carbon_emission', stage),  # 碳排
                       staging_to_app('reporting_summary', stage),  # 首頁
                       staging_to_app('energy_overview', stage),  # 總覽比較
                       staging_to_app(
                           'carbon_emission_overview', stage),  # 碳排放量
                       staging_to_app(
                           'renewable_energy_overview', stage),  # 可再生能源
                       staging_to_app('electricity_overview', stage),  # 用電
                       staging_to_app('water_overview', stage),  # 用水
                       staging_to_app(
                           'electricity_unit_overview', stage)  # 單臺用電
                       )


@app.route("/upload_new_report", methods=['POST'])
def route_upload_new_report():
    if 'filename' not in request.json:
        return {'msg': 'please give filename'}

    filename = request.json['filename']
    print(filename)
    return upload_newbase(filename)


@app.route("/exec_cron", methods=['GET'])
def route_exec_cron():
    def runCronJob(e):
        # add record
        db.add(e)
        db.commit()
        # execute main
        main(stage)
        # update record
        e.done = True
        e.done_at = datetime.now()
        db.commit()

    stage = get_stage()

    try:
        from sqlalchemy import desc
        from models.etl_status import EtlStatus
        from models.engine import session
        from main import main

        db = session()

        lastItem = db.query(EtlStatus).order_by(desc('id')).first()

        if lastItem is None:
            e = EtlStatus(False, datetime.now(), None, datetime.now())
            runCronJob(e)
        elif not lastItem.done:
            return {'msg': 'ETL is running.', 'execute_date': lastItem.executed_at}
        else:
            e = EtlStatus(False, datetime.now(), None, datetime.now())
            runCronJob(e)

            return {'msg': 'success'}
    except Exception as inst:
        e.done = True
        db.commit()

        print(inst)
        mail = MailService('[failed][{}] etl cron job report'.format(stage))
        mail.send('failed: {}'.format(inst))
        return {'msg': 'fail'}, 400


@app.route("/carbon", methods=['POST'])
def update_carbon():
    stage = get_stage()

    try:
        etl_sql.run_sql_file('./sqls/staging_carbon_emission.sql')
        raw_to_staging('carbon_emission', stage)
        staging_to_app('reporting_summary', stage)
        staging_to_app('carbon_emission_overview', stage)

        return {'msg': 'success'}
    except Exception as inst:
        print(inst)
        return {'msg': 'fail'}, 400


@app.route("/csr_data_import", methods=['POST'])
def route_csr_data_import():
    try:
        from jobs.csr_data_import import main as csr_data_import_main
        csr_data_import_main()

        return {'msg': 'success'}
    except Exception as inst:
        return {'msg': inst.args}, 401


@app.route("/copy_target_green", methods=['POST'])
def route_copy_target_green():
    try:
        from jobs.data_transfer import main as copy_target_green_main
        copy_target_green_main()

        return {'msg': 'success'}
    except Exception as inst:
        return {'msg': inst.args}, 401


@app.route("/upload_revenue", methods=['POST'])
def route_upload_revenue():
    if 'filename' not in request.json:
        return {'msg': 'please give filename'}
    stage = get_stage()
    filename = request.json['filename']
    print(filename)
    try:
        excel_to_raw.excel_to_raw(os.path.dirname(
            __file__) + '/files/{}'.format(filename))
        print('import wmx revenue done')
        raw_to_staging('revenue', stage),  # 營收
        print('raw_to_staging revenue done')
        staging_to_app('reporting_summary', stage),  # 首頁
        print('reporting_summarye done')
        staging_to_app('energy_overview', stage),  # 總覽比較
        print('energy_overview done')
        staging_to_app('electricity_overview', stage),  # 用電
        print('electricity_overview done')
        staging_to_app('water_overview', stage),  # 用水
        print('water_overview done')
        staging_to_app('waste_overview', stage)
        print('waste_overview done')

        return {'msg': 'success'}
    except Exception as inst:
        print(inst)
        return {'msg': 'fail'}, 400
    # return upload_base(filename,
    #                     raw_to_staging('revenue', stage),  # 營收
    #                     staging_to_app('reporting_summary', stage),  # 首頁
    #                     staging_to_app('energy_overview', stage),  # 總覽比較
    #                     staging_to_app('electricity_overview', stage),  # 用電
    #                     staging_to_app('water_overview', stage),  # 用水
    #                     staging_to_app('waste_overview', stage)
    #                    )


@app.route("/update_revenue_adjustment", methods=['POST'])
def route_update_revenue_adjustment():
    stage = get_stage()
    raw_to_staging('revenue', stage)
    staging_to_app('reporting_summary', stage)  # 首頁
    staging_to_app('energy_overview', stage)  # 總覽比較
    staging_to_app('carbon_emission_overview', stage)  # 碳排放量
    staging_to_app('electricity_overview', stage)  # 用電
    staging_to_app('water_overview', stage)  # 用水
    staging_to_app('waste_overview', stage)  # 廢棄物

    return {'msg': 'success'}
