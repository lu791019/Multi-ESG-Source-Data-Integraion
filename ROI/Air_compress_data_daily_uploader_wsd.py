#!/usr/bin/python
from elasticsearch import Elasticsearch,helpers
from elasticsearch.helpers import scan
from es_client import *
# import es_body
import pandas as pd
import datetime
from sqlalchemy import create_engine
from models import engine


def scan_es_rawdata(es_client,es_search_body,es_index,es_type):
    #scan es with body query
    es_result = scan(
        client = es_client,
        query = es_search_body,
        scroll = '1m',
        index = es_index,
        doc_type = es_type,
        timeout='10s'
    )
    # print(es_result)
    #clean data
    final_result = []
    for item in es_result:
        (item['_source']).update(item['_source']['detail'])
        final_result.append(item['_source'])
    return final_result

def es_body_geneter(start_time, end_time):
    # ES body structure
    es_search_body = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "periodEnd": {
                                            "from": start_time,
                                            "to": end_time,
                                            "include_lower": True,
                                            "include_upper": True,
                                            "boost": 1
                                        }
                                    }
                                }
                            ],
                            "disable_coord": False,
                            "adjust_pure_negative": True,
                            "boost": 1
                        }
                    }
                ],
                "disable_coord": 'false',
                "adjust_pure_negative": 'true',
                "boost": 1
            }
        }
    }
    return es_search_body

def data_uploader(data, db_name, table_name):
    # Truncate table
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    # conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name,conn,index= False, if_exists = 'append',schema=db_name, chunksize = 10000)
    return 0

def data_uploader_wsd_main_fn():
    try:
        print('Upload data is start!')
        # ES inital value setting
        ES_SERVERS = [{
            'host': '10.66.28.41',
            'port': 9200
        }]
        es_client = Elasticsearch(
            hosts=ES_SERVERS
        )
        es_index = 'accs*'
        es_type = "machine_info_detail"
        now_time = datetime.datetime.now()
        start_time = datetime.datetime.timestamp(now_time-pd.Timedelta(days=1))*1000
        end_time = datetime.datetime.timestamp(now_time)*1000
        # Generate es body
        print('Get es body.')
        es_search_body = es_body_geneter(start_time, end_time)
        # Download data from es env
        print('Get raw data.')
        result = scan_es_rawdata(es_client,es_search_body,es_index,es_type)
        # Parse list to dataframe
        print('To pandas dataframe.')
        result_dataframe = pd.DataFrame(result)
        result_dataframe = result_dataframe.drop(columns = {'detail'})
        result_dataframe.columns = [x.lower() for x in result_dataframe.columns]
        # POSTGREL DB inital value
        print('Set db setting')
        # Upload dataset to DB
        print('Upload data to db.')
        for x in ['C','F2','Fab12']:
            data_uploader(result_dataframe.loc[result_dataframe.building==x,:].reset_index(drop=True),'raw','accs_data')
            print('Upload data for '+ x +' building is successful!')
        print('Upload data is finished!')
        return 0
    except Exception as e:
        error = str(e)
        return error