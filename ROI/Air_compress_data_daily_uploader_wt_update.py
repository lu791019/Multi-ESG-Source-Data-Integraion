#!/usr/bin/python
from elasticsearch import Elasticsearch,helpers
from elasticsearch.helpers import scan
from es_client import *
import http.client
import json
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from models import engine
import psycopg2
import re

def scan_api_rawdata(api_payload,api_index,api_type,building,data_type):
    # set connection and query statement
    if (building=='KD') and (data_type=='energy'):
        conn = http.client.HTTPConnection("10.66.28.41", 9200, timeout = 60)
    elif (building=='P1') or (building=='Other'):
        conn = http.client.HTTPSConnection("dfbnifip05.wistron.com", 9200)
    elif (building=='W1'):
        conn = http.client.HTTPSConnection("wti40cdhdatap02.wistron.azure-southeastasia", 9200)
    else:
        conn = http.client.HTTPConnection("10.41.241.6", 9200, timeout = 60)
    # use ES search API to get data
    payload = json.dumps(api_payload)
    if (building=='P1') or (building=='Other'):
        headers = {
            'Authorization': 'Basic d2tzZXM6RlNwQlgkanFsNw==',
            'Content-Type': 'application/json'
        }
        conn.request("GET", "/"+api_index+"/_search", payload, headers)
    elif (building=='W1'):
        from base64 import b64encode
        headers = {
            # 'Authorization': 'Basic d2tzZXM6RlNwQlgkanFsNw==',
            'Authorization': "Basic {}".format(
                b64encode(bytes(f"{'wtes7adm'}:{'W!stronwtes7'}", "utf-8")).decode("ascii")
            ),
            'Content-Type': 'application/json'
        }
        conn.request("GET", "/"+api_index+"/_search", payload, headers)
    else:
        headers = {
            'Content-Type': 'application/json'
        }
        conn.request("GET", "/"+api_index+"/"+api_type+"/_search", payload, headers)
    res = conn.getresponse()
    data = res.read()
    # print(data)

    # # decode json data
    data = data.decode("utf-8").replace("'", '"')
    return data

def scan_es_rawdata(es_search_body,es_index,es_type):
    # ES inital value setting
    ES_SERVERS = [{
        'host': '10.41.241.6',
        'port': 9200
    }]
    es_client = Elasticsearch(
        hosts=ES_SERVERS
    )
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
        # (item['_source']).update(item['_source']['detail'])
        final_result.append(item['_source'])
    return final_result

def machine_info_generater_tb1(start_time, end_time, building):
    if (building=='TB1') or (building=='P1') or (building=='Other'):
        time_index = 'UploadTime'
        building_index = 'Building'
    elif (building=='KD') or (building=='W1'):
        time_index = 'evt_dt'
        building_index = 'Building'
    payload = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "range": {
                                                    time_index: {
                                                        "from": start_time,
                                                        "to": end_time,
                                                        "include_lower": True,
                                                        "include_upper": True,
                                                        "boost": 1
                                                    }
                                                }
                                            },
                                            {
                                                "match_phrase": {
                                                    building_index: {
                                                        "query": building,
                                                        "slop": 0,
                                                        "boost": 1
                                                    }
                                                }
                                            }
                                        ],
                                        # "disable_coord": False,
                                        "adjust_pure_negative": True,
                                        "boost": 1
                                    }
                                }
                            ],
                            # "disable_coord": False,
                            "adjust_pure_negative": True,
                            "boost": 1
                        }
                    }
                ],
                # "disable_coord": False,
                "adjust_pure_negative": True,
                "boost": 1
            }
        }
    }
    return payload

def machine_info_generater(start_time, end_time, building):
    if (building=='TB1') or (building=='P1') or (building=='Other'):
        time_index = 'Uploadtime'
        press_index = 'CanPress'
        temp_index = 'CanTemperature'
        name_index = 'ID'
        plant_index = 'Plant'
        building_index = 'Building'
    # elif building=='KD':
    #     time_index = 'evt_dt'
    #     press_index = 'OutlePres'
    #     temp_index = 'OutleTemp'
    #     name_index = 'Name'
    #     plant_index = 'site'
    #     building_index = 'building'
    else:
        time_index = 'evt_dt'
        press_index = 'OutlePres'
        temp_index = 'OutleTemp'
        name_index = 'Name'
        plant_index = 'Site'
        building_index = 'Building'
    payload = {
        "query": {
            "bool": {

                "filter": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "range": {
                                                    time_index: {
                                                        "from": start_time,
                                                        "to": end_time,
                                                        "include_lower": True,
                                                        "include_upper": True,
                                                        "boost": 1
                                                    }
                                                }
                                            },
                                            {
                                                "match_phrase": {
                                                    "Building": {
                                                        "query": building,
                                                        "slop": 0,
                                                        "boost": 1
                                                    }
                                                }
                                            }
                                        ],
                                        # "disable_coord": False,
                                        "adjust_pure_negative": True,
                                        "boost": 1
                                    }
                                }
                            ],
                            # "disable_coord": False,
                            "adjust_pure_negative": True,
                            "boost": 1
                        }
                    }
                ],
                # "disable_coord": False,
                "adjust_pure_negative": True,
                "boost": 1
            }
        },
        "_source": {
            "includes": [
                "AVG",
                "AVG",
                "MAX",
                "MIN"
            ],
            "excludes": []
        },
        "aggregations": {
            name_index: {
                "terms": {
                    "field": name_index+".raw",
                    "size": 1000,
                    "min_doc_count": 1,
                    "shard_min_doc_count": 0,
                    "show_term_doc_count_error": False,
                    "order": [
                        {
                            "_count": "desc"
                        },
                        {
                            "_term": "asc"
                        }
                    ]
                },
                "aggregations": {
                    'Site': {
                        "terms": {
                            "field": plant_index+".raw",
                            "size": 1000,
                            "min_doc_count": 1,
                            "shard_min_doc_count": 0,
                            "show_term_doc_count_error": False,
                            "order": [
                                {
                                    "_count": "desc"
                                },
                                {
                                    "_term": "asc"
                                }
                            ]
                        },
                        "aggregations": {
                            "Building": {
                                "terms": {
                                    "field": building_index+".raw",
                                    "size": 1000,
                                    "min_doc_count": 1,
                                    "shard_min_doc_count": 0,
                                    "show_term_doc_count_error": False,
                                    "order": [
                                        {
                                            "_count": "desc"
                                        },
                                        {
                                            "_term": "asc"
                                        }
                                    ]
                                },
                                "aggregations": {
                                    "Time": {
                                        "date_histogram": {
                                            "field": time_index,
                                            "format": "yyyy-MM-dd HH:mm:ss",
                                            "time_zone": "+08:00",
                                            "interval": "1h",
                                            "offset": 0,
                                            "order": {
                                                "_key": "asc"
                                            },
                                            "keyed": False,
                                            "min_doc_count": 0
                                        },
                                        "aggregations": {
                                            "OutlePres": {
                                                "avg": {
                                                    "field": press_index
                                                }
                                            },
                                            "OutleTemp": {
                                                "avg": {
                                                    "field": temp_index
                                                }
                                            },
                                            "RunTime_max": {
                                                "max": {
                                                    "field": "RunTime"
                                                }
                                            },
                                            "RunTime_min": {
                                                "min": {
                                                    "field": "RunTime"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return payload

def flow_generater_f1(start_time, end_time, building):
    if building=='F1':
        time_index = 'evt_dt'
        building_index = 'building'
    payload = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "range": {
                                                    time_index: {
                                                        "from": start_time,
                                                        "to": end_time,
                                                        "include_lower": True,
                                                        "include_upper": True,
                                                        "boost": 1
                                                    }
                                                }
                                            },
                                            {
                                                "match_phrase": {
                                                    building_index: {
                                                        "query": building,
                                                        "slop": 0,
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
                            "disable_coord": False,
                            "adjust_pure_negative": True,
                            "boost": 1
                        }
                    }
                ],
                "disable_coord": False,
                "adjust_pure_negative": True,
                "boost": 1
            }
        }
    }
    return payload

def flow_generater(start_time, end_time, building):
    if (building=='KD') or (building=='W1'):
        # plant_index = 'Area'
        # building_index = building+'_1F'
        # reading_index = 'Total_flow'
        # meter_index = 'Meterid'
        plant_index = 'building'
        building_index = building
        reading_index = 'reading'
        meter_index = 'meterId'
    elif (building=='P1') or (building=='Other'):
        plant_index = 'Area'
        building_index = building
        reading_index = 'Total_flow'
        meter_index = 'Meterid'
    else:
        plant_index = 'area'
        building_index = building
        reading_index = 'reading'
        meter_index = 'meterId'
    payload = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "match_phrase": {
                                                    plant_index: {
                                                        "query": building_index,
                                                        "slop": 0,
                                                        "boost": 1
                                                    }
                                                }
                                            },
                                            {
                                                "range": {
                                                    "evt_dt": {
                                                        "from": start_time,
                                                        "to": end_time,
                                                        "include_lower": True,
                                                        "include_upper": True,
                                                        "boost": 1
                                                    }
                                                }
                                            }
                                        ],
                                        # "disable_coord": False,
                                        "adjust_pure_negative": True,
                                        "boost": 1
                                    }
                                }
                            ],
                            # "disable_coord": False,
                            "adjust_pure_negative": True,
                            "boost": 1
                        }
                    }
                ],
                # "disable_coord": False,
                "adjust_pure_negative": True,
                "boost": 1
            }
        },
        "_source": {
            "includes": [
                "MAX",
                "MIN"
            ],
            "excludes": []
        },
        "aggregations": {
            "meterId": {
                "terms": {
                    "field": meter_index+".raw",
                    "size": 1000,
                    "min_doc_count": 1,
                    "shard_min_doc_count": 0,
                    "show_term_doc_count_error": False,
                    "order": [
                        {
                            "_count": "desc"
                        },
                        {
                            "_term": "asc"
                        }
                    ]
                },
                "aggregations": {
                    "Time": {
                        "date_histogram": {
                            "field": "evt_dt",
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "time_zone": "+08:00",
                            "interval": "1h",
                            "offset": 0,
                            "order": {
                                "_key": "asc"
                            },
                            "keyed": False,
                            "min_doc_count": 0
                        },
                        "aggregations": {
                            "MAX(reading)": {
                                "max": {
                                    "field": reading_index
                                }
                            },
                            "MIN(reading)": {
                                "min": {
                                    "field": reading_index
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return payload

def energy_generater(start_time, end_time, building):
    if (building=='KD') or (building=='P1') or (building=='Other') or (building=='W1'):
        # plant_index = 'building'
        if building=='Other':
            building = 'F1'
        payload = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "match_phrase": {
                                                        "building": {
                                                            "query": building,
                                                            "slop": 0,
                                                            "boost": 1
                                                        }
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "evt_dt": {
                                                            "from": start_time,
                                                            "to": end_time,
                                                            "include_lower": True,
                                                            "include_upper": True,
                                                            "boost": 1
                                                        }
                                                    }
                                                }
                                            ],
                                            # "disable_coord": False,
                                            "adjust_pure_negative": True,
                                            "boost": 1
                                        }
                                    }
                                ],
                                # "disable_coord": False,
                                "adjust_pure_negative": True,
                                "boost": 1
                            }
                        }
                    ],
                    # "disable_coord": False,
                    "adjust_pure_negative": True,
                    "boost": 1
                }
            },
            "_source": {
                "includes": [
                    "MAX",
                    "MIN"
                ],
                "excludes": []
            },
            "aggregations": {
                "meterId": {
                    "terms": {
                        "field": "meterId.raw",
                        "size": 1000,
                        "min_doc_count": 1,
                        "shard_min_doc_count": 0,
                        "show_term_doc_count_error": False,
                        "order": [
                            {
                                "_count": "desc"
                            },
                            {
                                "_term": "asc"
                            }
                        ]
                    },
                    "aggregations": {
                        "Time": {
                            "date_histogram": {
                                "field": "evt_dt",
                                "format": "yyyy-MM-dd HH:mm:ss",
                                "time_zone": "+08:00",
                                "interval": "1h",
                                "offset": 0,
                                "order": {
                                    "_key": "asc"
                                },
                                "keyed": False,
                                "min_doc_count": 0
                            },
                            "aggregations": {
                                "MAX(reading)": {
                                    "max": {
                                        "field": "reading"
                                    }
                                },
                                "MIN(reading)": {
                                    "min": {
                                        "field": "reading"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    elif building=='F1':
        plant_index = 'area'
        payload = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "bool": {
                                                                    "must_not": [
                                                                        {
                                                                            "match_phrase": {
                                                                                "area": {
                                                                                    "query": building,
                                                                                    "slop": 0,
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
                                                        "disable_coord": False,
                                                        "adjust_pure_negative": True,
                                                        "boost": 1
                                                    }
                                                },
                                                {
                                                    "match_phrase": {
                                                        "building": {
                                                            "query": building,
                                                            "slop": 0,
                                                            "boost": 1
                                                        }
                                                    }
                                                },
                                                {
                                                    "match_phrase": {
                                                        "evt_pubBy": {
                                                            "query": "WTAD",
                                                            "slop": 0,
                                                            "boost": 1
                                                        }
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "evt_dt": {
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
                                "disable_coord": False,
                                "adjust_pure_negative": True,
                                "boost": 1
                            }
                        }
                    ],
                    "disable_coord": False,
                    "adjust_pure_negative": True,
                    "boost": 1
                }
            },
            "_source": {
                "includes": [
                    "MAX",
                    "MIN"
                ],
                "excludes": []
            },
            "aggregations": {
                "meterId": {
                    "terms": {
                        "field": "meterId.raw",
                        "size": 1000,
                        "min_doc_count": 1,
                        "shard_min_doc_count": 0,
                        "show_term_doc_count_error": False,
                        "order": [
                            {
                                "_count": "desc"
                            },
                            {
                                "_term": "asc"
                            }
                        ]
                    },
                    "aggregations": {
                        "Time": {
                            "date_histogram": {
                                "field": "evt_dt",
                                "format": "yyyy-MM-dd HH:mm:ss",
                                "time_zone": "+08:00",
                                "interval": "1h",
                                "offset": 0,
                                "order": {
                                    "_key": "asc"
                                },
                                "keyed": False,
                                "min_doc_count": 0
                            },
                            "aggregations": {
                                "MAX(reading)": {
                                    "max": {
                                        "field": "reading"
                                    }
                                },
                                "MIN(reading)": {
                                    "min": {
                                        "field": "reading"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    else:
        plant_index = 'area'
        payload = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "bool": {
                                                                    "must_not": [
                                                                        {
                                                                            "match_phrase": {
                                                                                "area": {
                                                                                    "query": building,
                                                                                    "slop": 0,
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
                                                        "disable_coord": False,
                                                        "adjust_pure_negative": True,
                                                        "boost": 1
                                                    }
                                                },
                                                {
                                                    "match_phrase": {
                                                        "building": {
                                                            "query": building,
                                                            "slop": 0,
                                                            "boost": 1
                                                        }
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "evt_dt": {
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
                                "disable_coord": False,
                                "adjust_pure_negative": True,
                                "boost": 1
                            }
                        }
                    ],
                    "disable_coord": False,
                    "adjust_pure_negative": True,
                    "boost": 1
                }
            },
            "_source": {
                "includes": [
                    "MAX",
                    "MIN"
                ],
                "excludes": []
            },
            "aggregations": {
                "meterId": {
                    "terms": {
                        "field": "meterId.raw",
                        "size": 1000,
                        "min_doc_count": 1,
                        "shard_min_doc_count": 0,
                        "show_term_doc_count_error": False,
                        "order": [
                            {
                                "_count": "desc"
                            },
                            {
                                "_term": "asc"
                            }
                        ]
                    },
                    "aggregations": {
                        "Time": {
                            "date_histogram": {
                                "field": "evt_dt",
                                "format": "yyyy-MM-dd HH:mm:ss",
                                "time_zone": "+08:00",
                                "interval": "1h",
                                "offset": 0,
                                "order": {
                                    "_key": "asc"
                                },
                                "keyed": False,
                                "min_doc_count": 0
                            },
                            "aggregations": {
                                "MAX(reading)": {
                                    "max": {
                                        "field": "reading"
                                    }
                                },
                                "MIN(reading)": {
                                    "min": {
                                        "field": "reading"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    return payload

def machine_info_data_generater(api_payload,api_index,api_type,building,data_type):    
    if (building!='TB1') & (building!='KD') & (building!='P1') & (building!='Other'):
        dataset_raw_json = scan_api_rawdata(api_payload,api_index,api_type,building,data_type)
        dataset_json = json.loads(dataset_raw_json)
        time_dataset_all = pd.DataFrame({})
        for index in range(len(dataset_json['aggregations']['Name']['buckets'])):
            dataset_json_sub = dataset_json['aggregations']['Name']['buckets'][index]
            name_sub = dataset_json_sub['key']
            site_sub = dataset_json_sub['Site']['buckets'][0]['key']
            building_sub = dataset_json_sub['Site']['buckets'][0]['Building']['buckets'][0]['key']
            time_record_json = dataset_json_sub['Site']['buckets'][0]['Building']['buckets'][0]['Time']['buckets']
            time_dataset = pd.DataFrame(time_record_json)
            time_dataset['Name'] = name_sub
            time_dataset['Site'] = site_sub
            time_dataset['Building'] = building_sub
            for feature_name in ['OutleTemp','RunTime_min','OutlePres','RunTime_max']:
                time_dataset[feature_name] = [x['value'] for x in time_dataset[feature_name]]
            time_dataset_all = time_dataset_all.append(time_dataset).reset_index(drop=True)
        time_dataset_all = time_dataset_all.rename(columns={'key':'Time'})
        # Seperate TB5B and other
        if building=='TB5B':
            time_dataset_all['Name'] = ['AC'+str(int(x[2:x.find('(')])+12) for x in time_dataset_all.Name]
        elif building=='TB5':
            # time_dataset_all['Name'] = [np.where(x[(x.find('(')-1):x.find('(')]=='B','AC'+str(int(x[2:(x.find('(')-1)])+12),'AC'+str(int(x[2:(x.find('(')-1)]))) for x in time_dataset_all.Name]
            time_dataset_all['Name'] = [machine_id_mapping(x) for x in time_dataset_all.Name]
            time_dataset_all['Name'] = time_dataset_all.Name.astype('str')
        else:
            # time_dataset_all['Name'] = [x[0:x.find('(')] for x in time_dataset_all.Name]
            time_dataset_all['Name'] = time_dataset_all['Name'].apply(lambda x: x if x.find('(')==-1 else x[0:x.find('(')])
            time_dataset_all['Name'] = [re.sub('-','',x) for x in time_dataset_all.Name]
        time_dataset_all['RunTime'] = time_dataset_all.RunTime_max-time_dataset_all.RunTime_min
        time_dataset_all = time_dataset_all.rename(columns={'Name':'ID','OutlePres':'press','OutleTemp':'temperature','RunTime':'runtime',
                                                            'Building':'building','Time':'periodEnd'})
        time_dataset_all = time_dataset_all.drop(['key_as_string','doc_count','RunTime_min','Site'],axis=1)
    elif building=='KD':
        dataset_raw_json = scan_es_rawdata(api_payload,api_index,api_type)
        time_dataset = pd.DataFrame(dataset_raw_json)
        time_dataset.columns = [x.lower() for x in time_dataset.columns]
        time_dataset['periodend'] = (round(time_dataset.evt_dt.astype('float'),-5)).astype('int64')
        time_dataset['evt_time'] = pd.to_datetime(time_dataset.periodend,unit='ms',origin='1970-01-01') + pd.Timedelta(hours=8)
        time_dataset['evt_time'] = [x[0:14]+'00:00' for x in time_dataset.evt_time.astype(str)]
        time_dataset['runtime2'] = time_dataset.runtime
        time_dataset_all = time_dataset.groupby(['name','building','periodend']).agg({'outlepres':'median','outletemp':'median','runtime':'max','runtime2':'min'}).reset_index(). \
                                   rename(columns={'outlepres':'press','outletemp':'temperature','runtime':'Runtime_max','runtime2':'Runtime_min','name':'ID','periodend':'periodEnd'})
        time_dataset_all['runtime'] = time_dataset_all.Runtime_max.astype('float')-time_dataset_all.Runtime_min.astype('float')
        # time_dataset_all['press'] = -1
        # time_dataset_all['ID'] = ['AC'+x for x in time_dataset_all.ID]
        time_dataset_all = time_dataset_all.drop(['Runtime_min'],axis=1)
    elif (building=='P1') or (building=='Other'):
        dataset_raw_json = scan_api_rawdata(api_payload,api_index,api_type,building,data_type)
        dataset_json = json.loads(dataset_raw_json)
        final_result = []
        for item in dataset_json['hits']['hits']:
            final_result.append(item['_source'])
        time_dataset = pd.DataFrame(final_result)
        time_dataset.columns = [x.lower() for x in time_dataset.columns]
        time_dataset['periodend'] = (round(time_dataset.uploadtime.astype('float'),-5)).astype('int64')
        time_dataset['evt_time'] = pd.to_datetime(time_dataset.periodend,unit='ms',origin='1970-01-01') + pd.Timedelta(hours=8)
        time_dataset['evt_time'] = [x[0:14]+'00:00' for x in time_dataset.evt_time.astype(str)]
        time_dataset['runtime2'] = time_dataset.runtime
        time_dataset_all = time_dataset.groupby(['id','building','periodend']).agg({'canpress':'median','cantemperature':'median','runtime':'max','runtime2':'min'}).reset_index(). \
                                   rename(columns={'canpress':'press','cantemperature':'temperature','runtime':'Runtime_max','runtime2':'Runtime_min','id':'ID','periodend':'periodEnd'})
        time_dataset_all['runtime'] = time_dataset_all.Runtime_max.astype('float')-time_dataset_all.Runtime_min.astype('float')
        # time_dataset_all['press'] = -1
        time_dataset_all['ID'] = ['AC'+x for x in time_dataset_all.ID]
        time_dataset_all = time_dataset_all.drop(['Runtime_min'],axis=1)
    else:
        dataset_raw_json = scan_es_rawdata(api_payload,api_index,api_type)
        time_dataset = pd.DataFrame(dataset_raw_json)
        if time_dataset.shape[0]!=0:
            time_dataset.columns = [x.lower() for x in time_dataset.columns]
            time_dataset['periodend'] = (round(time_dataset.uploadtime.astype('float'),-5)).astype('int64')
            time_dataset['evt_time'] = pd.to_datetime(time_dataset.periodend,unit='ms',origin='1970-01-01') + pd.Timedelta(hours=8)
            time_dataset['evt_time'] = [x[0:14]+'00:00' for x in time_dataset.evt_time.astype(str)]
            time_dataset['runtime2'] = time_dataset.runtime
            time_dataset_all = time_dataset.groupby(['id','building','periodend']).agg({'cantemperature':'median','runtime':'max','runtime2':'min'}).reset_index(). \
                                       rename(columns={'cantemperature':'temperature','runtime':'Runtime_max','runtime2':'Runtime_min','id':'ID','periodend':'periodEnd'})
            time_dataset_all['runtime'] = time_dataset_all.Runtime_max.astype('float')-time_dataset_all.Runtime_min.astype('float')
            time_dataset_all['press'] = -1
            time_dataset_all['ID'] = ['AC'+x for x in time_dataset_all.ID]
            time_dataset_all = time_dataset_all.drop(['Runtime_min'],axis=1)
        else:
            time_dataset_all = pd.DataFrame({})
    return time_dataset_all

def machine_id_mapping(real_id):
    # print(real_id)
    if real_id.find('(')!=-1:
        machine_id = np.where(real_id[(real_id.find('(')-1):real_id.find('(')]=='B','AC'+str(int(real_id[2:(real_id.find('(')-1)])+12),'AC'+str(int(real_id[2:(real_id.find('(')-1)])))
    else:
        # if (len(real_id[2:-1])==0) | (real_id=='sensor'):
        if (real_id=='sensor'):
             machine_id = '-1'
        elif real_id[-1]!='B':
            machine_id = 'AC'+str(int(real_id[2:]))
        elif real_id[-1]=='B':
            machine_id = 'AC'+str(int(float(real_id[2:-1]))+12)
        else:
            machine_id = 'AC'+str(int(float(real_id[2:-1])))
    return machine_id

def energy_flow_data_generater(api_payload,api_index,api_type,data_type,meter_mapping_data,building):    
    if building=='Other':
        building_index=='F1'
    else:
        building_index=building
    dataset_raw_json = scan_api_rawdata(api_payload,api_index,api_type,building_index,data_type)
    dataset_json = json.loads(dataset_raw_json)
    if (data_type=='flow') & (building=='F1'):
        final_result = []
        for item in dataset_json['hits']['hits']:
            # (item['_source']).update(item['_source']['detail'])
            final_result.append(item['_source'])
        final_result = pd.DataFrame(final_result)
        final_result['periodEnd'] = (round(final_result.evt_dt.astype('float'),-5)).astype('int64')
        final_result['periodEnd'] = pd.to_datetime(final_result.periodEnd,unit='ms',origin='1970-01-01') + pd.Timedelta(hours=8)
        # final_result['periodEnd'] = [str(x)[0:13]+':00:00' for x in final_result.periodEnd]
        final_result['periodEnd'] = [datetime.datetime.strptime(str(x)[0:13]+':00:00','%Y-%m-%d %H:%M:%S') for x in final_result.periodEnd]
        final_result['reading2'] = final_result.reading
        time_dataset_all = final_result.groupby(['meterId','periodEnd']).agg({'reading':'min','reading2':'max'}).rename(columns={'reading':'MIN(reading)','reading2':'MAX(reading)'}).reset_index()
        time_dataset_all['key_as_string']=-1
        time_dataset_all['doc_count']=-1
    else:
        time_dataset_all = pd.DataFrame({})
        for index in range(len(dataset_json['aggregations']['meterId']['buckets'])):
            dataset_json_sub = dataset_json['aggregations']['meterId']['buckets'][index]
            meterid_sub = dataset_json_sub['key']
            time_record_json = dataset_json_sub['Time']['buckets']
            time_dataset = pd.DataFrame(time_record_json)
            time_dataset['meterId'] = meterid_sub
            for feature_name in ['MAX(reading)','MIN(reading)']:
                time_dataset[feature_name] = [x['value'] for x in time_dataset[feature_name]]
            time_dataset_all = time_dataset_all.append(time_dataset).reset_index(drop=True)
        time_dataset_all = time_dataset_all.rename(columns={'key':'periodEnd'})
    # print(time_dataset_all.meterId)
    if data_type=='flow':
        if building=='Other':
            time_dataset_all['building'] = 'Other'
        time_dataset_all[data_type] = time_dataset_all['MAX(reading)'].astype(float)-time_dataset_all['MIN(reading)'].astype(float)
        # if building=='F1':
        #     time_dataset_all[data_type] = time_dataset_all[data_type]*60
        time_dataset_all = pd.merge(time_dataset_all,
                                    meter_mapping_data[['flow_meter_id','machine_id']].rename(columns={'flow_meter_id':'meterId','machine_id':'ID'}),
                                    on=['meterId'],how='inner')
        print(time_dataset_all.shape[0])
    else:
        time_dataset_all[data_type] = time_dataset_all['MAX(reading)'].astype(float)-time_dataset_all['MIN(reading)'].astype(float)
        print(time_dataset_all.shape[0])
        time_dataset_all = pd.merge(time_dataset_all,
                                    meter_mapping_data[['meter_id','machine_id']].rename(columns={'meter_id':'meterId','machine_id':'ID'}),
                                    on=['meterId'],how='inner')
    return time_dataset_all.drop(['key_as_string','doc_count','MAX(reading)','MIN(reading)','meterId'],axis=1)

def power_per_definer(id_seq, building):
    if building=='TB2':
        power_result = [np.where(x[2:]<='16',75,np.where((x[2:].find('17')!=-1) or (x[2:].find('18')!=-1),130,110)) for x in id_seq]
        per_result = [np.where((x[2:].find('11')!=-1) or (x[2:].find('12')!=-1),9.4,
                             np.where((x[2:].find('17')!=-1) or (x[2:].find('18')!=-1),8.7,
                                      np.where((x[2:].find('19')!=-1) or (x[2:].find('20')!=-1),10.9,9.2))) for x in id_seq]
    elif building=='TB1':
        power_result = [np.where(x[2:]=='1',75,90) for x in id_seq]
        per_result = [np.where(x[2:]=='1',9.2,13.3) for x in id_seq]
    elif building=='TB3':
        power_result = [np.where((x[2:]=='1A') or (x[2:]=='3A') or (x[2:]=='5A') or (x[2:]=='6A') or (x[2:]=='1B') or (x[2:]=='2B') or (x[2:]=='5B') or (x[2:]=='6B'),75,90) for x in id_seq]
        per_result = [np.where((x[2:]=='1A') or (x[2:]=='5B') or (x[2:]=='6B'),9.3,
                                 np.where((x[2:]=='3A') or (x[2:]=='1B'),9.2,
                                          np.where((x[2:].find('5A')!=-1) or (x[2:].find('6A')!=-1),9.4,
                                                   np.where((x[2:].find('2B')!=-1),11.4,13.3)))) for x in id_seq]
        
    elif building=='TB5':
        power_result = [np.where(x[2:]<='12',75,90) for x in id_seq]
        per_result = [np.where(x[2:]<='5',9.4,
                               np.where((x[2:]>='6') and (x[2:]<='10'),9.3,
                                        np.where((x[2:].find('11')!=-1) or (x[2:].find('12')!=-1),9.2,13.3))) for x in id_seq]
    elif building=='KD':
        power_result = [np.where(x[2:]=='1' or x[2:]=='8',75,
                               np.where(x[2:]=='5' or x[2:]=='6' or x[2:]=='11',110,
                                        np.where(x[2:]=='2' or x[2:]=='3' or x[2:]=='4' or x[2:]=='7',160,250))) for x in id_seq]
        per_result = [np.where(x[2:]=='1' or x[2:]=='8',9.3,
                               np.where(x[2:]=='5' or x[2:]=='6',9.8,
                                        np.where(x[2:]=='11',10.5,
                                                 np.where(x[2:]=='2' or x[2:]=='3' or x[2:]=='4',9.2,
                                                          np.where(x[2:]=='7',10.5,10.9))))) for x in id_seq]
    elif building=='OB1':
        # power_result = id_seq.apply(lambda x: 110 if (x[2:]<='5') or (x[2:].find('8')!=-1) else 90)
        # per_result = id_seq.apply(lambda x: 10.4 if (x[2:]<='5') or (x[2:].find('8')!=-1) else 13.3)
        power_result = [np.where(x[2:]=='1' or x[2:]=='2' or x[2:]=='3' or x[2:]=='4' or x[2:]=='5' or x[2:]=='8',110,90) for x in id_seq]
        per_result = [np.where(x[2:]=='1' or x[2:]=='2' or x[2:]=='3' or x[2:]=='4' or x[2:]=='5' or x[2:]=='8',10.4,13.3) for x in id_seq]
    elif building=='F1':
        power_result = [110 for x in id_seq]
        per_result = [np.where(x[2:]=='1' or x[2:]=='2' or x[2:]=='3' or x[2:]=='4' or x[2:]=='5' or x[2:]=='8',10.7,
                               np.where(x[2:]=='9' or x[2:]=='13' or x[2:]=='14',9.3,
                                        np.where(x[2:]=='7' or x[2:]=='11' or x[2:]=='12',10.5,
                                                 np.where(x[2:]=='6',10.8,9.8)))) for x in id_seq]
    elif building=='P1':
        power_result = [110 for x in id_seq]
        per_result = [10.5 for x in id_seq] # flow=19.2
    elif building=='W1':
        power_result = [200 for x in id_seq]
        per_result = [np.where(x[2:]=='2' or x[2:]=='3' or x[2:]=='4' or x[2:]=='5',12.6,
                               np.where(x[2:]=='1',12,
                                        np.where(x[2:]=='6',12.9,13.5))) for x in id_seq]
    else:
        power_result = [np.where((x[2:]<='5') or (x[2:].find('8')!=-1),110,90) for x in id_seq]
        per_result = [np.where((x[2:]<='5') or (x[2:].find('8')!=-1),10.4,13.3) for x in id_seq]
    return power_result, per_result

def data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

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

def data_uploader_wt_main_fn():
    try:
        print('Upload data is start!')
        print('Load meter data is start!')
        # data = pd.DataFrame({'dev_prd':[env]})
        # if data["dev_prd"][0]=='dev':
        #     host0 = "active.eco-ssot-devdev.service.paas.wistron.com"
        #     port0 = "15063"
        # elif data["dev_prd"][0]=='qas':
        #     host0 = "active.eco-ssot-qasdev.service.paas.wistron.com"
        #     port0 = "15118"
        # else:
        #     host0 = "active.eco-ssot-prdprd.service.paas.wistron.com"
        #     port0 = "15068"
        # host0 = "active.eco-ssot-devdev.service.paas.wistron.com"
        # port0 = "15063"
        # database0 = "postgres"
        # user0 = ""
        # password0 = ""
        # conn = psycopg2.connect(host=host0, port=port0, database=database0, 
        #                         user=user0, password=password0)
        connect_string = engine.get_connect_string()
        conn = create_engine(connect_string, echo=True)
        load_meter_mapping_data_query = "SELECT * FROM raw.meter_mapping;"
        meter_mapping_data = data_loader(conn, load_meter_mapping_data_query)
        meter_mapping_data
        print('Load meter data is end!')
        
        now_time = datetime.datetime.now()
        start_time = int(datetime.datetime.timestamp(now_time-pd.Timedelta(days=1))*1000)
        end_time = int(datetime.datetime.timestamp(now_time)*1000)
        for building in ['TB2','TB1','TB5','OB1','TB3','F1','KD','P1','W1']:
            print('Building = '+building)
            # Machine Info data
            print('Get machine info data.')
            if building=='TB1':
                machine_info_payload = machine_info_generater_tb1(start_time,end_time,building)
                machine_info_data = machine_info_data_generater(machine_info_payload,'fem_air_compressor_*','bydata',building,'info')
            elif building=='KD':
                machine_info_payload = machine_info_generater_tb1(start_time,end_time,building)
                machine_info_data = machine_info_data_generater(machine_info_payload,'fem_air_compressor_wcq_*','',building,'info')
            elif building=='F1':
                machine_info_payload = machine_info_generater(start_time,end_time,building)
                machine_info_data = machine_info_data_generater(machine_info_payload,'fem_air_compressor_*','compressor',building,'info')
            elif (building=='P1') or (building=='Other'):
                machine_info_payload = machine_info_generater_tb1(start_time,end_time,building)
                machine_info_data = machine_info_data_generater(machine_info_payload,'fem_airbox_status*','',building,'info')
            elif (building=='W1'):
                machine_info_payload = machine_info_generater(start_time,end_time,building)
                machine_info_data = machine_info_data_generater(machine_info_payload,'fem_air_compressor_bydata_*','',building,'info')
            else:
                machine_info_payload = machine_info_generater(start_time,end_time,building)
                machine_info_data = machine_info_data_generater(machine_info_payload,'fem_air_compressor_*','bydata',building,'info')
            # Flow data
            print('Get flow data.')
            if building=='KD':
                flow_payload = flow_generater(start_time,end_time,building)
                flow_data = energy_flow_data_generater(flow_payload,'fem_airbox*','','flow',meter_mapping_data,building)
            elif building=='F1':
                flow_payload = flow_generater_f1(start_time,end_time,building)
                flow_data = energy_flow_data_generater(flow_payload,'bms_airflow_wcd_*','airflow','flow',meter_mapping_data,building)
            elif (building=='P1') or (building=='Other'):
                flow_payload = flow_generater(start_time,end_time,building)
                flow_data = energy_flow_data_generater(flow_payload,'fem_airbox_err_*','','flow',meter_mapping_data,building)
            elif (building=='W1'):
                flow_payload = flow_generater(start_time,end_time,building)
                flow_data = energy_flow_data_generater(flow_payload,'fem_err_*','','flow',meter_mapping_data,building)
            else:
                flow_payload = flow_generater(start_time,end_time,building)
                flow_data = energy_flow_data_generater(flow_payload,'fem_meterreading_*','meterreading.all','flow',meter_mapping_data,building)
            # Energy data
            print('Get energy data.')
            if building=='KD':
                energy_payload = energy_generater(start_time,end_time,building)
                energy_data = energy_flow_data_generater(energy_payload,'fem_energy_wcq_*','raw_consumption','energy',meter_mapping_data,building)
            elif (building=='P1') or (building=='Other') or (building=='W1'):
                energy_payload = energy_generater(start_time,end_time,building)
                energy_data = energy_flow_data_generater(energy_payload,'fem_meterreading_*','','energy',meter_mapping_data,building)
            else:
                energy_payload = energy_generater(start_time,end_time,building)
                energy_data = energy_flow_data_generater(energy_payload,'fem_meterreading_*','meterreading.all','energy',meter_mapping_data,building)
            print('Merge all data.')
            if (machine_info_data.shape[0]>0) & (energy_data.shape[0]>0) & (flow_data.shape[0]>0):
                data_upload_final = pd.merge(energy_data,machine_info_data, on=['ID','periodEnd'], how='left')
                if building=='F1':
                    data_upload_final['periodend'] = pd.to_datetime(data_upload_final.periodEnd,unit='ms',origin='1970-01-01') + pd.Timedelta(hours=8)
                    data_upload_final = pd.merge(data_upload_final,flow_data.rename(columns={'periodEnd':'periodend'}), on=['ID','periodend'], how='left')
                    data_upload_final = data_upload_final.drop(columns={'periodend'})
                else:
                    data_upload_final = pd.merge(data_upload_final,flow_data, on=['ID','periodEnd'], how='left')
                data_upload_final = data_upload_final.loc[(~data_upload_final.runtime.isna()) & (~data_upload_final.flow.isna()) & (~data_upload_final.energy.isna()),:].reset_index(drop=True)
                data_upload_final['eer'] = data_upload_final.flow/np.where(data_upload_final.energy==0,0.01,data_upload_final.energy)
                data_upload_final['eer'] = data_upload_final['eer'].astype('float')
                power_result, per_result = power_per_definer(data_upload_final['ID'],building)
                data_upload_final['power'] = power_result
                data_upload_final['power'] = data_upload_final['power'].astype('int')
                data_upload_final['per'] = per_result
                data_upload_final['per'] = data_upload_final['eer']/data_upload_final['per']
                data_upload_final['per'] = data_upload_final['per'].astype('float')
                data_upload_final.columns = [x.lower() for x in data_upload_final.columns]
                # POSTGREL DB inital value
                print('Upload all data.')
                # Upload dataset to DB
                data_uploader(data_upload_final.drop(columns={'runtime_max'}).reset_index(drop=True),'raw','accs_data')
        print('Upload data is finished!')
    except Exception as e:
        error = str(e)
        print(error)