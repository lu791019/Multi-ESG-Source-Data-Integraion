#!/usr/bin/python
import http.client
import json
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from models import engine

def scan_api_rawdata(api_payload,api_index,api_type):
    # set connection and query statement
    conn = http.client.HTTPConnection("10.41.241.6", 9200, timeout = 60)
    # use ES search API to get data
    payload = json.dumps(api_payload)
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

def machine_info_generater(start_time, end_time):
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
                                                    "evt_dt": {
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
                                                        "query": "TB2",
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
            "Name": {
                "terms": {
                    "field": "Name.raw",
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
                    "Site": {
                        "terms": {
                            "field": "Site.raw",
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
                                    "field": "Building.raw",
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
                                            "OutlePres": {
                                                "avg": {
                                                    "field": "OutlePres"
                                                }
                                            },
                                            "OutleTemp": {
                                                "avg": {
                                                    "field": "OutleTemp"
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

def flow_generater(start_time, end_time):
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
                                                    "area": {
                                                        "query": "TB2",
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

def energy_generater(start_time, end_time):
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
                                                                                "query": "TB2",
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
                                                        "query": "TB2",
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

def machine_info_data_generater(api_payload,api_index,api_type):    
    dataset_raw_json = scan_api_rawdata(api_payload,api_index,api_type)
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
    time_dataset_all['Name'] = [x[0:x.find('(')] for x in time_dataset_all.Name]
    time_dataset_all['RunTime'] = time_dataset_all.RunTime_max-time_dataset_all.RunTime_min
    time_dataset_all = time_dataset_all.rename(columns={'Name':'ID','OutlePres':'press','OutleTemp':'temperature','RunTime':'runtime',
                                                        'Building':'building','Time':'periodEnd'})
    return time_dataset_all.drop(['key_as_string','doc_count','RunTime_min','RunTime_max','Site'],axis=1)

def energy_flow_data_generater(api_payload,api_index,api_type,data_type):    
    dataset_raw_json = scan_api_rawdata(api_payload,api_index,api_type)
    dataset_json = json.loads(dataset_raw_json)
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
    if data_type=='flow':
        time_dataset_all[data_type] = time_dataset_all['MAX(reading)']-time_dataset_all['MIN(reading)']
        time_dataset_all['ID'] = ['AC'+x[6:] for x in time_dataset_all['meterId']]
    else:
        time_dataset_all[data_type] = time_dataset_all['MAX(reading)']-time_dataset_all['MIN(reading)']
        id_name = [x[3:] for x in time_dataset_all['meterId']]
        time_dataset_all['ID'] = [id_tranfomer(x) for x in id_name]
        time_dataset_all['ID'] = time_dataset_all['ID'].astype('string')
    return time_dataset_all.drop(['key_as_string','doc_count','MAX(reading)','MIN(reading)','meterId'],axis=1)

def id_tranfomer(ori_id):
    
    if ori_id[0]=='7':
        new_id = np.where(ori_id[2:4]=='00','AC1',
                        np.where(ori_id[2:4]=='01','AC2',
                                np.where(ori_id[2:4]=='02','AC3',
                                        np.where(ori_id[2:4]=='03','AC4',
                                                np.where(ori_id[2:4]=='04','AC5',
                                                         np.where(ori_id[2:4]=='05','AC16',
                                                                    np.where(ori_id[2:4]=='06','AC11',
                                                                            np.where(ori_id[2:4]=='07','AC12',
                                                                                    np.where(ori_id[2:4]=='08','AC13',
                                                                                             np.where(ori_id[2:4]=='09','AC10',
                                                                                                      np.where(ori_id[2:4]=='10','AC8','-1')))))))))))
    elif ori_id[0]=='8':
        new_id = np.where(ori_id[2:4]=='00','AC9',
                     np.where(ori_id[2:4]=='01','AC7',
                              np.where(ori_id[2:4]=='02','AC6',
                                       np.where(ori_id[2:4]=='03','AC14',
                                                np.where(ori_id[2:4]=='04','AC15','-1')))))
    else:
        new_id = np.where(ori_id=='341_2','AC19',
                     np.where(ori_id=='341_3','AC18',
                              np.where(ori_id=='341_4','AC17',
                                       np.where(ori_id=='341_6','AC20','-1'))))
    return new_id

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
        now_time = datetime.datetime.now()
        start_time = int(datetime.datetime.timestamp(now_time-pd.Timedelta(days=1))*1000)
        end_time = int(datetime.datetime.timestamp(now_time)*1000)
        # Machine Info data
        print('Get machine info data.')
        machine_info_payload = machine_info_generater(start_time,end_time)
        machine_info_data = machine_info_data_generater(machine_info_payload,'fem_air_compressor_*','bydata')
        # Flow data
        print('Get flow data.')
        flow_payload = flow_generater(start_time,end_time)
        flow_data = energy_flow_data_generater(flow_payload,'fem_meterreading_*','meterreading.all','flow')
        # Energy data
        print('Get energy data.')
        energy_payload = energy_generater(start_time,end_time)
        energy_data = energy_flow_data_generater(energy_payload,'fem_meterreading_*','meterreading.all','energy')
        print('Merge all data.')
        data_upload_final = pd.merge(energy_data,machine_info_data, on=['ID','periodEnd'], how='left')
        data_upload_final = pd.merge(data_upload_final,flow_data, on=['ID','periodEnd'], how='left')
        data_upload_final = data_upload_final.loc[(~data_upload_final.runtime.isna()) & (~data_upload_final.flow.isna()) & (~data_upload_final.energy.isna()),:].reset_index(drop=True)
        data_upload_final['eer'] = data_upload_final.flow/np.where(data_upload_final.energy==0,0.01,data_upload_final.energy)
        data_upload_final['eer'] = data_upload_final['eer'].astype('float')
        data_upload_final['power'] = [np.where(x[2:]<='16',75,np.where((x[2:].find('17')!=-1) or (x[2:].find('18')!=-1),130,110)) for x in data_upload_final['ID']]
        data_upload_final['power'] = data_upload_final['power'].astype('int')
        data_upload_final['per'] = [np.where((x[2:].find('11')!=-1) or (x[2:].find('12')!=-1),9.4,
                                             np.where((x[2:].find('17')!=-1) or (x[2:].find('18')!=-1),8.7,
                                                      np.where((x[2:].find('19')!=-1) or (x[2:].find('20')!=-1),10.9,9.2))) for x in data_upload_final['ID']]
        data_upload_final['per'] = data_upload_final['eer']/data_upload_final['per']
        data_upload_final['per'] = data_upload_final['per'].astype('float')
        data_upload_final.columns = [x.lower() for x in data_upload_final.columns]
        # POSTGREL DB inital value
        print('Upload all data.')
        # Upload dataset to DB
        data_uploader(data_upload_final.drop(columns={'runtime_max'}).reset_index(drop=True),'raw','accs_data')
        print('Upload data is finished!')
        return 0
    except Exception as e:
        error = str(e)
        return error
        print(error)
