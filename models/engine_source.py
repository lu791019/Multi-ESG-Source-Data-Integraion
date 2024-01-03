import json
import os

# path = './config/database_source.json'

# config_file = open(path)
# config = json.load(config_file)

# def get_connect_string_csr():
#     key = 'csr'
#     return 'mssql+pymssql://' + \
#         config[key]['username'] + ':' + \
#         config[key]['password'] + '@' + \
#         config[key]['host'] + ':' + \
#         config[key]['port'] + '/'+config[key]['database']


# def get_connect_string_wzsplt():
#     key = 'wzsplt'
#     return 'mysql+pymysql://' + \
#         config[key]['username'] + ':' + \
#         config[key]['password'] + '@' + \
#         config[key]['host'] + ':' + \
#         config[key]['port'] + '/'+config[key]['database']


CSR_DATABASE = os.getenv("CSR_DATABASE")
CSR_HOST = os.getenv("CSR_HOST")
CSR_PORT = os.getenv("CSR_PORT")
CSR_USERNAME = os.getenv("CSR_USERNAME")
CSR_PASSWORD = os.getenv("CSR_PASSWORD")

WZSPLT_DATABASE = os.getenv("WZSPLT_DATABASE")
WZSPLT_HOST = os.getenv("WZSPLT_HOST")
WZSPLT_PORT = os.getenv("WZSPLT_PORT")
WZSPLT_USERNAME = os.getenv("WZSPLT_USERNAME")
WZSPLT_PASSWORD = os.getenv("WZSPLT_PASSWORD")

def get_connect_string_csr():
    return 'mssql+pymssql://' + \
        CSR_USERNAME + ':' + \
        CSR_PASSWORD + '@' + \
        CSR_HOST + ':' + \
        CSR_PORT + '/'+ CSR_DATABASE


def get_connect_string_wzsplt():
    return 'mysql+pymysql://' + \
        WZSPLT_USERNAME + ':' + \
        WZSPLT_PASSWORD + '@' + \
        WZSPLT_HOST + ':' + \
        WZSPLT_PORT + '/'+ WZSPLT_DATABASE

