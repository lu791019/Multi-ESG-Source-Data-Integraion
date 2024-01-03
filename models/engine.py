import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse

# path = './config/database.json'

# config_file = open(path)
# config = json.load(config_file)
# stage = os.environ['STAGE'] if 'STAGE' in os.environ else 'development'



# def get_connect_string():

#     # 對密碼進行URL編碼
#     encoded_password = urllib.parse.quote(config[stage]['password'], safe='')

#     return 'postgresql://' + \
#         config[stage]['username'] + ':' + \
#         encoded_password + '@' + \
#         config[stage]['host'] + ':' + \
#         config[stage]['port'] + '/postgres'

username = os.getenv("ECO_SSOT_RDS_USERNAME")
password = os.getenv("ECO_SSOT_RDS_PASSWORD")
host = os.getenv("ECO_SSOT_RDS_HOST")
port = os.getenv("ECO_SSOT_RDS_PORT")


def get_connect_string():
    return 'postgresql://' + \
        username + ':' + \
        password + '@' + \
        host + ':' + \
        port + '/postgres'


engine = create_engine(get_connect_string())

session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
