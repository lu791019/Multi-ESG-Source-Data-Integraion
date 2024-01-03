# -*- coding: utf-8 -*- 
import pandas as pd
import numpy as np
from datetime import datetime
import requests 
import re
from sqlalchemy import create_engine
from models import engine


def target_refine(x):
    num = re.sub(r"[-+\\\/_\<\>,*^A-Za-z\s]","", str(x))
    if num=='':
        return np.nan
    else:
        return float(num)


# 另一個可以查看碳排放期貨價格的網址，但目前沒有辦法爬到價格
# https://www.macromicro.me/charts/34353/ice-eua-futures


def catch_carbon_price():

    # try:
    # 設定要抓取的網站網址
    url = 'https://www.stockq.org/commodity/FUTRCRBN.php'
    # 將該網站文字的每一行做切割
    html = requests.get(url).text

    # 'row2\'>\n<td nowrap align=center> 此行後面接的是碳價，抓取此行後面7個字母，取數值部分即為碳價
    text = "'row2\'>\n<td nowrap align=center>"
    f = re.search(text, html)

    target = html[f.span()[1]:(f.span()[1]+7)]

    carbon_price = target_refine(target)
    # print('碳排放期貨價格 :',carbon_price, '(歐元/噸)')

    #######################################################
    # ICE : 美國洲際交易所 ( Intercontiental Exchange, ICE )
    # EUA : 歐盟排放權配額 ( EU Allowances, EUAs )
    # 單位 : 歐元/噸
    #######################################################
    df = pd.DataFrame(data = [['ICE','EUA',carbon_price,'歐元/噸']],columns=['exchange','type','price','unit'])
    df['last_update_time'] = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)

    df.to_sql(name='carbon_futures_price',schema='raw',if_exists='append',index=False,con=conn)


    return True
        
    # except Exception as e:
    #     print(e)
    #     return False





