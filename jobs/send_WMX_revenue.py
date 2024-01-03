import pandas as pd
import numpy as np
from datetime import datetime as dt, date, timedelta
from sqlalchemy import *
import calendar
from models import engine
import smtplib
from email.message import EmailMessage

class MailService:
    def __init__(self, subject):
        self.host = 'whqsmtp.wistron.com'
        self.port = 25
        self.smtp = smtplib.SMTP(self.host, self.port)

        self.msg = EmailMessage()
        self.msg['Subject'] = subject
        self.msg['From'] = 'eco_ssot@wistron.com'
        self.msg['To'] = 'Carlos_Minor@wistron.com,Vicente_Martinez@wistron.com'
        self.msg['CC'] = 'C.C._LEE@WISTRON.COM'
        self.msg['bcc'] = 'vincent_ku@wistron.com,Emily_Hsieh@wistron.com'

    def send(self, content):
        self.msg.set_content(content, subtype='html')

        self.smtp.send_message(self.msg)
        self.smtp.quit()


def send_WMX_revenue(stage):

    if stage == 'production':
        connect_string = engine.get_connect_string()
        db = create_engine(connect_string, echo=True)
        conn = db.connect()
        #set time
        if dt.now().month == 1 :
            period_year = str(dt.now().year-1)
            period_year_start = date(dt.now().year-1, 1, 1).strftime("%Y-%m-%d")
            period_start = date(dt.now().year-1, 12, 1).strftime("%Y-%m-%d")
        else:
            period_year = str(dt.now().year)
            period_year_start = date(dt.now().year, 1, 1).strftime("%Y-%m-%d")
            period_start = date(dt.now().year, dt.now().month - 1, 1).strftime("%Y-%m-%d")

        #create new status
        if dt.now().day == 1:
            revenue_wmx_mail = pd.read_sql(f"SELECT * FROM raw.revenue_wmx_mail LIMIT 1", con=db)
            revenue_wmx_mail['period_start'] = period_start
            revenue_wmx_mail['status'] =  0
            revenue_wmx_mail['last_update_time'] =  dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
            revenue_wmx_mail.to_sql('revenue_wmx_mail', conn, index= False, if_exists='append',schema='raw', chunksize = 10000)

        #check mail status
        revenue_wmx_mail = pd.read_sql(f"SELECT * FROM raw.revenue_wmx_mail WHERE period_start = '{period_start}'", con=db)

        if (dt.now().day >= 10) & (revenue_wmx_mail['status'][0] == 0) :
            #check revenue in WMX
            revenue_WMX = pd.read_sql(f"SELECT plant,amount,period_start FROM raw.revenue WHERE plant = 'WMX' AND period_start = '{period_start}' ", con=db)
            #WMX_WYHQ
            revenue_WMX_WYHQ = pd.read_sql(f"SELECT plant,amount,period_start FROM raw.revenue_wmx WHERE plant = 'WMX' AND period_start = '{period_start}' ", con=db)

            if (revenue_WMX.shape[0] > 0) & (revenue_WMX_WYHQ.shape[0] > 0) :
                #generate all month
                year_month = pd.date_range(period_year+'0101',period_year+'1231',freq='MS').tolist()
                year_month = pd.DataFrame({"period_start":year_month})
                year_month['period_start'] = year_month['period_start'].apply(lambda x : x.strftime("%Y-%m-%d"))
            
                #get revenue WMX WHQ
                revenue_WMX = pd.read_sql(f"SELECT plant,amount,period_start FROM raw.revenue WHERE plant = 'WMX' AND period_start >= '{period_year_start}' ", con=db)
                #get revenue WMX WYHQ
                revenue_WMX_WYHQ = pd.read_sql(f"SELECT plant,amount,period_start FROM raw.revenue_wmx WHERE plant = 'WMX' AND period_start >= '{period_year_start}' ", con=db)
                #calculate all WMX revenue
                revenue_all = revenue_WMX.append(revenue_WMX_WYHQ).reset_index(drop = True)
                
                #fill all month
                revenue_WMX['period_start'] = revenue_WMX['period_start'].apply(lambda x : x.strftime("%Y-%m-%d"))
                revenue_WMX_sum = revenue_WMX['amount'].sum().round(3)
                revenue_WMX['amount'] = revenue_WMX['amount'].round(3)
                revenue_WMX = revenue_WMX.merge(year_month,on = 'period_start',how = 'right')
                revenue_WMX = revenue_WMX.fillna("")
                
                #fill all month
                revenue_WMX_WYHQ['period_start'] = revenue_WMX_WYHQ['period_start'].apply(lambda x : x.strftime("%Y-%m-%d"))
                revenue_WMX_WYHQ_sum = revenue_WMX_WYHQ['amount'].sum().round(3)
                revenue_WMX_WYHQ['amount'] = revenue_WMX_WYHQ['amount'].round(3)
                revenue_WMX_WYHQ = revenue_WMX_WYHQ.merge(year_month,on = 'period_start',how = 'right')
                revenue_WMX_WYHQ = revenue_WMX_WYHQ.fillna("")
                
                #fill all month
                revenue_all = revenue_all.groupby(['plant','period_start']).sum().reset_index()
                revenue_all['period_start'] = revenue_all['period_start'].apply(lambda x : x.strftime("%Y-%m-%d"))
                revenue_all_sum = revenue_all['amount'].sum().round(3)
                revenue_all['amount'] = revenue_all['amount'].round(3)
                revenue_all = revenue_all.merge(year_month,on = 'period_start',how = 'right')
                revenue_all = revenue_all.fillna("")

                message = f"""<!doctype html>
                        <html lang="en">
                        <body>
                        <div style="font-size: 16px">
                            <div style="font-size: 1.125em; font-weight: bold; margin-bottom: 1em;">Dear Carlos & Vicente,</div>
                            
                            <div style="font-size: 1.125em; font-weight: bold; margin-bottom: 1em;">Revenue in WMX as follows:</div>
                            
                            <div style="font-size: 1.125em; font-weight: bold; margin-bottom: 1em;">Billion NT$</div>

                            <table style="font-size: 1.125em;border: 1px solid black;border-collapse: collapse;">
                            <tr>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Y{period_year}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Jan</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Feb</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Mar</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Apr</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">May</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Jun</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Jul</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Aug</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Sep</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Oct</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Nov</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Dec</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Total</th>
                            </tr>
                            <tr>
                                <td style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">WHQ</td>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][0]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][1]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][2]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][3]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][4]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][5]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][6]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][7]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][8]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][9]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][10]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX['amount'][11]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_sum}</th>
                            </tr>
                            <tr>
                                <td style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">WYHQ</td>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][0]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][1]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][2]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][3]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][4]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][5]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][6]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][7]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][8]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][9]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][10]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ['amount'][11]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_WMX_WYHQ_sum}</th>
                            </tr>
                            <tr>
                                <td style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">Total</td>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][0]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][1]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][2]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][3]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][4]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][5]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][6]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][7]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][8]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][9]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][10]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all['amount'][11]}</th>
                                <th style="text-align: center; padding-left: 0.5em; padding-right: 0.5em;border: 1px solid black;border-collapse: collapse;">{revenue_all_sum}</th>
                            </tr>
                            </table>

                            <div style="margin-top: 0.5em">This mail is sent by Robot, please do not reply.</div>
                        </div>
                        </body>

                        </html>
                        """
                mail = MailService('REVENUE WMX')
                mail.send(message)

                #update mail status
                revenue_wmx_mail['status'] = 1
                revenue_wmx_mail['last_update_time'] =  dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
                conn.execute(f"DELETE FROM raw.revenue_wmx_mail WHERE  period_start ='{period_start}'")
                revenue_wmx_mail.to_sql('revenue_wmx_mail', conn, index= False, if_exists='append',schema='raw', chunksize = 10000)

                conn.close()