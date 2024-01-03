import smtplib
from email.message import EmailMessage


class MailService:
    def __init__(self, subject):
        self.host = 'whqsmtp.wistron.com'
        self.port = 25
        self.smtp = smtplib.SMTP(self.host, self.port)

        self.msg = EmailMessage()
        self.msg['Subject'] = subject
        self.msg['From'] = 'ECO_SSOT@wistron.com'
        self.msg['To'] = 'Felix_ye@wistron.com,vincent_ku@wistron.com,dex_lu@wistron.com,shelly_shiu@wistron.com,sark_liu@wistron.com,zack_li@wistron.com,irene_ty_yang@wistron.com'
        self.msg['CC'] = 'leo_zy_lin@wistron.com'

    def send(self, content):
        self.msg.set_content(content)

        self.smtp.send_message(self.msg)
        self.smtp.quit()
