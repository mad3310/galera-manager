# -*- coding: utf-8 -*-

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.Header import Header
import traceback


def sendMail(subject, content, sender='mcluster@letv.com', receiver='wangzhen3@letv.com', cc=[], priority='0'):
    if type(receiver) == str:
        receiver = [receiver]
    receiver1 = list(receiver)
    msgRoot = MIMEMultipart('related')
    msgRoot['From'] = sender
    msgRoot['To'] = ', '.join(receiver1)
    msgRoot['X-Priority'] = priority
    if cc:
        if type(cc) == str:
            cc = [cc]
        receiver1.extend(cc)
        msgRoot['Cc'] = ', '.join(cc)
    msgRoot['Subject'] = subject

    msgText = MIMEText(content, 'html', 'gbk')
    msgRoot.attach(msgText)

    smtp = None
    try:
        smtp = smtplib.SMTP()
        smtp.connect('mail.letv.com')
        smtp.sendmail(sender, receiver1, msgRoot.as_string())
        print receiver1
    except:
        print '[sendmail] ', traceback.format_exc()
    finally:
        try:
            if smtp:
                smtp.quit()
        except:
            print '[sendmail] ', traceback.format_exc()

if __name__ == '__main__':
    sendMail('test', 'eeee', priority='1')
