#!/usr/bin/env python
import sys
import optparse
import re
import MySQLdb 
import datetime
import time
import smtplib
import os
import random
import string

from datetime import timedelta
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

HOST    = '10.4.2.207'
DBNAME  = 'TRENDSDB'
BASIC_LIST = ['jhansi@headrun.com', 'niranjansagar@headrun.com']
ADV_LIST  = ['sports@headrun.com', 'delivery@headrun.com']
#CC      = ['jhansi@headrun.com', 'delivery@headrun.com']
CC = []
SUBJECT = 'TrendsDB'
FROM    = 'vmonitor@headrun.com'

message = ''
cursor  = MySQLdb.connect(host=HOST, user="root", db=DBNAME).cursor()

def send_mail(to, subject, text, fro="", files=[], cc=CC, bcc=[], server="10.4.1.112"):
    assert type(to)     == list
    assert type(files)  == list
    assert type(cc)     == list
    assert type(bcc)    == list

    message             = MIMEMultipart()
    message['From']     = fro
    message['To']       = COMMASPACE.join(to)
    message['Date']     = formatdate(localtime=True)
    message['Subject']  = subject
    message['Cc']       = COMMASPACE.join(cc)
    message.attach(MIMEText(text, 'html'))


    for f in files:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(f, 'rb').read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        message.attach(part)

    addresses = []
    for x in to:
        addresses.append(x)
    for x in cc:
        addresses.append(x)
    for x in bcc:
        addresses.append(x)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(fro, addresses, message.as_string())
    smtp.close()


def main(options):
    one_hour_minus = datetime.datetime.utcnow() - timedelta(hours=1)
    time    = datetime.datetime.now() + datetime.timedelta(minutes=240)
    trends  = {}
    imagedb = {}

    query = 'select count(*) from hourly_trends where source="google" and time > "%s"' %time
    print query
    cursor.execute(query)
    count = cursor.fetchall()    

    if count:
        trends['Google'] = count[0][0]
    else:
        trends['Google'] = 0

    query = 'select count(*) from hourly_trends where source="yahoo" and time > "%s"' %time
    print query
    cursor.execute(query)
    count = cursor.fetchall()

    if count:
        trends['Yahoo'] = count[0][0]
    else:
        trends['Yahoo'] = 0

    query = 'select count(*) from hourly_trends where source="twitter" and time > "%s"' %time
    print query 
    cursor.execute(query)
    count = cursor.fetchall()

    if count:
        trends['Twitter'] = count[0][0]
    else:
        trends['Twitter'] = 0

    
    query = 'select count(*) from hourly_trends where source="youtube" and time > "%s"' %time
    print query
    cursor.execute(query)
    count = cursor.fetchall()

    if count:
        trends['Youtube']   = count[0][0]
    else:
        trends['Youtube']   = 0

    types = ['video', 'topic', 'related_searches', 'trends_link' ]
    for typ in types:
        query = 'select count(*) from hourly_trends_related where last_seen < "%s"' %one_hour_minus
        cursor.execute(query)
        count = cursor.fetchall()

        if not count:
            send_mail(TO, SUBJECT, "Hourly Trends Related Table not updated", FROM)


    query = 'select count(*)  from  hourly_trends_urls where modified_at < "%s"' %one_hour_minus
    cursor.execute(query)
    count = cursor.fetchall()

    if not count:
        send_mail(TO, SUBJECT, "Hourly Trends urls not updatenot updated", FROM) 


    query = 'select count(*) from news_trends  where created_at > "%s"' %time
    cursor.execute(query)
    count = cursor.fetchall()

    if count:
        trends['News Trends']   = count[0][0]
    else:
        trends['News Trends']   = 0

    for source in ['twitter', 'google', 'youtube']:
        query = "select count(*) from trends where last_time > curdate() - 1 and source='%s'" %source
        cursor.execute(query)
        count = cursor.fetchall()

        print count
        if not count:
            send_mail(TO, SUBJECT, "Trends Table not updated for %s" %source, FROM)

    query = 'select count(*) from hourly_trends_locations where modified_at > curdate() - 1'
    cursor.execute(query)
    count = cursor.fetchall()
    if not count:
        send_mail(TO, SUBJECT, "Hourly trends location table not update", FROM)


    query = 'select count(*) from hourly_trends_locations where modified_at > curdate() - 1'
    cursor.execute(query)
    count = cursor.fetchall()
    if not count:
        send_mail(TO, SUBJECT, "Hourly trends location table not update", FROM)


    query = 'select count(*) from hourly_trends_locations where modified_at > curdate() - 1'
    cursor.execute(query)
    count = cursor.fetchall()
    if not count:
        send_mail(TO, SUBJECT, "Hourly trends location table not update", FROM)



    if not trends['Twitter'] == 10:
        send_mail(TO, SUBJECT, "Twitter trends Broken", FROM)

    if not trends['Yahoo'] == 20:
        send_mail(['niranjan@headrun.com'], SUBJECT, "Yahoo trends broken", FROM)

    if trends['Youtube'] < 20:
        send_mail(TO, SUBJECT, "Youtube trends broken", FROM)

    if trends['Google'] < 18:
        send_mail(TO, SUBJECT, "Google Trends broken", FROM)

    if trends['News Trends'] < 7:
        send_mail(TO, SUBJECT, "NEWS TRENDS BROKEN %s" %trends['News Trends'], FROM)


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-a', '--address', help='Address of the dbserver to pump records to')

    (options, args) = parser.parse_args()

    main(options)
