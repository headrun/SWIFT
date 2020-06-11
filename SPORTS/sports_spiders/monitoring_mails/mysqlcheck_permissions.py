#!/usr/bin/env python

import os
from datetime import date
import datetime
import sys 
import logging
import random
import string
import optparse
import re
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase 
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

sys.path.append('/home/veveo/release/server')
import ssh_utils

#machines list
#ip_lists  = {'10.4.18.183':['SPORTSDB']}
ip_lists  = {'10.4.15.171': ['SPOTIFY', 'AMAZON', 'ITUNESDB', 'GOOGLEPLAY', 'APPDB','autocrawl'],                                                                                                                           '10.4.18.56' : ['NBASTATSDB'], '10.4.18.183': ['EDB', 'LASTFM', 'SPOTIFY','SPORTSDB', 'YTEDB', 'TVDB']}


date_now = today = date.today().strftime('%Y-%m-%d')
#receivers = ['veveo@headrun.com']

receivers = ['keerthi@headrun.com']
sender = 'veveo@headrun.com'

ALPHANUM = string.letters + string.digits

def run_remote_cmd(ip, command,
                   username,
                   password):
    tmpfname = '/tmp/%s' % ''.join([random.choice(ALPHANUM) for i in xrange(10)])
    tmperrfname = tmpfname + '.err'
    command = '%s 2> %s > %s' % (command, tmperrfname, tmpfname)

    try:    
        # execute remote command
        result = ssh_utils.ssh_cmd(ip, username, password, command)

        # fetch stdout and stderr files
        src = '%s@%s:%s' % (username, ip, tmpfname)
        status = ssh_utils.scp(password, src, tmpfname)

        src = '%s@%s:%s' % (username, ip, tmperrfname)
        estatus = ssh_utils.scp(password, src, tmperrfname)

        stdout = open(tmpfname, 'rb').read()
        stderr = open(tmperrfname, 'rb').read()

        return result, stdout, stderr

    finally:

        if os.path.exists(tmpfname): os.remove(tmpfname)
        if os.path.exists(tmperrfname): os.remove(tmperrfname)

        ssh_utils.ssh_cmd(ip, username, password, 'rm -f %s' % tmpfname)
        ssh_utils.ssh_cmd(ip, username, password, 'rm -f %s' % tmperrfname)


def main():
    permissions_list = {'dir': 'drwxr-xr-x', 'file': '-rw-r--r--'}
    p_list = []
    c  =0
    permission_fail = open("permission_fail", "a+")
    
    for ip, db_detils in ip_lists.iteritems():
        if not '10.4.15.171' in ip :
            cmd = "cd /data/DATABASES/; ls -lhrt"
            result_data = run_remote_cmd(ip, cmd, 'veveo', 'veveo123')
            res,stdout, err = result_data
            stdout = stdout.split('\n')
            #print "stdout", stdout
            for data in stdout[1:]:
                if 'XFINDER' in data or "dish_epg_08262013.dmp" in data or 'WIKIDB' in data or 'DATATESTDB' in data:
                    continue
                if data and permissions_list['dir'] not in data:
                    data_dbs = "".join(data.split(' ')[-1:])
                    data_all = ip+" "+data_dbs+" "+data
                    p_list.append(data_all)
                    permission_fail.write("%s\t%s\t%s\n" %(data,cmd,ip))
            for db in db_detils:
                if "EDB" not in db: #or "XFINDER" not in db:
                    cmd = "cd /data/DATABASES/%s; ls -lhrt" %db
                    result_data = run_remote_cmd(ip, cmd, 'veveo', 'veveo123')
                    res,stdout, err = result_data
                    stdout = stdout.split('\n')
                    for files in stdout[1:]:
                        if 'QUICKSEARCH_TESTDB' not in cmd:
                            if files and permissions_list['file'] not in files:
                                files_dbs = ip+" "+db+" "+files
                                p_list.append(files_dbs)
                                permission_fail.write("%s\t%s\t%s\n" % (files,cmd,ip))
                else:
                    cmd = "cd /data/DATABASES/EDB; ls -lhrt"
                    result_data = run_remote_cmd(ip, cmd, 'veveo', 'veveo123')
                    res,stdout, err = result_data
                    stdout = stdout.split('\n')
                    for data in stdout[1:]:
                        if 'edb' in data and  'edb_20121004.tgz' not in data and 'edb_files' not in data and 'edb_20120831' not in data and 'edb_20120929' not in data  and 'edb_tmp' not in data and permissions_list['dir'] not in data:
                            data_list = "".join(data.split(' ')[-1:])
                            data_lists = ip+" "+data_list+" "+data
                            p_list.append(data_lists)
                            permission_fail.write("%s\t%s\n" %(data,ip))
                        elif 'edb' in data and  'edb_20121004.tgz' not in data and 'edb_files' not in data and 'edb_20120831' not in data and 'edb_20120929' not in data and 'edb_tmp' not in data:
                            if "edb" in data:
                                db = 'edb'
                                cmd = "cd /data/DATABASES/EDB/%s; ls -lhrt" % db
                                result_data = run_remote_cmd(ip, cmd, 'veveo', 'veveo123')
                                res,stdout, err = result_data
                                stdout = stdout.split('\n')
                                for edb_files in stdout[1:]:
                                    if edb_files and 'arc' not in edb_files and 'map_programs' not in edb_files and permissions_list['file'] not in edb_files:
                                        edb_list = ip+" "+db+" "+edb_files
                                        p_list.append(edb_list)
                                        permission_fail.write("%s\t%s\t%s\n" % (edb_files,cmd,ip))
        else:
            cmd = "cd /var/lib/mysql; ls -lhrt"
            result_data = run_remote_cmd(ip, cmd, 'veveo', 'veveo123')
            res,stdout, err = result_data
            stdout = stdout.split('\n')
            for data in stdout[1:]:
                for db in db_detils:
                    if db in data and 'APPDB_DEV'not in data and 'APPDB_PROD' not in data and 'APPDB_FOLD' not in data:
                        if data and permissions_list['dir'] not in data:
                            data_db = "".join(data.split(' ')[-1:])
                            data_171 = ip+" "+data_db+" "+data
                            p_list.append(data_171)
                            permission_fail.write("%s\t%s\t%s\n" % (data,cmd,ip))
            for db in db_detils:
                cmd = "cd /var/lib/mysql/%s; ls -lhrt" %db
                result_data = run_remote_cmd(ip, cmd, 'veveo', 'veveo123')
                res,stdout, err = result_data
                stdout = stdout.split('\n')
                for files in stdout[1:]:
                    if not 'arc' in files and 'cddb_android' not in files:
                        if files and permissions_list['file'] not in files:
                            file_list_171 = ip+" "+db+" "+files
                            p_list.append(file_list_171)
                            permission_fail.write("%s\t%s\t%s\n" % (files,cmd,ip))
    if len(p_list)>0: 
        send_mail(p_list)       
    
def p_data_status_msg(p_list):
    
    #msg = '<table border="1"><thead><th><b>Dir and File Permissions list</b></th></thead>'
    msg = '<table border="1"><thead><th><b>ip</b></th><th><b>DB_Name</b></th><th><b>Dir and File Permissions list</b></th></thead>'
    for pl in p_list:
        pl = str(pl)
        pl_sp = pl.split(' ')
        ip_db = pl_sp[0]
        db_name = pl_sp[1]
        permission = " ".join(pl_sp[2:])
        #msg += '<tr><td>%s</td></tr>' % pl  
        msg += '<tr><td>%s</td><td>%s</td><td>%s</td></tr>' % (ip_db, db_name, permission)
    msg += '</table>'
    return msg

def send_mail(p_list):
    print "send_mail"
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = 'MYSQL PERMISSIONS STATUS REPORT'
    msgRoot['From'] = 'headrun'
    msgRoot['To'] = ', '.join(receivers)
    msgRoot.preamble = 'This is a multi-part message in MIME format.'

    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)


    msg = 'Hi, <br />'
    msg += '<br /><b>Started time:</b> %s<br />' % datetime.datetime.now()
    msg += p_data_status_msg(p_list)
    msgText = MIMEText(msg, 'text')
    msgAlternative.attach(msgText)
    
    msgText = MIMEText(msg, 'html')
    msgAlternative.attach(msgText)
    
    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect('10.4.1.112')
    smtp.sendmail(sender, receivers, msgRoot.as_string())
    smtp.quit() 

if __name__ == '__main__':
    main()
