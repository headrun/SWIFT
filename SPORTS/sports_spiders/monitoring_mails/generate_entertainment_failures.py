#!/usr/bin/env python

################################################################################
#$Id: generate_entertainment_failures.py,v 1.1 2015/12/14 11:10:57 headrun Exp $
################################################################################

import os, re
import glob
import ssh_utils
import MySQLdb
from datetime import datetime, timedelta
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2


CUR_DATE      = str(datetime.utcnow().date())
YESTERDAY     = str((datetime.utcnow() - timedelta(days=1)).date())
CUR_DIR       = os.getcwd()
DATAGEN_DIR   = os.path.join(CUR_DIR, 'datagen_test')
ERROR_PATTERN = 'FIELD: %s'
RY_PATTERN    = ERROR_PATTERN % 'Ry'
TOU_GID       = 'TOURNAMENTS DB GID: (.*?):'
GID_PATTERN   = 'GID: (.*?):'
GOT           = 'GOT: (.*?) '
NEED          = 'NEED: (.*?) '
SUITE_ID      = 'SUITE: (.*?) GID:'
TEST_ID       = 'ID: (.*?) SUITE:'

def check_dir(path=DATAGEN_DIR):
    if not os.path.isdir(path):
        os.mkdir(path)

def get_table_data(title, headers, body):
    text = '<br /><br /><b>%s</b><br /><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        text += '<th>%s</th>' % header
    text += '</tr>'

    for data in body:
        text += '<tr>'
        for d in data:
            text += '<td>%s</td>' % d
        text += '</tr>'
    text += '</table>'
    return text


class DatagenFailures:

    def __init__(self):
        self.failed_scripts = []
        self.scripts_dict   = {}
        self.log_ip         = "10.4.2.187"
        self.logger         = initialize_timed_rotating_logger('datagen_errors.log')
        self.conn           = MySQLdb.connect(user="root", db="WIKIDB", host=self.log_ip)
        self.cursor         = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'Entertainment Failures'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'
        receivers = ['sudheer@headrun.com']
        receivers.extend(['raman@veveo.net'])
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def copy_log_file(self):
        check_dir()
        status = ssh_utils.scp('veveo123', 'veveo@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.log_ip, CUR_DATE), '/home/veveo/sports_spiders/monitoring_mails/datagen_test')
        if status:
            status = ssh_utils.scp('veveo123', 'veveo@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.log_ip, YESTERDAY), '/home/veveo/sports_spiders/monitoring_mails/datagen_test')

    def get_failure_cases(self):
        text = ''
        ry_failures = set()
        os.chdir(DATAGEN_DIR)
        datagen_file = max(glob.iglob('*.log'), key=os.path.getctime)
        print datagen_file
        file_pointer = open(datagen_file)
        for data in file_pointer:
            if RY_PATTERN not in data:
                continue
            gid  = re.findall(GID_PATTERN, data)
            need = re.findall(NEED, data)
            got  = re.findall(GOT, data)
            test_id = re.findall(TEST_ID, data)
            if gid[0].startswith('WIKI'):
                self.cursor.execute('select title from title_guid_map where gid = "%s"' % gid[0])
                title = self.cursor.fetchone()
                if title:
                    title = title[0]
            else:
                title = ''
            ry_failures.add((gid[0], title, need[0], got[0], test_id[0]))

        file_pointer.close()
        text += "Report Log File: <b>%s</b>" % datagen_file
        if ry_failures:
            text += get_table_data('Ry Failures', ['Gid', 'title', 'NEED', 'GOT', 'Test ID'], ry_failures)
            self.send_mail(text)

    def cleanup(self):
        self.cursor.close()

    def run_main(self):
        self.copy_log_file()
        self.get_failure_cases()
        self.cleanup()


if __name__ == '__main__':

    OBJ = DatagenFailures()
    OBJ.run_main()
