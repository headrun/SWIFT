#!/usr/bin/env python

################################################################################
#$Id: sports_test_cases.py,v 1.1 2015/12/14 11:10:57 headrun Exp $
################################################################################

import os, re
import glob
import ssh_utils
import MySQLdb
from datetime import datetime
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2


def check_dir(path=''):
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

def get_dict_data(title, headers, body):
    text = '<br /><br /><b>%s</b><br /><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        text += '<th>%s</th>' % header
    text += '</tr>'

    for key, value in body.iteritems():
        text += '<tr>'
        text += '<td rowspan="%s">%s</td>' % (len(value) + 1, key)
        for data_list in value:
            text += '<tr>'
            for v in data_list:
                text += '<td>%s</td>' % v
            text += '</tr>'
        text += '</tr>'
    text += '</table>'
    return text


class DatagenFailures:

    def __init__(self):
        self.failed_scripts = []
        self.scripts_dict   = {}
        self.text           = ''
        self.logger         = initialize_timed_rotating_logger('sports_tests.log')
        self.conn           = MySQLdb.connect(user="veveo", db="SPORTSDB", passwd='veveo123', host="10.28.218.81")
        self.cursor         = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'Sports Tests'
        server     = '10.4.1.112'
        sender     = 'sudheer@headrun.com'
        receivers = ['sudheer@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def clean(self):
        self.conn.close()

    def run_tests(self):
        query = "select id, title, DATE(season_start), DATE(season_end), game from sports_tournaments where season_start < now() and season_end > now()"
        self.cursor.execute(query)
        data     = self.cursor.fetchall()
        tou_dict = {}
        for d in data:
            tou_dict.setdefault(d[-1], []).append(d[:-1])
        self.text += get_dict_data('Ongoing Tournaments', ['Game', 'Tournament Id', 'Title', 'Season Start', 'Season End'], tou_dict)
        self.send_mail(self.text)

    def run_main(self):
        self.run_tests()
        self.clean()


if __name__ == '__main__':

    obj = DatagenFailures()
    obj.run_main()
