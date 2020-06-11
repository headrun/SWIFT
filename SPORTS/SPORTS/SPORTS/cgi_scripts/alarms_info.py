#!/usr/bin/env python

################################################################################
#$Id: alarms_info.py,v 1.1 2016/03/23 12:50:45 headrun Exp $
#Copyright(c) 2005 Veveo.tv
################################################################################


import sys
import os
from datetime import date, datetime, timedelta
import jinja2
import codecs
import re
from collections import OrderedDict
import MySQLdb

from vtv_utils import copy_file, get_latest_file, VTV_DATAGEN_CURRENT_DIR
from vtv_task import VtvTask, vtv_task_main
import vtv_utils

pattern = '%'

REPORT_DIR    = '/data/REPORTS/ALARMS/'
TIME_INTERVAl = 72

PUSH_REGEX    = re.compile('Push\s+(?P<tgz>\w+\.tgz)\s+(?P<status>\w+):\s+(?P<info>.*)')
RUN_REGEX     = re.compile('(?P<status>\w+):\s+data_file:\s+(?P<prefix>/home/veveo/image/metadata_\d+_)(?P<tgz>.*?\.tgz)')
UPGRADE_REGEX = re.compile('(?P<status>Starting|Started|Successful\s+|Failed\s+):\s+(?P<info>.*)')
TGZ_REGEX     = re.compile('(?P<prefix>\w+)_[L]?(?P<time>\d+T\d+)[_\.](?P<suffix>.*)')

ROW_NAME_LIST = [ 'DATAID', 'VKC', 'SSC', 'TGZ', 'DAYS', 'STATUS' ]
ROW_DATA_ID, ROW_VKC, ROW_SSC, ROW_TGZ, ROW_DAYS, ROW_STATUS = [ ROW_NAME_LIST.index(n) for n in ROW_NAME_LIST ]
ROW_SKIP_LIST = [ ROW_DATA_ID, ROW_VKC, ROW_SSC ]

DEFAULT_DAYS = -99


class DisplayAlarmsInfo(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)

        self.mysql_ip = '10.4.18.8'
        self.db_name  = 'ALARMSDB'
       
        my_name = 'ALARMS_INFO'
        self.OUT_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, my_name)

        self.date_now = date.today()
        self.cur_date = self.date_now.strftime("%Y-%m-%d")
     
        self.vkc_event_list = [ 'Push Data' ]
        self.vkc_ip_list    = [ '10.4.18.183', '10.4.18.155', '10.4.18.168', '10.4.18.140', '10.4.18.139', '10.4.18.23', '10.4.18.69' ]

        self.ssc_event_list = [ 'Run Data', 'Upgrade Data' ]
        self.ssc_ip_list    = [ '10.4.18.142', '10.4.18.169', '10.4.18.149', '10.4.18.147', '10.4.18.93', '10.4.18.131', '10.4.18.97', '10.4.18.128', '10.4.18.67', '10.4.15.134', '10.4.18.144', '10.4.18.153' ]

        self.vkc_list = [ self.vkc_event_list, self.vkc_ip_list ]
        self.ssc_list = [ self.ssc_event_list, self.ssc_ip_list ]

    def get_alarm_query(self, event_list, ip_list):
        query = "select time, monitor_ip, event_type, event_value from ALARMSDB.alarms where event_type in (%s) and time > curdate() - interval %s hour and monitor_ip in (%s) order by time" % (', '.join(["'%s'" % x for x in event_list]), TIME_INTERVAl, ', '.join(["'%s'" % x for x in ip_list]))
        print query
        return query

    def get_event_info(self, row):
        time_str, monitor_ip, event_type, event_value = row

        tgz_name, status, info = '', '', ''
        if event_type == 'Push Data':
            obj = PUSH_REGEX.findall(event_value)
            tgz_name, status, info = obj[0]
        elif event_type == 'Run Data':
            obj = RUN_REGEX.findall(event_value)
            status, prefix, tgz_name = obj[0]
        elif event_type == 'Upgrade Data':
            obj = UPGRADE_REGEX.findall(event_value)
            try:
                status, tgz_name = obj[0]
            except IndexError:
                print row
                return []
            if status in [ 'Started', 'Successful ', 'Failed ' ]:
                if 'data_tgz_name' in tgz_name and 'data_tgz_name= ' not in tgz_name:
                    tgz_name = tgz_name.split('data_tgz_name=')[1].split()[0]
                elif 'data_version' in tgz_name:
                    tgz_name = tgz_name.split('data_version=')[1].split()[0] + '.tgz'

        row = [ x.strip() for x in [ time_str.strftime('%Y%m%dT%H%M%S'), monitor_ip, event_type, status, tgz_name, info ] ]

        return row

    def get_badge_status_color(self, status):
        COLOR_DICT = { 'SUCCESS' : 'badge-success', 'FAIL' : 'badge-important', 'DORMANT' : 'badge-warning', 'ONGOING' : 'badge-info', 'NOPUSH' : 'badge-inverse' }
        return '<span class="badge %s">%s</span>' % (COLOR_DICT.get(status, ''), status)

    def get_label_status_color(self, status):
        COLOR_DICT = { 'SUCCESS' : 'label-success', 'FAIL' : 'label-important', 'DORMANT' : 'label-warning' }
        return '<span class="label %s">%s</span>' % (COLOR_DICT.get(status, ''), status.lower())

    def get_days_color(self, days):
        COLOR_DICT = { DEFAULT_DAYS : 'badge-warning', 0 : 'badge-success' }
        if days >= -1:
            days = 0
        
        return '<span class="badge %s">%s</span>' % (COLOR_DICT.get(days, 'badge-important'), days)

    def get_data_id(self, tgz_name):
        obj = TGZ_REGEX.findall(tgz_name)
        if not obj or not obj[0]:
            return ''

        data_id = '%s_%s' % (obj[0][0], obj[0][-1])
        data_id = data_id.replace('.tgz', '').replace('_tgz', '').replace('data_', '')

        return data_id.strip()
        
    def get_alarms(self):
        tgz_info = {}

        STATUS_DICT = { 'Success' : 'SUCCESS', 'Successful' : 'SUCCESS', 'Succeeded' : 'SUCCESS', 'Failed' : 'FAIL', 'Fail' : 'FAIL', 'Started' : 'STARTED' }

        self.unique_event_value = OrderedDict()
        self.open_cursor(self.mysql_ip, self.db_name)

        for event_list, ip_list in [self.vkc_list, self.ssc_list]:
            query = self.get_alarm_query(event_list, ip_list)
            self.cursor.execute(query)
            for row in self.get_fetchmany_results():
                event_row = self.get_event_info(row)
                if not event_row:
                    continue

                time_str, monitor_ip, event_type, status, tgz_name, info = event_row
                if status not in STATUS_DICT:
                    self.logger.error('Got Unknown Status: %s' % event_row)
                    continue

                if 'untarring' in tgz_name:
                    self.logger.error('Got Unknown TGZ: %s' % event_row)
                    continue

                status = STATUS_DICT.get(status, status)

                e_info = tgz_info.setdefault(tgz_name, [[], {}])
                p_list, ip_dict = e_info
                if event_type == 'Push Data':
                    if not p_list or p_list[0] < time_str:
                        e_info[0] = [time_str, monitor_ip, status, info]
                else:
                    monitor_dict = ip_dict.setdefault(monitor_ip, {})
                    monitor_dict.setdefault(event_type, []).append([time_str, status, info])

        self.close_cursor()

        if self.options.debug:
            print tgz_info

        alarm_list = []
        for tgz_name, (p_list, ip_dict) in tgz_info.items():
            if p_list:
                p_time_str, vkc_ip, p_status, p_info = p_list
            else:
                p_time_str, vkc_ip, p_status, p_info = '', '', '', ''

            full_status = p_status
            if not full_status:
                full_status = 'NOPUSH'

            data_id = self.get_data_id(tgz_name)

            for ssc_ip in ip_dict:
                row = [data_id, vkc_ip, ssc_ip, tgz_name, DEFAULT_DAYS, full_status, p_time_str, p_time_str, self.get_label_status_color(p_status)]

                if not data_id:
                    self.logger.info('Skipping Empty Data ID: %s', row)
                    continue

                for event_type in [ 'Run Data', 'Upgrade Data' ]:    
                    start_time, end_time, e_status = '', '', ''

                    if event_type in ip_dict[ssc_ip]:
                        for s_time_str, s_status, s_info in ip_dict[ssc_ip][event_type]:
                            e_status = s_status
                            if s_status == 'STARTED':
                                start_time = s_time_str
                            else:
                                end_time = s_time_str

                    if full_status != 'FAIL' and e_status == 'FAIL': 
                        full_status = 'FAIL'
                    if full_status != 'FAIL' and e_status == '': 
                        full_status = 'ONGOING'

                    row = row + [ start_time, end_time, self.get_label_status_color(e_status) ]
                row[ROW_STATUS] = self.get_badge_status_color(full_status)
                alarm_list.append(row)

            if not ip_dict:
                full_status = self.get_badge_status_color('ONGOING')
                row = [data_id, vkc_ip, '', tgz_name, DEFAULT_DAYS, full_status, p_time_str, p_time_str, self.get_label_status_color(p_status)] + [ '' ] * 6
                self.logger.info('Skipping Empty SSC: %s', row)
                #alarm_list.append(row)

        if self.options.debug:
            print alarm_list

        alarm_list.sort()

        new_alarm_list = []
        prev_row = []
        for row in alarm_list:
            last_date_str = row[-2]
            if last_date_str:
                last_date = datetime.strptime(last_date_str.split('T')[0], '%Y%m%d').date()
                days = (last_date - self.date_now).days
            else:
                days = row[ROW_DAYS]
            row[ROW_DAYS] = self.get_days_color(days)

            if prev_row and prev_row[ROW_DATA_ID] == row[ROW_DATA_ID] and prev_row[ROW_SSC] == row[ROW_SSC]:
                for i in range(len(row)):
                    if i in ROW_SKIP_LIST:
                        continue

                    if i == ROW_DAYS:
                        if 'ONGOING' in row[ROW_STATUS]:
                            row[ROW_DAYS] = prev_row[ROW_DAYS]
                        prev_row[i] = row[i]
                    else:
                        prev_row[i] = '%s<br/>%s' % (row[i], prev_row[i])
            else:
                new_alarm_list.append(row)
                prev_row = row

        return new_alarm_list

    def dump_report(self):

        test_report_file_format = 'ALAMRS_INFO.html'
        test_report_file_name = test_report_file_format + '_%s.html' %  self.cur_date
        test_report_file_name = os.path.join(REPORT_DIR, test_report_file_name)

        return test_report_file_name, test_report_file_format

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', 'alarms_info*log'), ])
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def run_main(self):
        alarm_list = self.get_alarms()

        test_report_file_names, latest_report_file_format = self.dump_report()
        copy_file(test_report_file_names, latest_report_file_format, self.logger)

        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template('alarms_info.jinja').render(today_date = datetime.now(), ssc_vkc_info = { 'alarms' : alarm_list })

        codecs.open(os.path.join(REPORT_DIR, 'ALARMS_INFO.html'), 'w', 'utf8').write(table_html)


if __name__ == '__main__':
    vtv_task_main(DisplayAlarmsInfo)
    sys.exit( 0 )
