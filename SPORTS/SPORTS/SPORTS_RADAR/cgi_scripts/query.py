#!/usr/bin/python

import os, sys, re, glob
import urllib
import time, datetime

sys.path.append('..')

from vtv_utils import get_compact_traceback, VTV_CONFIG_DIR, VTV_SQL_QUERIES_FILE
from vtv_db import get_mysql_connection
from vtv_xml import DB_STATS_TAG, QUERY_STATS_TAG, STATS_TAG, VALUE_TAG, \
                    minidom, get_data_from_tag
from httpd_ui import CONFIG_FILE_TO_PARSE, \
                     get_xml_request_from_args_and_tags, my_main

import ssh_utils

from globals import VTV_ARGS_KEY, VTV_SUB_CMD_KEY


COUNTRY_CODE_LIST = ['US', 'GB', 'IN', 'CA', 'FR', 'IE', 'ID', 'MX', 'DE', 'IT', 'SA', 'ES', 'ZA', 'NO', 'NL']
COUNTRY_NAME_LIST = ['United States', 'United Kingdom', 'India', 'Canada', 'France', 'Ireland', 'Indonesia', 
                     'Mexico', 'Germany', 'Italy', 'Saudi Arabia', 'Spain', 'South Africa', 'Norway', 'Netherlands']
COUNTRY_TABLE     = dict(zip(COUNTRY_NAME_LIST, COUNTRY_CODE_LIST))

ALARM_LEVEL_LIST = ['critical', 'major', 'minor', 'info', 'All']


def translate_field(args_table, field, name):
    if field not in args_table:
        return

    value = args_table[field]
    if value == 'All':
        cond = " 1 = 1 "
    else:
       cond = " %s = '%s' " % (name, value)
    args_table[field] = cond


def translate_args(args_table):
    if 'user_agent' in args_table:
        agent = args_table['user_agent']
        if agent == 'All':
            cond = " 1 = 1 "
        elif agent == 'wince':
            cond = " user_agent in ('wince', 'wince_sp', 'wince_ppc', 'wince_other', 'hs-xhtml1') "
        elif agent == 'j2me':
            cond = " user_agent in ('j2me', 'midp') "
        elif agent == 'pc':
            cond = " user_agent in ('pc', 'PCWidget') "
        else:
            cond = " user_agent = '%s' " % agent
    
        args_table['user_agent'] = cond

    if 'country' in args_table:
        country = args_table['country']
        args_table['country'] = COUNTRY_TABLE.get(country, country)

    for field, name in [('alarm_type', 'event_type'), ('alarm_server', 'monitor_ip'), ('alarm_level', 'event_level')]:
        translate_field(args_table, field, name) 


def read_query_file(file_name = VTV_SQL_QUERIES_FILE):
    fp = open(file_name)
    text = fp.read()
    fp.close()
    text = re.sub('[\t\n]*', '', text)
    return text


def get_query_dom(file_name):
    text = read_query_file(file_name)
    doc = minidom.parseString(text)
    return doc


def write_query_file(doc):
    fp = open(VTV_SQL_QUERIES_FILE,"w")
    fp.write(doc.toprettyxml())
    fp.close()


def get_query(group, title):
    doc = get_query_dom(VTV_SQL_QUERIES_FILE)

    queries_elem = doc.getElementsByTagName("queries")[0]

    query = ''
    for query_group_node in queries_elem.getElementsByTagName("query_group"):
        query_group = query_group_node.getAttribute('group')
        if query_group != group:
            continue
        for query_title_node in query_group_node.getElementsByTagName("query"):
            query_title = query_title_node.getAttribute('title')
            if query_title == title:
                query = get_data_from_tag(query_title_node)
                break
        
    doc.unlink()

    return query


def get_date_list():
    DAILY_LIST = []
    WEEKLY_LIST = []
    MONTHLY_LIST = []
    MAX_DAYS = 30
    MAX_WEEKS = 53
    MAX_MONTHS = 6
    now = datetime.datetime.now()
    weekday = now.isoweekday()
    today = str(now).split()[0]
    day = datetime.date(*(tuple([int(x) for x in today.split('-')])))
    for i in xrange(MAX_DAYS - 1):
        day_str = day.strftime('%Y-%m-%d')
        day -= datetime.timedelta(1)
        DAILY_LIST.append(day_str) 

    week = now - datetime.timedelta(weekday)
    weekday = str(week).split()[0]
    day = datetime.date(*(tuple([int(x) for x in weekday.split('-')])))
    for i in xrange(MAX_WEEKS - 1):
        day_str = day.strftime('%Y-%m-%d')
        day -= datetime.timedelta(7)
        WEEKLY_LIST.append(day_str) 

    month = now.strftime('%Y-%m-01') 
    day = datetime.date(*(tuple([int(x) for x in month.split('-')])))
    for i in xrange(MAX_MONTHS - 1):
        day_str = day.strftime('%Y-%m-01')
        day -= datetime.timedelta(30)
        MONTHLY_LIST.append(day_str) 

    return DAILY_LIST, WEEKLY_LIST, MONTHLY_LIST


def get_alarm_server_list(args_table, my_logger):
    alarm_type_list = ['All']
    alarm_server_list = ['All']
    if args_table['system'] != 'vkc':
        return alarm_server_list, alarm_type_list

    alarms_cursor, alarms_db = get_mysql_connection(my_logger, db_name='ALARMSDB')
    if alarms_cursor:
        alarms_cursor.execute("select distinct event_type from alarms")
        rows = alarms_cursor.fetchall()
        alarm_type_list = ['All'] + [row[0] for row in rows]

    doc = get_query_dom(CONFIG_FILE_TO_PARSE)
    for node in doc.getElementsByTagName("alarm_server"):
        server = node.getAttribute(VALUE_TAG)
        alarm_server_list.append(str(server))

    return alarm_server_list, alarm_type_list


def get_query_in_json(args_table, my_logger):
    doc = get_query_dom(VTV_SQL_QUERIES_FILE)
    queries_elem = doc.getElementsByTagName("queries")[0]

    DAILY_LIST, WEEKLY_LIST, MONTHLY_LIST = get_date_list()
    alarm_server_list, alarm_type_list = get_alarm_server_list(args_table, my_logger)

    button_table = { "app_type"     : [ "WinCE", "XHTML", "iPhone", "GetJar", 'AdMob', "PCWidget", "BlackBerry", "j2me" ],
                     "user_agent"   : [ "All", "wince", "xhtml", "iPhone", "pc", "j2me" ],
                     "country"      : COUNTRY_NAME_LIST,
                     "daily"        : DAILY_LIST,
                     "weekly"       : WEEKLY_LIST,
                     "monthly"      : MONTHLY_LIST,
                     "alarm_server" : alarm_server_list,
                     "alarm_type"   : alarm_type_list,
                     "alarm_level"  : ALARM_LEVEL_LIST,
                   }

    button_list = []
    group_table = {}
    sorted_group_list = []
    sorted_group_title_table = {}
    for query_group_node in queries_elem.getElementsByTagName("query_group"):
        query_group = query_group_node.getAttribute('group')
        sorted_group_list.append(query_group)
        new_button_list = []
        if query_group_node.getAttribute('button'):
            button_str = query_group_node.getAttribute('button')
            new_button_list = button_str.split(',')
            for button in new_button_list:
                if button not in button_list:
                    button_list.append(button)
        
        group_table[query_group] = {}
        if new_button_list:
            group_table[query_group]['button'] = [str(x) for x in new_button_list]
        sorted_title_list = []
        for query_title_node in query_group_node.getElementsByTagName("query"):
            query_title = query_title_node.getAttribute('title')
            sorted_title_list.append(query_title)
            query_format = query_title_node.getAttribute('format')
            group_table[query_group][query_title] = query_format
        if new_button_list:
            sorted_title_list.append('button')
        sorted_group_title_table[query_group] = sorted_title_list

    json_str = '{'
    for button in button_list:
        button_str = ' "%s" : %s,' % (button, str(button_table[button]))
        button_str = button_str.replace("'", '"')
        json_str += button_str
        
    group_table_list = []
    for group in sorted_group_list:
        group_list = []
        sorted_title_list = sorted_group_title_table[group]
        for title in sorted_title_list:
            if title == 'button':
                button_str = str(group_table[group][title])
                button_str = button_str.replace("'", '"')
                group_list.append('"%s" : %s' % (title, button_str))
            else:
                group_list.append('"%s" : "%s"' % (title, group_table[group][title]))
        group_table_list.append('"%s" : { %s }' % (group, ', '.join(group_list)))
 
    json_str += ' "group" : { %s }' % ', '.join(group_table_list)
    json_str += '}'

    return json_str


def query_encode_xml_request(args_table):
    max_args = 2
    args_list = args_table[VTV_ARGS_KEY]
    group, title = args_list[0:max_args]
    query = get_query(group, title) 
    if not query:
        return

    format_args = {}
    if len(args_list) > max_args:
        a = args_list[max_args].split(',')
        format_args = dict([(a[i], a[i+1]) for i in range(0, len(a), 2)])
        translate_args(format_args)

    query = urllib.unquote(query)
    
    query = query.replace('<', '&lt;').replace('>', '&gt;')

    query = query % format_args

    tag_list = [(QUERY_STATS_TAG, {}), (DB_STATS_TAG, {})]

    request = get_xml_request_from_args_and_tags(args_table, tag_list, STATS_TAG, query)
    return request


def query_decode_xml_response(client, xml_doc):
    print xml_doc.toxml()


def get_query_files():
    cwd = os.getcwd()
    os.chdir(VTV_CONFIG_DIR)
    file_list = glob.glob("*queries.xml")
    query_file_list = [file_name.split('.')[0].split('_')[0] for file_name in file_list]
    os.chdir(cwd)
    return query_file_list


def main(args_table):
    cmd = args_table[VTV_SUB_CMD_KEY]

    my_logger = None
    if cmd == 'list':
        print get_query_in_json(args_table, my_logger)
    elif cmd == 'query':
        my_main(query_encode_xml_request, query_decode_xml_response, args_table)


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
