#!/usr/bin/python

import os, sys, re
import cgi, hmac
import time;

sys.path.append('..')

from vtv_utils import VTV_CONFIG_XML_FILE_NAME, VTV_SERVER_DIR, VTV_ALARMS_DATA_DIR, \
                      initialize_logger, get_compact_traceback, get_sysd_ip_in_monitor, \
                      get_startup_cfg_attributes, VTV_APACHE_LOG
from vtv_xml import get_config_node_values
from vtv_user import get_vtv_user_name_and_password, get_user_password_from_xml, \
                     get_user_level_from_xml, USER_LEVEL_DICT
from globals import   VTV_ARGS_KEY, get_xml_data, generate_html_from_jinja

from data_version import VTV_VERSION_FILE_NAME


USER_LEVEL_LIST = ['invalid', 'sadmin', 'admin', 'user']

challenge_regex = re.compile('input type="hidden" name="challenge" value=\"(?P<challenge>\d+)\"')

def get_nms_monitor_list():
    xml_data = open(VTV_CONFIG_XML_FILE_NAME).read()
    xml_doc = get_xml_data(xml_data, None)
    monitor_list = get_config_node_values('alarm_server', xml_doc, default_value = [])
    monitor_list.sort()

    # For testing only
    if not monitor_list:
        cwd = os.getcwd()
        os.chdir(VTV_ALARMS_DATA_DIR)
        monitor_list = os.listdir(os.curdir)
        os.chdir(cwd)

    monitor_list.insert(0, 'all')
    return monitor_list


def get_error_text(index_html_text, error_str):
    new_text = index_html_text.replace('display:none', 'display:inline')
    new_text = new_text.replace('ERROR_STRING', error_str)

    return new_text


def main(args_table):
    my_logger = initialize_logger(VTV_APACHE_LOG)
    arg_list = args_table[VTV_ARGS_KEY]

    cwd = os.getcwd()
    index_file_name = os.path.join(cwd, os.pardir, 'html', 'index.html')
    index_html_text = open(index_file_name).read()

    for tag_match in challenge_regex.finditer(index_html_text):
        challenge = tag_match.group('challenge')
        break
    else:
        print "Error: Did not match challenge"
        return

    username = arg_list[0]
    password_digest = arg_list[1]

    attribute_table = get_startup_cfg_attributes(my_logger)
    system_type = attribute_table.get('system_type', '')
    ui_system_type = args_table.get('system_type', '')
    if ui_system_type:
        system_type = ui_system_type 

    password = get_user_password_from_xml(username)
    if not password:
        print get_error_text(index_html_text, 'User not found!')
        return
    
    obj = hmac.new(str(password), challenge)

    if obj.hexdigest() != password_digest:
        print get_error_text(index_html_text, 'Incorrect password!')
        return

    level = get_user_level_from_xml(username)
    level = USER_LEVEL_DICT.get(level, 0)
    user_level = USER_LEVEL_LIST[level]

    system_type = system_type.lower()
    sub_system_type = system_type.upper()

    server_version_file = os.path.join(os.pardir, os.pardir, VTV_VERSION_FILE_NAME)

    attribute_table = get_startup_cfg_attributes(my_logger, server_version_file)
    server_version = attribute_table.get('server_version', 'server unofficial')

    sysd_ip = get_sysd_ip_in_monitor()

    if sub_system_type == 'NMS':
        monitor_list = get_nms_monitor_list()
    else:
        monitor_list = []

    parameters = { 'login' : { 'sysd_ip' : sysd_ip, 'sub_system_type' : sub_system_type, 'system_type' : system_type, 'user_name' : username, 'user_level' : user_level, 'server_version' : server_version, 'monitor_list' : monitor_list } }
    generate_html_from_jinja(parameters)


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)

