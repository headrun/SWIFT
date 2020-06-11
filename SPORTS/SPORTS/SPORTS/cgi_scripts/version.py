#!/usr/bin/python

import os, sys

sys.path.append('..')

from vtv_utils import remove_file, initialize_logger, get_compact_traceback, get_startup_cfg_attributes, \
                      VTV_APACHE_LOG, VTV_REBOOT_TMP_DIR

from data_version import VTV_SERVER_VERSION_FILE, VTV_VERSION_FILE_NAME

import ssh_utils

from globals import CONFIG_FILE_TO_PARSE, ALL_PROCESSES, \
                    Option, get_server_process_table


SERVER_VERSION_FILE = os.path.join(os.pardir, os.pardir, VTV_VERSION_FILE_NAME)


def main(arg_list):
    logger = initialize_logger(VTV_APACHE_LOG)

    options = Option()
    
    passwd = "%s" % options.vtv_password

    xml_file_name = CONFIG_FILE_TO_PARSE
    attr_dict = get_server_process_table(None, xml_file_name, ['vtvsysd', 'vtvnoded'], [])
    server_list = attr_dict.keys()

    tmp_file = os.path.join(VTV_REBOOT_TMP_DIR, 'tmp_version.txt')

    server_dict = {}
    for server_ip in server_list:
        installation_file = "%s@%s:%s" % (options.vtv_user_name, server_ip, VTV_SERVER_VERSION_FILE) 
        ssh_utils.scp(passwd, installation_file, tmp_file)
        if os.access(tmp_file, os.F_OK):
            attr_dict = get_startup_cfg_attributes(logger, tmp_file)
            server_dict[server_ip] = {}
            server_dict[server_ip]['server_version'] = attr_dict.get('server_version', '')
            server_dict[server_ip]['data_version'] = attr_dict.get('data_version', '')
            remove_file(tmp_file, logger)

    print '<table border=1 class="table table-condensed">'

    for server_ip in server_dict:
        server_version = server_dict[server_ip]['server_version']
        data_version = server_dict[server_ip]['data_version']

        print '<tr><th>Server</th><th>Type</th><th>Data ID</th><th>Version</th></tr>'

        print '<tr><td>%s</td><td></td><td></td><td></td></tr>' % server_ip
        print '<tr><td></td><td>Server Version</td><td></td><td></td></tr>'
        print '<tr><td></td><td></td><td>%s</td><td>%s</td></tr>' % ('image', server_version)
        print '<tr><td></td><td></td><td></td><td></td></tr>'

        print '<tr><td></td><td>Data Version</td><td></td><td></td></tr>'
        data_version_list = [ x.split(': ') for x in data_version.split(',') ]
        for data_id, data_version in data_version_list:
            print '<tr><td></td><td></td><td>%s</td><td>%s</td></tr>' % (data_id, data_version)

    print "</table>"


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
