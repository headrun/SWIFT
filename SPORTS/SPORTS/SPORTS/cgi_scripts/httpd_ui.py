#!/usr/bin/python

import sys, os, time, re, string
import datetime, time, glob, stat
import optparse
import xml.etree.ElementTree as ET

sys.path.append('..')

from vtv_utils import CRITICAL_STR, INFO_STR, MAJOR_STR, \
                      get_compact_traceback, get_startup_cfg_attributes, initialize_logger, \
                      remove_file, vtv_unpickle, get_param_from_file, \
                      VTV_ALARMS_DATA_DIR, VTV_ARCHIVE_DATA_DIR, VTV_ARCHIVE_IMAGE_DIR, \
                      VTV_ARCHIVE_SEED_DIR, VTV_CONTENT_ARCHIVE_CONTENTDB_DIR, VTV_CRASH_DIR, \
                      VTV_LATEST_DATA_DIR, VTV_LATEST_IMAGE_DIR, \
                      VTV_LATEST_SEED_DIR, VTV_PREVIOUS_DATA_DIR, VTV_PREVIOUS_IMAGE_DIR, \
                      VTV_PREVIOUS_SEED_DIR, VTV_PY_EXT, VTV_RELEASE_DIR, VTV_ROOT_DATA_DIR, \
                      VTV_SERVER_DIR, VTV_STARTUP_CFG_FILE_NAME, \
                      TIMEOUT_OCCURED, VTV_APACHE_LOG, VTV_REBOOT_TMP_DIR

from data_schema import VERSION_SEPARATOR, VERSION_ID_SEPARATOR

from data_version import VTV_DATA_VERSION_FILE_NAME, VTV_SEED_VERSION_FILE_NAME, \
                         VTV_VERSION_FILE_NAME, VTV_SERVER_VERSION_FILE 

from vtv_db import get_mysql_connection, \
                   VTV_MYSQL_PORT, MASTER_LOG_FILE_INDEX, MASTER_LOG_POS_INDEX, RELAY_LOG_FILE_INDEX, \
                   EXEC_MASTER_POS, MASTER_HOST_INDEX, SLAVE_IO_STATE_INDEX, TAG_TO_TBLNAME_MAP
from vtv_user import get_vtv_user_name_and_password, get_user_list, vigenere_crypt, gnupg_decrypt_data, \
                     USERNAME_TAG, LEVEL_TAG, USER_LEVEL_DICT

from vtv_xml import   DB_HOST_TAG, DB_NAME_TAG, DB_STATS_TAG, \
                      ALARMS_TAG, COMMAND_TAG, MONITOR_TAG, MYSQL_STATS_TAG, \
                      PROCESS_TAG, QUERYD_TAG, QUERY_STATS_TAG, \
                      SERVER_TAG, STATS_TAG, TIME_TAG, \
                      USERADMIN_COMMANDS_TAG, VALUE_TAG, \
                      add_attributes, get_data_from_tag

from vtv_event import event_loop, VtvTimerObject, VtvTimerManager
from vtv_http import VtvHttpClient, VTV_CONNECT_CLIENT

import ssh_utils

from globals import   Option, CONFIG_DIR, SYSD_IP, SYSD_PORT, LOCAL_HOST, CONFIG_FILE_TO_PARSE, \
                      VTV_ARGS_KEY, VTV_CMD_KEY, VTV_SUB_CMD_KEY, ALL_PROCESSES, ALL_SERVERS, \
                      NUMBER_OF_LOG_FILES, NUMBER_OF_LOG_LINES, \
                      get_xml_body, get_xml_data, get_xml_request_from_args_and_tags, \
                      get_xml_request_from_tags, parser_server_and_process_option, \
                      get_server_process_table, generate_html_from_jinja


RESP_TIMEOUT        = 60
HOME_ALARM_INTERVAL = 12 # hours
MAIL_ALARM_INTERVAL = 15 # minutes

MAX_HOURS = 24

EVENT_VALUE_REGEX = re.compile('data_(?P<time>\d\d\d\d\d\d\d\dT\d\d\d\d\d\d)_(?P<data_id>\w+)')

SERVER_VERSION_FILE = os.path.join(os.pardir, os.pardir, VTV_VERSION_FILE_NAME)

server_to_monitor_table = {}

xml_request_to_send     = ''
xml_response_received   = ''
need_xml_response_count = 0
got_xml_response_count  = 0


class SysdUiClient(VtvHttpClient):
    def __init__(self, server_ip, server_port, logger, timer_mgr, decode):
        try:
            VtvHttpClient.__init__(self, logger, VTV_CONNECT_CLIENT, (server_ip, server_port))

            self.decode = decode

            self.timer_mgr = timer_mgr
            self.timer_obj = self.timer_mgr.add_timer_obj(self, RESP_TIMEOUT, False)

            self.context = {}

            self.logger = logger
 
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception, e:
            logger.info("ui client Exception: %s\n" % get_compact_traceback(e))

    def parse_xml(self, xml_doc, xml_data):
        try:
            root_node = xml_doc.childNodes
            if root_node:
                monitor_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                add_attributes(root_node[0], {TIME_TAG : monitor_time})

            if self.decode:
                self.decode(self, xml_doc)
            else:
                print xml_doc.toxml()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception, e:
            self.logger.info("decode Exception: %s\n" % get_compact_traceback(e))

        self.close()

        return None

    def handle_timeout(self, timer_obj):
        self.logger.warning("Time out happened")
        print "Content-Type: text/html\n\n"
        print "<h2>%s: %s\n</h2>" % (TIMEOUT_OCCURED, str(datetime.datetime.now()))

        self.close()

    def close(self):
        if self.timer_obj:
            self.timer_mgr.cancel_timer_obj(self.timer_obj)
            self.timer_obj = None

        self.del_channel()
        self.socket.close()

        self.timer_mgr.abort = True


def my_main(encode_xml_request, decode_xml_response, args_table, context = {}, sysd_ip = SYSD_IP, my_logger = None):
    global need_xml_response_count

    xml_request = ""

    try:
        if not my_logger:
            my_logger = initialize_logger(VTV_APACHE_LOG)

        timer_mgr = VtvTimerManager(my_logger)

        sysd_ip = args_table[MONITOR_TAG]
        server_ip_str = args_table[SERVER_TAG]
        if server_ip_str:
            server_ip_list = server_ip_str.split(',')
        else:
            server_ip_list = []
        process_str = args_table[PROCESS_TAG]
        if process_str:
            process_list = process_str.split(',')
        else:
            process_list = []
        old_id_tag = "%s-0" % os.getpid()

        need_xml_response_count = 0
        server_index = 0
        for server_ip in server_ip_list:
            need_xml_response_count += len(process_list[server_index].split(':'))
            server_index += 1

        count = 0
        server_index = 0
        for server_ip in server_ip_list:
            args_table[SERVER_TAG] = server_ip
            new_sysd_ip = sysd_ip
            if sysd_ip == 'all':
                new_sysd_ip = server_to_monitor_table.get(server_ip, server_ip)

            server_process_list = process_list[server_index].split(':')
            for process_name in server_process_list:
                count += 1
                timer_mgr.abort = False

                args_table[PROCESS_TAG] = process_name
                client = SysdUiClient(new_sysd_ip, SYSD_PORT, my_logger, timer_mgr, decode_xml_response)
                client.context = context

                xml_request = encode_xml_request(args_table)
                new_id_tag = "%s%s-0" % (os.getpid(), count)
                xml_request = xml_request.replace(old_id_tag, new_id_tag) 
                if ',' in server_ip_str:
                    xml_request = xml_request.replace("server=''", "server='%s'" % server_ip) 
                if ',' in process_str or ':' in process_str:
                    xml_request = xml_request.replace("process=''", "process='%s'" % process_name) 
                client.send_msg(xml_request)

            server_index += 1

        if count <= 0:
            new_sysd_ip = sysd_ip
            timer_mgr.abort = False

            client = SysdUiClient(new_sysd_ip, SYSD_PORT, my_logger, timer_mgr, decode_xml_response)
            client.context = context

            xml_request = encode_xml_request(args_table)
            new_id_tag = "%s%s-0" % (os.getpid(), count)
            xml_request = xml_request.replace(old_id_tag, new_id_tag) 
            xml_request = xml_request.replace("server=''", "server='%s'" % ALL_SERVERS) 
            xml_request = xml_request.replace("process=''", "process='%s'" % ALL_PROCESSES) 
            client.send_msg(xml_request)

        event_loop(my_logger, timer_mgr)

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception, e:
        print "Main: %s %s" % (xml_request, get_compact_traceback(e))


def default_encode_request(args_table):
    return xml_request_to_send


DEFAULT_BADGE_COLOR = "vtv-badge-dormant"
def get_format_color(color, text):
    if not color:
        color = ''

    return """<table border="0"><tr><td class="color_box badge %s"><center style="font-weight: bold; color: black">%s</center></td></tr></table>""" % (color, text)


def format_state_cell(state):
    state_color_table = { 'up'      : 'vtv-badge-up',
                          'dormant' : 'vtv-badge-dormant', 
                          'down'    : 'vtv-badge-down',
                          'errors'  : 'vtv-badge-error',
                          'locked'  : 'vtv-badge-locked'
                        }

    return get_format_color(state_color_table.get(state, DEFAULT_BADGE_COLOR), '<center style="display:none;">%s</center>' % state)


def format_progress_cell(progress):
    progress_color_table   = { 'upgrade_started'    : (' 0% ',   'vtv-badge-started'),
                               'upgrade_proceeding' : (' 25% ',  'vtv-badge-proceeding'),
                               'upgrade_progress'   : (' 75% ',  'vtv-badge-progress'),
                               'upgrade_success'    : (' 100% ', 'vtv-badge-success'),
                               'upgrade_failed'     : ('',       'vtv-badge-fail'),
                               'restart_progress'   : ('',       'vtv-badge-restart'),
                               'idle'               : ('',       'vtv-badge-dormant'),
                               'undefined'          : ('',       'vtv-badge-dormant')
                             }

    percent, color = progress_color_table.get(progress, ('', DEFAULT_BADGE_COLOR))
    if not percent:
        text = ''

    return get_format_color(color, percent)


def handle_system_stats_reponse(xml_response):
    found = False

    root = ET.fromstring(xml_response.strip())
    if root.tag !='stats':
        return found

    SERVER_TAGS_LIST  = [ 'ST', 'ALS', 'PS', 'name', 'UT', 'CRT', 'RC', 'CM', 'CC', 'CU', 'TM', 'MU', 'TS', 'WP', 'LA_1', 'LA_3', 'LA_15' ]
    PROCESS_TAGS_LIST = [ 'ST', 'ALS', 'PS', 'server_ip', 'name', 'ID', 'pid', 'UT', 'CRF', 'RC', 'CU', 'MU' ]

    STATE_ID, ALARM_ID, PROGRESS_ID = range(3)

    parameters = { 'server_stats' : [], 'process_stats' : [] }
    for child in root:
        if child.tag != 'system_stats':
            continue

        found = True
        for node in child:
            if node.tag == 'server':
                server_value_list = [ '' ] * len(SERVER_TAGS_LIST)
                for s in node:
                    if s.tag == 'process':
                        process_value_list = [ '' ] * len(PROCESS_TAGS_LIST)
                        for p in s:
                            if p.tag in PROCESS_TAGS_LIST:
                               value = p.text or ''
                               if p.tag == 'ST':
                                   value = format_state_cell(value)
                               elif p.tag == 'PS':
                                   value = format_progress_cell(value)
                               process_value_list[PROCESS_TAGS_LIST.index(p.tag)] = value
                        parameters['process_stats'].append(process_value_list)
                    else:
                        if s.tag in SERVER_TAGS_LIST:
                           value = s.text or ''
                           if s.tag == 'ST':
                               value = format_state_cell(value)
                           elif s.tag == 'PS':
                               value = format_progress_cell(value)
                           server_value_list[SERVER_TAGS_LIST.index(s.tag)] = value
                parameters['server_stats'].append(server_value_list)

    if found:
        parameters['server_stats']
        generate_html_from_jinja(parameters)

    return found
    

def default_decode_response(client, xml_doc):
    global xml_request_to_send
    global xml_response_received
    global got_xml_response_count
 
    got_xml_response_count += 1
    xml_response = xml_doc.toxml()
    token_list = xml_response.split('>')
    if len(token_list) > 3:
        xml_response_received += '%s>' % '>'.join(token_list[2:-2])
    if got_xml_response_count >= need_xml_response_count:
        client.timer_mgr.abort = True
        if need_xml_response_count <= 1:
            print xml_response
        elif len(token_list) > 3:
            print '%s>%s>%s%s>' % (token_list[0], token_list[1], xml_response_received, token_list[-2])
        else:
            start_tag = token_list[1][1:-1]
            print '%s>%s>%s</%s>' % (token_list[0], start_tag, xml_response_received, start_tag.split(' ')[0][1:])

    xml_request_to_send = ''


def process_get_config_schema(my_logger, system):
    file_name = "system_config.xml"

    str = ''

    str += '<config_schema>'
    str += file(file_name).read()
    str += '</config_schema>'

    return str


def process_get_config_xml(my_logger, file_name):
    str = ''
    str += '<config_xml>'
    str += file(file_name).read()
    str += '</config_xml>'

    return str


def process_get_config_schema_and_xml(my_logger, system, file_name):
    str = ''

    str += process_get_config_schema(my_logger, system)
    str += process_get_config_xml(my_logger, file_name)

    return str


def process_home(my_logger, system, args_list):
    file_name = 'last_processed_alarms.txt'
    if os.access(file_name, os.F_OK):
        text = open('last_processed_alarms.txt').read()
    else:
        text = "%s:  UP" % system.upper()

    return text

def process_core_dump(my_logger, args_list):
    str = '<core_dump>'

    cwd = os.getcwd()
    os.chdir(VTV_CRASH_DIR)
    crash_list = glob.glob("*_*.out_*")

    if len(args_list) > 3:
        mod_time, server, process, pid = args_list[:4]
    else:
        mod_time, server, process, pid = [''] * 4

    need_tuple = (mod_time, server, process, pid)

    trace_out_file = ''
    #Format: vtvlogd_6218.out_127.0.0.1
    for crash_file_name in crash_list:
        file_mod_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.stat(crash_file_name)[stat.ST_MTIME]))
        file_name = crash_file_name.replace(".out_", "_")
        words = file_name.split('_')
        if len(words) < 3:
            continue
        
        got_tuple = (file_mod_time, words[-1], words[0], words[1])
        str += ("<crash><time>%s</time><server>%s</server><process>%s</process><pid>%s</pid></crash>" % got_tuple)
        if need_tuple == got_tuple:
            trace_out_file = crash_file_name

    str += '</core_dump>'

    if trace_out_file:
        text = open(trace_out_file).read()
        str += '<core_stack>'
        str += text
        str += '</core_stack>'

    os.chdir(cwd)

    return str


def process_show_users(my_logger, args_list):
    str = ''

    doc, user_list = get_user_list()
    for user in user_list:
        name = user.getElementsByTagName(USERNAME_TAG)[0].firstChild.wholeText
        level = user.getElementsByTagName(LEVEL_TAG)[0].firstChild.wholeText
        str  += ("<user><name>%s</name><level>%s</level></user>" % (name, level))

    return str


def process_list_images(my_logger, system, args_list):
    if system == "ssc" :
        version_table = {
            "latest" : (VTV_LATEST_IMAGE_DIR, VTV_LATEST_DATA_DIR),
            "previous" : (VTV_PREVIOUS_IMAGE_DIR, VTV_PREVIOUS_DATA_DIR),
            "archive" : (VTV_ARCHIVE_IMAGE_DIR, VTV_ARCHIVE_DATA_DIR),
        }
        version_file_name = (VTV_VERSION_FILE_NAME, VTV_DATA_VERSION_FILE_NAME)
        default_version_path = (VTV_RELEASE_DIR, os.path.dirname(('%s/%s' % (VTV_ROOT_DATA_DIR, VTV_DATA_VERSION_FILE_NAME))))
        default_version = ("server unofficial", "data unofficial")
        version_params = ("server_version", "data_version")
    else:
        version_table = {
            "latest" : (VTV_LATEST_IMAGE_DIR, VTV_LATEST_SEED_DIR),
            "previous" : (VTV_PREVIOUS_IMAGE_DIR, VTV_PREVIOUS_SEED_DIR),
            "archive" : (VTV_ARCHIVE_IMAGE_DIR, VTV_ARCHIVE_SEED_DIR),
        }
        version_file_name = (VTV_VERSION_FILE_NAME, VTV_SEED_VERSION_FILE_NAME)
        default_version_path = (VTV_RELEASE_DIR, os.path.dirname(('%s/%s' % (VTV_ROOT_DATA_DIR, VTV_LATEST_SEED_DIR))))
        default_version = ("server unofficial", "seed unofficial")
        version_params = ("server_version", "seed_version")

    version_list = [ "latest", "previous", "archive" ]

    str = ''
    got_default = False
    for version in version_list:
        dir_list = version_table[version] 
        image_dir, data_dir = dir_list
        if not os.path.exists(image_dir):
            got_default = True
            dir_list = default_version_path

        version_files = [os.path.join(dir_list[i], version_file_name[i]) for i in range(len(dir_list))]

        entity_version = [''] * len(dir_list) 
        for i in range(len(dir_list)):
            file_name = os.path.join(dir_list[i], version_file_name[i])
            entity_version[i] = get_param_from_file(file_name, version_params[i], default_version[i])
            
        image, data = entity_version
        str += '<version><name>%s</name><image>%s</image><data>%s</data></version>' % (version, image, data)

        if got_default:
            break
 
    return str

def process_version(my_logger, system, ip):
    vtv_user_name, vtv_password = get_vtv_user_name_and_password()

    xml_file_name = CONFIG_FILE_TO_PARSE
    attr_dict = get_server_process_table(None, xml_file_name, ['vtvsysd', 'vtvnoded'], [])
    server_list = attr_dict.keys()

    tmp_file = os.path.join(VTV_REBOOT_TMP_DIR, 'tmp_version.txt')

    server_dict = {}
    for server_ip in server_list:
        installation_file = "%s@%s:%s" % (vtv_user_name, server_ip, VTV_SERVER_VERSION_FILE)
        ssh_utils.scp(vtv_password, installation_file, tmp_file)
        if os.access(tmp_file, os.F_OK):
            attr_dict = get_startup_cfg_attributes(my_logger, tmp_file)
            server_dict[server_ip] = {}
            server_dict[server_ip]['server_version'] = attr_dict.get('server_version', '')
            server_dict[server_ip]['data_version'] = attr_dict.get('data_version', '')
            remove_file(tmp_file, my_logger)

    html_str = '<table border=1 class="table table-condensed">'

    html_str += '<tr><th>Server</th><th>ID</th><th>Version</th></tr>'

    for server_ip in server_dict:
        server_version = server_dict[server_ip]['server_version']
        data_version = server_dict[server_ip]['data_version']

        html_str += '<tr><td>%s</td><td></td><td></td></tr>' % server_ip
        html_str += '<tr><td></td><td>%s</td><td>%s</td></tr>' % ('image', server_version)

        data_version_list = [ x.split(VERSION_ID_SEPARATOR) for x in data_version.split(VERSION_SEPARATOR) if VERSION_ID_SEPARATOR in x ]
        data_version_list.sort()
        for data_id, data_version in data_version_list:
            html_str += '<tr><td></td><td>%s</td><td>%s</td></tr>' % (data_id, data_version)

    html_str += "</table>"

    return html_str


def process_server_bandwidth(my_logger, args_list):
    str = ''

    server = args_list[SERVER_TAG]
    
    options = Option()
    
    bandwidth_file = os.path.join(VTV_REBOOT_TMP_DIR, "bandwidth.txt")
    scp_file_name = "%s@%s:%s" % (options.vtv_user_name, server, bandwidth_file)

    try:
        remove_file(bandwidth_file, my_logger)
        cmd = "rm -f %s" % (bandwidth_file)
        status = ssh_utils.ssh_cmd(server, options.vtv_user_name, options.vtv_password, cmd)
        if status:
            str = "<ERROR>%s failed with status: %s remote file: %s</ERROR>"  % (cmd, status, scp_file_name)
            return str
        cmd = "cp /proc/net/dev %s" % (bandwidth_file)
        status = ssh_utils.ssh_cmd(server, options.vtv_user_name, options.vtv_password, cmd)
        if status:
            str = "<ERROR>%s failed with status: %s remote file: %s</ERROR>"  % (cmd, status, scp_file_name)
            return str
        status = ssh_utils.scp(options.vtv_password, scp_file_name, bandwidth_file)
        if status:
            str = "<ERROR>scp from %s to %s failed with status: %s</ERROR>"  % (scp_file_name, bandwidth_file, status)
            return str
    except Exception, e:
        str += "<ERROR>%s threw an exception: %s</ERROR>" % (cmd, get_compact_traceback(e))
        return str
    
    data = file(bandwidth_file).read()
    lines = data.split('\n')[2:-1]
    for line in lines:
        iface, fields = line.split(':')
        iface = iface.strip()
        if iface.startswith("sit"):
            continue
        field_list = [iface] + fields.split()
        field_name_list = ["name", "txb", "txp", "txe", "txd", "txff", "txfr", "txc", "txm", 
           "rxb", "rxp", "rxe", "rxd", "rxff", "rxcl", "rxcr", "rxc"]
        str += '<if>'
        for i in range(len(field_list)):
            str += ('<%s>%s</%s>' % (field_name_list[i], field_list[i], field_name_list[i]))
        str += '</if>'

    return str
    

def process_configure(my_logger, system, sub_cmd, args_list, sysd_ip):
    request = '<%s_config>' % sub_cmd

    if sub_cmd in ['display', 'schema_view', 'tree_view']: 
        file_name = CONFIG_FILE_TO_PARSE
        if sysd_ip != SYSD_IP:
            file_name = os.path.join(VTV_ALARMS_DATA_DIR, '%s/%s' % (sysd_ip, os.path.basename(CONFIG_FILE_TO_PARSE)))
        request += process_get_config_schema_and_xml(my_logger, system, file_name)
    elif sub_cmd == 'show':
        num = args_list[0]
        file_name = CONFIG_DIR + "vtv.xml.%s" % num
        request += process_get_config_schema_and_xml(my_logger, system, file_name)
    elif sub_cmd == 'revert':
        cwd = os.getcwd()
        if os.access(CONFIG_DIR, os.F_OK):
            os.chdir(CONFIG_DIR)
            file_list = glob.glob("vtv.xml.*")
            file_list.sort()
            number_list = [file.split(".")[2] for file in file_list]
            request += (','.join(number_list))
        os.chdir(cwd)

    request += ('</%s_config>' % sub_cmd)

    return request


def local_logs(ip, file_name):
    str = ''

    options = Option()
    
    log_session = None

    curr_dir = os.getcwd()

    if file_name[0] != '/':
        log_dir = os.path.join(curr_dir,"../../logs")
        file_name = os.path.join(log_dir, file_name)
    
    try:
        status, log_session = ssh_utils.ssh_cmd_output(ip, options.vtv_user_name, options.vtv_password, "tail -%d %s" % (NUMBER_OF_LOG_LINES, file_name))
    except Exception, e:
        str += ("Failed to Open connection to : %s error: %s" % (ip, get_compact_traceback(e)))
        return str
    
    str += ("<h2>The Contents of Log File: %s </h2>" % file_name)
    
    data = log_session.before.replace('<', '&lt;').replace('>','&gt;').split('\n')[1:-1]
    str += string.join(data, '<br>')
    return str


def remote_logs(logd_ip):
    str = ''

    options = Option()
    
    log_session = None
    
    curr_dir = os.getcwd()
    log_dir = os.path.join(curr_dir,"../../logs")
    try:
        status, log_session = ssh_utils.ssh_cmd_output(logd_ip, options.vtv_user_name, options.vtv_password, 
            "/bin/ls -t -1 %s | head -%d" % (log_dir, NUMBER_OF_LOG_FILES))
    except Exception, e:
        str += ("Failed to Open connection to : %s" % logd_ip)
        return str
    
    file_list = log_session.before.split('\r\n')[1:-1]
    server_table = {}
    for line in file_list:
        parts = line.split('-') 
        if len(parts) < 3 or parts[0] != 'vtv.log':
            continue
        prefix, server_ip, start_time = parts[:3]
        if server_ip not in server_table:
            server_table[server_ip] = []
        server_table[server_ip].append(line) 
    
    str += '<div id="logs">'
    for server in server_table.keys():
        str += ("<h2>%s</h2>" % server)
        counter = 0 
        for file in server_table[server]:
            str += '<h5><a class="Dark" href="javascript:common_request'
            str += ( '(\'display_log\',\'%s\',\'%s\',\'%s\')">%s</a></h5>' %('copy', logd_ip, file, file))
            counter += 1
            if counter == 10:
                break
        str += "<h1></h1>"
    str += '</div>'
    
    log_session.close(0)

    return str


def run_remote_command(my_logger, sysd_ip, server_ip, process_name, cmd, sub_cmd, args_list):
    if server_ip == '' or server_ip == 'undefined':
        server_ip = sysd_ip
    monitor_ip = server_to_monitor_table.get(server_ip, server_ip)

    user_name, password = get_vtv_user_name_and_password()
    try:
        ssh_cmd = 'cd %s/cgi-bin/; python httpd_ui.%s --cmd "%s" --sub-cmd "%s"' % (VTV_SERVER_DIR, VTV_PY_EXT, cmd, sub_cmd)
        status, log_session = ssh_utils.ssh_cmd_output(monitor_ip, user_name, password, "%s" % (ssh_cmd))
    except Exception, e:
        str = "Failed to run %s %s error: %s" % (cmd, sub_cmd, get_compact_traceback(e))
        return str

    text = log_session.before
    pos = text.find('<stats ')
    if pos <= 0:
        pos = 0

    return text[pos:]


def process_data_push_stats(my_logger, args_list):
    str = ''
    str += "<push_stats>"
    cwd = os.getcwd()
    daily_dir = os.path.join(VTV_CONTENT_ARCHIVE_CONTENTDB_DIR, 'dataClips/daily')
    os.chdir(daily_dir)
    data_file_list = glob.glob('data_*_*.tgz')
    for file_name in data_file_list:
        prefix, data_file_time, file_tag = file_name.replace('.tgz', '_tgz').split('_')[:3]
        file_time = datetime.datetime(*time.strptime(data_file_time, '%Y%m%dT%H%M%S')[:6]).strftime('%Y-%m-%d %H:%M:%S')

        str += "<push><name>%s</name><time>%s</time><tag>%s</tag>" % (file_name, file_time, file_tag)
        
        content_version_file_name = 'contentVersion_%s_%s.txt' % (data_file_time, file_tag)
        stats_str = ''
        if os.path.exists(content_version_file_name):
            attribute_table = get_startup_cfg_attributes(my_logger, content_version_file_name)
            for stat_name in ['clips_seen', 'clips_dead', 'clips_error', 'clips_final', 'folds', 'ssc']:
                if stat_name in attribute_table:  
                    if stat_name == 'ssc':
                        s_list = []
                        f_list = []
                        for z in attribute_table[stat_name].split(','):
                            x,y = z.split('-')
                            if y == 'Success':
                                s_list.append(x)
                            else:
                                f_list.append(x)
                        ssc_str = ''
                        if s_list:
                            ssc_str += ('Pass: ' + ', '.join(s_list))
                        if f_list:
                            ssc_str += ('Fail: ' + ', '.join(f_list))
                        attribute_table[stat_name] = ssc_str
                    stats_str += ('<%s>%s</%s>' % (stat_name, attribute_table[stat_name], stat_name))

        str += ("%s</push>" % stats_str)
            
    os.chdir(cwd)
    str += "</push_stats>"

    return str


def get_db_rows(logger, mysql_ip, query):
    cursor, db = get_mysql_connection(logger, server=mysql_ip)
    if not cursor:
        return ''
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception, e:
        logger.error("Exception while querying db: %s query: %s status: %s" % (mysql_ip, query, get_compact_traceback(e)))
        rows = []

    cursor.close()
    db.close()

    return rows


def get_replication_stats(logger, monitor_ip, server_ip, process_name, args_list):
    mysql_ip = server_ip
    slave_ip_list = []
    new_slave_ip_list = []

    xml_str = '<mysql_stats><replication_stats>'

    rows = get_db_rows(logger, mysql_ip, 'show master status')
    if rows:
        master_file, position, db, pad = rows[0]
        xml_str += '<master_status><ip>%s</ip><file>%s</file><db>%s</db><pos>%s</pos></master_status>' % (mysql_ip, master_file, db, position)
        rows = get_db_rows(logger, mysql_ip, 'show processlist')
        for row in rows:
            id, user, host, db_name, command, seconds, state, info = row[:8]
            if command == 'Binlog Dump':
                ip = host.split(':')[0]
                count = new_slave_ip_list.count(ip)
                new_slave_ip_list.append(ip)
                if count > 0:
                    ip = '%s:%d' % (ip, VTV_MYSQL_PORT + count)
                slave_ip_list.append(ip)
    else:
         rows = get_db_rows(logger, mysql_ip, 'show slave status')
         slave_ip_list = [mysql_ip]
         new_slave_ip_list = [mysql_ip]

    SECONDS_BEHIND_MASTER = 32
    xml_str += '<SlaveList>'
    for slave_ip in slave_ip_list:
        rows = get_db_rows(logger, slave_ip, 'show slave status')
        for row in rows:
            Master_Log_File         = row[MASTER_LOG_FILE_INDEX]
            Read_Master_Log_Pos     = row[MASTER_LOG_POS_INDEX]
            Relay_Master_Log_File   = row[RELAY_LOG_FILE_INDEX]
            Exec_Master_Log_Pos     = row[EXEC_MASTER_POS]
            master_ip               = row[MASTER_HOST_INDEX]
            slave_state             = row[SLAVE_IO_STATE_INDEX]
            slave_behind            = row[SECONDS_BEHIND_MASTER]
            xml_str += '<slave_status><ip>%s</ip><mip>%s</mip><master_file>%s</master_file><slave_io>%s</slave_io><slave_file>%s</slave_file><slave_pos>%s</slave_pos><slave_behind>%s</slave_behind></slave_status>' % (slave_ip, master_ip, Master_Log_File, slave_state, Relay_Master_Log_File, Exec_Master_Log_Pos, slave_behind)

    xml_str += '</SlaveList>'
    xml_str += '</replication_stats></mysql_stats>'

    return xml_str


def get_push_run_upgrade_data_status(logger, vkc_ip, args_list):
    mysql_ip = '127.0.0.1'
    status_str = ''
    cursor, db = get_mysql_connection(logger, mysql_ip)
    if not cursor:
        logger.error("Connecting to Mysql %s Failed" % (mysql_ip))
        return 1

    ssc_ip = args_list[0]
    ip_list = (vkc_ip, ssc_ip)
    query = ''' select time, monitor_ip, event_type, event_value from ALARMSDB.alarms where event_type in ('Push Data', 'Run Data', 'Upgrade Data') and time > curdate() - interval %s hour and monitor_ip in %s order by time ''' % (MAX_HOURS, str(ip_list))
    try:
        logger.info("Executing query: %s" % (query))
        cursor.execute(query)
        rows = cursor.fetchall()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception, e:
        logger.error("Exception while taking alarms db : %s" % (get_compact_traceback(e)))
        rows = []

    data_id_table = {}
    for row in rows:
        time_obj, monitor_ip, event_type, event_value = row
        event_type = ' '.join([x.capitalize() for x in event_type.split()])
        time_str = time_obj.strftime('%Y-%m-%dT%H:%M:%S')
        id_time_str = time_obj.strftime('%Y%m%dT%H%M%S')
        
        obj = EVENT_VALUE_REGEX.search(event_value)
        if not obj:
            continue
        timestamp, data_id = obj.groups()
        if data_id not in data_id_table:
            data_id_table[data_id] = {}
        if timestamp not in data_id_table[data_id]:
            data_id_table[data_id][timestamp] = {}
        status_str = 'Invalid'
        if 'Push' in event_type:
            if '%s-Fail' % ssc_ip in event_value: 
                status_str = 'Fail'
            else:
                status_str = 'Success'
        elif monitor_ip == ssc_ip:
           if 'Fail' in event_value:
                status_str = 'Fail'
           elif 'Success' in event_value or 'Succeeded' in event_value:
                status_str = 'Success'
           elif 'Started' in event_value and status_str != 'Success':
                status_str = 'Started'
           elif 'Starting' in event_value and status_str not in ('Success', 'Started'):
                status_str = 'Starting'
        else:
            continue

        if event_type not in data_id_table[data_id][timestamp]:
            data_id_table[data_id][timestamp][event_type] = (status_str, time_str)
        else:
            old_status_str, old_time_str = data_id_table[data_id][timestamp][event_type]
            if old_time_str < time_str:
                data_id_table[data_id][timestamp][event_type] = (status_str, time_str)

        # hack to fix the bug in upgrade alarm
        if event_type == 'Upgrade Data' and status_str == 'Success': 
            timestamp_list = [ x for x in data_id_table[data_id].keys() if x < id_time_str ] 
            for timestamp in timestamp_list:
                if event_type in data_id_table[data_id][timestamp]:
                    old_status_str, old_time_str = data_id_table[data_id][timestamp][event_type]
                    data_id_table[data_id][timestamp][event_type] = (status_str, old_time_str)

    MAX_TIMESTAMP = 10
    data_id_list = data_id_table.keys()
    data_id_list.sort()
    xml_str = '<data_flow>'
    for data_id in data_id_table:
        timestamp_list = data_id_table[data_id].keys()
        timestamp_list.sort(reverse = True)
        timestamp_list = timestamp_list[:MAX_TIMESTAMP]
        for timestamp in timestamp_list:
            value_list = [data_id, timestamp]
            finish_time = '00000000T000000'
            if 'Push Data' not in data_id_table[data_id][timestamp]:
                continue
            for event_type in [ 'Push Data', 'Run Data', 'Upgrade Data']:
                new_finish_time = '-'
                if event_type in data_id_table[data_id][timestamp]:
                    status_str, new_finish_time = data_id_table[data_id][timestamp][event_type] 
                    if new_finish_time > finish_time:
                        finish_time = new_finish_time
                else:
                    status_str = "-"
                value_list.append('%s %s' % (status_str, new_finish_time))
            value_list.append(finish_time)
            if 'Started' in ' '.join(value_list):
                value_list[0] = '%s (*)' % value_list[0]
            xml_str += "<row><dataid>%s</dataid><time>%s</time><push>%s</push><run>%s</run><upgrade>%s</upgrade><finish>%s</finish></row>" % tuple(value_list)
    xml_str += '</data_flow>'

    return xml_str


def get_stats_per_server_and_process(my_logger, args_table, func, func_args_list):
    server_list = args_table[SERVER_TAG].split(',')
    process_list = args_table[PROCESS_TAG].split(',')
    monitor_ip = args_table[MONITOR_TAG]

    xml_str = ''
    server_index = 0
    for server_ip in server_list:
        server_process_list = process_list[server_index].split(':')
        server_index += 1
        for process_name in server_process_list:
            xml_str += func(my_logger, monitor_ip, server_ip, process_name, *func_args_list)
    return xml_str


def get_memcached_server_stats(my_logger, monitor_ip, server_ip, process_name):
    import memcache

    port = process_name.split('_')[1]
    mc = memcache.Client(['%s:%s' % (server_ip, port)], debug=0)
    stats_list = mc.get_stats()
    stats_table = stats_list[0][1]

    stats_str = '<server>%s</server><process>%s</process>' % (server_ip, process_name)
    for name in stats_table:
         stats_str += '<%s>%s</%s>' % (name, stats_table[name], name) 

    xml_str = '<ServerStats>%s</ServerStats>' % stats_str
    
    return xml_str


def httpd_send_query_to_db(args_table, query):
    tag_list = [(QUERY_STATS_TAG, {}), (DB_STATS_TAG, {})]
    request = get_xml_request_from_args_and_tags(args_table, tag_list, STATS_TAG, query)
    return request


# This Method is to process the XML(request) that this script sends to YUI to parse. Using it for Alarm processing.
def process_request_for_alarm(request):
    xml_doc = get_xml_data(request, None)
    critical_str = ''
    warning_str = ''

    # "tag_freq_dict" contains each tag as key and values are list of ['expected freq', 'upper bound freq']. List used for ['warning', 'critical'] resp.
    # Note: In the below dict, 20 mins should be given as 0.33 as it is 0.33 hours, 30 mins should be given as 0.5
    tags_freq_dict = {
                      'news':     (1.5, 2.5),
                      'myvideo':  (1.5, 2.5),
                      'medeo':    (6.0, 12.0),
                      'featured': (6.0, 12.0),
                      'vat':      (18.0, 36.0),
                      'phrases':  (36.0, 60.0),
                     }
                      #'vtapusers':(36.0, 60.0),
    default_freq   = (18.0, 36.0)
    data_ids_with_default_freq = TAG_TO_TBLNAME_MAP.keys()
    x = [tags_freq_dict.setdefault(data_id, default_freq) for data_id in data_ids_with_default_freq]

    DAY_IN_SECONDS = 24 * 60 * 60
    DEFAULT_TIME = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 3 * DAY_IN_SECONDS))
    tag_suffix_format = re.compile(r'\d{5}$')
    tag_table = {}
    x = [tag_table.setdefault(data_id, DEFAULT_TIME) for data_id in tags_freq_dict]
    for row_element in xml_doc.getElementsByTagName('row'):
        data_id_node = row_element.getElementsByTagName('dataid')
        full_data_id = get_data_from_tag(data_id_node[0])
        data_id = full_data_id.split('(')[0].strip()
        data_id = tag_suffix_format.sub('', data_id)
        tag_table.setdefault(data_id, DEFAULT_TIME)

        if '(*)' in full_data_id:
            continue

        upgrade_time_node = row_element.getElementsByTagName('upgrade')[0]
        upgrade_time = get_data_from_tag(upgrade_time_node)
        if 'Success' not in upgrade_time:
            continue

        upgrade_time = upgrade_time.strip('Success ').strip()
        if tag_table[data_id] < upgrade_time:
            tag_table[data_id] = upgrade_time

    current_time = time.localtime()
    IGNORE_DATA_ID_LIST = [ 'azn', 'crt', 'medeo', 'myvideo', 'new', 'trk', 'aud', 'myv', 'gog', 'msp' ]
    for data_id in tag_table:
        if data_id in IGNORE_DATA_ID_LIST:
            continue

        freq_tuple = tags_freq_dict.get(data_id)
        if not freq_tuple:
            continue
        warning_duration, critical_duration = freq_tuple

        upgrade_time = tag_table[data_id]
        upgrade_time_in_format = time.strptime(upgrade_time, "%Y-%m-%dT%H:%M:%S")

        # Difference between Current time and the upgrade time
        diff_in_time_delta = datetime.datetime(*current_time[:6]) - datetime.datetime(*upgrade_time_in_format[:6])
        diff_in_secs = diff_in_time_delta.days * DAY_IN_SECONDS + diff_in_time_delta.seconds
        diff_in_mins, diff_in_secs = divmod(diff_in_secs, 60) 
        diff_in_hours, diff_in_mins = divmod(diff_in_mins, 60) 
        # Converts 1 hour 30 mins to 1.5 hours
        diff_in_decimal_hours = float(diff_in_hours) + (float(diff_in_mins) / 60.0)

        # Send alarm based on Level of criticality
        tag_level = 0
        if diff_in_decimal_hours > critical_duration:
            tag_level = CRITICAL_STR
        elif diff_in_decimal_hours > warning_duration:
            tag_level = MAJOR_STR
        else:
            tag_level = INFO_STR

        if tag_level != INFO_STR:
            err_str = "<tr><td>%s</td><td>%d Hours %d Mins Ago</td><td>%s</td></tr>" % (data_id, diff_in_hours, diff_in_mins, tag_level)
            if tag_level == MAJOR_STR:
                warning_str += err_str
            elif tag_level == CRITICAL_STR:
                critical_str += err_str

    if warning_str or critical_str:
        alarm_str = '<table border=1>%s%s</table>' % (critical_str, warning_str)
        return alarm_str

    return ''


def httpd_send_request(sysd_ip, cmd, args_table, send_alarm = False):
    global xml_request_to_send
    global server_to_monitor_table
 
    my_logger = initialize_logger(VTV_APACHE_LOG)

    #my_logger.info('%s' % args_table) 

    file_name = VTV_STARTUP_CFG_FILE_NAME
    attribute_table = get_startup_cfg_attributes(my_logger, file_name)
    ui_system = attribute_table.get('system_type', '')

    if sysd_ip != SYSD_IP:
        file_name = os.path.join(VTV_ALARMS_DATA_DIR, '%s/%s' % (sysd_ip, os.path.basename(VTV_STARTUP_CFG_FILE_NAME)))
    attribute_table = get_startup_cfg_attributes(my_logger, file_name)
    system = attribute_table.get('system_type', '')

    args_table['system'] = attribute_table.get('system_type', '')

    args_list = args_table.get(VTV_ARGS_KEY, [])
    sub_cmd = args_table.get(VTV_SUB_CMD_KEY, '')
    server_ip_list = args_table.get(SERVER_TAG, '').split(',')
    server_ip = server_ip_list[0]
    process_list = args_table.get(PROCESS_TAG, '').split(',')
    process_name = process_list[0]

    if sysd_ip == 'all':
        pickle_file_name = os.path.join(VTV_ALARMS_DATA_DIR, 'all/server_to_monitor.txt')
        server_to_monitor_table = vtv_unpickle(pickle_file_name, my_logger)

    request = ''

    if cmd == 'home':
        request = process_home(my_logger, system, args_list)
    elif cmd == 'data_flow':
        request = get_push_run_upgrade_data_status(my_logger, server_ip, args_list)
    elif cmd == 'core_dump':
        request = process_core_dump(my_logger, args_list)
    elif cmd == 'show_users':
        if ui_system == 'nms':
            request = get_stats_per_server_and_process(my_logger, args_table, run_remote_command, [cmd, sub_cmd, args_list])
        else:
            request = process_show_users(my_logger, args_list)
    elif cmd == 'list_images':
        if ui_system == 'nms':
            request = get_stats_per_server_and_process(my_logger, args_table, run_remote_command, [cmd, sub_cmd, args_list])
        else:
            request = process_list_images(my_logger, system, args_list)
    elif cmd == 'version':
        request = process_version(my_logger, system, args_table[SERVER_TAG])
    elif cmd == 'server_bandwidth':
        request = process_server_bandwidth(my_logger, args_table)
    elif cmd == 'get_config':
        file_name = CONFIG_FILE_TO_PARSE
        if sysd_ip != SYSD_IP:
            file_name = os.path.join(VTV_ALARMS_DATA_DIR, '%s/%s' % (sysd_ip, os.path.basename(CONFIG_FILE_TO_PARSE)))
        request = process_get_config_xml(my_logger, file_name)
    elif cmd == 'configure':
        request = process_configure(my_logger, system, sub_cmd, args_list, sysd_ip)
    elif cmd == MYSQL_STATS_TAG and sub_cmd == 'replication_stats':
        request = get_stats_per_server_and_process(my_logger, args_table, get_replication_stats, [args_list])
    elif cmd == 'display_log':
        request = ''
        if sub_cmd == 'monitor':
            request = local_logs(LOCAL_HOST, "../server/vtv.log")
        elif sub_cmd == 'server':
            request = remote_logs(server_ip)
        elif sub_cmd == 'copy':
            request = local_logs(args_list[0], args_list[1])
        elif sub_cmd == 'http':
            request = local_logs(LOCAL_HOST, "/var/log/httpd/error_log")

        print request
        sys.exit(0)
    elif cmd == 'memdb_loader_stats' and sub_cmd == 'server':
        request = get_stats_per_server_and_process(my_logger, args_table, get_memcached_server_stats, [])

    if send_alarm:
        alarm_msg = process_request_for_alarm(request)
        if alarm_msg:
            print alarm_msg
            raise NotImplementedError
        else:
            sys.exit(0)
    else:
        if request:
            print get_xml_body(request, system)
            sys.exit(0)

    old_sysd_ip = sysd_ip
    if sysd_ip == 'all':
        sysd_ip = server_to_monitor_table.get(server_ip, server_ip)

    tag_list = []
    if cmd == USERADMIN_COMMANDS_TAG:
        command = "%s" % args_list[0]
        command = vigenere_crypt(command, False)
        command = command.strip()
        request = get_xml_request_from_tags(tag_list, COMMAND_TAG, command, USERADMIN_COMMANDS_TAG)
    elif cmd == 'alarms': 
        if sub_cmd == 'pending':
            request = get_xml_request_from_tags(tag_list, ALARMS_TAG)
        elif sub_cmd == 'recent':
            query =  "select * from LOGDB.alarms order by time desc limit 100"
            request = httpd_send_query_to_db(args_table, query)
    elif cmd == MYSQL_STATS_TAG and sub_cmd == 'status':
        query =  "show status"
        request = httpd_send_query_to_db(args_table, query)
    elif cmd == MYSQL_STATS_TAG and sub_cmd == 'processlist':
        query =  "show processlist"
        request = httpd_send_query_to_db(args_table, query)
    elif cmd == MYSQL_STATS_TAG and sub_cmd == 'fullprocesslist':
        query =  "show full processlist"
        request = httpd_send_query_to_db(args_table, query)
    elif cmd == MYSQL_STATS_TAG and sub_cmd == 'variables':
        query =  "show variables"
        request = httpd_send_query_to_db(args_table, query)
    elif cmd == QUERYD_TAG: 
        opt_dict = eval(args_list[0])
        type = opt_dict['type']
        server_ip = opt_dict['server_ip']
        db_host   = opt_dict.get(DB_HOST_TAG, '')
        db_name   = opt_dict.get(DB_NAME_TAG, '')

        tag_list = [(type, {DB_HOST_TAG: db_host, DB_NAME_TAG: db_name}), (SERVER_TAG, { VALUE_TAG : server_ip }), (QUERYD_TAG, {})]
        body = opt_dict.get('queries', '')
        request = get_xml_request_from_tags(tag_list, STATS_TAG, body)
    elif cmd in [ 'query', 'commands', 'history' ]:
        if ui_system == 'nms' and old_sysd_ip != 'all':
            request = get_stats_per_server_and_process(my_logger, args_table, run_remote_command, [cmd, sub_cmd, args_list])
            if cmd == 'query' and sub_cmd == 'list':
                print request
            else:
                print get_xml_body(request, system)
            sys.exit(0)
        else:
            m = __import__(cmd, globals(),  locals(), [])
            m.main(args_table)
            sys.exit(0)
    elif cmd in ['sproxy_stats', 'search_all_stats', 'auth_stats']:
        if cmd == 'sproxy_stats':
            cmd = 'search_all_stats'
        tag_list = [(cmd, {})]
        if sub_cmd:
            tag_list.insert(0, (sub_cmd, {}))
        if len(args_list) > 0:
            attrib = { VALUE_TAG : "%s" % args_list[0]}
            tag_list.insert(0, (SERVER_TAG, attrib))
        request = get_xml_request_from_args_and_tags(args_table, tag_list, STATS_TAG, '')

    if not request:
        if cmd in ['memdb_loader_stats', 'memdb_servicer_stats']:
            tag_list = [('memdb_stats', {VALUE_TAG : sub_cmd})]
        else:
            tag_list = [(cmd, {})]
        if sub_cmd:
            tag_list.insert(0, (sub_cmd, {}))
        body = ''
        if len(args_list) > 0:
            body = ','.join(args_list) 
        request = get_xml_request_from_args_and_tags(args_table, tag_list, STATS_TAG, body)

    if request:
        xml_request_to_send = request
        my_main(default_encode_request, default_decode_response, args_table, {}, sysd_ip, my_logger = my_logger)


if __name__ == '__main__':
    try:
        parser = optparse.OptionParser()
        parser.add_option('', '--monitor', dest='monitor', default=SYSD_IP,  metavar='monitor ip', help='monitor ip')
        parser.add_option('', '--cmd', dest='cmd', default='home',  metavar='command', help='command')
        parser.add_option('', '--sub-cmd', dest='sub_cmd', default='',  metavar='sub command', help='sub command')
        parser.add_option('', '--server', default='127.0.0.1',  metavar='server', help='list of server')
        parser.add_option('', '--process', default='vtvsysd',  metavar='process', help='list of process')
        parser.add_option('', '--send-alarm', default=False, metavar='BOOL', action='store_true', help='set to send alarm')

        options, args = parser.parse_args(sys.argv[1:])

        args_table = {}
        args_table[MONITOR_TAG] = options.monitor
        args_table[SERVER_TAG] = options.server
        args_table[PROCESS_TAG] = options.process
        args_table[VTV_CMD_KEY] = options.cmd
        args_table[VTV_SUB_CMD_KEY] = options.sub_cmd
        args_table[VTV_ARGS_KEY] = args

        server_name, process_name = parser_server_and_process_option(options.monitor, args_table)

        httpd_send_request(options.monitor, options.cmd, args_table, options.send_alarm)

    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except NotImplementedError:
        sys.exit(1)
    except:
        e = sys.exc_info()[2]
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
