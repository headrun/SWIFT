#!/usr/bin/python

import os, sys, cgi, time, re
import jinja2

sys.path.append('..')

from vtv_utils import VTV_ALARMS_DATA_DIR

from vtv_xml import STATS_TAG, SHOW_TAG, ID_TAG, PROCESS_TAG, SERVER_TAG, VALUE_TAG, \
                    ALL_TAG, ALL_PROCESSES, ALL_SERVERS, \
                    get_child_dict_for_tag, get_data_from_tag, get_xml_data

from vtv_user import get_vtv_user_name_and_password


CONFIG_DIR           = "../../../config/"
CONFIG_FILE_TO_PARSE = CONFIG_DIR + "vtv.xml"

MAX_ARGS        = 10
VTV_CMD_KEY     = 'cmd'
VTV_SUB_CMD_KEY = 'sub_cmd'
VTV_ARGS_KEY    = 'args'

NUMBER_OF_LOG_FILES     = 50
NUMBER_OF_LOG_LINES     = 1000

X_MSG_TYPE      = 1
SYSD_PORT       = 8600
SYSD_IP         = '127.0.0.1'
LOCAL_HOST      = '127.0.0.1'

prompt = '#'

OPTION_EXISTS        = 0
OPTION_NOT_EXISTS    = 1

TEMP_SQL_QUERIES_FILE       = CONFIG_DIR + 't_queries.xml'

REFRESH_BUTTON_STR  = '<h4><center>[<a class="Dark" href="javascript:request_the_server()">Refresh</a>]</center></h4>'

#variables for color-coding
normal_color    = '#F0FFFF'
diff_color      = '#FFF8DC'
process_color   = '#f5f5f5'
color           = 0


MENU_ARGS_REGEX = re.compile('process\s+:\s+\'(?P<process>\w+)\',\s+module\s+:\s+\'(?P<module>\w+)\',.*?args\s+:\s+\[\'(?P<cmd>\w+)\',\s+\'(?P<sub_cmd>\w*)\'\]', re.DOTALL)


class Option:
    def __init__(self):
        self.vtv_user_name, self.vtv_password = get_vtv_user_name_and_password(CONFIG_DIR)



def tag_exists(server_list, tag):
    for server_node in server_list:
        server_dict = get_child_dict_for_tag(server_node)
        if tag in server_dict:
            return True
    return False


def get_node_value(node, tag):
    value = node.getAttribute(tag)
    if value:
        return value
    else:
        return "True"


def print_tag_value(node_dict, tag):
    if tag in node_dict:
        print '<td>%s</td>' % get_node_value(node_dict[tag], VALUE_TAG)
    else:
        print '<td>---</td>'


def wrap_with_tag(tag, body = "", attrib = {}):
    prefix = "%s" % tag
    for key in attrib:
        prefix += (" %s='%s'" % (key, attrib[key]))
    return "<%s>%s</%s>" % (prefix, body, tag)


def get_xml_body(body, system):
    monitor_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    str = ''
    str += '<stats T="%s" system="%s" id="%s-0">' % (monitor_time, system, os.getpid())
    str += body
    str += '</stats>'

    return str


def get_xml_request_from_tags(tag_list, top_tag = STATS_TAG, body = "", root_tag = SHOW_TAG):
    tag_list += [(top_tag, {}), (root_tag, {ID_TAG : "%s-0" % os.getpid()})]

    xml_request = body
    for tag in tag_list:
        name, value = tag
        xml_request = wrap_with_tag(name, xml_request, value)

    return xml_request


def get_xml_request_from_args_and_tags(arg_table, tag_list, top_tag = STATS_TAG, body = "", root_tag = SHOW_TAG):
    attrib = { ID_TAG : "%s-0" % os.getpid() }
    server_ip = arg_table.get(SERVER_TAG, '')
    if ',' in server_ip:
        server_ip = '' 
    attrib[SERVER_TAG] = server_ip

    process_name = arg_table.get(PROCESS_TAG, '')
    if ',' in process_name or ':' in process_name:
        process_name = '' 
    attrib[PROCESS_TAG] = process_name

    tag_list += [(top_tag, {}), (root_tag, attrib)]

    xml_request = body
    for tag in tag_list:
        name, value = tag
        xml_request = wrap_with_tag(name, xml_request, value)

    return xml_request


def get_sorted_ip_list(ip_list):
    if not ip_list:
        return ip_list

    server_ip_list = []
    for server_ip in ip_list:
        server_tuple = map(int, server_ip.split('.'))
        server_ip_list.append((server_tuple, server_ip))

    server_ip_list.sort()
    server_ip_list = [ server_ip for (server_tuple, server_ip) in server_ip_list ]
    return server_ip_list


def number_format(num, places=0):
   """Format a number with grouped thousands and given decimal places"""

   places = max(0,places)
   tmp = "%.*f" % (places, num)
   point = tmp.find(".")
   integer = (point == -1) and tmp or tmp[:point]
   decimal = (point != -1) and tmp[point:] or ""

   count =  0
   formatted = []
   for i in range(len(integer), 0, -1):
       count += 1
       formatted.append(integer[i - 1])
       if count % 3 == 0 and i - 1:
           formatted.append(",")

   integer = "".join(formatted[::-1])
   return integer+decimal 

   
def get_formatted_int_value(dict, tag):
    value  = get_data_from_tag(dict[tag])
    value = number_format(int(value))
    return value

def print_formatted_int_value(dict, tag):   
    print '<td>', get_formatted_int_value(dict, tag), '</td>'

def get_byte_sizes_for_integer(number):
    number = float(number)
    suffix_list = [ "", "K", "M", "G", "T" ]
    for index, suffix in enumerate(suffix_list):
        if number < 1024.0:
            return "%.2f%s" % (number, suffix)
        number = number / 1024.0

    return str(-1)


def get_byte_sizes(dict, tag):
    return get_byte_sizes_for_integer(get_data_from_tag(dict[tag]))


def get_formatted_float_value(dict, tag):
    value  = get_data_from_tag(dict[tag])
    value = number_format(float(value), 2)
    return value


def print_formatted_float_value(dict, tag):
    print '<td>', get_formatted_float_value(dict, tag), '</td>'
    

def print_data_from_tag_and_dict(tag, dict):
    print '<td>'
    if tag in dict:
        print get_data_from_tag(dict[tag])
    else:
        print '0'
    print '</td>'


def print_mma_from_tag_and_dict(tag, dict):
    if tag in dict:
        min, max, avg = eval(get_data_from_tag(dict[tag]))
        print '<tr class="row_type_1"><th>', tag,'</th></td>', '<td>', min, '</td>', '<td>', max, '</td>', '<td>', avg, '</td></tr>'


def prefix_zero(num):
    if num < 10:
        str = "0%d" % num
    else:
        str = "%d" % num
    return str


def print_select_range(name, max_range, value, func = "changetimer"):
    option_str = ''
    for i in range(max_range):
        prefix = prefix_zero(i + 1)
        select_attr = ''
        if i + 1 == value:
            select_attr = ' selected="yes" '
        option_str += '<option value="%s"%s>  %s' % (prefix, select_attr, prefix)
    print '<select name="%s" onfocus="%s();">%s</select>&nbsp;' % (name, func, option_str)


def print_monitor_time():
    remote_ip = cgi.escape(os.environ["REMOTE_ADDR"])
    monitor_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    print '<h2><table border="0" width="100%">'
    print '<tr>'
    print '<td align="left">'
    print ""
    print '</td>'
    print '<td align="right"> %s&nbsp;&nbsp;&nbsp;</td>' % (monitor_time)
    print '</tr>'
    print '</table></h2>'


'''
returns a table {server_ip: {process_name: { process_value: {attr_name: attr_val}}}}
'''
def get_server_process_table(logger, xml_file_name, process_list, attr_list, deep = False):
    if not os.access(xml_file_name, os.F_OK|os.R_OK):
        logger.error('Error in accessing config file: %s' %(xml_file_name))
        return {}

    process_table = {}

    xml_data = open(xml_file_name).read()
    doc = get_xml_data(xml_data, logger)

    for server_node in doc.getElementsByTagName(SERVER_TAG):
        server_ip    = str(server_node.getAttribute(VALUE_TAG))
        server_table = None
        for process_name in process_list:
            proc_table = None
            for proc_node  in server_node.getElementsByTagName(process_name):
                proc_value = str(proc_node.getAttribute(VALUE_TAG))
                attr_dict  = get_child_dict_for_tag(proc_node)

                attr_table = None
                if not attr_list:
                    new_attr_list = ['']
                    item_list = [('', None)]
                else:
                    new_attr_list = attr_list
                    item_list = attr_dict.iteritems()

                for attr_name, attr_val in item_list:
                    if attr_name not in new_attr_list:
                        if deep:
                            attr_val = None
                            for new_attr_name in new_attr_list:
                                attr_node_list = proc_node.getElementsByTagName(new_attr_name)
                                if attr_node_list:
                                    attr_val = attr_node_list[0]
                                    break
                            if not attr_val:
                                continue
                        else:    
                            continue

                    if server_table is None:
                        server_table = process_table.setdefault(server_ip, {})
                    if proc_table is None:
                        proc_table = server_table.setdefault(process_name, {})
                    if attr_table is None:
                        attr_table = proc_table.setdefault(proc_value, {})

                    if attr_val:
                        attr_table[str(attr_name)] = str(attr_val.getAttribute(VALUE_TAG))
                    else:
                        attr_table[str(attr_name)] = ''

    return process_table


def parser_server_and_process_option(sysd_ip, args_table):
    server_str = args_table[SERVER_TAG]
    process_str = args_table[PROCESS_TAG]

    process_name = process_str
    if not process_name.startswith(ALL_PROCESSES):
        return server_str, process_name

    process_list = process_name.split('_')
    if len(process_list) <= 1:
        return server_str, process_name

    process_name = process_list[1] 
    if sysd_ip == SYSD_IP and process_name == 'vtvsysd':
        process_name = process_list[0]
        args_table[PROCESS_TAG] = process_name
        return server_str, process_name

    text = open('../html/js/vtv_menu.js').read()
    menu_list = MENU_ARGS_REGEX.findall(text)
    menu_table = {}
    for menu_process, menu_module, menu_cmd, menu_sub_cmd in menu_list:
        if menu_process == menu_module:
            menu_module = ''
        menu_table[(menu_process, menu_cmd, menu_sub_cmd)] = menu_module 

    attr_list = []
    cmd = args_table.get('cmd', '')
    sub_cmd = args_table.get('sub_cmd', '')
    menu_module = menu_table.get((process_name, cmd, sub_cmd), '')
    if menu_module:
        attr_list = [menu_module]

    xml_file_name = CONFIG_FILE_TO_PARSE
    if sysd_ip != SYSD_IP:
        xml_file_name = os.path.join(VTV_ALARMS_DATA_DIR, '%s/%s' % (sysd_ip, os.path.basename(CONFIG_FILE_TO_PARSE)))

    deep = False
    if attr_list:
        deep = True
    attr_dict = get_server_process_table(None, xml_file_name, [process_name], attr_list, deep)

    if server_str == ALL_SERVERS or server_str == ALL_TAG:
        server_list = attr_dict.keys()
    else:
        server_list = server_str.split(',')

    new_server_list = []
    new_process_list = []
    for server_ip in server_list:
        if server_ip not in attr_dict:
            continue
        server_table = attr_dict[server_ip]
        if process_name not in server_table:
            continue
        new_server_list.append(server_ip)
        server_process_list = []
        for instance_name in server_table[process_name]:
            separator = ''
            if instance_name:
                separator = '_'
            server_process_list.append('%s%s%s' % (process_name, separator, instance_name))
        new_process_list.append(':'.join(server_process_list))
            
    server_name = ','.join(new_server_list)
    process_name = ','.join(new_process_list)

    args_table[SERVER_TAG] = server_name
    args_table[PROCESS_TAG] = process_name

    return server_name, process_name


JINJA_FILE_NAME = 'vtv_ems.jinja'
def generate_html_from_jinja(parameters):
    jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
    html = jinja_environment.get_template(JINJA_FILE_NAME).render(parameters = parameters)
    print html


