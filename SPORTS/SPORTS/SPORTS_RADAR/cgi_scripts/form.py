#!/usr/bin/python

import sys, os, time
import cgi, fcntl, errno
from xml.sax.saxutils import escape as xml_escape

sys.path.append('..')

if os.environ.has_key('VTV_ROOT_DIR'):
    VTV_ROOT_DIR  = os.environ['VTV_ROOT_DIR']
else:
    VTV_ROOT_DIR  = os.path.realpath("../../..")
    os.environ['VTV_ROOT_DIR'] = VTV_ROOT_DIR

from vtv_utils import get_compact_traceback, get_pid_from_pid_file, remove_file, VTV_LOGIN_HISTORY_FILE

from globals import PROCESS_TAG, SYSD_IP, VTV_ARGS_KEY, VTV_CMD_KEY, \
                    parser_server_and_process_option

from httpd_ui import httpd_send_request


VTV_CGI_REMOTE_IP_FILE_FORMAT = "client_%s"


def get_cgi_remote_ip_file(ip):
    return VTV_CGI_REMOTE_IP_FILE_FORMAT % ip


def create_cgi_remote_ip_file(cgi_pid_file_name):
    """
    if os.path.exists(cgi_pid_file_name):
        sys.exit(1)
    """
    try:
        file(cgi_pid_file_name, 'w').write("%s" % os.getpid())
    except:
        return


def check_cgi_remote_ip_file(cgi_pid_file_name):
    pid = get_pid_from_pid_file(cgi_pid_file_name)
    return pid != str(os.getpid())


def write_history(remote_ip, start_time):
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    history_str = "%s\t%s\t%s\t%s\n" % (remote_ip, start_time, end_time, os.getpid())

    history_file = file(VTV_LOGIN_HISTORY_FILE, 'a')
    fcntl.flock(history_file.fileno(), fcntl.LOCK_EX)
    history_file.write(history_str)
    fcntl.flock(history_file.fileno(), fcntl.LOCK_UN)
    history_file.close()


def main():
    print "Content-Type: text/html\n"

    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    remote_ip = cgi.escape(os.environ["REMOTE_ADDR"])
    cgi_pid_file_name = get_cgi_remote_ip_file(remote_ip)

    cwd = os.getcwd()
    create_cgi_remote_ip_file(cgi_pid_file_name)

    form = cgi.FieldStorage()
    items = form.keys()

    try:
        sysd_ip = SYSD_IP
        if form.has_key('monitor'):
            sysd_ip = form['monitor'].value
        if form.has_key(VTV_CMD_KEY):
            args_table = {}
            cmd = form[VTV_CMD_KEY].value

            length = 0
            for key in form.keys():
                if key.startswith("arg"):
                    length += 1
            arg_list = []
            if length > 0: 
                arg_list = [''] * length 
 
            for key in form.keys():
                if key.startswith("arg"):
                    i = int(key[3:])
                    arg_list[i] = form[key].value
                else:
                    args_table[key] = form[key].value
 
            args_table[VTV_ARGS_KEY] = arg_list

            if cmd == 'login': 
                m = __import__(cmd, globals(),  locals(), [])
                m.main(args_table)
                write_history(remote_ip, start_time)
                sys.exit(0)

            if PROCESS_TAG in args_table:
                server_name, process_name = parser_server_and_process_option(sysd_ip, args_table)

            httpd_send_request(sysd_ip, cmd, args_table)
        else:
            print '<strong><center><font color="red">User has not logged in or invalid URL is given.</font></center></strong>'

        cwd = os.getcwd()
        if os.path.exists(cgi_pid_file_name):
            remove_file(cgi_pid_file_name, None)

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception, e:
        print "<h2>parse error: %s items: %s</h2>" % (xml_escape(get_compact_traceback()), xml_escape(str(items)))
        return 1


if __name__ == '__main__':
    main()

