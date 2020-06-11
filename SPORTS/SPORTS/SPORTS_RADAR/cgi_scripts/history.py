#!/usr/bin/python

import os, sys, time

sys.path.append('..')

from vtv_utils import get_compact_traceback, VTV_COMMAND_HISTORY_FILE, VTV_LOGIN_HISTORY_FILE
from system_config_xml_tags import VTVCLID_TAG
from globals import VTV_SUB_CMD_KEY


HISTORY_LIMIT = 20
LOGIN_LIMIT   = 20


class HistoryStats:
    def __init__(self):
         self.run_cnt             = 0
         self.success_cnt         = 0
         self.failure_cnt         = 0
         self.last_success_time   = '-'
         self.last_failure_time   = '-'

    def check_status(self, time_str, status):
        self.run_cnt += 1
        if status == 'Successful':
            if not self.success_cnt:
                self.last_success_time = time_str
            self.success_cnt += 1
        else:
            if not self.failure_cnt:
                self.last_failure_time = time_str
            self.failure_cnt += 1
            

class LoginStats:
    def __init__(self):
         self.run_cnt             = 0
         self.recent_start_time   = '-'
         self.first_start_time     = '-'


def get_commands_history():
    file_name = VTV_COMMAND_HISTORY_FILE
    
    if os.access(file_name,os.F_OK):
        command_str = '<command_history>'
        fp = file(file_name)
        lines = fp.readlines()
        lines.reverse()
        command_table = {}
        count = 1
        for line in lines:
            line = line.strip()
            if not line:
                continue
            fields = line.split('::')
            time_str, cmd, status = fields[:3]
            if cmd.find(VTVCLID_TAG) != -1:
                cmd = 'config'
            title = cmd.split()[0]
            if title in command_table: 
                obj = command_table[title]
            else:
                obj = HistoryStats()
                command_table[title] = obj
            obj.check_status(time_str, status)

            if count <= HISTORY_LIMIT:
                command_str += "<history><count>%d</count><time>%s</time><cmd>%s</cmd><status>%s</status></history>" % (count, time_str, cmd, status)
            count += 1
        command_str += '</command_history>'

        summary_str = '<command_summary>'

        cmd_list = command_table.keys()
        cmd_list.sort()
        for cmd in cmd_list:
            obj = command_table[cmd]
            summary_str += "<summary><cmd>%s</cmd><runs>%s</runs><sc>%s</sc><lst>%s</lst><fc>%s</fc><lft>%s</lft></summary>" % (cmd, obj.run_cnt, obj.success_cnt, obj.last_success_time, obj.failure_cnt, obj.last_failure_time)
            
        summary_str += '</command_summary>'

        monitor_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print '<stats T="%s" id="%s-0">' % (monitor_time, os.getpid())
        print summary_str
        print command_str
        print '</stats>'
    else:
        print "<h2>The Command History file doesn't exist</h2>"
        

def get_login_history():
    file_name = VTV_LOGIN_HISTORY_FILE
    history_str = '<login_history>'

    login_table = {}

    host_list = []

    lines = file(file_name).readlines()
    lines.reverse()
    count = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue

        words = line.split('\t')
        remote_host, start_time, end_time = words[:3]

        if count <= LOGIN_LIMIT:
            history_str += "<history><host>%s</host><st>%s</st><et>%s</et></history>" % (remote_host, start_time, end_time)

        if remote_host in login_table:
            obj = login_table[remote_host]
            obj.first_start_time = start_time
        else:
            obj = LoginStats()
            login_table[remote_host] = obj
            obj.recent_start_time = start_time
            obj.first_start_time = start_time
            host_list.append((start_time, remote_host))
        obj.run_cnt += 1
        count += 1

    history_str += '</login_history>'

    summary_str = '<login_summary>'

    host_list.sort()
    host_list.reverse()
    for start_time, host in host_list:
        obj = login_table[host]
        summary_str += "<summary><host>%s</host><runs>%s</runs><rst>%s</rst><fst>%s</fst></summary>" % (host, obj.run_cnt, obj.recent_start_time, obj.first_start_time)

    summary_str += '</login_summary>'

    monitor_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print '<stats T="%s" id="%s-0">' % (monitor_time, os.getpid())
    print summary_str
    print history_str
    print '</stats>'


def main(args_table):
    request = args_table.get(VTV_SUB_CMD_KEY, '')
    if request == 'commands':
        get_commands_history()
    elif request == 'logins':
        get_login_history()


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
