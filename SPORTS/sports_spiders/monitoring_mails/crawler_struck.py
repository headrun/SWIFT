#!/usr/bin/env python
import sys
import optparse
import glob
import os
import traceback
import ConfigParser
from subprocess import Popen, PIPE
import commands
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2


class Memorycheck(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.process_ids_list = []
        self.long_run_processes = ''

    def make_table(self, rows):
        headers = ('S.No', 'Process-id', 'Processe-name', 'Process-time')
        table = '<br><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>'
        for header in headers:
            table += '<th>%s</th>' % (header)
        table += '</tr>%s</table>' % rows

        return table

    def generate_table_rows(self, records):
        table_body = '<tr>'
        for record in records:
            table_body += '<td>%s</td>' % record
        table_body += '</tr>'

        return table_body
 	
    def send_mail(self, text):
        subject    = '216.45 : SPORTS Processes which contains process time > 5 hours'
        server     = 'localhost'
        #sender     = 'sports@headrun.com'
        sender     = 'headrun@veveo.net'
        receivers  = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def start_process(self):
        process_dict = {}
        text = ''
        scrapy_cmd = 'pgrep -fl scrapy'
        scrapy_files = commands.getoutput(scrapy_cmd).split('\n')
        for _file in scrapy_files:
            if 'scrapy' in _file or 'python' in _file:
                process_id, process_name = _file.split(' ')[0], _file.split(' ')[1]
                process_dict[process_id] = process_name
        count = 0
        for key, value in process_dict.iteritems():
            try:
                new_cmd = 'ps -eo etime,pid,comm|grep -w %s'%key.strip()
                data = commands.getoutput(new_cmd)
                if data:
                    process_data = data.strip().split(' ')[0]
                    if '-' in process_data:
                        count = count + 1
                        table_list = [count, key.strip(), process_name, process_data]
                        self.long_run_processes += self.generate_table_rows(table_list)
                    elif process_data and len(process_data.split(':'))==5 and int(process_data.split(':')[0])>=5:
                        count = count + 1
                        table_list = [count, key.strip(), process_name, process_data]
                        self.long_run_processes += self.generate_table_rows(table_list)
            except:
                pass
        if self.long_run_processes:
            text = 'Hi Team,<br><br>Please verify the process state of below processes <br>'
            text += self.make_table(self.long_run_processes)
            text += '<br><font color="red"><b>Note : Please verify above processes as a high priority. </b></font><br>'
            self.send_mail(text)


if __name__ == "__main__":
    obj = vtv_task_main(Memorycheck)
    obj.start_process()
