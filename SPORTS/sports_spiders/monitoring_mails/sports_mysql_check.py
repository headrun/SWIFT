import sys
import os
import MySQLdb
from ssh_utils import scp
from datetime import date
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'


class SportsMysqlInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.location_dict = {}
        self.text          = ''
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def get_html_table(self, title, headers, table_body):
        table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
        for header in headers:
            table_data += '<th>%s</th>' % header
        table_data += '</tr>'

        for data in table_body:
            table_data += '<tr>'
            for index, row in enumerate(data):
                table_data += '<td>%s</td>' % (str(row))
            table_data += '</tr>'
        table_data += '</table>'

        return table_data


    def send_mail(self, text):
        subject    = 'SPORTSDB MySQL Check'
        server    = 'localhost'
        #sender    = 'sports@headrun.com'
        sender = 'noreply@headrun.com'
        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def get_logsinfo(self):
        date_ = date.today()
        query = 'SELECT count(*), DB, COMMAND, STATE, Time FROM information_schema.processlist WHERE db like "%SPORTS%" and time>200 group by COMMAND, STATE, DB'
        self.cursor.execute(query)
        records = self.cursor.fetchall() 
        for record in records:
            response, status, reference, source, time = record
            headers = ('Count', 'DB', 'Command', 'State', 'Time')
            self.location_dict.setdefault('Sports Mysql Queries', []).append([response, status, reference, source, time])
        for key, value in self.location_dict.iteritems():
            self.text += self.get_html_table(key, headers, value)
    
    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','sports_mysql_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

          
    def run_main(self):
        self.get_logsinfo()
        if self.text:
            self.send_mail(self.text)


if __name__ == '__main__':
    vtv_task_main(SportsMysqlInfo)
    sys.exit( 0 )


