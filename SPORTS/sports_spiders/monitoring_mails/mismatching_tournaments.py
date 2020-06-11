#!/usr/bin/env python
import datetime
import os
from vtv_utils import vtv_send_html_mail_2
from vtv_task import VtvTask, vtv_task_main

DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

class MissingTournaments(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.today         = str(datetime.datetime.now().date())
        self.stats_dir     = '/home/veveo/SPORTS/sports_spiders/SPORTS_STATS_DIR'
        self.file_         = open(os.path.join(self.stats_dir, "missing_tournaments_list_%s" % self.today), 'r+')
        self.text          = ''
        self.value_list    = []     
        self.db_ip      = "10.28.218.81"
        self.db_name    = "WEBSOURCEDB"


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
        subject    = 'Mismatching Tournament List'
        server     = 'localhost'
        sender     = 'headrun@veveo.net'
        #receivers = ['Raman.Arunachalam@tivo.com','Vineet.Agarwal@tivo.com','sports@headrun.com']
        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def missing_tournaments(self):
        self.open_cursor(self.db_ip, self.db_name)
        for data in self.file_:
            data = eval(data.strip())
            for key, value in data.iteritems():
                headers = ('Game', 'Tournament Name')
                if value:
                    if [value[-1]] not in self.value_list:
                        self.value_list.append(value)
                        #self.text += self.get_html_table(key, headers, value)
                        game = value[-1][0]
                        title = value[-1][1]
    
                        insert_qry = 'insert into sports_tournaments_mismatching(title, game, status, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                        values = (title, game, 0)
                        self.cursor.execute(insert_qry, values)
        '''if self.text:
            self.send_mail(self.text)'''

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.', 'mismatching_tournaments*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.missing_tournaments()


if __name__ == '__main__':
    vtv_task_main(MissingTournaments)

