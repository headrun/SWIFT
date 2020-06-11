import os
import sys
import time
import MySQLdb
import traceback
import datetime
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

class SksStatsScript(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.set1_dict     = {}
        self.set2_dict     = {}
        self.today         = datetime.datetime.now().date()
        self.text          = ''
        self.sks_dir       = 'SKS_DIR'
        self.step_back     = '..'
        self.file_path     = os.path.join(os.getcwd(), self.step_back, self.sks_dir)
        self.out_file      = open('%s/missed_participants_sks_%s' % (self.file_path, self.today), 'r').readlines()
        self.out_file      = open('/home/veveo/sports_spiders/SKS_DIR/missed_participants_sks_2015-11-03', 'r').readlines()

    def get_html_table(self, title, headers, table_body):
        title = 'Please check the below missed sks for Team/Player'
        table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
        for header in headers:
            table_data += '<th>%s</th>' % header
        table_data += '</tr>'

        for data in table_body:
            table_data += '<tr>'
            for row in data:
                table_data += '<td>%s</td>' % (str(row))
            table_data += '</tr>'
        table_data += '</table>'

        return table_data


    def send_mail(self, text):
        subject    = 'Missed SKS Stats In Sports Spiders'
        server    = 'localhost'
        sender    = 'headrun@veveo.net'
        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    ### checking for newly added locations ###
    def get_details(self):
        try:
            headers = ('Spider/Game', 'Source', 'Source Key', 'Ref Url')
            for record in self.out_file:
                data = [rec.strip() for rec in record.split('<>')]

                if len(data) == 3:
                    data.insert(3, 'not available')
                    self.set1_dict.setdefault('Rosters', []).append(data)
                else:
                    self.set1_dict.setdefault('Rosters', []).append(data)

            for key, value in self.set1_dict.iteritems():
                self.text += self.get_html_table(key, headers, value)
        except:
            print traceback.format_exc()


    def run_main(self):
        self.get_details()
        if self.text:
            self.send_mail(self.text)

if __name__ == '__main__':
    vtv_task_main(SksStatsScript)
    sys.exit( 0 )

