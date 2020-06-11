import sys
import os
from ssh_utils import scp
from datetime import date
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'


class SportsLogsInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.location_dict    = {}
        self.text          = ''
        self.machine_ip = '10.28.216.45'
        self.logs_path = '/home/veveo/SPORTS/sports_spiders/sports_spiders/spiders/'
        self.log_pat = str(date.today()) + "_logfiles.log"

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

    def copy_latest_file(self):
        mc_path  = "%s%s" % (self.logs_path, self.log_pat)
        source   = '%s@%s:%s' % ("veveo", self.machine_ip, mc_path)
        status   = scp("veveo123", source, '.')
        if status != 0:
             self.logger.info('Failed to copy the file: %s:%s' % (self.machine_ip, self.log_pat))
             sys.exit()


    def send_mail(self, text):
        subject    = 'Sports Logs Info'
        server    = 'localhost'
        sender    = 'sports@headrun.com'
        receivers = ['bibeejan@headrun.com', 'sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def get_logsinfo(self):
        date_ = date.today()
        file_name = str(date_) + "_logfiles.log"
        data = os.path.isfile(file_name)
        if data == True:
            with open(file_name, 'r') as file_:
                uniqlines = set(file_.readlines())
                for record in uniqlines:
                    if "WARNING" not in record:
                        continue
                    response, status, reference, source = record.split(' - ')
                    headers = ('Warning', 'Status', 'Reference', 'Source')
                    self.location_dict.setdefault('Sports Logs Info', []).append([response.replace('WARNING:root:', ''), status, reference, source])
                for key, value in self.location_dict.iteritems():
                    self.text += self.get_html_table(key, headers, value)
    
    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','sports_logs_report*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

          
    def run_main(self):
        self.copy_latest_file()
        self.get_logsinfo()
        if self.text:
            self.send_mail(self.text)


if __name__ == '__main__':
    vtv_task_main(SportsLogsInfo)
    sys.exit( 0 )


