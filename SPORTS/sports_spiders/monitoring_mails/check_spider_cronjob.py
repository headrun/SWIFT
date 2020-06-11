from shutil import copyfile
import xml.etree.ElementTree as ET
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2


allowed_paths = ['/home/veveo/sports_spiders']

def get_table_header(title, headers):
    table_header = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' %title
    for header in headers:
        table_header += '<th>%s</th>' %header
    table_header += '</tr>'
    return table_header

def get_table_body(removed_list):
    body = ''
    for data in removed_list:
        body += '<tr>'
        for d in data:
            body += '<td>%s</td>' %d
        body += '</tr>'
    body += '</table>'
    return body

class CheckSpiders:
    def __init__(self):
        self.wrong_paths = {}
        self.server      = '10.4.1.112'
        self.receivers   = ['sports@headrun.com']
        self.sender      = "headrun@veveo.net"
        self.logger      = initialize_timed_rotating_logger('cronjob_check.log')

    def collect_data(self):
        copyfile('/home/veveo/config/vtv.xml', './vtv.xml')
        tree = ET.parse('vtv.xml')
        for node in tree.iter('cronjob'):
            cronjob_name = node.get('value')
            command = node.find('command').get('value')
            disable_status = node.find('disable')
            disabled = False
            if disable_status:
                disable_status = disable_status.get('status', '')
                if disable_status == "inserted":
                    disabled = True
            if disabled:
                continue

            for path in allowed_paths:
                if path in command:
                    break
            else:
                self.wrong_paths[cronjob_name] = (cronjob_name, command)

        text = ''
        if self.wrong_paths:
            subject = "SPORTS spiders running from wrong paths !!!"
            headers = ('Cronjob Name', 'Run Path')
            text += get_table_header('Spiders not running in production path', headers)
            text += get_table_body(self.wrong_paths.values())
            vtv_send_html_mail_2(self.logger, self.server, self.sender, self.receivers, subject, '', text, '')

if __name__ == "__main__":
    obj = CheckSpiders()
    obj.collect_data()
