#!/usr/bin/env python

################################################################################
#$Id: scripts_validator.py,v 1.1 2016/03/23 07:19:22 headrun Exp $
################################################################################


import py_compile
import os
import re
from subprocess import Popen, PIPE
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

DIR           = os.path.dirname(os.path.realpath(__file__))
PATH, CUR_DIR = os.path.split(DIR)
SPIDERS       = 'spiders'
SPORTS_DIR    = os.path.join(DIR, CUR_DIR)
SPIDERS_DIR   = os.path.join(DIR, CUR_DIR, SPIDERS)
CHECK_DIRS    = [ DIR, SPORTS_DIR, SPIDERS_DIR ]


def get_table_header(title, headers):
    table_header = '<br /><br /><b>%s</b><br /><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        table_header += '<th>%s</th>' % header
    table_header += '</tr>'
    return table_header

def get_table_body(removed_list):
    body = ''
    for data in removed_list:
        body += '<tr>'
        for d in data:
            body += '<td>%s</td>' % d
        body += '</tr>'
    body += '</table>'
    return body

class ScriptValidator:

    def __init__(self):
        self.failed_scripts = []
        self.scripts_dict   = {}
        self.logger         = initialize_timed_rotating_logger('failed_scripts.log')

    def send_mail(self, text):
        subject    = 'Scripts stats in SPORTS production'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'
        receivers  = ['headrun@veveo.net']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def get_pylint_ratings(self, scripts):
        for script in scripts:
            cmd = ['pylint', script]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out = proc.communicate()
            rating = re.findall('Your code has been rated at (.*?) \(', out[0])
            if not rating:
                rating = ''
            else:
                rating = rating[0]
            patterns = ['.*Bad indentation.*',
                        'Unused import .*',
                        '.*Operator not followed by a space.*\n.*',
                        '.*Comma not followed by a space.*\n.*',
                        '.*Operator not preceded by a space.*\n.*',
                        '.*Unused variable.*']
            errors = []
            for pattern in patterns:
                matched_data = re.findall(pattern, out[0])
                errors.extend(matched_data)

            errors = '<br />'.join(errors)
            if rating:
                self.scripts_dict[script] = (script, rating, errors)
            else:
                self.scripts_dict[script] = (script, 'Failed with errors in script', errors)

    def check_files(self, scripts = []):
        for script in scripts:
            try:
                py_compile.compile(script, doraise=True)
            except:
                self.failed_scripts.append(script)

    def get_text(self):
        text = ''
        if self.failed_scripts:
            text += "<b>Scripts failed to compile</b><br />%s<br /><br />" % '<br />'.join([str(t) for t in self.failed_scripts])
        if self.scripts_dict:
            script_values = set()
            for data in self.scripts_dict.values():
                script_values.add(data)
            headers = ('Script Location', 'Pylint Rating', 'Errors')
            text += get_table_header('Pylint errors and ranking', headers)
            text += get_table_body(script_values)
        return text


if __name__ == '__main__':

    validator = ScriptValidator()
    for dirs in CHECK_DIRS:
        scripts = [ os.path.join(dirs, f)\
                    for f in os.listdir(dirs)\
                    if f.endswith('.py') and f != '__init__.py'
                  ]

        validator.check_files(scripts)
        validator.get_pylint_ratings(scripts)

    text = validator.get_text()
    validator.send_mail(text)
