'''#!/usr/bin/env python

################################################################################
#$Id: generate_datagen_failures.py,v 1.1 2016/03/23 07:12:22 headrun Exp $
################################################################################

import os, re
import glob
import ssh_utils
import MySQLdb
from datetime import datetime, timedelta
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2


CUR_DATE      = str(datetime.utcnow().date())
YESTERDAY     = str((datetime.utcnow() - timedelta(days=1)).date())
CUR_DIR       = os.getcwd()
DATAGEN_DIR   = os.path.join(CUR_DIR, 'datagen_test')
ERROR_PATTERN = 'FIELD: %s'
TE_PATTERN    = ERROR_PATTERN % 'Te'
TO_PATTERN    = ERROR_PATTERN % 'To'
TOU_GID       = 'TOURNAMENTS DB GID: (.*?):'
GID_PATTERN   = 'GID: (.*?):'
GOT           = 'GOT: (.*?) '
NEED          = 'NEED: (.*?) '
SUITE_ID      = 'SUITE: (.*?) GID:'
TEST_ID       = 'ID: (.*?) SUITE:'

def check_dir(path=DATAGEN_DIR):
    if not os.path.isdir(path):
        os.mkdir(path)

def get_table_data(title, headers, body):
    text = '<br /><br /><b>%s</b><br /><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        text += '<th>%s</th>' % header
    text += '</tr>'

    for data in body:
        text += '<tr>'
        for d in data:
            text += '<td>%s</td>' % d
        text += '</tr>'
    text += '</table>'
    return text


class DatagenFailures:

    def __init__(self):
        self.failed_scripts = []
        self.scripts_dict   = {}
        self.logger         = initialize_timed_rotating_logger('datagen_errors.log')
        self.conn           = MySQLdb.connect(user="root", db="SPORTSDB", host="10.4.18.183")
        self.cursor         = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'Datagen Failures'
        server     = '10.4.1.112'
        sender     = 'sudheer@headrun.com'
        receivers = ['sudheer@headrun.com']
        receivers  = ['sports@headrun.com', 'sudheer@headrun.com']
        receivers.extend(['raman@veveo.net', 'harshad@veveo.net', 'arun.n@veveo.net'])
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def clean(self):
        self.conn.close()

    def get_title(self, wiki_gid='', result_type='team'):
        if wiki_gid and '<>' in wiki_gid[0]:
            wiki_gids = wiki_gid[0].split('<>')
            if 'WIKI' in wiki_gids[0]:
                self.cursor.execute("select wiki_gid, sports_gid from sports_wiki_merge where wiki_gid in (%s)" % (', '.join('"' + item + '"' for item in wiki_gids)))
                team_gids =  self.cursor.fetchall()
                found_wiki = [gid[0] for gid in team_gids]
                missing_wiki_gids = set(wiki_gids) - set(found_wiki)
                wiki_map = {value: key for key, value in team_gids}
                team_gids = [gid[1] for gid in team_gids]
                self.cursor.execute("select gid, title from sports_tournaments where gid in (%s)" % (', '.join('"' + item + '"' for item in team_gids)))
                player_titles = self.cursor.fetchall()
                player_titles = [p[1] + ' {%s}' % wiki_map.get(p[0]) for p in player_titles]
                if missing_wiki_gids:
                    player_titles.extend(missing_wiki_gids)
                player_titles = '<>'.join(player_titles)
                return player_titles
            elif wiki_gids:
                return wiki_gids[0]
            else:
                return ''

        if wiki_gid and 'WIKI' in wiki_gid[0]:
            self.cursor.execute("select sports_gid from sports_wiki_merge where wiki_gid = %s", (wiki_gid[0], ))
            team_gid = self.cursor.fetchone()
            if not team_gid:
                return wiki_gid[0]
            query = "select title from sports_tournaments where gid = %s"
            if result_type == "team":
                query = "select title from sports_participants where gid = %s"
            self.cursor.execute(query, (team_gid[0], ))
            team_title = self.cursor.fetchone()[0]
            return team_title + ' {%s}' % wiki_gid[0]
        elif wiki_gid and "TOU" in wiki_gid[0]:
            self.cursor.execute("select title from sports_tournaments where gid = %s", (wiki_gid[0], ))
            team_title = self.cursor.fetchone()[0]
            return team_title + ' {%s}' % wiki_gid[0]
        elif wiki_gid:
            return wiki_gid[0]
        else:
            return ''

    def copy_log_file(self):
        check_dir()
        status = ssh_utils.scp('veveo123', 'veveo@10.4.18.155:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % YESTERDAY, '/home/veveo/sports_spiders/monitoring_mails/datagen_test')
        status = ssh_utils.scp('veveo123', 'veveo@10.4.18.155:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % CUR_DATE, '/home/veveo/sports_spiders/monitoring_mails/datagen_test')

    def get_failure_cases(self):
        text = ''
        to_failures = set()
        te_failures = set()
        os.chdir(DATAGEN_DIR)
        datagen_file = max(glob.iglob('*.log'), key=os.path.getctime)
        print datagen_file
        for data in open(datagen_file):
            if TE_PATTERN in data:
                suite_name = re.findall(SUITE_ID, data)
                test_id = re.findall(TEST_ID, data)[0]
                if not suite_name:
                    suite_name = ''
                else:
                    suite_name = suite_name[0]
                wiki_gid = re.findall(GID_PATTERN, data)
                team_title = self.get_title(wiki_gid)

                got = re.findall(GOT, data)
                player_title = self.get_title(got)

                need = re.findall(NEED, data)
                needed_titles = self.get_title(need)

                te_failures.add((team_title, player_title, needed_titles, suite_name + ': ' + test_id))

            if TO_PATTERN in data:
                suite_name = re.findall(SUITE_ID, data)
                test_id = re.findall(TEST_ID, data)[0]
                if not suite_name:
                    suite_name = ''
                else:
                    suite_name = suite_name[0]
                tou_wiki = re.findall(TOU_GID, data)
                team_title = self.get_title(tou_wiki, result_type="tournament")

                if not tou_wiki:
                    wiki_gid = re.findall(GID_PATTERN, data)
                    team_title = self.get_title(wiki_gid)

                got = re.findall(GOT, data)
                player_title = self.get_title(got, result_type="tournament")

                need = re.findall(NEED, data)
                needed_titles = self.get_title(need, result_type="tournament")

                to_failures.add((team_title, player_title, needed_titles, suite_name + ': ' + test_id))

        if to_failures:
            text += "Report Log File: <b>%s</b>" % datagen_file
            text += get_table_data('Tournaments Failure (Total Count: %s)' %(str(len(to_failures))), ['Team', 'Got Tournament', 'Expected Tournament', 'Suite Name: Test ID'], [list(x) for x in set(tuple(x) for x in to_failures)])
        if te_failures:
            text += get_table_data('Teams Failure (Total Count: %s)' %(str(len(te_failures))), ['Player Title', 'Got Team', 'Expected Team', 'Suite Name: Test ID'], [list(x) for x in set(tuple(x) for x in te_failures)])

        if text:
            self.send_mail(text)

    def run_main(self):
        self.copy_log_file()
        self.get_failure_cases()
        self.clean()


if __name__ == '__main__':

    obj = DatagenFailures()
    obj.run_main()'''
