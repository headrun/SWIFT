#!/usr/bin/env python

################################################################################
#$Id: generate_datagen_failures.py,v 1.5 2016/02/03 08:25:28 headrun Exp $
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
ERROR_PATTERN = 'GROUP: %s'
SE_PATTERN    = ERROR_PATTERN % 'SEED'
CO_PATTERN    = ERROR_PATTERN % 'CONTENT'
SP_PATTERN    = ERROR_PATTERN % 'SPORTS'
TOU_GID       = 'TOURNAMENTS DB GID: (.*?):'
GID_PATTERN   = 'GID: (.*?):'
GOT           = 'GOT: (.*?) NEED:'
NEED          = 'NEED: (.*?) FLAG:'
SUITE_ID      = 'SUITE: (.*?) GID:'
TEST_ID       = 'ID: (.*?) SUITE:'
FIELD         = 'FIELD: (.*?) GOT:'

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
        self.log_ip         = "10.28.218.80"
        self.db_ip          = "10.28.218.81"
        self.gid_ip         = "10.28.218.81"
        self.logger         = initialize_timed_rotating_logger('datagen_errors.log')
        self.conn           = MySQLdb.connect(user="veveo", passwd="veveo123", db="SPORTSDB", host=self.db_ip)
        self.cursor         = self.conn.cursor()
        self.gid_merge_con  = MySQLdb.connect(user="veveo", passwd="veveo123", db="GUIDMERGE", host=self.gid_ip)
        self.gid_merge_cursor = self.gid_merge_con.cursor()

        self.suite_list     = ['PLAYERS TOP', 'PLAYERS TENNIS', 'PLAYERS SOCCER', 'PLAYERS PRO', 'PLAYERS CHESS', 'SPORTS TYPE', 'STADIUMS SPORTS', 'TEAMS CRICKET', 'TEAMS HOCKEY', 'TEAMS NCAA', 'TEAMS PRO', 'TEAMS SOCCER', 'TOURNAMENTS DB', 'TOURNAMENTS GROUP']
    def send_mail(self, text):
        subject    = 'Datagen Failures'
        server     = 'localhost'
        sender     = 'sports@headrun.com'
        #receivers  = ['akram@notemonk.com']
        receivers  = ['sports@headrun.com']
        #receivers.extend(['Raman.Arunachalam@tivo.com', 'Dheeraj.Mukka@tivo.com', 'Gurdeep.Mittal@tivo.com', 'Jagadeswar.Mettupalli@tivo.com', 'Binitha.Sobha@tivo.com', 'swathi@headrun.com', 'Vinay.Kumar@tivo.com', 'VenkataBabji.Perambattu@tivo.com'])
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def clean(self):
        self.conn.close()
        self.gid_merge_con.close()

    def get_title(self, wiki_gid='', result_type=''):
        if wiki_gid and '<>' in wiki_gid[0]:
            wiki_gids = wiki_gid[0].replace('#', '').split('<>')
            if 'WIKI' in wiki_gids[0]:
                
                self.gid_merge_cursor.execute("select exposed_gid, child_gid from sports_wiki_merge where exposed_gid in (%s)" % (', '.join('"' + item + '"' for item in wiki_gids)))
                team_gids = self.gid_merge_cursor.fetchall()
                found_wiki = [gid[0] for gid in team_gids]
                missing_wiki_gids = set(wiki_gids) - set(found_wiki)
                wiki_map = {value: key for key, value in team_gids}
                team_gids = [gid[1] for gid in team_gids]
                if not team_gids:
                    return wiki_gids[0]
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
            self.gid_merge_cursor.execute("select child_gid from sports_wiki_merge where exposed_gid = %s", (wiki_gid[0], ))
            team_gid = self.gid_merge_cursor.fetchone()
            if not team_gid:
                return wiki_gid[0]
            query = "select title from sports_tournaments where gid = %s"
            if "TEAM" in team_gid[0] or "PL" in team_gid[0]:
                query = "select title from sports_participants where gid = %s"
            if "STAD" in team_gid[0]:
                query = "select title from sports_stadiums where gid = %s"
            if "SPORT" in team_gid[0]:
                query = "select title from sports_types where gid = %s"
            self.cursor.execute(query, (team_gid[0], ))
            team_title = self.cursor.fetchone()
            if team_title:
                team_title = team_title[0]
                return team_title + ' {%s}' % wiki_gid[0]
        elif wiki_gid and "TOU" in wiki_gid[0]:
            self.cursor.execute("select title from sports_tournaments where gid = %s", (wiki_gid[0], ))
            team_title = self.cursor.fetchone()[0]
            return team_title + ' {%s}' % wiki_gid[0]
        elif wiki_gid and "STAD" in wiki_gid[0]:
            self.cursor.execute("select title from sports_stadiums where gid = %s", (wiki_gid[0], ))
            team_title = self.cursor.fetchone()[0]
            return team_title + ' {%s}' % wiki_gid[0]
        elif wiki_gid and ("PL" in wiki_gid[0] or "TEAM" in wiki_gid[0]):
            self.cursor.execute("select title from sports_participants where gid = %s", (wiki_gid[0], ))
            team_title = self.cursor.fetchone()[0]
            return team_title + ' {%s}' % wiki_gid[0]
            
        elif wiki_gid and "SPORT" in wiki_gid[0]:
            self.cursor.execute("select title from sports_types where gid = %s", (wiki_gid[0].split('{')[-1].replace('}', '').strip(), ))
            #import pdb;pdb.set_trace()
            try:
                team_title = self.cursor.fetchone()[0]
            except:
                team_title = ''
            return team_title + ' {%s}' % wiki_gid[0].split('{')[-1].replace('}', '').strip()


        elif wiki_gid:
            return wiki_gid[0]
        else:
            return ''

    def copy_log_file(self):
        check_dir()
        status = ssh_utils.scp('veveo123', 'veveo@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.log_ip, CUR_DATE), '/home/veveo/SPORTS/sports_spiders/monitoring_mails/datagen_test')
        print 'veveo@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' %(self.log_ip, CUR_DATE)
        if status:
            status = ssh_utils.scp('veveo123', 'veveo@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.log_ip, YESTERDAY), '/home/veveo/SPORTS/sports_spiders/monitoring_mails/datagen_test')
        print 'veveo@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.log_ip, YESTERDAY)

    def get_failure_cases(self):
        text = ''
        sp_failures  = set()
        co_failures  = set()
        se_failures  = set()
        oth_failures = set()
        os.chdir(DATAGEN_DIR)
        datagen_file = max(glob.iglob('*.log'), key=os.path.getctime)
        #datagen_file = 'datagen_test_20160921T020001.log'
        print datagen_file

        for data in open(datagen_file):
            if ('ERROR: GROUP: SEED ID' not in data) and ('ERROR: GROUP: SPORTS ID' not in data) and ('ERROR: GROUP: CONTENT' not in data):
                continue

            field = re.findall(FIELD, data)[0]
            suite_name = ''
            suite_name = re.findall(SUITE_ID, data)

            if suite_name:
                suite_name = suite_name[0]

            if CO_PATTERN in data and suite_name not in self.suite_list:
                continue

            if SE_PATTERN in data and suite_name not in self.suite_list:
                continue

            if SP_PATTERN in data:
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

                sp_failures.add((team_title, player_title, needed_titles, suite_name + ': ' + test_id, field))
                  
            elif CO_PATTERN in data:
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

                co_failures.add((team_title, player_title, needed_titles, suite_name + ': ' + test_id, field))

            elif SE_PATTERN in data:
                import pdb;pdb.set_trace()
                try:
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

                    se_failures.add((team_title, player_title, needed_titles, suite_name + ': ' + test_id, field))
                except:
                    pass
            else:
                try:
                    suite_name = re.findall(SUITE_ID, data)
                    test_id = re.findall(TEST_ID, data)[0]
                    if not suite_name:
                        suite_name = ''
                    else:
                        suite_name = suite_name[0]
                    stadium_wiki = re.findall('SPORTS GID: (.*?):', data)
                    stadium_title = self.get_title(stadium_wiki, result_type='stadium')

                    if not stadium_wiki:
                        wiki_gid = re.findall(GID_PATTERN, data)
                        stadium_title = self.get_title(wiki_gid)
                    else:
                        stadium_wiki = stadium_wiki[0]

                    got = re.findall(GOT, data)
                    if got: got  = '<>'.join(got)
                    need = re.findall(NEED, data)
                    if need: need = '<>'.join(need)
                    oth_failures.add((stadium_wiki, got, need, suite_name + ': ' + test_id, field))

                except:
                    pass

        text += "Report Log File: <b>%s</b>" % datagen_file
        if sp_failures:
            text += get_table_data('Sports Failure (Total Count: %s)' %(str(len(sp_failures))), ['Title', 'Got', 'Need', 'Suite Name: Test ID', 'Field'], [list(x) for x in set(tuple(x) for x in sp_failures)])
        if co_failures:
            text += get_table_data('Content Failure (Total Count: %s)' %(str(len(co_failures))), ['Title', 'Got', 'Need', 'Suite Name: Test ID', 'Field'], [list(x) for x in set(tuple(x) for x in co_failures)])
        if se_failures:
            text += get_table_data('Seed Failure (Total Count: %s)' %(str(len(se_failures))), ['Title', 'Got', 'Need', 'Suite Name: Test ID', 'Field'], [list(x) for x in set(tuple(x) for x in se_failures)])

        if oth_failures:
            text += get_table_data('SPORTS Group Faliure (Total Count: %s)' % (str(len(oth_failures))), ['Gid', 'Got', 'Expected', 'Suite Name: Test ID', 'Field'], [list(x) for x in set(tuple(x) for x in oth_failures)])

        if se_failures or co_failures or oth_failures or sp_failures:
            #import pdb;pdb.set_trace()
            #self.send_mail(text)
            s = ''
            

    def run_main(self):
        #self.copy_log_file()
        self.get_failure_cases()
        self.clean()


if __name__ == '__main__':

    obj = DatagenFailures()
    obj.run_main()
