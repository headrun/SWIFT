#!/usr/bin/env python

import MySQLdb
from vtv_task import VtvTask, vtv_task_main
import ssh_utils
import sys, traceback
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

SPORTS_MERGE_LIST_FILE = '/home/veveo/datagen/current/sports_merge_data/sports.to.wiki.guid_merge.list'

IP = '10.4.2.187'

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

class CheckDiff(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)

        self.args           = ''
        self.text           = ''
        self.merge_diff     = {}
        self.merge_list     = {}
        self.sports_db_dict = {}
        self.tou_data       = []
        self.team_data      = []
        self.pl_data        = []
        self.gr_data        = []
        self.sports_db_data = []
        self.logger         = initialize_timed_rotating_logger('wiki_failures.log')
        self.remote_conn    = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
        self.remote_cursor  = self.remote_conn.cursor()
        self.wiki_conn      = MySQLdb.connect(host=IP, user="root", db="WIKIDB")
        self.wiki_cursor    = self.wiki_conn.cursor()

        if len(sys.argv) > 1:
            self.args = sys.argv[1]

    def send_mail(self):
        subject    = 'Difference in sports.to.wiki.guid_merge.list count'
        server     = '10.4.1.112'
        sender     = 'sudheer@headrun.com'
        receivers = ['sudheer@headrun.com']
        #receivers  = ['sudheer@headrun.com', 'raman@veveo.net', 'jagadeswarm@veveo.net', 'vinay@veveo.net']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', self.text, '')

    def get_merge_file_data(self):
        status, process = ssh_utils.ssh_cmd_output(IP, self.vtv_username, self.vtv_password, 'cat %s' % SPORTS_MERGE_LIST_FILE)
        merge_list = process.before.strip().split()
        self.merge_list = {li.split('<>')[1]: li.split('<>')[0] for li in merge_list}

    def get_merge_db_data(self):
        query = 'select sports_gid, wiki_gid from sports_wiki_merge where action in (\'override\', \'check\')'
        self.remote_cursor.execute(query)
        db_data = self.remote_cursor.fetchall()
        self.sports_db_dict = {d[0]: d[1] for d in db_data}
        self.sports_db_data = [dat[0] for dat in db_data]

    def get_diff(self):
        diff_data = set(self.merge_list) - set(self.sports_db_data)
        self.merge_diff = {self.merge_list[d]: d for d in diff_data}

    def get_wiki_title(self, wiki_title):
        query = "select title from title_guid_map where gid = '%s'" % wiki_title
        self.wiki_cursor.execute(query)
        title = self.wiki_cursor.fetchone()
        if title:
            title = title[0]
        return '%s {%s}' % (title, wiki_title)

    def compare_title(self, title1, title2):
        if '{' in title1:
            title1 = title1.split('{')[0].strip()
        if '{' in title2:
            title2 = title2.split('{')[0].strip()
        if title1.lower() == title2.lower():
            return '<font color="green">OK</font>'
        return '<font color="red">NOT OK</font>'

    def get_file_differences(self):
        for key, value in self.merge_list.iteritems():
            if not self.sports_db_dict.get(key, ''):
                continue
            wiki_db_gid = self.sports_db_dict[key]
            if value != wiki_db_gid:
                wiki_file_title = self.get_wiki_title(value)
                wiki_db_title   = self.get_wiki_title(wiki_db_gid)
                sports_title    = self.get_query_result(key)
                status = self.compare_title(sports_title, wiki_file_title)
                values = (status, sports_title, wiki_file_title, wiki_db_title)
                self.append_data(sports_title, values)

        self.text += 'Difference between SPORTSDB.wiki_merge count and sports.to.wiki.guid_merge.list count'
        header = ['Status', 'GID', 'WIKI File', 'WIKI DB']
        if self.tou_data:
            self.tou_data = sorted(self.tou_data, key=lambda tup: tup[0])
            self.text += get_table_data('Tournament Differences' , header, self.tou_data)
        if self.team_data:
            self.team_data = sorted(self.team_data, key=lambda tup: tup[0])
            self.text += get_table_data('Team Differences' , header, self.team_data)
        if self.pl_data:
            self.pl_data = sorted(self.pl_data, key=lambda tup: tup[0])
            self.text += get_table_data('Player Differences' , header, self.pl_data)
        if self.gr_data:
            self.gr_data = sorted(self.gr_data, key=lambda tup: tup[0])
            self.text += get_table_data('Group Differences' , header, self.gr_data)

        if self.tou_data or self.team_data or self.pl_data or self.gr_data:
            self.send_mail()

    def get_query_result(self, diff_gid=''):
        if "TEAM" in diff_gid or "PL" in diff_gid:
            query_string = 'sports_participants'
            title = 'title'
            game = ', game'
        elif "TOU" in diff_gid:
            query_string = 'sports_tournaments'
            title = 'title'
            game = ', game'
        elif "GR" in diff_gid:
            query_string = 'sports_tournaments_groups'
            title = "group_name"
            game = ''

        query = "select %s%s from %s where gid = '%s'" % (title, game, query_string, diff_gid)
        self.remote_cursor.execute(query)
        record = self.remote_cursor.fetchone()
        if record:
            title = record[0]
        title = '%s {%s}' % (title, diff_gid)
        if record and len(record) == 2:
            title += ', %s' % record[1]
        return title

    def append_data(self, diff_gid, data):
        if 'TOU' in diff_gid:
            self.tou_data.append(data)
        elif 'TEAM' in diff_gid:
            self.team_data.append(data)
        elif 'PL' in diff_gid:
            self.pl_data.append(data)
        elif 'GR' in diff_gid:
            self.gr_data.append(data)


    def populate_missing(self):
        self.get_diff()
        query = "select wiki_gid, sports_gid from sports_wiki_merge where wiki_gid in %s" % str(tuple(self.merge_diff))
        self.remote_cursor.execute(query)
        data = self.remote_cursor.fetchall()
        for d in data:
            current_gid = self.get_wiki_title(d[0])

            sports_title = self.get_query_result(d[1])
            diff_gid = self.merge_diff[d[0]]
            old_title = self.get_query_result(diff_gid)

            status = self.compare_title(old_title, current_gid)
            values = (status, old_title, sports_title, current_gid)
            self.append_data(old_title, values)

        self.text += 'Difference between SPORTDB.wiki_merge count and sports.to.wiki.guid_merge.list line count'
        header = ['Status', 'Missed Gid', 'Existing Gid', 'WIKI Gid']
        if self.tou_data:
            self.tou_data = sorted(self.tou_data, key=lambda tup: tup[0])
            self.text += get_table_data('Tournament Differences', header, self.tou_data)
        if self.team_data:
            self.team_data = sorted(self.team_data, key=lambda tup: tup[0])
            self.text += get_table_data('Team Differences', header, self.team_data)
        if self.pl_data:
            self.pl_data = sorted(self.pl_data, key=lambda tup: tup[0])
            self.text += get_table_data('Player Differences', header, self.pl_data)
        if self.gr_data:
            self.gr_data = sorted(self.gr_data, key=lambda tup: tup[0])
            self.text += get_table_data('Group Differences', header, self.gr_data)

        if self.tou_data or self.team_data or self.pl_data or self.gr_data:
            self.send_mail()

    def cleanup(self):
        try:
            self.remote_conn.close()
            self.wiki_conn.close()
        except:
            pass

    def run_main(self):
        try:
            self.get_merge_file_data()
            self.get_merge_db_data()
            if self.args == 'db_diff':
                self.populate_missing()
            elif self.args == "file_diff":
                self.get_file_differences()
        except:
            print "Exception raised"
            traceback.print_exc()
        self.cleanup()

if __name__ == '__main__':

    if len(sys.argv) == 2 and sys.argv[1] not in ['db_diff', 'file_diff']:
        print "Please use db_diff or file_diff in args"
    elif len(sys.argv) > 2 or len(sys.argv) < 2:
        print "Please use db_diff or file_diff in args"
    else:
        vtv_task_main(CheckDiff)
        sys.exit( 0 )
