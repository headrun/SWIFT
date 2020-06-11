import sys
import time
import MySQLdb
import traceback
from datetime import datetime
from datetime import timedelta
from optparse import OptionParser
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

QUERY = 'select participant_id from sports_tournaments_results where result_sub_type="rank" and result_value in (1,2,3) and tournament_id=%s and season="%s"'

tou_query = "select distinct tournament_id from sports_tournaments_results where result_type='standings'"

TEAM_QUERY = 'select result_sub_type, result_value from sports_tournaments_results where tournament_id=%s and season="%s" and participant_id=%s'

DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'
class MonitoringScript(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.mysql_ip      = '10.28.218.81'
        self.db_name       = 'SPORTSDB'
        self.one_hr_diff   = datetime.utcnow() - timedelta(hours=1)
        self.one_hr_diff   = datetime.now() - timedelta(hours=1)
        self.location_dict = {}
        self.players_wiki_merge = {}
        self.stand_dict    = {}
        self.text          = ''

        self.check_dicts   = {'locations' : 'get_locations()',
                              'standings': 'self.standings_check()'}

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
        subject    = 'SPORTS Check'
        server    = 'localhost'
        sender    = 'headrun@veveo.net'
        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    ### checking for newly added locations ###
    def get_locations(self):
        try:
            query = 'select * from sports_locations where created_at > "%s"' % self.one_hr_diff
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for result in results:
                self.location_dict.setdefault('sports_locations table', []).append(result)
            headers = ('Id', 'continent', 'country', 'state', 'city', 'street', 'zipcode', 'latlong', 'created_at', 'modified_at')
            for key, value in self.location_dict.iteritems():
                self.text += self.get_html_table(key, headers, value)
        except:
            print traceback.foRMat_exc()

    def get_team_title(self, team_id):
        query = 'select title from sports_participants where id=%s'
        self.cursor.execute(query % (team_id))
        data = self.cursor.fetchone()
        return data[0]

    def get_standings_time_diff(self, modified_at):
        import datetime
        if isinstance(modified_at, str):
            modified_at = datetime.datetime(*time.strptime(modified_at, '%Y-%m-%d %H:%M:%S')[0:6])
        now = datetime.datetime.now()
        diff = now - modified_at
        if diff and not '-' in str(diff.days):
            return True, now
        else:
            return False, now

    def get_tournament_name(self, tou_id):
        query = 'select title, season_start, season_end from sports_tournaments where id="%s"'
        self.cursor.execute(query % tou_id)
        data = self.cursor.fetchone()
        if data:
            tou_name, start, end = data
        return tou_name, start, end

    def standings_check(self):
        title = 'Tournaments Standings not updated in less than 24 hours'
        query = 'select max(modified_at) from sports_tournaments_results where tournament_id = "%s"'
        self.cursor.execute(tou_query)
        tou_ids = self.cursor.fetchall()
        for tou_id in tou_ids:
            self.cursor.execute(query % tou_id[0])
            modified_at = self.cursor.fetchone()
            diff, now = self.get_standings_time_diff(modified_at[0])
            if not diff:
                continue

            tou_name, start, end = self.get_tournament_name(tou_id[0])
            if start < now and end > now:
                self.stand_dict.setdefault(title, []).append([tou_name, str(modified_at[0])])
        if self.stand_dict:
            headers = ['Tou Name', 'Last modified_at']
            for key, value in self.stand_dict.iteritems():
                self.text += self.get_html_table(key, headers, value)

    def get_duplicate_players(self):
        self.open_cursor('10.4.2.187', 'GUIDMERGEDB')
        query = 'select exposed_gid, child_gid from sports_wiki_merge'
        self.cursor.execute(query)
        for row in self.get_fetchmany_results():
            wiki_gid, sports_gid = row
            if 'PL' in sports_gid:
                self.players_wiki_merge.setdefault(wiki_gid, []).append(sports_gid)
        count = 0
        for data in self.players_wiki_merge.values():
            if len(data) >= 2:
                count += 1
        print count

    def set_options(self):
        self.parser.add_option('-t', '--check_type', default='', help='Locations or standings')

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','monitoring_script*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def run_main(self):
        #self.get_duplicate_players()
        self.open_cursor(self.mysql_ip, self.db_name)
        if self.options.check_type.strip() == 'locations':
            self.get_locations()
        elif self.options.check_type.strip() == 'standings':
            self.standings_check()
        else:
            print 'needed check type - locations or standings'
            sys.exit( 0 )
        if self.text:
            self.send_mail(self.text)


if __name__ == '__main__':
    vtv_task_main(MonitoringScript)
    sys.exit( 0 )

