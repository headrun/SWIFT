from vtv_task import VtvTask, vtv_task_main
from datetime import datetime
import jinja2
import codecs
import os

continent_query = "select continent, count(st.id) from sports_tournaments st, sports_locations sl where type='tournament' and location_ids=sl.id group by continent"

REPORT_DIR = '/home/veveo/reports/'

class SportsStats(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.mysql_ip   =  '10.4.18.183'
        self.db_name    =  'SPORTSDB'
        self.tou_stats  = {}
        self.stats_dict = {}
        self.stats_list = []
        self.basic_list = []

    def execute_query(self, query):
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]

        return count

    def get_basic_stats(self):
        tou_count = self.execute_query("select count(*) from sports_tournaments where type='tournament'")
        self.stats_dict['total_tou'] = tou_count
        title = 'Number of Tournaments'
        self.stats_list.append((tou_count, title))

        active_tou_count = self.execute_query("select count(*) from sports_tournaments where type='tournament' and affiliation!='obsolete'")
        self.stats_dict['active_tou_count'] = active_tou_count
        title = 'Number of Active Tournaments'
        self.stats_list.append((active_tou_count, title))

        player_count = self.execute_query("select count(*) from sports_participants where id in (select participant_id from sports_players)")
        self.stats_dict['players_count'] = player_count
        title = 'Number of Players'
        self.stats_list.append((player_count, title))

        active_pl_count = self.execute_query("select count(*) from sports_participants where participant_type='player'")
        self.stats_dict['active_pl_count'] = active_pl_count
        title = 'Number of Active Players'
        self.stats_list.append((active_pl_count, title))

        teams_count = self.execute_query("select count(*) from sports_participants where id in (select participant_id from sports_teams)")
        self.stats_dict['teams_count'] = teams_count
        title = 'Number of Teams'
        self.stats_list.append((teams_count, title))

        active_team_count = self.execute_query("select count(*) from sports_participants where participant_type='team'")
        self.stats_dict['active_team_count'] = active_team_count
        title = 'Number of Active Teams'
        self.stats_list.append((active_team_count, title))

        tou_grp_count = self.execute_query("select count(*) from sports_tournaments_groups")
        self.stats_dict['tou_grp_count'] = tou_grp_count
        title = 'Number of Tournament Groups'
        self.stats_list.append((tou_grp_count, title))

        total_leagues = self.execute_query("select count(*) from sports_tournaments where subtype='league'")
        self.stats_dict['total_leagues'] = total_leagues
        title = 'Number of Leagues that are present in sports_tournaments'
        self.stats_list.append((total_leagues, title))

        total_games = self.execute_query("select count(distinct game) from sports_games")
        self.stats_dict['total_games'] = total_games
        title = 'Number of Games'
        self.stats_list.append((total_games, title))

        self.stats_list.sort()
        for record in self.stats_list:
            count, title = record
            self.basic_list.append((title, count))

    def get_tou_stats(self):
        self.cursor.execute(continent_query)
        for row in self.get_fetchmany_results():
            continent, count = row
            self.tou_stats[continent] = str(count)

    def run_main(self):
        self.open_cursor(self.mysql_ip, self.db_name)
        self.get_basic_stats()
        self.get_tou_stats()
        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template('stats_script.jinja').render(today_date = datetime.now(), big_list=self.basic_list, continent_tous= self.tou_stats)
        codecs.open(os.path.join(REPORT_DIR, 'SPORTSDB_STATS_REPORT.html'), 'w', 'utf8').write(table_html)


if __name__ == '__main__':
    vtv_task_main(SportsStats)

