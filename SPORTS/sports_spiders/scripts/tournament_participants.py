import MySQLdb
import sys
from optparse import OptionParser

query1 = "select distinct participant_id from sports_games_participants where game_id in (select id from sports_games where tournament_id=%s and game_datetime >= '%s' and game_datetime <= '%s'  and status!='hole' and event_id not in ('320','899','1493','900'))"

tou_update = "update sports_tournaments_participants set season='', status='inactive' where tournament_id= %s"

insert_query = "insert ignore into sports_tournaments_participants (participant_id, tournament_id, season, status, status_remarks, standing, seed, created_at, modified_at) values ('%s', '%s', '%s', 'active', '', '','', now(), now()) on duplicate key update season='%s', status='active'"

team_q = "select title from sports_participants where id= %s"

class CheckTeams:

    def mysql_connection(self):
        conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
        cursor = conn.cursor()
        return conn, cursor

    def main(self, options):
        conn, cursor = self.mysql_connection()
        self.tou_id       = options.tou_id.strip()
        self.season_start = options.season_start.strip()
        self.season_end   = options.season_end.strip()
        if not (self.tou_id and self.season_start and self.season_end):
            print 'Please provide tournament id, season start and end dates'
            sys.exit(1)
        start_year        = self.season_start.split('-')[0]
        #end_year          = self.season_end.split('-')[0].replace('2020', '20').replace('2019','19')
        end_year          = self.season_end.split('-')[0].replace('2021', '21').replace('2020', '20').replace('2019', '19')
        if '21' in end_year and "2020" in start_year:
            self.season='2020-21'
        elif '20' in end_year and "2019" in start_year:
            self.season='2019-20'
        elif end_year in start_year:
            self.season = start_year
        else:
            self.season = start_year + "-" + end_year
        print self.season

        values = (self.tou_id, self.season_start, self.season_end)
        cursor.execute(query1 % values)

        teams = cursor.fetchall()
        if teams:
            cursor.execute(tou_update % self.tou_id)
            for team in teams:
                team_id = team[0]
                cursor.execute(team_q % team_id)
                team_name = cursor.fetchone()[0]
                if team_name.lower() in ['tbd', 'tba']:
                    print team_name
                    continue
                values = (team_id, self.tou_id, self.season, self.season)
                cursor.execute(insert_query % values)
            print 'populated for the tournament>>>>>', self.tou_id
        else:
            print 'No teams present for tournaments>>>', self.tou_id, self.season
        conn.close()
        cursor.close()
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-t', '--tou_id',        default='soccer', help='Tournament ID')
    parser.add_option('-s', '--season_start',  default='',       help='Tourament Season Start')
    parser.add_option('-e', '--season_end',    default='',       help='Tourament Season End')
    (options, args) = parser.parse_args()
    OBJ = CheckTeams()
    OBJ.main(options)
