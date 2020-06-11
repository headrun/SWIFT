from vtv_task import VtvTask, vtv_task_main
#import MySQLdb


class TournamentStats(VtvTask):
    
    def __init__(self):
        VtvTask.__init__(self)
        self.sports_db = '10.28.218.81'
        self.sports_dbname = 'SPORTSDB'
        self.tou_stats_file = open('tou_stats', 'w+')

    def tournament_details(self):
        tou_query = 'select id, title, game, season_start, season_end from sports_tournaments where type = "tournament" and affiliation !="obsolete"'
        self.cursor.execute(tou_query) 
        data = self.cursor.fetchall()
        for data_ in data:
            tou_id = str(data_[0])
            tou_title = str(data_[1])
            game = str(data_[2])
            season_start = str(data_[3]).split(' ')[0]
            season_end = str(data_[4]).split(' ')[0]
            start_year = season_start.split('-')[0]
            end_year = season_end.split('-')[0].replace('20', '') 
            if end_year in start_year:
                season = start_year
            else:
                season = start_year + "-" + end_year
            tou_par = 'select count(*) from sports_tournaments_participants where tournament_id=%s and season=%s and status="active"'
            values = (tou_id, season)
            self.cursor.execute(tou_par, values)
            tou_par_count = self.cursor.fetchone()
            tou_games = 'select count(*) from sports_games where tournament_id =%s and game_datetime >=%s and status !="Hole"' 
            values = (tou_id, season_start)
            self.cursor.execute(tou_games, values)
            tou_games_count = self.cursor.fetchone()
            roster_qry = 'select count(*) from sports_roster where team_id in (select participant_id  from sports_tournaments_participants where tournament_id =%s) and status="active"'
            values = (tou_id)
            self.cursor.execute(roster_qry% values)
            roster_count = self.cursor.fetchone()
            events_qry = 'select count(*) from sports_tournaments_events where tournament_id = %s' %(tou_id)
            self.cursor.execute(events_qry)
            event_count = self.cursor.fetchone()
            sub_evats_qry = 'select count(*) from sports_tournaments_events where tournament_id in (select event_id from sports_tournaments_events where tournament_id=%s)' %(tou_id)
            self.cursor.execute(sub_evats_qry)
            sub_evats_count = self.cursor.fetchone()
            total_event_count = int(str(event_count[0])) + int(str(sub_evats_count[0]))
            leagues_qry = 'select count(*) from sports_tournaments_groups where tournament_id = %s and group_type = "league"' %(tou_id)
            self.cursor.execute(leagues_qry)
            leagues_count = self.cursor.fetchone()
            conf_qry = 'select count(*) from sports_tournaments_groups where tournament_id = %s and group_type = "Conference"' %(tou_id)
            self.cursor.execute(conf_qry)
            conf_count = self.cursor.fetchone()
            div_qry = 'select count(*) from sports_tournaments_groups where tournament_id = %s and group_type = "Division"' %(tou_id)
            self.cursor.execute(div_qry)
            div_count = self.cursor.fetchone()
            group_qry = 'select count(*) from sports_tournaments_groups where tournament_id = %s and group_type = "Group"' %(tou_id)
            self.cursor.execute(group_qry)
            group_count = self.cursor.fetchone()

            self.tou_stats_file.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(tou_id, tou_title, game, str(tou_par_count[0]), total_event_count, str(leagues_count[0]), str(group_count[0]), str(div_count[0]), str(conf_count[0]), str(tou_games_count[0]), str(roster_count[0])))





    def run_main(self):
        self.open_cursor(self.sports_db, self.sports_dbname)
        self.tournament_details()


if __name__ == '__main__':
    vtv_task_main(TournamentStats)

