import logging
from vtv_db import get_mysql_connection
from datetime import timedelta

class SportsRadarGamesMerge:
    def __init__(self):
        self.failed_games = open('failed_games.txt', 'wb')
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger = logging.getLogger('sportsRadarMerge.log') 
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123') 

    def get_tournament_merge(self):
        print "In get_tournament_merge ..."
        self.tournamet_mapping = {}
        query = 'select radar_id, sportsdb_id from sports_radar_merge where type="tournament"'
        self.cursor.execute(query)
        records = self.cursor.fetchall()

        for record in records:
            radar_id, sportsdb_id = record
            self.tournamet_mapping[radar_id] = sportsdb_id 
        print "Got all tournaments merge ... %s" %(len(self.tournamet_mapping))

    def get_teams_merge(self):
        print "in get_teams_merge ..."
        self.team_mapping = {}
        query = 'select radar_id, sportsdb_id from sports_radar_merge where type="team"'
        self.cursor.execute(query)
        records = self.cursor.fetchall()

        for record in records: 
            radar_id, sportsdb_id = record
            self.team_mapping[radar_id] = sportsdb_id
        print "Got all team merges %s ..." %(len(self.team_mapping))

    def get_participants(self, game_id):
        query = 'select participant_id, is_home from sports_games_participants where game_id = %s order by is_home desc'
        self.cursor.execute(query, (game_id))
        records = self.cursor.fetchall()

        list_of_pars = []
        for record in records:
            participant_id, is_home = record
            list_of_pars.append(participant_id)
        return list_of_pars

    def get_sub_events(self, event_id):
        tournament_id = ''
        query = 'select tournament_id, event_id from SPORTSDB.sports_tournaments_events where event_id = %s'
        self.cursor.execute(query, (event_id))
        record = self.cursor.fetchall()
        if record:
            tournament_id, event_id = record[0][0], record[0][1]   
        return tournament_id
 
    def get_req_games(self, game_datetime, tournament_id):
        tournament_id   = self.tournamet_mapping[tournament_id]

        '''if tournament_id == "33" and str(game_datetime) =="2017-04-21 13:30:00":
            game_datetime = game_datetime + timedelta(hours=24)
        if tournament_id == "33" and str(game_datetime) =="2017-05-05 13:30:00":
            game_datetime = game_datetime + timedelta(hours=24)
        if tournament_id == "33" and str(game_datetime) =="2017-04-28 13:30:00":
            game_datetime = game_datetime + timedelta(hours=24)'''

        one_day_plus    = game_datetime + timedelta(hours=3) 
        one_day_minus   = game_datetime - timedelta(hours=3)
        tou_id          = self.get_sub_events(tournament_id)
        if tou_id:
            tournament_id = tou_id
        query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime > %s and Sg.game_datetime < %s and Sg.status !="Hole"'
        #query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sg.status !="Hole"'
        game_date_ = '%' + str(game_datetime).split(' ')[0] + '%'
        #values = (tournament_id, game_date_)
        values = (tournament_id, one_day_minus, one_day_plus) 
        if tournament_id == '215':
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s) and Sg.game_datetime like %s and Sg.status !="Hole"'
            
            values = (tournament_id, '1091', game_date_)
        if tournament_id == '216':
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s) and Sg.game_datetime like %s and Sg.status !="Hole"'

            values = (tournament_id, '1095', game_date_)
        if tou_id == '9':
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s, %s) and Sg.game_datetime > %s and Sg.game_datetime < %s and Sg.status !="Hole"'
            values = (tournament_id, '1072', '1073', one_day_minus, one_day_plus)
        
        if tournament_id == '586':
            game_date_ = '%' + str(game_datetime - timedelta(hours=24)).split(' ')[0] + '%'
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id =%s and Sg.game_datetime like %s and Sg.status !="Hole"'
            values = (tournament_id, game_date_)
        if tournament_id == '213':
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s) and Sg.game_datetime like %s and Sg.status !="Hole"'

            values = (tournament_id, '469', game_date_)



        print query % values
        self.cursor.execute(query, values)
        games = self.cursor.fetchall()

        '''if not games:
            game_datetime_nt = game_datetime - timedelta(hours=20)
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime=%s'
            values = (tournament_id, game_datetime_nt)
            self.cursor.execute(query, values)
            games = self.cursor.fetchall()'''
        games_dict = {}
        for record in games:
            game_id, par_id = record
            games_dict.setdefault(game_id, []).append(par_id)
            games_dict[game_id].append(str(par_id))
        return games_dict

    def get_games_by_date(self, game_date, tournament_id):
        tournament_id   = self.tournamet_mapping[tournament_id]
        tou_id          = self.get_sub_events(tournament_id)
        if tou_id: tournament_id = tou_id
        query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and date(Sg.game_datetime) = %s and Sg.status !="Hole"'
        values = (tournament_id, game_date)
        if tournament_id == '9':
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s, %s) and date(Sg.game_datetime) = %s and Sg.status !="Hole"'
            values = (tournament_id, '1072', '1073', game_date)
        if tournament_id == '213':
            query = 'select Sg.id, Sgp.participant_id from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s) and date(Sg.game_datetime) = %s and Sg.status !="Hole"'
            values = (tournament_id, '469', game_date)
        print query % values
        self.cursor.execute(query, values)
        games = self.cursor.fetchall()

        games_dict = {}
        for record in games:
            game_id, par_id = record
            games_dict.setdefault(game_id, []).append(par_id)
            games_dict[game_id].append(str(par_id))
        return games_dict
        

    def get_all_games(self):
        query = 'select id, game_datetime, tournament_id from sports_games where tournament_id in ("6889669b-927e-4eca-9a87-795e1f89ae42", "sr:tournament:7", "sr:tournament:34", "sr:tournament:38", "6889669b-927e-4eca-9a87-795e1f89ae42", "4353138d-4c22-4396-95d8-5f587d2df25c", "ncaamb", "2fa448bc-fc17-4d3d-be03-e60e080fdc26", "fd560107-a85b-4388-ab0d-655ad022aff7", "54cf60c3-93f3-4f28-8579-5513deb26b5f", "NFL", "sr:tournament:35", "sr:tournament:23", "sr:tournament:679", "ncaafb", "ncaamb", "sr:tournament:242", "sr:tournament:325", "sr:tournament:352", "sr:tournament:17", "sr:tournament:8")'
        #query = 'select id, game_datetime, tournament_id from sports_games where tournament_id in ("sr:tournament:8", "sr:tournament:23", "sr:tournament:35", "sr:tournament:17")'
        #query = 'select id, game_datetime, tournament_id from sports_games where tournament_id in ("fd560107-a85b-4388-ab0d-655ad022aff7")'
        self.cursor.execute(query)
        records = self.cursor.fetchall()

        guid_merge_list = file("game_merge.list" , "w+")
        game_merge_query = 'insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values ( %s,  %s, %s, now(), now()) on duplicate key update modified_at = now()' 
        print "Got all games ... %s" %len(records)
        for record in records:
            id, game_datetime, tournament_id = record
            try:
                home_id, away_id = self.get_participants(id)
            except:
                self.failed_games.write('%s\n'%id)
            all_games = self.get_req_games(game_datetime, tournament_id)
            if not all_games:
                all_games = self.get_games_by_date(str(game_datetime.date()), tournament_id)
            try:
                home_id = self.team_mapping[home_id]
                away_id = self.team_mapping[away_id]
            except:
                print "Didn't find participants ... %s - %s" %(home_id, away_id)
                continue
            for game_id, participants in all_games.iteritems():
                if home_id in participants and away_id in participants:
                    print "RadarId: %s -- Sportsdb: %s" %(id, game_id)
                    merge = "%s<>%s" %(game_id, id)
                    guid_merge_list.write("%s\n" %merge)
                    values = (id, game_id, "game")
                    self.cursor.execute(game_merge_query, values)

    def run_main(self):
        self.get_tournament_merge()
        self.get_teams_merge()

        self.get_all_games()
        #self.merge_games()
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    SportsRadarGamesMerge().run_main()
