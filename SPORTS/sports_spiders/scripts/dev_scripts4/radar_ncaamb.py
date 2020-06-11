import MySQLdb
import datetime
from datetime import timedelta

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'


class EUSoccer:

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.4.18.183', user='root', db= 'SPORTSDB')
        self.cursor = self.conn.cursor()
        self.missed_file = open('eu_missd_players', 'a+')

    def add_sk(self, data, radar_sk):
        values = (data, 'participant', 'radar', radar_sk)
        self.cursor.execute(INSERT_SK, values)

    def radar_sk_exist(self, radar_sk):
        query = 'select entity_id from sports_source_keys where source="radar" and source_key=%s'
        self.cursor.execute(query, radar_sk)
        data = self.cursor.fetchone()
        if data:
            return True
        else:
            return False

    def main(self):
        self.player_details()

    def player_details(self):
        records = open('release2_ncaa', 'r')

        for record in records:
            name,market,founded,title,city,state,country = eval(record.strip())    
            market = market.replace(u'\u2013', '-'). \
            replace('Cal Lutheran', 'California Lutheran'). \
            replace('Penn State', 'Penn St').replace('Austin', 'Austin College'). \
            replace('(', '').replace(')', '').replace(' at ', '-')
            name = name.replace(u'\u2013', '-')
            query = "select P.id, P.gid, P.title, display_title, city, sl.country, stadium_id from sports_teams T, sports_participants P, sports_tournaments ST, sports_locations sl where P.id = T.participant_id and P.location_id=sl.id and T.tournament_id = ST.id and T.tournament_id= 213 and P.title like %s"
            values = ('%' + market + ' ' + name + " men's basketball" + '%')
            self.cursor.execute(query, values)
            team_id = [str(dt[0]) for dt in self.cursor.fetchall()]
            if founded and not team_id and "College" in values or "University" in values:
                market = market.replace('College', '').strip().replace('University', '').strip()
                values = ('%' + market + ' ' + name + " men's basketball" + '%')
                self.cursor.execute(query, values)
                team_id = [str(dt[0]) for dt in self.cursor.fetchall()]
                
            elif founded and not team_id:
                print values
                print formed

            if team_id and len(team_id) == 1:
                team_id = team_id[0]
                if len(founded) == 4:
                    formed = '%s-01-01' % founded
                    query = 'update sports_teams set formed=%s where participant_id=%s'
                    values = (formed, team_id)
                    self.cursor.execute(query, values)
                    continue
                if not title:
                    continue
                title = title.replace(u'\u2013', '-')
                query = 'select id from sports_stadiums where title like %s'
                std = '%' + title + '%'
                self.cursor.execute(query, std)
                data = self.cursor.fetchone()
                if data:
                    data = data[0]
                    self.update_id(data, team_id)
                else:
                    query = 'select id from sports_stadiums where aka like %s'
                    name = '%' + title + '%' 
                    self.cursor.execute(query, name)
                    data = self.cursor.fetchone()
                    if data:
                        data = data[0]
                        self.update_id(data, team_id)

                    else:
                        print title, team_id

    def update_id(self, stadium_id, team_id):
        query = 'update sports_teams set stadium_id=%s where participant_id=%s and stadium_id=0'
        values = (stadium_id, team_id)
        self.cursor.execute(query, values)


if __name__ == '__main__':
    OBJ = EUSoccer()
    OBJ.main()
