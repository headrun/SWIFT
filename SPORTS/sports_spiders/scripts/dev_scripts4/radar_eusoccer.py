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
        #self.team_details()

    def player_details(self):
        records = open('eu_player_details_file', 'r')
        for record in records:
            name, birth_date, radar_sk, team_name, full_first_name, full_last_name = eval(record)
            pl_exists = self.radar_sk_exist(radar_sk)
            if pl_exists == True:
                continue
            if not birth_date:
                continue
            query = 'select sp.id from sports_participants sp, sports_players pl where game="soccer" and gid like %s and sp.id=pl.participant_id and title =%s and birth_date=%s'
            gid = '%' + 'PL' + '%'
            values = (gid, name, birth_date)
            self.cursor.execute(query, values)
            data = self.cursor.fetchone()
            if not data and full_first_name:
                name = full_first_name + " " + full_last_name
                values = (gid, name, birth_date)
                self.cursor.execute(query, values)
                data = self.cursor.fetchone()
            if data:
                data = str(data[0])
                print 'added sk', name
                self.add_sk(data, radar_sk)
            else:
                birth_date = datetime.datetime.strptime(birth_date, '%Y-%m-%d')
                birth_date += datetime.timedelta(days=1)
                values = (gid, name, str(birth_date))
                self.cursor.execute(query, values)
                data = self.cursor.fetchone()
                if data:
                    data = str(data[0])
                    print 'added sk', name
                    self.add_sk(data, radar_sk)
                else:
                    self.missed_file.write('%s\n' % record.strip())


if __name__ == '__main__':
    OBJ = EUSoccer()
    OBJ.main()
