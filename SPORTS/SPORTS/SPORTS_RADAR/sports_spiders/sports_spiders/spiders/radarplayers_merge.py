from vtv_db import get_mysql_connection
from datetime import timedelta
import logging

PL_NAME_QUERY = 'select P.id from SPORTSDB.sports_participants P, SPORTSDB.sports_players PL where P.title=%s and P.sport_id=%s and P.id=PL.participant_id and PL.birth_date=%s'
PL_AKA_QUERY = 'select P.id from SPORTSDB.sports_participants P, SPORTSDB.sports_players PL where P.aka=%s and P.sport_id=%s and P.id=PL.participant_id and PL.birth_date=%s'

INST_QRY = 'insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

class SportsRadarPlayersMerge:
    def __init__(self):
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger = logging.getLogger('sportsRadarPlayersMerge.log')
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
    def get_players_merge(self):
        sel_qry = 'select P.id, P.title, P.birth_date, P.sport_id, P.full_first_name, P.full_last_name from sports_players P left join sports_radar_merge GP on GP.radar_id=P.id where GP.radar_id is Null'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for data_ in data:
            id_   = data_[0]
            pl_name = data_[1]
            dob   = data_[2]
            sport_id = data_[3]
            first_name = data_[4]
            last_name = data_[5]
            if not dob:
                dob = "0000-00-00 00:00:00"
            try:
                pl_id = self.get_plid(pl_name, dob, sport_id, first_name, last_name)
            except:
                continue
            if pl_id:
                values = (id_, pl_id, 'player')
                self.cursor.execute(INST_QRY, values) 
            else:
                print id_, pl_name, dob
                 
    def get_plid(self, pl_name, dob, sport_id, first_name, last_name):
        values = (pl_name, sport_id, dob)
        self.cursor.execute(PL_NAME_QUERY, values)
        data = self.cursor.fetchall()
        if not data:
            self.cursor.execute(PL_AKA_QUERY, values)
            data = self.cursor.fetchall()
        if not data:
            pl_name_ = first_name + " " + last_name
            values_ = (pl_name_, pl_name, dob)
            self.cursor.execute(PL_NAME_QUERY, values_)
            data = self.cursor.fetchall()
                
        if data and len(data) == 1:
            pl_id = data[0][0]
        else:
            sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=%s and participant_type="player"'
            values = (pl_name, sport_id)
            self.cursor.execute(sel_qry, values)
            data = self.cursor.fetchall()
            if not data:
                pl_name_ = first_name + " " + last_name
                values = (pl_name_, sport_id)
                self.cursor.execute(sel_qry, values)
                data = self.cursor.fetchall()

            if len(data) !=1:
                pl_id = ''
            else:
                pl_id = data[0][0]
        return pl_id

    def run_main(self):
        self.get_players_merge()
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    SportsRadarPlayersMerge().run_main()
                                                                                                                                                                                               

