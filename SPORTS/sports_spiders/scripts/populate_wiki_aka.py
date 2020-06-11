# -*- coding: utf-8 -*-
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection

DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

INT_QRY = 'insert into sports_tournaments_phrases(tournament_id, phrase, language, culture, region, field, weight, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INT_GR_QRY = 'insert into sports_groups_phrases(group_id, phrase, language, culture, region, field, weight, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INT_PAR_QRY = 'insert into sports_participants_phrases(participant_id, phrase, language, culture, region, field, weight, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INT_TY_QRY = 'insert into sports_types_phrases(sport_id, phrase, language, culture, region, field, weight, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INT_STD_QRY = 'insert into sports_stadiums_phrases(stadium_id, phrase, language, culture, region, field, weight, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

class SportsLanguagesInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                    user = 'veveo', passwd='veveo123')


    def get_stadiums(self):
        sel_qry = 'select id, title, aka from sports_stadiums'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            title    = _data[1]
            aka      = _data[2].split('###')
            values = (id_, title, 'eng', '', '', 'Ti', '100')
            self.cursor.execute(INT_STD_QRY, values)

            for aka_ in aka:
                aka_name = aka_
                if not aka_name:
                    continue
                aka_values = (id_, aka_name, 'eng', '', '', 'Ak', '100')
                self.cursor.execute(INT_STD_QRY, aka_values)

    def get_tou_data(self):
        sel_qry = 'select id, title, aka, keywords, ex_akas from sports_tournaments'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            title    = _data[1]
            aka      = _data[2].split('###')
            keywords = _data[3].split('###')
            ex_akas  = _data[4].split('###')
            values = (id_, title, 'eng', '', '', 'Ti', '100')
            self.cursor.execute(INT_QRY, values)
            
            for key in keywords:
                key_name = key.split('{')[0].strip()
                if not key_name:
                    continue
                weight = key.split('{')[-1].strip().replace('}', '')
                key_values = (id_, key_name, 'eng', '', '', 'Ke', weight)
                self.cursor.execute(INT_QRY, key_values)
            
            for aka_ in aka:
                aka_name = aka_
                if not aka_name:
                    continue
                aka_values = (id_, aka_name, 'eng', '', '', 'Ak', '100')
                self.cursor.execute(INT_QRY, aka_values)

            for ex_aka_ in ex_akas:
                ex_aka_name = ex_aka_
                if not ex_aka_name:
                    continue
                ex_aka_values = (id_, ex_aka_name, 'eng', '', '', 'Za', '100')
                self.cursor.execute(INT_QRY, ex_aka_values)
                '''up_qry = 'update sports_tournaments_phrases set field="Za" where tournament_id=%s and phrase=%s limit 1'
                values = (id_, ex_aka_name)
                self.cursor.execute(up_qry, values) '''

        sel_qry = 'select id, title, aka, keywords from sports_types'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            title    = _data[1]
            aka      = _data[2].split('###')
            keywords = _data[3].split('###')
            values = (id_, title, 'eng', '', '', 'Ti', '100')
            self.cursor.execute(INT_TY_QRY, values)

            for key in keywords:
                key_name = key.split('{')[0].strip()
                if not key_name:
                    continue
                weight = key.split('{')[-1].strip().replace('}', '')
                key_values = (id_, key_name, 'eng', '', '', 'Ke', weight)
                self.cursor.execute(INT_TY_QRY, key_values)

            for aka_ in aka:
                aka_name = aka_
                if not aka_name:
                    continue
                aka_values = (id_, aka_name, 'eng', '', '', 'Ak', '100')
                self.cursor.execute(INT_TY_QRY, aka_values)
 

    def get_group_data(self):
        sel_qry = 'select id, group_name, aka, keywords from sports_tournaments_groups'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            title    = _data[1]
            aka      = _data[2].split('###')
            keywords = _data[3].split('###')
            values = (id_, title, 'eng', '', '', 'Ti', '100')
            self.cursor.execute(INT_GR_QRY, values)

            for key in keywords:
                key_name = key.split('{')[0].strip()
                if not key_name:
                    continue
                weight = key.split('{')[-1].strip().replace('}', '')
                key_values = (id_, key_name, 'eng', '', '', 'Ke', weight)
                self.cursor.execute(INT_GR_QRY, key_values)

            for aka_ in aka:
                aka_name = aka_
                if not aka_name:
                    continue
                aka_values = (id_, aka_name, 'eng', '', '', 'Ak', '100')
                self.cursor.execute(INT_GR_QRY, aka_values)

    def get_participants_data(self):
         
        sel_qry = 'select SP.id, SP.title, SP.aka, T.keywords from sports_participants SP, sports_teams T where T.participant_id=SP.id and SP.participant_type="Team"'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            title    = _data[1]
            aka      = _data[2].split('###')
            keywords = _data[3].split('###')
            values = (id_, title, 'eng', '', '', 'Ti', '100')
            self.cursor.execute(INT_PAR_QRY, values)

            for key in keywords:
                key_name = key.split('{')[0].strip()
                if not key_name:
                    continue
                weight = key.split('{')[-1].strip().replace('}', '')
                key_values = (id_, key_name, 'eng', '', '', 'Ke', weight)
                self.cursor.execute(INT_PAR_QRY, key_values)

            for aka_ in aka:
                aka_name = aka_
                if not aka_name:
                    continue
                if aka_name="":continue
                aka_values = (id_, aka_name, 'eng', '', '', 'Ak', '100')
                self.cursor.execute(INT_PAR_QRY, aka_values)

        sel_qry = 'select id, title, aka from sports_participants where participant_type="player"'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            title    = _data[1]
            aka      = _data[2].split('###')
            values = (id_, title, 'eng', '', '', 'Ti', '100')
            self.cursor.execute(INT_PAR_QRY, values)

            for aka_name in aka:
                if not aka_name:
                    continue
                aka_values = (id_, aka_name, 'eng', '', '', 'Ak', '100')
                self.cursor.execute(INT_PAR_QRY, aka_values)
  
    def get_db_data(self):
        sel_qry = 'select entity_id, entity_type, title from sports_titles_regions where iso = "zh-Hans"'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for _data in data:
            id_      = _data[0]
            type_    = _data[1]
            title    = _data[2]
            values = (id_, title, 'zho', '', '', 'Ti', '100')
            if type_ == "team":
                self.cursor.execute(INT_PAR_QRY, values)
            if type_ == "tournament":
                self.cursor.execute(INT_QRY, values)

    def populate_STRT(self):
        sel_qrys =  ['select participant_id, short_title, display_title from sports_teams']
        for sel_qry in sel_qrys:
            self.cursor.execute(sel_qry)
            data = self.cursor.fetchall()
            for _data in data:
                id_      = _data[0]
                st_title = _data[1]
                dt_title = _data[2] 
                values = (id_, st_title, 'eng', '', '', 'Rt', '')
                dt_values = (id_, dt_title, 'eng', '', '', 'Dt', '')
                if st_title:
                    self.cursor.execute(INT_PAR_QRY, values)
                if dt_title:
                    self.cursor.execute(INT_PAR_QRY, dt_values)
                    
    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','populate_languees*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.populate_STRT()
        self.get_stadiums()
        self.get_tou_data()
        self.get_group_data()
        self.get_participants_data()
        #self.get_db_data()

if __name__ == '__main__':
    vtv_task_main(SportsLanguagesInfo)
