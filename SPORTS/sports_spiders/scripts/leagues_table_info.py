import datetime
import requests
from pprint import pprint
import time
import MySQLdb
from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd, make_dir_list, move_file_list, VTV_DATAGEN_CURRENT_DIR
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
import os
from vtv_db import get_mysql_connection
import json


tou_inset_qry = "insert into sports_tournaments_participants(participant_id, tournament_id, status, status_remarks, standing, seed, season, created_at, modified_at) values(%s, %s, 'active', '', '', '', %s, now(), now()) on duplicate key update modified_at=now(), season=%s, status=%s"

wiki_gid_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid=%s'
stadium_id_qry = 'select id, gid, location_id from sports_stadiums where title=%s'
team_id_qry = 'select id from sports_participants where title=%s and sport_id=%s and participant_type="team"'
team_gid_qry = 'select id from sports_participants where gid=%s'
stadium_gid_qry = 'select id, location_id from sports_stadiums where gid=%s'
up_team_title = 'update sports_participants set title=%s where id=%s limit 1'
up_stadium_title = 'update sports_stadiums set title=%s where id=%s limit 1'
up_team_touid= 'update sports_teams set tournament_id=%s where participant_id=%s limit 1'
up_tm_stid = 'update sports_teams set stadium_id=%s where participant_id=%s limit 1'
stadium_title = 'select id, location_id from sports_stadiums where title=%s'
wiki_inst_qry = "insert into GUIDMERGE.sports_wiki_merge(exposed_gid, child_gid, action, modified_date) values(%s, %s, 'override', now()) on duplicate key update modified_date=now()"
sel_location = 'select id from sports_locations where city=%s'
up_loc_info = 'update sports_participants set location_id=%s where id=%s limit 1'
up_capacity = 'update sports_stadiums set capacity=%s where id=%s limit 1'

class SportsLeaguesInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name = "SPORTSDB"
        self.db_ip   = "10.28.218.81"
        self.wiki_db = "GUIDMERGE"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        self.sport_id=7
        self.tou_id  =598

    def get_wiki_gids(self, team_gid, stadium_gid):
        team_ids = (team_gid)
        self.cursor.execute(wiki_gid_qry, team_ids)
        team_data = self.cursor.fetchall()
        team_id = ''
        stadium_id = ''
        location_id = ''
        if len(team_data) == 1:
            team_id = team_data[0][0]
        stadium_ids = (stadium_gid)
        self.cursor.execute(wiki_gid_qry, stadium_ids)
        stadium_data = self.cursor.fetchall()
        if len(stadium_data) == 1:
            stadium_id = stadium_data[0][0] 
            lo_values = (stadium_id)
            self.cursor.execute(stadium_gid_qry, lo_values)
            loc_data = self.cursor.fetchone()
            if loc_data:
                location_id = loc_data[1]
                stadium_id = loc_data[0]
                
        return team_id, stadium_id, location_id

    def check_st_title(self, stadium_name, stadium_gid, city, stadium_ot_name, capacity):
        values = (stadium_name)
        self.cursor.execute(stadium_id_qry, values)
        st_data = self.cursor.fetchone()
        if not st_data:
            values = (stadium_ot_name)
            self.cursor.execute(stadium_id_qry, values)
            st_data = self.cursor.fetchone()


        if st_data:
            stadium_id = st_data[0]
            st_gid = st_data[1]
            location_id = st_data[2]
            wiki_values = (stadium_gid, st_gid)
            self.cursor.execute(wiki_inst_qry, wiki_values)
        else:
            loc_values = (city)
            self.cursor.execute(sel_location, loc_values)
            loc_data = self.cursor.fetchone()
            location_id = ''
            if loc_data:
                location_id = loc_data[0]
            self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_stadiums' and TABLE_SCHEMA='SPORTSDB'")
            count = self.cursor.fetchone()
            st_gid = 'STAD%s' % str(count[0])

            query = 'insert into sports_stadiums (id, gid, title, location_id, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'
            values = (count[0], st_gid, stadium_name, location_id)
            self.cursor.execute(query, values)
            wiki_values = (stadium_gid, st_gid)
            stadium_id = count[0]
        return stadium_id, location_id
   
             
        pass

    def get_date(self):
        data = open('generic_parsing_output.data', 'r+')
        for data_ in data:
            json_data = json.loads(data_)
            team_     = json_data.values()[0].get('Team', '')
            city      = json_data.values()[0].get('City', '').split('{')[0].strip().split(',')[0].strip()
            capacity   = json_data.values()[0].get('Capacity', '').split('{')[0].strip().split('<>')[0].strip()
            stadium   = json_data.values()[0].get('Stadium', '')
            team_name = team_.split('{')[0].strip()
            team_gid  = team_.split('{')[-1].strip().split('<>')[0].strip().replace('}', '')
            stadium_ot_name = stadium.split('<>')[-1].replace('None', '').strip()
            stadium_name = stadium.split('{')[0].strip().split('(')[0].strip().replace('<>','')
            stadium_gid = stadium.split('{')[-1].strip().split('<>')[0].strip().replace('}', '')
            team_id, stadium_id, location_id = self.get_wiki_gids(team_gid, stadium_gid)
            if team_id:
                team_values = (team_id)
                self.cursor.execute(team_gid_qry, team_values)
                team_data = self.cursor.fetchall()
                if team_data:
                    team_ = team_data[0][0]
                    team_info = (team_name, team_)
                    team_tou_info = (self.tou_id, team_)
                    tou_part_info = (team_, self.tou_id, '2017', '2017', 'active')
                    self.cursor.execute(up_team_title, team_info)
                    self.cursor.execute(up_team_touid, team_tou_info)
                    self.cursor.execute(tou_inset_qry, tou_part_info)
                    if stadium_id:
                        st_values = (stadium_id, team_)
                        up_st_values = (stadium_name, stadium_id)
                        self.cursor.execute(up_stadium_title, up_st_values)
                        self.cursor.execute(up_tm_stid, st_values)
                        if location_id:
                            loc_values = (location_id, team_)
                            self.cursor.execute(up_loc_info, loc_values)
                        '''if capacity:
                            cap_values = (capacity, stadium_id)
                            self.cursor.execute(up_capacity, cap_values)'''

                    else:
                        stadium_, location_id = self.check_st_title(stadium_name, stadium_gid, city, stadium_ot_name, capacity)
                        if stadium_:
                            st_values = (stadium_, team_)
                            self.cursor.execute(up_tm_stid, st_values)
                            up_st_values = (stadium_name, stadium_)
                            self.cursor.execute(up_stadium_title, up_st_values)

                        if location_id:
                            loc_values = (location_id, team_)
                            self.cursor.execute(up_loc_info, loc_values)

                        '''if capacity:
                            cap_values = (capacity, stadium_)
                            self.cursor.execute(up_capacity, cap_values)'''
            else:
                print str(team_name)
                    
                             
 

    def run_main(self):             
        self.get_date()


if __name__ == '__main__':
    vtv_task_main(SportsLeaguesInfo)


