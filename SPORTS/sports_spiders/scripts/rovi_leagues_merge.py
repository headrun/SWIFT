# -*- coding: utf-8 -*-
import sys
import os
import json
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

UP_QRY = 'update sports_rovi_merge set rovi_id=%s where entity_type="tournament" and entity_id=%s limit 1'
INST_QRY = "insert into sports_rovi_merge(entity_type, entity_id, rovi_id, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()"
INS_MAP_QRY = "insert into sports_leagues_tournaments(league_id, tournament_id, created_at, modified_at)values(%s, %s, now(), now()) on duplicate key update modified_at=now()"
LEAGUE = "insert into sports_leagues(id, gid, title, sport_id, affiliation, created_at, modified_at) values(%s, %s, %s, %s, %s, now(), now())"
MAX_ID = 'select max(id) from sports_leagues'
L_QRY = 'select L.id, L.title, RV.rovi_id from sports_leagues L, sports_rovi_merge RV where RV.entity_id=L.id and RV.entity_type="league" and RV.rovi_id=%s'

class LeagueRoviMergeInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def leagues_titles(self):
        file_ = open('League.txt', 'r+')
        for file_data in file_:
            rovi_id   = file_data.split('|')[0].strip()
            league_name = file_data.split('|')[1].strip()
            values = (rovi_id)
            self.cursor.execute(L_QRY, values)
            data = self.cursor.fetchone()
            if data:
                league_id = data[0]
                league_title = data[1]
                if league_name !=league_title:
                    values = (league_name, league_id)
                    up_qry = 'update sports_leagues set title=%s where id = %s limit 1'
                    self.cursor.execute(up_qry, values)
            else:
                print file_data
            
    def teams_merge_info(self):
        file_ = open('League.txt', 'r+')
        for file_data in file_:
            if 'league id' in file_data or 'None' in file_data or 'Eastern' in file_data or 'Western' in file_data or 'Group ' in file_data or 'NFL' in file_data or 'MLB' in file_data:
                continue
            rovi_id   = file_data.split('|')[0].strip()
            league_name = file_data.split('|')[1].strip()
            league_desc = file_data.split('|')[2].strip()
            if not league_desc:
                continue
            league_name = league_name.replace('Gambrinus Liga', 'Czech First League').replace('1st Liga', 'Austrian Football First League').replace('A Group', 'First Professional Football League').replace('Isreali', 'Israeli').replace('South Korean Soccer League','K League')
            if "Spanish" in league_desc and 'Primera Div' in league_name:
                league_name = 'La Liga'
            if "Romanian" in league_desc and 'Premier League' in league_name:
                league_name = "Liga I"
            if "Slovakian" in league_desc:
                league_name =league_name.replace('Super League', 'Slovak Super Liga')
            if "Greek" in league_desc:
                league_name = league_name.replace('Super League', 'Superleague Greece')
            if "Brazilian Soccer League" in league_desc:
                league_name = 'Campeonato Brasileiro ' + league_name
            if "Premier" in league_name and "English" not in league_desc and 'Isreali' not in league_desc:
                league_name = league_desc.split(' ')[0] + ' '+ league_name
            if "Segunda" in league_name and "Spanish" not in league_desc:
                league_name = league_desc.split(' ')[0] + ' ' + league_name
            if "Primera D" in league_name:
                league_name = league_desc.split(' ')[0] + ' '+ league_name
            if "Ecuadorian" in league_desc:
                league_name = 'Ecuadorian ' + league_name
            if "UEFA" in league_desc or 'Scottish' in league_desc or "Swiss" in league_desc or 'Danish' in league_desc or 'Serbian' in league_desc or "Albanian" in league_desc:
                league_name = league_desc.split(' ')[0] + ' '+ league_name.replace('Super Liga', 'SuperLiga')
            league_name = league_name.replace('Belgacom League', 'Belgian Second Division').replace('Primera B', 'Primera B Nacional').replace('CFA', 'Championnat de France Amateur').replace('CHL', 'Canadian Hockey League').replace('Welsh Welsh', 'Welsh').replace('USL', 'United Soccer League').replace('NASL', 'North American Soccer League').replace('Belarus', 'Belarusian').replace('HNL', 'Croatian First Football League').replace("Ligat Ha'al", 'Israeli Premier League').replace('1. SNL', 'Slovenian PrvaLiga').replace('Azerbaijani', 'Azerbaijan')
            sel_qry = 'select id from sports_rovi_merge where rovi_id=%s and entity_type="league"'
            values = (rovi_id)
            self.cursor.execute(sel_qry, values)
            data = self.cursor.fetchone()
            sport_id = ''
            if "Soccer" in league_desc and "Basketball" not in league_desc:
                sport_id = '7'
            if not data:
                tou_qry = 'select id, sport_id, affiliation from sports_tournaments where title=%s'
                tou_values = (league_name)
                if sport_id:
                    tou_qry = 'select id, sport_id, affiliation from sports_tournaments where title=%s and sport_id=%s'       
                    tou_values = (league_name, sport_id)
                self.cursor.execute(tou_qry, tou_values)
                tou_data = self.cursor.fetchall()
                self.cursor.execute(MAX_ID)
                max_data = self.cursor.fetchone()
                if max_data:
                    league_id = int(max_data[0]) + 1
                    league_gid = 'LEAGUE' + str(league_id)
                if len(tou_data) == 1:
                    tou_id = tou_data[0][0]
                    sport_id = tou_data[0][1]
                    aff = tou_data[0][2]
                    league_values = (league_id, league_gid, league_name, sport_id, aff)
                    up_values = (rovi_id, tou_id)
                    #tou_merge = ('tournament', tou_id, rovi_id)
                    league_merge = ('league', league_id, rovi_id)
                    map_values = (league_id, tou_id)
                    self.cursor.execute(UP_QRY, up_values)
                    #self.cursor.execute(INST_QRY, tou_merge)
                    self.cursor.execute(INST_QRY,league_merge)
                    self.cursor.execute(LEAGUE, league_values)
                    self.cursor.execute(INS_MAP_QRY, map_values) 
                    print tou_id, file_data.strip()
                else:
                    print file_data.strip()

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','rovi_leagues_merge*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def run_main(self):
        #self.teams_merge_info()
        self.leagues_titles()

if __name__ == '__main__':
    vtv_task_main(LeagueRoviMergeInfo)

