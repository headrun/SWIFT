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


class TournamentsInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')


    def tournaments_details(self):
        #sel_qry = 'select T.id, T.gid, T.title, T.season_start, T.season_end, Ty.title from sports_tournaments T, sports_types Ty where T.id in (2841, 2850, 1891, 3419, 558, 564, 1105, 599, 569, 570, 575, 578, 609, 559, 567, 28, 2207, 5476, 571, 3601, 574, 4096, 580, 3838, 610, 3626, 240, 197, 88, 229, 35, 32, 33, 2553, 631, 579, 29, 573, 215, 216, 1825, 2209, 1895, 1, 9, 15, 34,70, 81, 92, 213, 214, 222, 231, 267, 378, 529, 562, 572, 585, 586, 589, 590, 591, 595, 596, 597, 598, 610, 631, 954, 1064, 1105, 1850, 2235, 2905, 1281, 1116, 1117, 577, 4992, 5982, 92, 218, 529, 7094, 1310, 251, 1115, 2201) and Ty.id=T.sport_id order by T.id'
        sel_qry = 'select T.id, T.gid, T.title, T.season_start, T.season_end, Ty.title from sports_tournaments T, sports_types Ty where T.id in (2841, 2850, 1891, 3419, 558, 564, 1105, 599, 569, 570, 575, 578, 609, 559, 567, 28, 2207, 5476, 571, 3601, 574, 4096, 580, 3838, 610, 3626, 240, 197, 88, 229, 35, 32, 33, 2553, 631, 579, 29, 573, 215, 216, 1825, 2209, 1895, 1, 9, 15, 34,70, 81, 92, 213, 214, 222, 231, 267, 378, 529, 562, 572, 585, 586, 589, 590, 591, 595, 596, 597, 598, 610, 631, 954, 1064, 1850, 2235, 2905, 1281, 63,91,509,68,1116, 1117, 577, 4992, 5982, 92, 218, 529, 7094, 1310, 251, 1115, 2201,3789, 3209, 1485, 53, 49, 50, 281, 962, 721) and Ty.id=T.sport_id order by T.id'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for data_ in data:
            tou_id, tou_gid, tou_title, tou_start, tou_end, sport = data_
            game_status=  min_datetime= max_datetime= tou_standings_season=tou_par_season=roster_season=groups_season=groups_par_season=''
            sel_game_qry = 'select min(game_datetime), max(game_datetime) from sports_games where tournament_id =%s and status !="Hole"'
            sel_status_qry = 'select distinct(status) from sports_games where tournament_id =%s and status in ("completed", "scheduled") order by status'
            sel_tou_qry = 'select distinct(season) from sports_tournaments_participants where tournament_id =%s order by season'
            sel_season_qry = 'select distinct(season) from sports_tournaments_seasons where tournament_id =%s order by season'
            sel_toure_qry = 'select distinct(season) from sports_tournaments_results where tournament_id =%s and result_type="standings" order by season'
            sel_gr_res_qry = 'select distinct(GP.season) from sports_groups_results GP, sports_tournaments_groups TG where TG.id=GP.group_id and TG.tournament_id=%s order by GP.season'
            sel_gr_par_qry = 'select distinct(GP.season) from sports_groups_participants GP, sports_tournaments_groups TG where TG.id=GP.group_id and TG.tournament_id=%s order by GP.season'
            sel_roster_qry = 'select distinct(R.season) from sports_roster R, sports_tournaments_participants TP where R.team_id=TP.participant_id and tournament_id=%s and R.status = "active" order by R.season'
            values = (tou_id)
            self.cursor.execute(sel_game_qry% values)
            data = self.cursor.fetchone()
            if data:
                min_datetime, max_datetime = data[0], data[1]
            self.cursor.execute(sel_status_qry% values)
            data = self.cursor.fetchall()
            status_list = []
            for data_ in data:
                game_status = str(data_[0])
                status_list.append(game_status)
            game_status = "<>".join(status_list)
            self.cursor.execute(sel_tou_qry% values)
            data  = self.cursor.fetchall()
            tou_par_list = []
            for data_ in data:
                tou_par_season = str(data_[0])
                tou_par_list.append(tou_par_season)
            tou_par_season = "<>".join(tou_par_list)
    
            self.cursor.execute(sel_season_qry% values)
            data  = self.cursor.fetchall()
            tou_season_list = []
            for data_ in data:
                tou_season = str(data_[0])
                tou_season_list.append(tou_season)
            tou_seasons = "<>".join(tou_season_list)

            self.cursor.execute(sel_toure_qry% values)
            data  = self.cursor.fetchall()
            tou_standings_list = []
            for data_ in data:
                tou_standings_season = str(data_[0])
                tou_standings_list.append(tou_standings_season)
            tou_standings_season = "<>".join(tou_standings_list)
            self.cursor.execute(sel_gr_res_qry% values)
            data  = self.cursor.fetchall()
            groups_season_list = []
            for data_ in data:
                groups_season = str(data_[0])
                groups_season_list.append(groups_season)
            groups_season = "<>".join(groups_season_list)
            self.cursor.execute(sel_gr_par_qry% values)
            data  = self.cursor.fetchall()
            groups_par_season_list = []
            for data_ in data:
                groups_par_season = str(data_[0])
                groups_par_season_list.append(groups_par_season)
            groups_par_season = "<>".join(groups_par_season_list)
            self.cursor.execute(sel_roster_qry% values)
            data  = self.cursor.fetchall()
            roster_season_list = []
            for data_ in data:
                roster_season = str(data_[0])
                roster_season_list.append(roster_season)
            roster_season = "<>".join(roster_season_list)
                
            web_data = (tou_id, tou_gid, tou_title, sport, tou_start, tou_end, game_status, min_datetime, max_datetime, tou_standings_season, tou_par_season, roster_season, groups_season, groups_par_season, tou_seasons, tou_start, tou_end, game_status, min_datetime, max_datetime, tou_standings_season, tou_par_season, roster_season, groups_season, groups_par_season, tou_seasons)
            ins_qry = 'insert into WEBSOURCEDB.sports_tournaments_details(id, gid, title, game, season_start, season_end, game_status, min_datetime, max_datetime, tou_standings_season, tou_par_season, roster_season, groups_season, groups_par_season, tournament_seasons, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), season_start=%s, season_end=%s, game_status=%s, min_datetime=%s, max_datetime=%s, tou_standings_season=%s, tou_par_season=%s, roster_season=%s, groups_season=%s, groups_par_season =%s, tournament_seasons=%s'
            self.cursor.execute(ins_qry, web_data) 

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','sports_tou_details*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def run_main(self):
        self.tournaments_details()


if __name__ == '__main__':
    vtv_task_main(TournamentsInfo)

