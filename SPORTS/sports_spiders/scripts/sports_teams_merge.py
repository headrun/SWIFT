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


class TeamsRoviMergeInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')


    def teams_merge_info(self):
        sel_qry = 'select T.id, T.title, SP.id, SP.title, Ty.title from sports_participants SP, sports_tournaments_participants TP, sports_tournaments T, sports_types Ty where T.id=TP.tournament_id and TP.participant_id=SP.id and Ty.id=T.sport_id and T.id in (2841, 2850, 1891, 3419, 558, 564, 1105, 599, 569, 570, 575, 578, 609, 559, 567, 28, 2207, 5476, 571, 3601, 574, 4096, 580, 3838, 610, 3626, 240, 197, 88, 229, 35, 32, 33, 2553, 631, 579, 29, 573, 215, 216, 1825, 2209, 1895, 954, 1850, 586, 2905, 34, 214, 562, 572, 585, 589, 590, 591, 595, 596, 597, 598,1064,2235) order by T.title'
        #sel_qry = 'select T.id, T.title, SP.id, SP.title, Ty.title from sports_participants SP, sports_tournaments_participants TP, sports_tournaments T, sports_types Ty where T.id=TP.tournament_id and TP.participant_id=SP.id and Ty.id=T.sport_id and T.id in (215) order by T.title'
        
        #mapping_qry = 'select TP.participant_id, TP.tournament_id, TM.tou_id, TM.rovi_id from sports_tournaments_participants TP, WEBSOURCEDB.sports_teams_merge TM where  TP.participant_id = TM.team_id and TP.tournament_id in (216) and TM.rovi_id !=0 and TM.tou_id in (216)'
        #team_merge = "select tou_id, team_id from WEBSOURCEDB.sports_teams_merge where tournament_id in (215)"

        self.cursor.execute(sel_qry)
        #self.cursor.execute(team_merge)
        data = self.cursor.fetchall()
        #mer_data = self.cursor.fetchall()
        for data_ in data:
            tou_id, tou_title, sp_id, sp_title, sport = data_
            rovi_values = (str(sp_id))
            rovi_qry = 'select rovi_id from sports_rovi_merge where entity_type="team" and entity_id=%s'
            self.cursor.execute(rovi_qry% rovi_values)
            rovi_data = self.cursor.fetchall()
            rovi_id = ''
            if rovi_data:
                rovi_id = str(rovi_data[0][0])

            web_data = (tou_id, tou_title, sp_id, sp_title, sport, rovi_id, rovi_id)
            if rovi_id != 0:
                ins_qry = 'insert into WEBSOURCEDB.sports_teams_merge(tou_id, tou_title, team_id, team_title, game, rovi_id, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), rovi_id=%s'
                self.cursor.execute(ins_qry, web_data)

        #for a in sel_qry in (T.id, SP.id):
            #for b in team_merge in (tou_id, team_id):
                #if data[a] == data[b]:
                    #self.cursor.execute()
        """check_qry  = "select rovi_id from WEBSOURCEDB.sports_teams_merge where rovi_id != 0"
        self.cursor.execute(check_qry)
        rov_ids = self.cursor.fetchall()"""
         
        self.cursor.close()
        self.conn.close()

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','sports_teams_merge*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def run_main(self):
        self.teams_merge_info()


if __name__ == '__main__':
    vtv_task_main(TeamsRoviMergeInfo)

