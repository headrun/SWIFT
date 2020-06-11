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

class CheckDuplicateInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def get_tournement(self):
        _data = open('teams.txt', 'r+')

        for data in _data:
            print data
            exposed = data.split('<>')[0]
            child_gids = data.split('<>')[1].strip('\n')
            gids = child_gids.split(',')
            for gid in gids:
                if gid.startswith('R'):
                    dgid = gid.replace('R','')
                    sel_qry = 'select id from sports_participants where gid =%s'
                    values = (dgid)
                    self.cursor.execute(sel_qry, values)
                    dteam_id = self.cursor.fetchall()
                    dteam_id = int(dteam_id[0][0])
                else:
                    pgid = gid
                    sel_qry = 'select id from sports_participants where gid =%s'
                    values = (pgid)
                    self.cursor.execute(sel_qry, values)
                    team_id = self.cursor.fetchall()
                    team_id = int(team_id[0][0])
            print pgid, dgid
            sel_qry = 'select aka from sports_participants where gid =%s'
            values = (dgid)
            self.cursor.execute(sel_qry, values)
            aka = self.cursor.fetchall()
            daka = str(aka[0][0])

            sel_qry = 'select title, aka from sports_participants where gid =%s'
            values = (pgid)
            self.cursor.execute(sel_qry, values)
            vakas = self.cursor.fetchall()
            akas = str(vakas[0][-1])
            title = str(vakas[0][0])

                
            print "Here"
                
            sel_qry = 'update sports_source_keys set entity_id = %s where entity_id = %s and entity_type = "participant"'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)
            print "H1"

            pidss = []
            dpidss = []
            sel_qry = 'update sports_roster set team_id = %s where team_id = %s'
            values = (team_id,dteam_id)
            try:
               self.cursor.execute(sel_qry, values)
            except:
               sel_qry = 'select player_id from sports_roster  where team_id = %s'
               values =(team_id)
               self.cursor.execute(sel_qry, values)
               pids = self.cursor.fetchall()
               for pid in pids:
                   pid = str(pid[0])
                   pidss.append(pid)
               sel_qry = 'select player_id from sports_roster  where team_id = %s'
               values = (dteam_id)
               self.cursor.execute(sel_qry, values)
               dpids = self.cursor.fetchall()
               for dpid in dpids:
                   dpid = str(dpid[0])
                   dpidss.append(dpid)
               difpids = set(dpidss)-set(pidss)
               difpids = sorted(difpids)
               for difpid in difpids:
                   sel_qry = 'update sports_roster set team_id = %s where team_id = %s and player_id=%s'
                   values = (team_id,dteam_id,difpid)
                   self.cursor.execute(sel_qry, values)
               cpids = set(dpidss)& set(pidss)
               cpids = sorted(cpids)
               for cpid in cpids:
                   sel_qry = 'delete from sports_roster where team_id = %s and player_id=%s'
                   values = (dteam_id,cpid)
                   self.cursor.execute(sel_qry, values)
                
            tidss = []
            dtidss = []
            sel_qry = 'update sports_tournaments_participants set participant_id = %s where participant_id = %s'
            values = (team_id,dteam_id)
            try:
                self.cursor.execute(sel_qry, values)
            except:
                sel_qry = 'select tournament_id from sports_tournaments_participants where participant_id = %s'
                values = (team_id)
                self.cursor.execute(sel_qry, values)
                tids = self.cursor.fetchall()
                for tid in tids:
                    
                    tid = str(tid[0])
                    tidss.append(tid)

                sel_qry = 'select tournament_id from sports_tournaments_participants where participant_id = %s'
                values = (dteam_id)
                self.cursor.execute(sel_qry, values)
                dtids = self.cursor.fetchall()
                for dtid in dtids:
                    dtid = str(dtid[0])
                    dtidss.append(dtid)
                diftids = set(dtidss)-set(tidss)
                diftids = sorted(diftids)
                for diftid in diftids:
                    sel_qry = 'update sports_tournaments_participants set participant_id = %s where participant_id = %s and tournament_id=%s'
                    values = (team_id,dteam_id,diftid)
                    self.cursor.execute(sel_qry, values)
                ctids = set(dtidss)& set(tidss)
                ctids = sorted(ctids)
                for ctid in ctids:
                    sel_qry = 'delete from sports_tournaments_participants where participant_id = %s and tournament_id=%s'
                    values = (dteam_id,ctid)
                    self.cursor.execute(sel_qry, values)

            sel_qry = 'update sports_tournaments_results set participant_id = %s where participant_id = %s'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)
        
            sel_qry = 'update sports_groups_participants set participant_id = %s where participant_id = %s'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)

            sel_qry = 'update sports_groups_results set participant_id = %s where participant_id = %s'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)

            sel_qry = 'update sports_games_participants set participant_id = %s where participant_id = %s'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)
            

            sel_qry = 'update sports_games_results set participant_id = %s where participant_id = %s'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)

            sel_qry = 'update sports_games_results set result_value = %s where result_value = %s and result_type = "winner"'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)

            del_qry = 'delete from sports_games_results where participant_id = %s'
            values = (dteam_id)
            self.cursor.execute(del_qry, values)

            sel_qry = 'update sports_tournaments_results set result_value = %s where result_value = %s and result_type = "winner"'
            values = (team_id,dteam_id)
            self.cursor.execute(sel_qry, values)
        
            sel_qry = 'update sports_awards_results set participants = %s where participants = %s'
            values = (pgid,dgid)
            self.cursor.execute(sel_qry, values)

            '''sel_qry = 'update AWARDS.sports_awards_history set participants = %s where participants = %s'
            values = (pgid,dgid)
            self.cursor.execute(sel_qry, values)'''


            up_qry = 'update sports_images_mapping set entity_id = %s where entity_id = %s and entity_type = "team"'
            values = (team_id,dteam_id)
            self.cursor.execute(up_qry, values)
            print "sports_images_mapping"


            up_qry = 'update sports_radar_images_mapping set entity_id = %s where entity_id = %s and entity_type = "team"'
            values = (team_id,dteam_id)
            self.cursor.execute(up_qry, values)
            print "sports_radar_images_mapping"
            
            draid_ = []
            raid_ = []
            up_qry = 'update SPORTSRADARDB.sports_radar_merge set sportsdb_id= %s where sportsdb_id = %s and type = "team"'
            values = (team_id,dteam_id)
            try:
                self.cursor.execute(up_qry, values)
            except:
                sel_qry = 'select radar_id from SPORTSRADARDB.sports_radar_merge where sportsdb_id = %s and type = "team"'
                values = (team_id)
                self.cursor.execute(sel_qry, values)
                raids = self.cursor.fetchall()
                for raid in raids:
                
                    raid = str(raid[0])
                    raid_.append(raid)

                sel_qry = 'select radar_id from SPORTSRADARDB.sports_radar_merge where sportsdb_id = %s and type = "team"'
                values = (dteam_id)
                self.cursor.execute(sel_qry, values)
                draids = self.cursor.fetchall()

                for draid in draids:
                    draid = str(draid[0])
                    draid_.append(draid)
        
                for raid in raid_:
                    for draid in draid_:
                        if raid == draid:
                            sel_qry = 'delete from SPORTSRADARDB.sports_radar_merge where sportsdb_id = %s and type = "team" and radar_id = %s'
                            values = (dteam_id,raid)
                            self.cursor.execute(sel_qry, values)
                        if raid != draid:
                            up_qry = 'update SPORTSRADARDB.sports_radar_merge set sportsdb_id= %s where sportsdb_id = %s and type = "team" and radar_id= %s'
                            values = (team_id,dteam_id,draid)
                            self.cursor.execute(up_qry, values)
                            
                    
                print "SPORTSRADARDB.sports_radar_merge"

            del_qry = 'delete from sports_participants where gid = %s'
            values = (dgid)
            self.cursor.execute(del_qry, values)
            print "deletd: %s" %(dgid)

            del_qry = 'delete from sports_teams where participant_id = %s'
            values = (dteam_id)
            self.cursor.execute(del_qry, values)
            print "deletd: %s" %(dteam_id)

            del_qry = 'delete from GUIDMERGE.teams_guid_merge where child_gid=%s'
            values = (dgid)
            self.cursor.execute(del_qry, values)
            print "deletd: %s" %(dgid)

            if  aka == '' and daka != title:
                sel_qry = 'update sports_participants set aka = %s where id = %s'
                values = (daka,team_id)
                self.cursor.execute(sel_qry, values)
            else:
                if daka in akas:
                    continue
                daka = akas + "###" + daka
                sel_qry = 'update sports_participants set aka = %s where id = %s'
                values = (daka,team_id)
                self.cursor.execute(sel_qry, values)

            print team_id,dteam_id 
 
            



    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','duplicates_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.get_tournement()


if __name__ == '__main__':
    vtv_task_main(CheckDuplicateInfo)


    



