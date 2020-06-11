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
        self.group_data = open('group_data', 'w+')


    def upd_qry (self,team_id,dteam_id,gid,team_gid):
        print team_id,dteam_id,gid,team_gid
        print "Here"
        up_qry = 'update sports_source_keys set entity_id = %s where entity_id = %s and entity_type = "participant"'
        values = (team_id,dteam_id)
        pskss = []
        dpskss = []

        try:
            self.cursor.execute(up_qry, values)
        except:
            sel_qry = 'select source_key from sports_source_keys where entity_id = %s and entity_type = "participant"'
            values = (team_id)
            self.cursor.execute(sel_qry, values)
            psks = self.cursor.fetchall()
            

            for psk in psks:
                psk = str(psk[0])
                pskss.append(psk)
            sel_qry = 'select source_key from sports_source_keys where entity_id = %s and entity_type = "participant"'
            values = (dteam_id)
            self.cursor.execute(sel_qry, values)
            dpsks = self.cursor.fetchall()
            for dpsk in dpsks:
                dpsk = str(dpsk[0])
                dpskss.append(dpsk)
            difsks = set(dpskss) - set(pskss)
            difsks = sorted(difsks)
            for difsk in difsks:
                up_qry = 'update sports_source_keys set entity_id = %s where entity_id = %s and entity_type = "participant" and source_key = %s'
                values = (team_id,dteam_id,difsk)
                self.cursor.execute(up_qry, values)
            csks = set(dpskss) & set(pskss)
            csks = sorted(csks)
            for csk in csks:
                del_qry = 'delete from sports_source_keys where entity_id = %s and entity_type = "participant" and source_key = %s'
                values = (dteam_id,csk)
                self.cursor.execute(del_qry, values)
            
        print "sports_source_keys 1"


        up_qry = 'update sports_games_participants set participant_id = %s where participant_id = %s'
        values = (team_id,dteam_id)
        self.cursor.execute(up_qry, values)
        print "sports_games_participants 2"


        up_qry = 'update sports_games_results set participant_id = %s where participant_id = %s '
        values = (team_id,dteam_id)
        pid_ = []
        dpid_ = []
        try:
            self.cursor.execute(up_qry, values)
        except:
            sel_qry = 'select result_type,result_value from sports_games_results  where participant_id = %s'
            values =(team_id)
            self.cursor.execute(sel_qry, values)
            pids = self.cursor.fetchall()
            for pid in pids:
                pid_.append(pid)
            sel_qry = 'select result_type,result_value from sports_games_results  where participant_id = %s'
            values =(dteam_id)
            self.cursor.execute(sel_qry, values)
            dpids = self.cursor.fetchall()
            for dpid in dpids:
                dpid_.append(dpid)
            cpid = set(pid_)& set(dpid_)
            for cpi in cpid:
                p1 = cpi[0]
                p2 = cpi[1]
                
                '''up_qry = 'update sports_games_results set participant_id = %s where participant_id = %s and result_type= %s and result_value= %s'
                values = (team_id,dteam_id,p1,p2)
                self.cursor.execute(up_qry, values)'''
            
                del_qry = 'delete from sports_games_results where participant_id = %s and result_type= %s and result_value= %s'
                values = (dteam_id,p1,p2)
                self.cursor.execute(del_qry, values)
            dpid = set(dpid_)- set(pid_)
            for dpi in dpid:
                p1 = dpi[0]
                p2 = dpi[1]

                up_qry = 'update sports_games_results set participant_id = %s where participant_id = %s and result_type= %s and result_value= %s'
                values = (team_id,dteam_id,p1,p2)
                self.cursor.execute(up_qry, values)
        






            
            print "Bleh"










        print "sports_games_results 3"

        up_qry = 'update sports_games_results set result_value = %s where result_value = %s'
        values = (team_id,dteam_id)
        self.cursor.execute(up_qry, values)
        print "sports_games_results result value 4"

        up_qry = 'update sports_roster set player_id= %s where player_id= %s'
        values = (team_id,dteam_id)
        pidss = []
        dpidss = []

        try:
            self.cursor.execute(sel_qry, values)
        except:
            
            sel_qry = 'select team_id from sports_roster  where player_id = %s'
            values =(team_id)
            self.cursor.execute(sel_qry, values)
            pids = self.cursor.fetchall()
            for pid in pids:
                pid = str(pid[0])
                pidss.append(pid)
            sel_qry = 'select team_id from sports_roster  where player_id = %s'
            values = (dteam_id)
            self.cursor.execute(sel_qry, values)
            dpids = self.cursor.fetchall()
            for dpid in dpids:
                dpid = str(dpid[0])
                dpidss.append(dpid)
            difpids = set(dpidss)-set(pidss)
            difpids = sorted(difpids)
            for difpid in difpids:
                sel_qry = 'update sports_roster set player_id = %s where player_id = %s and team_id=%s'
                values = (team_id,dteam_id,difpid)
                self.cursor.execute(sel_qry, values)
            cpids = set(dpidss)& set(pidss)
            cpids = sorted(cpids)
            for cpid in cpids:
                sel_qry = 'delete from sports_roster where player_id = %s and team_id=%s'
                values = (dteam_id,cpid)
                self.cursor.execute(sel_qry, values)
        print "sports_roster 5"
        
        up_qry = 'update sports_radar_images_mapping set entity_id = %s where entity_id = %s and entity_type = "player"'
        values = (team_id,dteam_id)
        self.cursor.execute(up_qry, values)
        print "sports_radar_images_mapping 6"


        up_qry = 'update sports_radar_tags set entity_id = %s where entity_type = "player" and entity_id = %s'
        values = (team_id,dteam_id)
        self.cursor.execute(up_qry, values)
        print "sports_radar_tags 7"

        up_qry = 'update sports_awards_results set participants = "%s" where participants = "%s"'
        values = (team_gid,gid)
        self.cursor.execute(up_qry, values)
        print "sports_awards_results 8"


        del_qry = 'delete from sports_tournaments_participants where participant_id = %s'
        values = (dteam_id)
        self.cursor.execute(del_qry, values)

        del_qry = 'delete from sports_tournaments_results where participant_id = %s'
        values = (dteam_id)
        self.cursor.execute(del_qry, values)

        del_qry = 'delete from sports_groups_results where participant_id = %s'
        values = (dteam_id)
        self.cursor.execute(del_qry, values)

        del_qry = 'delete from sports_groups_participants where participant_id = %s'
        values = (dteam_id)
        self.cursor.execute(del_qry, values)

        del_qry = 'delete from sports_tournaments_participants where participant_id = %s'
        values = (dteam_id)
        self.cursor.execute(del_qry, values)

        del_qry = 'delete from SPORTSRADARDB.sports_radar_merge where sportsdb_id = %s and type = "player"'
        values = (dteam_id)
        try:
            self.cursor.execute(del_qry, values)
        except:
            import pdb;pdb.set_trace()

        del_qry = 'delete from sports_players where participant_id = %s limit 1'
        values = (dteam_id)
        self.cursor.execute(del_qry, values) 

        del_qry = 'delete from sports_participants where gid = %s limit 1'
        values = (gid)
        self.cursor.execute(del_qry, values)   

    def get_tournement(self):
        _data = open('player_equal.txt', 'r+')

        for data in _data:
            print data
            sptid = []
            list_ = []
            data = data.strip()
            if not data:
                continue
            team_gid = data.split(',')[0].split('<>')[1] 
            if team_gid == 'NO CHILD':
                ts = []
                teams_gid = data.split(',')[1:] 
                for teams in teams_gid:
                    team = teams.split('<>') [0]
                    team = team.replace('PL','')
                    ts.append(team)
                if len(ts) <= 2:
                    if int(ts[0]) < int(ts[1]):
                        team_gid = 'PL' + str(int(ts[0]))
                    else:
                        team_gid = 'PL' + str(int(ts[1]))
                elif len(ts) == 3:
                    if int(ts[0]) < int(ts[1]):
                        tempgid = int(ts[0])
                        if tempgid < int(ts[2]):
                            team_gid = 'PL' + str(tempgid)
                        else:
                            team_gid = 'PL' + str(ts[2])
                    else:
                        tempgid = int(ts[1])
                        if tempgid < int(ts[2]):
                            team_gid = 'PL' + str(tempgid)
                        else:
                            team_gid = 'PL' + str(ts[2])
            
                    
                print team_gid


            sel_qry = 'select id from sports_participants where gid =%s'
            values = (team_gid)
            self.cursor.execute(sel_qry, values)
            team_id = self.cursor.fetchall()
            if not team_id:
                continue
            else:
                team_id = str(team_id[0][0])


            sel_qry = 'select aka from sports_participants where gid =%s'
            values = (team_gid)
            self.cursor.execute(sel_qry, values)
            aka = self.cursor.fetchall()
            aka = str(aka[0][0])

            dteam_gid = data.split(',')[1:]
            for gid in dteam_gid:
                gid = gid.split('<>')[0]
                if team_gid == gid:
                    continue

                sel_qry = 'select exposed_gid from GUIDMERGE.sports_wiki_merge where child_gid = %s'
                values = (gid)
                self.cursor.execute(sel_qry, values)
                dmerge = self.cursor.fetchall()
                if dmerge:
                    dmerge = str(dmerge[0][0])
                    continue




                print gid
                #sel_qry = 'select gid, group_name from sports_tournaments_groups where gid=%s'
                sel_qry = 'select id from sports_participants where gid =%s'
                values = (gid)
                self.cursor.execute(sel_qry, values)
                dteam_id = self.cursor.fetchall()
                if not dteam_id:
                    continue
                else:
                    dteam_id = str(dteam_id[0][0])

                sel_qry = 'select title from sports_participants where gid =%s'
                values = (gid)
                self.cursor.execute(sel_qry, values)
                daka = self.cursor.fetchall()
                daka = str(daka[0][0])
   
                self.upd_qry(team_id,dteam_id,gid,team_gid)

                if aka == daka:
                    print aka,daka 
                    continue 
                else:
                    sel_qry = 'select title from sports_participants where gid =%s'
                    values = (team_gid)
                    self.cursor.execute(sel_qry, values)
                    title = self.cursor.fetchall()
                    title = str(title[0][0])

                    sel_qry = 'select aka from sports_participants where gid =%s'
                    values = (team_gid)
                    self.cursor.execute(sel_qry, values)
                    akas = self.cursor.fetchall()
                    akas = str(akas[0][0])
                if  aka == '' and daka != title:
                    sel_qry = 'update sports_participants set aka = %s where id = %s'
                    values = (daka,team_id)
                    self.cursor.execute(sel_qry, values)
                else:
                    if daka in akas:
                       continue
                    if daka == title:
                       continue
                    if aka != '':
                        daka = akas + "###" + daka
                    else:
                        daka = daka
                    sel_qry = 'update sports_participants set aka = %s where id = %s'
                    values = (daka,team_id)
                    self.cursor.execute(sel_qry, values)

                print "Here"

                

                
                
                
            
                
                
                '''sel_qry = 'update sports_source_keys set entity_id = %s where entity_id = %s and entity_type = "participant"'
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

                sel_qry = 'update sports_tournaments_results set result_value = %s where result_value = %s and result_type = "winner"'
                values = (team_id,dteam_id)
                self.cursor.execute(sel_qry, values)
            
                sel_qry = 'update sports_awards_results set participants = %s where participants = %s'
                values = (team_gid,gid)
                self.cursor.execute(sel_qry, values)

                sel_qry = 'update AWARDS.sports_awards_history set participants = %s where participants = %s'
                values = (team_gid,gid)
                self.cursor.execute(sel_qry, values)

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
                values = (gid)
                self.cursor.execute(del_qry, values)

                del_qry = 'delete from sports_teams where participant_id = %s'
                values = (dteam_id)
                self.cursor.execute(del_qry, values)

                del_qry = 'delete from GUIDMERGE.sports_wiki_merge where child_gid=%s'
                values = (gid)
                self.cursor.execute(del_qry, values)

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
                    self.cursor.execute(sel_qry, values)'''

            print team_id,dteam_id
 
            



    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','duplicates_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.get_tournement()


if __name__ == '__main__':
    vtv_task_main(CheckDuplicateInfo)


    



