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
        self.db_name    = "SPORTSDB_DEV"
        self.db_ip      = "10.28.216.45"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        #self.group_data = open('dups_data', 'w+')

    def update_duplicates(self):
        _data = open('triple_duplicates.txt', 'r+')
        for data in _data:
            dlist = []
            dname = []
            data = data.strip()
            if not data:
                continue
            data = data.split(',')
            wiki_gid = data[0].split(',')[0]
            wiki = wiki_gid.split('<>')[0]
            cgid = wiki_gid.split('<>')[1]
            sel_qry = 'select id from sports_stadiums where gid = %s'
            values = (cgid)
            self.cursor.execute(sel_qry, values)
            stad_id = self.cursor.fetchall()
            if not stad_id:
                continue
            stad_id = str(stad_id[0][0])
            sel_qry = 'select title from sports_stadiums where id = %s'
            values = (stad_id)
            self.cursor.execute(sel_qry, values)
            stad_name = self.cursor.fetchall()       
            stad_name = stad_name[0][0]
 
            rdups = data[1:]
            print cgid
            
            for rdup in rdups:
                fdup = rdup.split('<>')[0]
                dlist.append(fdup)
                fdup = rdup.split('<>')[1]
                dname.append(fdup)
    
            for dna in dname:
                if type(dna) == str:
                    continue
                    
                if dna == stad_name:
                    continue
                else:
                    sel_qry = 'select aka from sports_stadiums where id = %s'
                    values = (stad_id)
                    self.cursor.execute(sel_qry, values)
                    aka_name = self.cursor.fetchall()
                    aka_name = aka_name[0][0]
                    if aka_name == '' and aka_name != stad_name:
                        sel_qry = 'update sports_stadiums set aka = %s where id = %s'
                        values = (dna,stad_id)
                        self.cursor.execute(sel_qry, values)
                    else:
                        if dna in aka_name:
                            continue
                        dna = aka_name + "###" + dna
                        sel_qry = 'update sports_stadiums set aka = %s where id = %s'
                        values = (dna,stad_id)
                        self.cursor.execute(sel_qry, values)
            for dli in dlist:
                if dli == cgid:
                    continue
                else:
                    sel_qry = 'select id from sports_stadiums where gid = %s'
                    values = (dli)
                    self.cursor.execute(sel_qry, values)
                    dstad_id = self.cursor.fetchone()
                    if not dstad_id:
                        continue
                    else:
                        dstad_id = str(dstad_id[0])

                    
                    sel_qry = 'update sports_games set stadium_id = %s where stadium_id = %s'
                    values = (stad_id,dstad_id)
                    self.cursor.execute(sel_qry, values)

                    sel_qry = 'update sports_teams set stadium_id = %s where stadium_id = %s'
                    values = (stad_id,dstad_id)
                    self.cursor.execute(sel_qry, values)

                    sel_qry = 'update sports_tournaments set stadium_ids = %s where stadium_ids = %s'
                    values = (stad_id,dstad_id)
                    self.cursor.execute(sel_qry, values)
                    
                    sel_qry = 'delete from sports_stadiums where id = %s'
                    values = (dstad_id)
                    self.cursor.execute(sel_qry, values)




    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','duplicates_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.update_duplicates()


if __name__ == '__main__':
    vtv_task_main(CheckDuplicateInfo)


    



