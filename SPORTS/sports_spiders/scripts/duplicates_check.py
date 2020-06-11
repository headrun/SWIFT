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
        self.group_data = open('dups_data', 'w+')

    def get_tournement(self):
        _data = open('dups.txt', 'r+')
        for data in _data:
            list_ = []
            data = data.strip()
            if not data:
                continue
            wiki_gid = data.split(',')[0]
            sel_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid=%s'
            values = (wiki_gid)
            self.cursor.execute(sel_qry, values)
            gp_data = self.cursor.fetchall()
            for gp_dat in gp_data:
                child_gid = str(gp_dat[0])
                wiki_data = wiki_gid + "<>" + child_gid
                list_.append(wiki_data)
            group_gid = data.split(',')[1:]
            for gid in group_gid:
                #sel_qry = 'select gid, group_name from sports_tournaments_groups where gid=%s'
                sel_qry = 'select gid, title,  location_id from sports_stadiums where gid =%s'
                values = (gid)
                self.cursor.execute(sel_qry, values)
                gp_data = self.cursor.fetchall()
                for gp_dat in gp_data:
                    child_gid = str(gp_dat[0])
                    name = str(gp_dat[1])
                    location_id = str(gp_dat[2])
                    if location_id == '0':
                        country = "NULL"
                        wiki_data = child_gid + "<>" + name + '<>' + location_id + '<>' + country
                    if location_id != '0':
                        sel_qry = 'select country from sports_locations where id =%s'
                        values = (location_id)
                        self.cursor.execute(sel_qry, values)
                        country = self.cursor.fetchall()
                        if country:
                            country = str(country[0][0])
                        else:
                            country = "NULL"
                    
                        wiki_data = child_gid + "<>" + name + '<>' + location_id + '<>' + country
                    #wiki_data = child_gid + "<>" + name
                    list_.append(wiki_data)
            '''if len(list_) <= 2:
                continue'''
            print list_






    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','duplicates_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.get_tournement()


if __name__ == '__main__':
    vtv_task_main(CheckDuplicateInfo)


    



