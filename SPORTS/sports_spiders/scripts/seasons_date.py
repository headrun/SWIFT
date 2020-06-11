# -*- coding: utf-8 -*-
import sys
import datetime
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

    def get_tournement(self):
        _data = open('seasons_id.txt', 'r+')
        none_ = []
        list_ = []
        for data in _data:
            data = data.strip()
            tid = int(data)
            yr = '%2017%'

            sel_qry = 'select season_start,season_end from sports_tournaments_seasons where tournament_id = %s'
            values = (tid)
            self.cursor.execute(sel_qry, values)
            oldies = self.cursor.fetchall()
            if not oldies:
                none_.append(tid)
                print tid
                print "No seasons"
                continue

            st_oldies = str(oldies[-1][0]).split('-')[0]
            end_oldies = str(oldies[-1][0]).split('-')[1]
            if st_oldies != end_oldies:
                list_.append(tid) 
                print tid
                print "NT Equal"
                upd_end_date = oldies[-1][1]+datetime.timedelta(days=60)
                end_date = str(upd_end_date)

                sel_qry = "select min(game_datetime),max(game_datetime) from sports_games where tournament_id = %s and game_datetime > %s"
                values = (tid,end_date)
                self.cursor.execute(sel_qry, values)
                dates = self.cursor.fetchall()
                if None in dates[0]:
                    continue
                season_start = str(dates[0][0]).split(' ')[0]
                season_end = str(dates[0][1]).split(' ')[0]
                print dates

                ins_qry = 'insert into sports_tournaments_seasons(tournament_id,season,season_start,season_end,created_at,modified_at) values(%s,%s,%s,%s,now(),now())'
                year = '2019-20'
                values = (tid,year,season_start,season_end)
                self.cursor.execute(ins_qry, values)


    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','duplicates_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.get_tournement()


if __name__ == '__main__':
    vtv_task_main(CheckDuplicateInfo)


    



