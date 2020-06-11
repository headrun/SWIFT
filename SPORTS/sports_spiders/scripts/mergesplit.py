# -*- coding: utf-8 -*-
import sys
import os
import json
import redis
import MySQLdb
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

class GuidMerge(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name1  = "GUIDMERGE"
        self.db_ip1    = "10.28.218.81"
        self.cur, self.con = get_mysql_connection(self.logger, self.db_ip1,
                                                      self.db_name1, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def set_options(self):
        #self.parser.add_option('', '--sports', default=False, help='loading sports')
        self.parser.add_option('', '--source', default=False, help='loading sports')

    def get_action(self):
        self.tab = {}
        typ = ['sports','teams','tournament','groups','stadiums','player']
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')
        for ty in typ:
            print ty
            self.tab = {'sports': 'sports_guid_merge','stadiums': 'stadiums_guid_merge',
                        'teams': 'teams_guid_merge','tournament': 'tournament_guid_merge','groups': 'groups_guid_merge',
                        'player': 'player_guid_merge'}
            
            sel_qry = 'select exposed_gid,child_gid,action from sports_wiki_merge'       
            self.cur.execute(sel_qry)
            dups = self.cur.fetchall() 
            for dup in dups:
                exp ,chi,act = dup
                if 'SPORT' in chi:
                    ty = 'sports'
                    merge = self.tab[ty]
                elif 'STAD' in chi:
                    ty = 'stadiums'
                    merge = self.tab[ty]
                elif 'TEAM' in chi:
                    ty = 'teams'
                    merge = self.tab[ty]
                elif 'TOU' in chi:
                    ty = 'tournament'
                    merge = self.tab[ty]
                elif 'GR' in chi:
                    ty = 'groups'
                    merge = self.tab[ty]
                elif 'PL' in chi:
                    ty = 'player'
                    merge = self.tab[ty]
                upd_qry = 'insert into %s(exposed_gid,child_gid,action,modified_date) values("%s","%s","%s",now()) on duplicate key update modified_date = now()' 
                values = (merge,exp,chi,act)
                self.cur.execute(upd_qry%values)



    def run_main(self):
        self.get_action()


if __name__ == '__main__':
    vtv_task_main(GuidMerge)

