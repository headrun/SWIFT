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
        self.db_name    = "MATCHING_TOOL"
        self.db_ip      = "10.8.24.136"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def set_options(self):
        #self.parser.add_option('', '--sports', default=False, help='loading sports')
        self.parser.add_option('', '--source', default=False, help='loading sports')

    def get_action(self):
        typ = ['sports','teams','tournament','groups','stadiums','player','genre','language','region','production_house','award']
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')
        for ty in typ:
            print ty
            tablename = "verification_%sverificationtable" % (ty)
            sel_qry = 'select distinct(action),guid1,guid2,min(date_updated) from %s group by guid1,guid2, action having count(*)>1' 
            values = (tablename)
            self.cursor.execute(sel_qry%values)
            db_data = self.cursor.fetchall()
            db_data = set(db_data)
            for dbd in db_data:
                action,guid1,guid2,date_updated = dbd
                pair = '%s<>%s' %(guid1,guid2)
                #out = '%s#<>#%s#<>#%s#<>#%s\n' % (guid1,guid2,action,date_updated)
                del_qry = 'delete from %s where guid_map = "%s" and action = "%s" and date_updated = "%s"'
                values = (tablename,pair,action,date_updated)
                self.cursor.execute(del_qry%values)


            
            print tablename




    def run_main(self):
        self.get_action()


if __name__ == '__main__':
    vtv_task_main(GuidMerge)
