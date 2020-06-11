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
        self.db_name1  = "GUIDMERGE"
        self.db_ip1    = "10.28.216.41"
        self.cur, self.con = get_mysql_connection(self.logger, self.db_ip1,
                                                      self.db_name1, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def set_options(self):
        #self.parser.add_option('', '--sports', default=False, help='loading sports')
        self.parser.add_option('', '--source', default=False, help='loading sports')
        
    def get_action(self):
        self.tab = {}
        typ = ['sports','teams','tournament','groups','stadiums','player','genre','language','region','production_house','award']
        cact = nact = nsact = 0
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')
        for ty in typ:
            print ty
            self.tab = {'sports': 'sports_wiki_merge','stadiums': 'sports_wiki_merge','region': 'region_guid_merge',
                        'teams': 'sports_wiki_merge','tournament': 'sports_wiki_merge','groups': 'sports_wiki_merge',
                        'player': 'sports_wiki_merge','genre': 'genre_guid_merge','language': 'language_guid_merge',
                        'production_house': 'productionhouse_guid_merge','award': 'award_guid_merge'}
            merge = self.tab[ty]
            tablename = "verification_%sverificationtable" % (ty)
            


            #sel_qry = 'select guid1,guid2 from %s where guid2 in (select guid1 from verification_genreverificationtable where guid2 like "%WIKI%")'
            sel_qry = 'select guid1,guid2,attributes from %s'
            values = (tablename)
            self.cursor.execute(sel_qry%values)
            dup = self.cursor.fetchall()
            dup = set(dup)
            for du in dup:
                g1 = du[0]
                g2 = du[1]
                g3 = du[2]
                if ty == 'language':
                    if (g1.startswith('W') and g2.startswith('L')) or (g1.startswith('W') and g2.startswith('R')) or (g1.startswith('L') and g2.startswith('R')):
                        continue
                    else:
                        print '%s<>%s<>%s' %(g1,g2,g3)
                if ty == 'region':
                    if (g1.startswith('W') and g2.startswith('R')) or (g1.startswith('R') and g2.startswith('RV')):
                        continue
                    else:
                        print '%s<>%s<>%s' %(g1,g2,g3)

                if ty == 'award':
                    if (g1.startswith('W') and g2.startswith('FRB')) or (g1.startswith('FRB') and g2.startswith('A')) or (g1.startswith('W') and g2.startswith('A')) :
                        continue
                    else:
                        print '%s<>%s<>%s' %(g1,g2,g3)
                if ty == 'production_house':
                    if (g1.startswith('W') and g2.startswith('PC')) or  (g1.startswith('W') and g2.startswith('RV')) or (g1.startswith('RV') and g2.startswith('P')) :
                        continue
                    else:
                        print '%s<>%s<>%s' %(g1,g2,g3)
                if ty == 'genre':
                    if (g1.startswith('W') and g2.startswith('G')) or (g1.startswith('W') and g2.startswith('SG')) or (g1.startswith('W') and g2.startswith('RVG')) or (g1.startswith('G') and g2.startswith('RVG')) or (g1.startswith('W') and g2.startswith('CG')) or (g1.startswith('SG') and g2.startswith('RVG')) or (g1.startswith('CG') and g2.startswith('RVG')):
                        continue
                    else:
                        print '%s<>%s<>%s' %(g1,g2,g3)
                
                        
        return ty,tablename 
    def redis_check(self,ty,tablename):
        '''typ = ['sports','teams','tournament','groups','stadiums','player','genre','language','region','production_house','award']
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')'''
        obj = redis.Redis('10.8.24.136', port = 6379, db = 8)        
        pipe = obj.pipeline()
        obj1 = redis.Redis('10.8.24.136', port = 6379, db = 7)
        pipe = obj1.pipeline()
        if ty:
            print ty
            if ty :
                dupval = open('%s_wmerge.txt' % ty)
                values = dupval.read()
                vals = values.split('\n')    #Values taken from my text file
            for value in vals:
                pmerge = value.split('<>')[0]    
                child_value = value.split('<>')[1]
                attr = value.split('<>')[2]
                stype = ty.upper()

                check = stype + '#<>#' + child_value
                parent_merge = obj1.get(check)   #values from redis 7
                
                redisparent = parent_merge.split('<<>>')
                redtype = redisparent[1]
                red = redisparent[0].split('#<>#')
                print red[0]
                if len(red) <= 1:
                    reparent = red[0].split('<>')[0]
                else:
                    print red[0]
                    import pdb;pdb.set_trace()
                if pmerge == reparent:
                    #removing from redis 7
                    print check
                    obj1.delete(check)
                    print "removed from redis 7"
                    
                    obj.lrem(redtype, child_value)
                    print "removed from redis 8"
                    del_qry = 'delete from %s where guid1 = "%s" and guid2 = "%s" limit 2'
                    values = (tablename,pmerge,child_value)
                    self.cursor.execute(del_qry%values)
                    self.conn.commit()
                    print "removed from db"
                    
                
                    




    def run_main(self):
        ty,tablename = self.get_action()
        self.redis_check(ty,tablename)


if __name__ == '__main__':
    vtv_task_main(GuidMerge)


    



