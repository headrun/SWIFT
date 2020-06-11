# -*- coding: utf-8 -*-
import sys
import os
import redis
import logging
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
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
        self.parser.add_option('', '--source', default=False, help='loading sports')
        
    def get_action(self):
        self.act = {}
        self.tim = {}
        self.tab = {}
        navals = []
        typ = ['sports', 'teams', 'tournament', 'groups', 'stadiums', 'player', 'genre', 'language', 'region', 'production_house', 'award']
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')
        for ty in typ:
            self.fields_file_1 = os.path.join('/home/veveo/aravind/dmukka/madhav','gid_fields_LA.dump')
            fd1 = open(self.fields_file_1, 'w')
            logging.info('Ty is %s' %ty)
            self.tab = {'sports': 'sports_wiki_merge', 'stadiums': 'sports_wiki_merge', 'region': 'region_guid_merge',
                        'teams': 'sports_wiki_merge', 'tournament': 'sports_wiki_merge', 'groups': 'sports_wiki_merge',
                        'player': 'sports_wiki_merge', 'genre': 'genre_guid_merge', 'language': 'language_guid_merge',
                        'production_house': 'productionhouse_guid_merge', 'award': 'award_guid_merge'}
            merge = self.tab[ty]
            tablename = "verification_%sverificationtable" % (ty)
            sel_qry = 'select guid1,guid2 from %s '
            values = (tablename)
            self.cursor.execute(sel_qry%values)
            db_data = self.cursor.fetchall()
            db_data = set(db_data)

            for dbv in db_data:
                guid1,guid2 = dbv
                if not (guid1.startswith('LA') and guid2.startswith('WIKI')):
                    continue
                '''if not guid2.startswith('AW'):
                    continue'''
                gi = '%s#<>#%s' % (guid1, guid2)
                fi = '%s#<>#%s\n' % (guid1, guid2)
                navals.append(gi)
                print dbv

                fd1.write(fi)
            fd1.close()

        
        return ty, tablename, navals

    def redis_check(self, ty, tablename, navals):
        obj = redis.Redis('10.8.24.136', port = 6379, db = 8)
        pipe = obj.pipeline()
        obj1 = redis.Redis('10.8.24.136', port = 6379, db = 7)
        pipe = obj1.pipeline()
        logging.info("navals = %s" % navals)
        for nav in navals:
            reactions = []
            nav = nav.split('#<>#')     
            exp = nav[0]
            chi = nav[1]
            stype = ty.upper()

            check = stype + '#<>#' + chi
            parent_merge = obj1.get(check)   #values from redis 7
            logging.info("Redis 7 values for the key %s,values: %s" %(check,parent_merge))
            try:
                redisparent = parent_merge.split('<<>>')
            except:
                continue
            redtype = redisparent[1]
            red = redisparent[0].split('#<>#')
            if red:
                reparent = red[0].split('<>')[0]
                logging.info("Deleting key from REDIS 7 %s" %check)
                obj1.delete(check)
                print "removed from redis 7"
                logging.info("Deleting child values from table %s and value %s" %(redtype,chi))
                #redtypes = ['AWARD-AWARD-less_confident-less_popular-reject','AWARD-AWARD-less_confident-less_popular-accept','AWARD-AWARD-less_confident-less_popular-not_attempted']
                redtypes = ['LANGUAGE-LANGUAGE-confident-more_popular-accept','LANGUAGE-LANGUAGE-confident-more_popular-not_attempted','LANGUAGE-LANGUAGE-confident-more_popular-reject','LANGUAGE-LANGUAGE-confident-more_popular-not_sure','LANGUAGE-LANGUAGE-confident-less_popular-accept']
                for redtype in redtypes:
                    obj.lrem(redtype, chi)
                print "removed from redis 8"

            del_qry = 'delete from %s where guid1 = "%s" and guid2 = "%s"'
            values = (tablename,exp,chi)
            self.cursor.execute(del_qry%values)
            self.conn.commit()
            logging.info("deleting from the table %s for values guid1: %s and guid2: %s" %(tablename,exp,chi))
            logging.info("---------------------------------------------------------------------------------------")

 


    


    def run_main(self):
        ty,tablename,navals = self.get_action()
        self.redis_check(ty,tablename,navals)


if __name__ == '__main__':
    vtv_task_main(GuidMerge)


    



