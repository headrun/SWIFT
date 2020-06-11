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
        self.db_name1  = "GUIDMERGE"
        self.db_ip1    = "10.28.216.41"
        self.cur, self.con = get_mysql_connection(self.logger, self.db_ip1,
                                                      self.db_name1, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def set_options(self):
        #self.parser.add_option('', '--sports', default=False, help='loading sports')
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
            self.fields_file_1 = os.path.join('/home/veveo/bibeejan/scripts/madhav', 'gid_fields_0.dump')
            fd1 = open(self.fields_file_1, 'w')
            logging.info('Ty is %s' %ty)
            self.tab = {'sports': 'sports_wiki_merge', 'stadiums': 'sports_wiki_merge', 'region': 'region_guid_merge',
                        'teams': 'sports_wiki_merge', 'tournament': 'sports_wiki_merge', 'groups': 'sports_wiki_merge',
                        'player': 'sports_wiki_merge', 'genre': 'genre_guid_merge', 'language': 'language_guid_merge',
                        'production_house': 'productionhouse_guid_merge', 'award': 'award_guid_merge'}
            merge = self.tab[ty]
            tablename = "verification_%sverificationtable" % (ty)
            sel_qry = 'select guid1,guid2,action,date_updated from %s '
            values = (tablename)
            self.cursor.execute(sel_qry%values)
            db_data = self.cursor.fetchall()
            db_data = set(db_data)
            
            for dbd in db_data:
                guid1,guid2,action,date_updated = dbd
                pair = '%s#<>#%s' %(guid1, guid2)
                value = '%s#<>#%s' %(action, date_updated)
                self.act.setdefault(pair,[]).append(value)

            sel_qry = 'select guid1,guid2 from %s'
            values = (tablename)
            self.cursor.execute(sel_qry%values)
            db_vals = self.cursor.fetchall()
            db_vals = set(db_vals)

            for dbv in db_vals:
                guid1,guid2 = dbv
                gi = '%s#<>#%s\n' % (guid1, guid2)
                fd1.write(gi)
            fd1.close()
            _data = open('gid_fields_0.dump').readlines() 


            for line in _data:
                pairs = line.split('#<>#')
                guid1 = pairs[0]
                guid2 = pairs[1].strip('\n')
                key = guid1 + "#<>#" + guid2
                gpair = key

            
                multiaction = self.act[key]
                self.tim = {}
                for mul in multiaction:
                    key1 = mul.split('#<>#')[0]
                    key2 = mul.split('#<>#')[1]
                    #self.tim.setdefault(key1,[]).append(key2)
                    self.tim.setdefault(key1,[]).append(key2)

                vs = self.tim.values()
                vp = []
                for v in vs:
                    if len(v) > 1:
                        v = sorted(v)
                        v = v[-1]
                    else:
                        v = v[0]    
                    vp.append(v)
                vs = sorted(vp)
                lavs = vs[-1]


                for key,values in self.tim.iteritems():
                    if lavs == values:
                        action = key
                    elif lavs in values:
                        action = key
                if action != 'not_attempted':
                    continue
                else:
                    navals.append(gpair)

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
            deval = [exp + "<>0.0<>" + "not_attempted"]
            print red
            if len(red) <= 1:
                reparent = red[0].split('<>')[0]
                logging.info("Deleting key from REDIS 7 %s" %check)
                obj1.delete(check)
                print "removed from redis 7"
                logging.info("Deleting child values from table %s and value %s" %(redtype,chi))
                obj.lrem(redtype, chi)
                print "removed from redis 8"

                del_qry = 'delete from %s where guid1 = "%s" and guid2 = "%s" and action = "not_attempted"'
                values = (tablename,exp,chi)
                self.cursor.execute(del_qry%values)
                self.conn.commit()
                logging.info("deleting from the table %s for values guid1: %s and guid2: %s" %(tablename,exp,chi))
                logging.info("---------------------------------------------------------------------------------------")
            else:

                #for redis 7
                red = set(red)
                new_red = list(red - set(deval))
                for re in new_red:
                    re = re.split('<>')
                    react = re[-1]
                    reactions.append(react)
                    reactions = list(set(reactions))
                if 'not_sure' in reactions:
                    act = 'not_sure'
                elif 'accept' in reactions:
                    act = 'accept'
                else:
                    act = 'reject'
                old_redtype = redtype
                redtype = redtype.split('-')
                redtype[-1] = act
                new_re = '-'.join(redtype)
                new_red = '#<>#'.join(new_red)
                new_values = new_red + '<<>>' + new_re

                #new values in redis 7
                logging.info("updating the key %s with new values %s" %(check,new_values))
                obj1.set(check,new_values)
                logging.info("removing from the redis 8 key %s and values %s" %(old_redtype,chi))
                #new_values in redis 8
                obj.lrem(old_redtype,chi)
                logging.info("adding new values into redis 8 key %s and value %s" %(new_re,chi))
                obj.lpush(new_re,chi)

                del_qry = 'delete from %s where guid1 = "%s" and guid2 = "%s" and action = "not_attempted" '
                values = (tablename,exp,chi)
                self.cursor.execute(del_qry%values)
                self.conn.commit()
                logging.info("deleting from the table %s for values guid1: %s and guid2: %s" %(tablename,exp,chi))
                logging.info("-------------------------------------------------------------------------------------------") 

 


    


    def run_main(self):
        ty,tablename,navals = self.get_action()
        self.redis_check(ty,tablename,navals)


if __name__ == '__main__':
    vtv_task_main(GuidMerge)


    



