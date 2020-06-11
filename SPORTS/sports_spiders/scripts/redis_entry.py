# -*- coding: utf-8 -*-
import sys
import os
import redis
import logging
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

class GuidMerge(VtvTask):


    def set_options(self):
        #self.parser.add_option('', '--sports', default=False, help='loading sports')
        self.parser.add_option('', '--source', default=False, help='loading sports')
        
    def get_action(self):
        obj = redis.Redis('10.8.24.136', port = 6379, db = 8)
        pipe = obj.pipeline()
        obj1 = redis.Redis('10.8.24.136', port = 6379, db = 7)
        pipe = obj1.pipeline()
        typ = ['sports', 'teams', 'tournament', 'groups', 'stadiums', 'player', 'genre', 'language', 'region', 'production_house', 'award']
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')
        for ty in typ:
            logging.info('Ty is %s' %ty)
            ty = ty.upper()
            _data = open('stad_redis.txt').readlines() 
            for line in _data:
                line = line.split(' ')
                key = line[0]
                source , red8 = key.split('#<>#')
                red7 = line[1].strip(':').strip('{').strip("'")
                action = line[2].strip('\n').strip('}').strip("'")
                redtype = '%s-%s-confident-less_popular-%s' %(ty,ty,action)
                merge = obj1.get(key)
                if not merge:
                    print "here"
                    keyvalue = "%s<>0.0<>%s<<>>%s" %(red7, action, redtype)
                    obj1.set(key, keyvalue)
                    obj.rpush(redtype, red8)
                else:
                    keyvalues , redtype = merge.split('<<>>')
                    vs = keyvalues.split('#<>#')
                    if len(vs) > 1:
                        kvalue = '%s<>0.0<>%s' %(red7 , action)
                        if kvalue in vs:
                            continue
                        vs.append(kvalue)
                        vs1 = '#<>#'.join(vs)
                        if action not in redtype:
                            if action == 'accept':
                                redtype = redtype.replace('reject','accept')
                        keyvalues = vs1 + '<<>>' + redtype
                        obj1.set(key, keyvalues)
                        obj.rpush(redtype, red8)
                    else:
                        wikikey, score, redaction = keyvalues.split('<>')
                        if (wikikey == red7 and redaction == action):
                            continue
                        else:
                            new_keyvalue = "%s<>0.0<>%s" %(red7, action)
                            keyvalues = keyvalues + "#<>#" + new_keyvalue
                            if action not in redtype:
                                if action == 'accept':
                                    redtype = redtype.replace('reject','accept')
                            keyvalues = keyvalues + '<<>>' + redtype
                            obj1.set(key, keyvalues)    
                            obj.rpush(redtype, red8)






    def run_main(self):
        self.get_action()


if __name__ == '__main__':
    vtv_task_main(GuidMerge)


    



