# -*- coding: utf-8 -*-
import sys
import os
import json
import MySQLdb
import redis
import logging
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp
from collections import OrderedDict
fields_file_1 = os.path.join('/home/veveo/aravind/dmukka/madhav', 'drop.dump')
fd1 = open(fields_file_1, 'w')

merge_tables = {'sports': 'sports_guid_merge',
                'tournament': 'tournament_guid_merge',
                'groups': 'groups_guid_merge',
                'stadiums': 'stadiums_guid_merge',
                'player': 'player_guid_merge',
                'teams': 'teams_guid_merge',
                'language': 'language_guid_merge',
                'region': 'region_guid_merge',
                'genre': 'genre_guid_merge',
                'production_house': 'productionhouse_guid_merge',

}


class GuidMerge(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name1  = "SPORTSDB"
        self.db_ip1    = "10.28.218.81"
        self.cur, self.con = get_mysql_connection(self.logger, self.db_ip1,
                                                      self.db_name1, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        self.db_name  = "MATCHING_TOOL"
        self.db_ip    = "10.8.24.136"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo',
                                                      passwd='veveo123')
        self.db_name2  = "SEED"
        self.db_ip2    = "10.28.218.81"
        self.cur2, self.con2 = get_mysql_connection(self.logger, self.db_ip2,
                                                      self.db_name2, cursorclass='',
                                                      user = 'veveo', passwd='veveo123') 
        self.db_name3  = "GUIDMERGE"
        self.db_ip3    = "10.28.218.81"
        self.cur3, self.con3 = get_mysql_connection(self.logger, self.db_ip3,
                                                      self.db_name3, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def get_action(self):
        tables_dict = {'tournament': 'sports_tournaments',
                  'player': 'sports_participants',
                  'teams': 'sports_participants',
                  'sports': 'sports_types',
                  'groups': 'sports_tournaments_groups',
                  'stadiums': 'sports_stadiums',
                  'language': 'languages',
                  'genre': 'genres',
                  'region': 'regions',
                  'production_house': 'productionhouses',

                 }  
        gid_list = []
        toolgid_list = []
        exposed_gid = OrderedDict()
        tables = tables_dict.keys()
        for source in tables:
            gid_list = []
            toolgid_list = []
            try:
                sel_qry = 'select gid from %s ;'
                table = tables_dict.get(source)
                self.cur.execute(sel_qry % table)
                gids = self.cur.fetchall()
            except:
                sel_qry = 'select gid from %s ;'
                table = tables_dict.get(source)
                self.cur2.execute(sel_qry % table)
                gids = self.cur2.fetchall()
            for gid in gids:
                gid_list.append(gid)
            sel_qry = 'select guid2 from verification_%sverificationtable'
            self.cursor.execute(sel_qry % source)
            tool_gids = self.cursor.fetchall()
            for tool_gid in tool_gids:
                toolgid_list.append(tool_gid)
            glist = set(gid_list)
            tlist = set(toolgid_list)
            vs = tlist - glist
            for v in vs:
                if v[0].startswith('RVLAN') or v[0].startswith('RVREG'):
                    continue
                sel_qry = 'select guid1 from verification_%sverificationtable where guid2 = "%s"'
                values = (source, v[0])
                self.cursor.execute(sel_qry % values)
                tool_gids = self.cursor.fetchall()
                tool_gids = list(set(tool_gids))
                fd1.write(v[0])
                fd1.write('\n')
                print v[0]


    def redis_check(self,):
        obj = redis.Redis('10.8.24.136', port = 6379, db = 8)
        pipe = obj.pipeline()
        obj1 = redis.Redis('10.8.24.136', port = 6379, db = 7)
        pipe = obj1.pipeline()
        drop_childs= open('drop.dump').readlines()
        for child in drop_childs:
            child = child.strip('\n')
            if child.startswith('STAD'):
                source = 'STADIUMS'
            if child.startswith('TE'):
                source = 'TEAMS'
            if child.startswith('PL'):
                source = 'PLAYER'
            if child.startswith('SP'):
                source = 'SPORTS'
            if child.startswith('TOU'):
                source = 'TOURNAMENT'
            if child.startswith('GR'):
                source = 'GROUPS'
            if child.startswith('RVLAN'):
                source = 'LANGUAGE'

            stype = source.lower()
            guidmerge = merge_tables[stype]
            check = source + '#<>#' + child
            parent_merge = obj1.get(check)   #values from redis 7
            logging.info("Redis 7 values for the key %s,values: %s" %(check,parent_merge))
            try:
                redisparent = parent_merge.split('<<>>')
            except:
                print "child: %s" %child
                continue
            redtype = redisparent[1]
            if redtype:
                logging.info("Deleting key from REDIS 7 %s" %check)
                obj1.delete(check)
                print "removed from redis 7"
                logging.info("Deleting child values from table %s and value %s" %(redtype,child))
                obj.lrem(redtype, child)
                print "removed from redis 8"

                del_qry = 'delete from verification_%sverificationtable where guid2 = "%s" '
                values = (stype,child)
                self.cursor.execute(del_qry%values)
                self.conn.commit()
                logging.info("---------------------------------------------------------------------------------------")

                del_qry = 'delete from %s where child_gid = "%s"' 
                values = (guidmerge, child)
                self.cur3.execute(del_qry%values)
                self.con3.commit()

    def left_drop(self,):
        obj = redis.Redis('10.8.24.136', port = 6379, db = 8)
        pipe = obj.pipeline()
        obj1 = redis.Redis('10.8.24.136', port = 6379, db = 7)
        pipe = obj1.pipeline()
        drop_childs= open('leftdrop.txt').readlines()
        for child in drop_childs:
            child = child.strip('\n')
            if child.startswith('STAD'):
                source = 'STADIUMS'
            if child.startswith('TE'):
                source = 'TEAMS'
            if child.startswith('PL'):
                source = 'PLAYER'
            if child.startswith('SP'):
                source = 'SPORTS'
            if child.startswith('TOU'):
                source = 'TOURNAMENT'
            if child.startswith('GR'):
                source = 'GROUPS'
            stype = source.lower()
            guidmerge = merge_tables[stype]
            del_qry = 'delete from verification_%sverificationtable where guid2 = "%s" '
            values = (stype,child)
            self.cursor.execute(del_qry%values)
            self.conn.commit()
            logging.info("---------------------------------------------------------------------------------------")

            del_qry = 'delete from %s where child_gid = "%s"'
            values = (guidmerge, child)
            self.cur3.execute(del_qry%values)
            self.con3.commit()




    def run_main(self):
        self.get_action()
        #self.redis_check()
        #self.left_drop()


if __name__ == '__main__':
    vtv_task_main(GuidMerge)

