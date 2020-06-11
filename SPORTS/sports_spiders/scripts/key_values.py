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
tables_dict = ['player',] #'groups','teams','stadiums','genre','production_house','tournament','language','region','awards','sports']
obj = redis.Redis('10.8.24.136', port = 6379, db = 8)
pipe = obj.pipeline()
obj1 = redis.Redis('10.8.24.136', port = 6379, db = 7)
pipe = obj1.pipeline()

class GuidMerge(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name  = "MATCHING_TOOL"
        self.db_ip    = "10.8.24.136"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo',
                                                      passwd='veveo123')


    def redis_check(self,):
        for table in tables_dict:
            table = table.upper()
            keys = table + '#<>#*'
            key_values = obj1.keys(keys)
            for key_value in key_values:
                val = key_value.split('#<>#')[1]
                merge_values = obj1.get(key_value)
                bucket = merge_values.split('<<>>')[1]
                if not bucket.startswith('PL'):
                    print key_value
                    merge_values = merge_values.replace('SPORTS','PLAYER')
                    bucket = bucket.replace('SPORTS','PLAYER')
                    obj1.set(key_value, merge_values)
                    obj.rpush(bucket, val)
                else:
                    continue





    def run_main(self):
        self.redis_check()
        #self.left_drop()


if __name__ == '__main__':
    vtv_task_main(GuidMerge)

