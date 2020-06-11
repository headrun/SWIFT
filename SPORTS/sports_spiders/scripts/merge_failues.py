#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys 
#import pyes
import json
import time
import traceback
import glob

#from pyes import *
from datetime import datetime
from datetime import timedelta
from data_schema import get_schema
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import copy_file,execute_shell_cmd
from ssh_utils import scp 
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

class MergeFailures(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.machine_ip = "10.28.218.71"
        self.logs_path  = "/home/veveo/datagen/SPORTS_MERGE_DATAGEN/"
        self.log_pat    = "sports_*_merge.txt"
        self.pattern1   = re.compile("In Old not in New: .*", re.I)
        self.pattern2   = re.compile('In Old not in New:\s(.*?)\s(.*?) #', re.I)
        self.db_ip      = "10.28.218.81"
        self.db_name    = "WEBSOURCEDB"

    def copy_latest_file(self):
        mc_path  = "%s%s" %(self.logs_path, self.log_pat)
        source       = '%s@%s:%s' % ("veveo", self.machine_ip, mc_path)
        status       = scp("veveo123", source, self.logs_path)
        if status != 0:
             self.logger.info('Failed to copy the file: %s:%s' %(self.machine_ip, self.log_pat))
             sys.exit()

    def read_files(self):
        all_files = glob.glob("%s%s" %(self.logs_path, self.log_pat))
        today  = datetime.today()
        self.open_cursor(self.db_ip, self.db_name)
        for fi in all_files:
            lines = file(fi, "r+").readlines()
            for line in lines:
                x = self.pattern1.findall(line)
                if x:
                    abc = x[0].split("#")
                    source  = abc[1].strip('"').strip("'"). \
                    replace('quidditch sport', 'quidditch').replace(' american ball game', '')
                    seed    = abc[2].strip('"').strip("'"). \
                    replace('quidditch sport', 'quidditch').replace(' american ball game', ''). \
                    replace(' basketball', '').replace(' football', '').replace('clube mg', 'clube')
                    if seed != source:
                        gid, gid2 = self.pattern2.findall(x[0])[0]
                        insert_qry = 'insert into sports_merge_failures(date, wiki_gid, sport_gid, need, got, status, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                        values = (today.date(), gid, gid2, source.replace(" '", "").replace("' ", ""), seed.replace(" '", "").replace("' ", ""), 0)
                        self.cursor.execute(insert_qry, values) 
 
    def run_main(self):
        self.copy_latest_file()
        self.read_files()

if  __name__ == "__main__":
    vtv_task_main(MergeFailures)
    sys.exit( 0 )

