# -*- coding: utf-8 -*-
import sys
import os
import json
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

class AddAttributeInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.machine_ip = '10.28.216.45'
        self.wiki_db    = "GUIDMERGE"
        self.logs_path  = "/var/tmp/"
        self.log_pat    = "wiki_sports_tournaments_from_templates.json"

        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def copy_latest_file(self):
        mc_path  = "%s%s" %(self.logs_path, self.log_pat)
        source   = '%s@%s:%s' % ("veveo", self.machine_ip, mc_path)
        status   = scp("veveo123", source, self.logs_path)
        if status != 0:
             self.logger.info('Failed to copy the file: %s:%s' %(self.machine_ip, self.log_pat))
             sys.exit()


    def get_tournement(self):
        _data = open('wiki_sports_tournaments_from_templates.json', 'r+')
        for data in _data:
            _data = json.loads(data.strip())
            leagues_list = ['league', 'domestic']
            sb_list = ['men', 'youth']
            for league in leagues_list:
                for sb_lis in sb_list:
                    try:         
                        major_leagues =  _data.values()[0].get('tournament', '').get(league, '').get(sb_lis, '')
                    except:
                        continue
                    for mg_leagues in major_leagues:
                        league_name = mg_leagues
                        leagune_nm = league_name.split('{')[0].strip()
                        leagune_gid = league_name.split('{')[-1].strip().replace('}', '')
                        attribute = league + "<>"+sb_lis
                        attribute = attribute.replace('<>men', '').title()
                        wiki_gid_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid=%s'
                        tm_values = (leagune_gid)
                        self.cursor.execute(wiki_gid_qry, tm_values )
                        tm_data = self.cursor.fetchone()
                        if tm_data:
                            tou_gid = tm_data[0]
                            sel_query = 'select attribute from sports_tournaments where gid=%s'
                            tou_values = (tou_gid)
                            self.cursor.execute(sel_query, tou_values)
                            att_data = self.cursor.fetchone()
                            if att_data:
                                if att_data[0] !='' and "League" not in att_data[0] and "Domestic" not in att_data[0]:
                                    attribute = att_data[0]+"<>" +attribute
                            up_qry = 'update sports_tournaments set attribute=%s where gid=%s'
                            values = (attribute, tou_gid)
                            self.cursor.execute(up_qry, values)


    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','add_attribute*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.copy_latest_file()
        self.get_tournement()


if __name__ == '__main__':
    vtv_task_main(AddAttributeInfo)

