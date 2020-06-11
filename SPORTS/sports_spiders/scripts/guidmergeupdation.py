# -*- coding: utf-8 -*-
import sys
import os
import json
import subprocess
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp
from guidPairLoaderUtils import guidloadGuidPairList
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
        self.db_ip1    = "10.28.218.81"
        self.cur, self.con = get_mysql_connection(self.logger, self.db_ip1,
                                                      self.db_name1, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def set_options(self):
        #self.parser.add_option('', '--sports', default=False, help='loading sports')
        self.parser.add_option('', '--source', default=False, help='loading sports')

    def get_action(self):
        self.tab = {}
        typ = ['sports', 'teams', 'tournament', 'groups', 'stadiums', 'player', 'genre', 'language', 'region', 'production_house', 'award', 'sequel']
        cact = nact = nsact = 0
        ext_typ = self.options.source
        if ext_typ:
            typ = ext_typ.split(',')
        gid_merge_file = os.path.join(self.datagen_dirs.wiki_guid_merge_data_dir, 'wiki_guid_merge.list')
        gid_merge_dict = {}
        guidloadGuidPairList(gid_merge_file, gid_merge_dict)
        for ty in typ:
            #self.fields_file_1 = os.path.join('/home/veveo/bibeejan/scripts/madhav', 'gid_fields_1.dump')
            #fd1 = open(self.fields_file_1, 'w')
            print ty
            self.tab = {'sports': 'sports_guid_merge','stadiums': 'stadiums_guid_merge','region': 'region_guid_merge',
                        'teams': 'teams_guid_merge','tournament': 'tournament_guid_merge','groups': 'groups_guid_merge',
                        'player': 'player_guid_merge','genre': 'genre_guid_merge','language': 'language_guid_merge',
                        'production_house': 'productionhouse_guid_merge','award': 'award_guid_merge','sequel': 'sequel_guid_merge'}
            merge = self.tab[ty]
            tablename = "verification_%sverificationtable" % (ty)
            sel_qry = "select guid1, guid2, action from %s where action in ('accept', 'reject') order by date_updated desc"
            values = (tablename)
            self.cursor.execute(sel_qry % values)
            filename = '/tmp/gmtest_%s_verified_pairs.gids' % ty
            fp = open(filename, 'w')
            fp_reject = open('%s.rejectgm' % filename, 'w')
            processed_pair_set = set()
            accepted_gid_set = set()
            for dbd in self.cursor.fetchall():
                guid1, guid2, action = dbd
                key = '%s<>%s' % (guid1, guid2)
                if key in processed_pair_set:
                    continue
                if (guid2 in accepted_gid_set and action == 'accept'):
                    continue
                if (action == 'accept'):
                    accepted_gid_set.add(guid2)
                processed_pair_set.add(key)
                if guid2.startswith('W'):
                    action = 'reject'
                    data = '%s<>%s\n' % (key, action)
                    fp_reject.write(data)
                data = '%s<>%s\n' % (key, action)
                fp.write(data)
            fp_reject.close()
            fp.close()
           

            processed_child_gid_set = set()
            for line in open(filename):
                guid1, guid2, action = line.strip().split('<>')[:3]
                guid1 = gid_merge_dict.get(guid1, guid1)
                if action == 'accept':
                    processed_child_gid_set.add(guid2)
                    ins_qry = 'insert into %s (exposed_gid, child_gid, action, created_at, modified_at) values("%s", "%s", "%s", now(),now()) on duplicate key update modified_at = now(), exposed_gid="%s", action="%s" '
                    action = 'override'
                    self.cur.execute(ins_qry % (merge, guid1, guid2, action, guid1, action))
                    self.con.commit()

            for line in open(filename):
                guid1, guid2, action = line.strip().split('<>')[:3]
                guid1 = gid_merge_dict.get(guid1, guid1)
                if (action == 'reject' and guid2 not in processed_child_gid_set):
                    ins_qry = 'insert into %s (exposed_gid, child_gid, action, created_at, modified_at) values("%s", "%s", "%s", now(),now()) on duplicate key update modified_at = now(), exposed_gid="%s", action="%s" '
                    action = 'ignore'
                    self.cur.execute(ins_qry % (merge, guid1, guid2, action, guid1, action))
                    self.con.commit()
                    processed_child_gid_set.add(guid2)
            
    def run_main(self):
        self.get_action()

if __name__ == '__main__':
    vtv_task_main(GuidMerge)
