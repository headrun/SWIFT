#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import ssh_utils
import foldFileInterface
from vtv_task import VtvTask, vtv_task_main
from data_schema import get_schema
import traceback
from guidPairLoaderUtils import guidloadGuidPairList
from foldFileInterface import dataFileDictIterator

class SportsRovi(VtvTask):
    def __init__(self, options=None):
        VtvTask.__init__(self)
#        self.sportsdb_ip           = "10.28.218.81"
	self.sportsdb_ip = "10.28.216.45"
        self.sportsdb_name         = "SPORTSDB_BKP"
        self.log_ip                = '10.28.218.80'
        self.sportsdb_players_dict = {}
        #self.copy_files()
        self.rovi_sports_merge_data = os.path.join(self.datagen_dirs.rovi_sports_merge_data_dir, 'rovi_sports_guid_merge.list')
        self.sports_game_merge      = os.path.join(self.datagen_dirs.vtv_avail_data_dir, 'DATA_MC_FILE')
        self.content_guid_merge     = os.path.join('/home/veveo/datagen/current/content_data', 'guid_merge.list')
        self.sports_guid_merge      = os.path.join(self.datagen_dirs.sports_merge_data_dir, 'sports_to_wiki_guid_merge.list')

    def copy_files(self):
	import datetime
	tem = str((datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d'))
        status = ssh_utils.scp('veveo123', 'veveo@%s:/home/veveo/datagen/current/rovi_sports_merge_data/rovi_sports_guid_merge.list' % (self.log_ip), '/home/veveo/datagen/current/rovi_sports_merge_data')

	status = ssh_utils.scp('veveo123', 'veveo@10.8.21.228:/home/veveo/datagen/ROVI_PUBLISH_USA_DATAGEN/logs_TVPublish_%s/past_merge.txt'%(tem), '/data/veveo/SPORTS/sports_spiders/scripts')
	
        #copying guid_merge.list file to content_data path
        status = ssh_utils.scp('veveo123', 'veveo@%s:/home/veveo/datagen/current/content_data/guid_merge.list' % self.log_ip, '/home/veveo/datagen/current/content_data')

        #copying sports_to_wiki_guid_merge.list file
        status = ssh_utils.scp('veveo123', 'veveo@%s:/home/veveo/datagen/current/sports_merge_data/sports_to_wiki_guid_merge.list' % (self.log_ip), '/home/veveo/datagen/current/sports_merge_data')


    def rovi_team(self):
        print self.rovi_sports_merge_data
        self.gidMap = {}
        guidloadGuidPairList(self.rovi_sports_merge_data, self.gidMap )

        for key, value in self.gidMap.iteritems():
            query = 'select id, gid, title from sports_participants where gid = %s'
            values = (value)
            self.cursor.execute(query, values)
            record = self.cursor.fetchall()

            if record:
                if value.startswith("TEAM"):
                    _id, gid, title = record[0]
                    query = 'insert into sports_rovi_merge(entity_type, entity_id, rovi_id, created_at, modified_at) values (%s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                    values = ("TEAM", _id, int(key.replace("RV", '')))
                    self.cursor.execute(query, values)
            else:
                print key, value

    def rovi_game(self):
	self.cursor, self.db = self.open_my_cursor('10.28.218.81', 'SPORTSDB')
        self.game_hash = {}
        schema  = get_schema(['Gi', 'Sk', 'Fd', 'Vt'])
        for record in foldFileInterface.dataFileDictIterator(self.sports_game_merge, schema):
            if record[schema['Vt']] == "game":
		gi = record[schema['Gi']]
                sk = record[schema['Sk']]
                fd = record[schema['Fd']]
                rovi_record = fd.split('<>')
                rovi_2_0 = ''
                for rv_record in rovi_record:
                    if "2.0" in rv_record:
                        rovi_2_0 = rv_record.split("#")[-1] 

                if not rovi_2_0:
			self.logger.info("%s" %(gi))
			continue

                if "<>" in rovi_2_0: continue
                text = "Gi: %s\nSk: %s" %(sk, rovi_2_0)

                query = 'select id from sports_games where gid = %s' 
                self.cursor.execute(query, gi.replace("VG", ""))
                gid_id = self.cursor.fetchone()

                if not gid_id: continue

                try:
                    query = 'insert into sports_rovi_games_merge(game_id, program_id, created_at, modified_at) values (%s, %s, now(), now()) on duplicate update modified_at=now()'
		    print gid_id[0], rovi_2_0
                    values = (gid_id[0], rovi_2_0)
                    self.cursor.execute(query, values)
                except:
                    self.logger.info('%s---%s' % (gid_id, sk))

	schema = {'Gi' : 0, 'Sk' : 1}
        for record in genericFileInterfaces.fileIterator("past_merge.txt", schema):
            gid, sk =  record[schema['Gi']], record[schema['Sk']]
            query = "select id, gid from sports_games where gid = %s"
            cursor.execute(query, (gid))
            id, gid = cursor.fetchall()[0]

            query = 'insert into sports_rovi_games_merge(game_id, program_id, schedule_id, modified_at, created_at) values(%s, %s, 0, now(), now()) on duplicate key update modified_at=now()'
            cursor.execute(query, (id, sk.replace("RV", "")))
	
    def load_player_sportsdb_gis(self):
	self.cursor, self.db = self.open_my_cursor('10.28.218.81', 'SPORTSDB')
        query = 'select id, gid from sports_participants'
        self.cursor.execute(query)
        for row in self.get_fetchmany_results():
            pl_id, pl_gid = row
            self.sportsdb_players_dict[pl_gid] = pl_id

    def rovi_players(self):
        self.content_gidMap    = {}
        self.sports_gidMap     = {}
        self.content_reversegid = {}
        self.load_player_sportsdb_gis()
        self.merge_cur, self.mergedb = self.open_my_cursor('10.28.218.81', 'GUIDMERGE')
        query = 'select exposed_gid, child_gid from sports_wiki_merge'
        self.merge_cur.execute(query)
        rows = self.merge_cur.fetchall()
        for row in rows:
            wiki_gid , sport_gid = row
            if 'PL' in sport_gid:
                self.sports_gidMap[sport_gid] = wiki_gid

        print "Loading gid maps"
        guidloadGuidPairList(self.content_guid_merge, self.content_gidMap, self.content_reversegid)
        #guidloadGuidPairList(self.sports_guid_merge, self.sports_gidMap)

        x_list = []
        count = 0
        for gids, value in self.sports_gidMap.iteritems():
            if gids.startswith('PL'):
                count += 1
                rovi_gid = self.content_reversegid.get(value, '')

                for gid in rovi_gid:
                    if gid.startswith('RV'):
                        if gids[0] not in x_list:
                            x_list.append(gids[0])
                        else:
                            print self.content_gidMap.get(gid)
                        entity_id = self.sportsdb_players_dict.get(gids, '')
                        rovi_id   = gid.replace("RV", "")
                        if entity_id and rovi_id:
                            query = 'insert into sports_rovi_merge(entity_type, entity_id, rovi_id, created_at, modified_at) values (%s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                            values = ("player", entity_id, rovi_id)
                            self.cursor.execute(query, values)


    def rovi_league(self):
        league_file = os.path.join(os.getcwd(), "League.txt")

        lines = file(league_file, "r+").readlines()
        for line in lines:
            league_id, rovi_league_id = line.strip().split("#<>#")
            query = 'insert into sports_rovi_merge(entity_type, entity_id, rovi_id, created_at, modified_at) values (%s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
            values = ("league", league_id, rovi_league_id)
            self.cursor.execute(query, values)

    def set_options(self):
        self.parser.add_option('', '--entity', default=None, help='')

    def run_main(self):
	self.copy_files()
        print 'Running for %s' %self.options.entity
        self.open_cursor(self.sportsdb_ip, self.sportsdb_name)
        if self.options.entity == "player":
            self.rovi_players()

        elif self.options.entity == "team":
            self.rovi_team()

        elif self.options.entity == "league":
            self.rovi_league()
        else:
            self.rovi_game()

if __name__ == "__main__":
    vtv_task_main(SportsRovi)
