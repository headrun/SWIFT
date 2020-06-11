#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import ssh_utils
import foldFileInterface
from vtv_task import VtvTask, vtv_task_main
from data_schema import get_schema
import traceback
import glob
from datetime import date, datetime, timedelta
import genericFileInterfaces
from guidPairLoaderUtils import guidloadGuidPairList
from foldFileInterface import dataFileDictIterator
from vtv_utils import copy_file,execute_shell_cmd

select_sports_participants_query = 'select id, gid from sports_participants'
sports_rovi_games_merge = 'insert into sports_rovi_games_merge(game_id, program_id, schedule_id, modified_at, created_at) values(%s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

class SportsRovi(VtvTask):
		def __init__(self, options=None):
			VtvTask.__init__(self)
			self.sportsdb_ip 	    = "10.28.216.45"
			self.tilden_ip		    = "10.8.21.228"
			self.log_ip                 = '10.28.218.80'

			self.OUT_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, "sports_rovi")
			self.sportsdb_name          = "SPORTSDB"
			self.sportsdb_players_dict  = {}
			self.rovi_sports_merge_data = os.path.join(self.datagen_dirs.rovi_sports_merge_data_dir, 'rovi_sports_guid_merge.list')
			self.sports_game_merge      = os.path.join(self.datagen_dirs.vtv_avail_data_dir, 'DATA_MC_FILE')
			self.content_guid_merge     = os.path.join('/home/veveo/datagen/current/content_data', 'guid_merge.list')
			self.sports_guid_merge      = os.path.join(self.datagen_dirs.sports_merge_data_dir, 'sports_to_wiki_guid_merge.list')
			self.s3c_path			    = "s3://vpc.veveo.net/production/ted_prod/rovi_sports/vkc/seed_vtv_20160524T141341/SPORTS_EVENTS_MERGE/sports_events_merge/data_sports_events_merge_%s*"
			self.rovi_publish_dir		= "/home/veveo/datagen/ROVI_PUBLISH_%s_DATAGEN/"
			self.region_list		    = ['USA', 'EU', 'LA']
			self.username				= "veveo"
			self.passwd					= "veveo123"

		def copy_files(self):
			#remove old file
			data_tgz_files = glob.glob(os.path.join(os.getcwd(), "data_sports_events_merge_*"))
			for i in data_tgz_files:
				cmd = "rm -rf %s" %(i)
				status, output = execute_shell_cmd(cmd, self.logger)
				cmd = "rm -rf SPORTS_EVENTS_MERGE"
				status, output = execute_shell_cmd(cmd, self.logger)

			timestamp = (datetime.now() - timedelta(hours=1)).strftime('%Y%m%dT%H')
			cmd = self.s3c_path % timestamp
			cmd = "s3cmd get %s" %cmd
			status, output = execute_shell_cmd(cmd, self.logger)
			self.untar_files()

		def untar_files(self):
			data_tgz_files = glob.glob(os.path.join(os.getcwd(), "data_sports_events_merge_*"))
			if data_tgz_files:
				cmd = "tar -xvf %s" %data_tgz_files[0]
				status, output = execute_shell_cmd(cmd, self.logger)

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
			self.game_hash = {}
			schema = {'Gi' : 0, 'Sk' : 1}

			for region in self.region_list:
				past_merge_file = os.path.join(os.getcwd(), "SPORTS_EVENTS_MERGE")
				self.game_hash.setdefault(region, {})
				for record in genericFileInterfaces.fileIterator(past_merge_file, schema):
					gid, sk =  record[schema['Gi']], record[schema['Sk']]
					self.game_hash[region][gid] = sk
			self.populate_game_merge()		
	
		def populate_game_merge(self):
			cursor, db = self.open_my_cursor('10.28.218.81', 'SPORTSDB')
			three_day = datetime.now() - timedelta(days = 3)
			usa_games_list = self.game_hash['USA'].keys()
			for region in self.region_list:
				for gid, sk in self.game_hash[region].iteritems():
					if region != "USA":
						if gid in usa_games_list:
							continue

					if "RVD" in sk: continue
					query = "select id, gid, game_datetime, tournament_id from sports_games where gid = %s"
					cursor.execute(query, (gid))
					id, gid, game_datetime, tou_id = cursor.fetchall()[0]

					if game_datetime < three_day:
						continue

					#print tou_id, game_datetime
					query = 'insert into sports_rovi_games_merge(game_id, program_id, schedule_id, modified_at, created_at) values(%s, %s, 0, now(), now()) on duplicate key update modified_at=now(), game_id=%s'
					self.logger.info("%s - %s" %(id, sk.replace("RV", "")))
					cursor.execute(query, (id, sk.replace("RV", ""), id))

		def load_player_sportsdb_gis(self):
			self.cursor, self.db = self.open_my_cursor('10.28.218.81', 'SPORTSDB')
			self.cursor.execute(select_sports_participants_query)

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


		def cleanup(self):
        		self.move_logs(self.OUT_DIR, [ ('.', '*log'), ('.', '*.folds'), ('.', '*.txt') ] )
        		self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

if __name__ == "__main__":
    vtv_task_main(SportsRovi)
