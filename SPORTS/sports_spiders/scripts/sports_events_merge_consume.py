#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import traceback
import foldFileInterface
import genericFileInterfaces
from vtv_task import VtvTask, vtv_task_main
from data_schema import get_schema
from datetime import date, datetime, timedelta
from foldFileInterface import dataFileDictIterator

sports_rovi_games_merge = 'insert into sports_rovi_games_merge(game_id, program_id, schedule_id, modified_at, created_at) values(%s, %s, 0, now(), now()) on duplicate key update modified_at=now(), game_id=%s'

class MergeConsume(VtvTask):
		def __init__(self, options=None):
			VtvTask.__init__(self)

			self.consume_dir			= "SPORTS_EVENTS_MERGE"
			self.OUT_DIR            	= os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.consume_dir)
			self.sportsdb_ip 	    	= "10.28.218.81"
			self.sportsdb_name          = "SPORTSDB"
			self.sports_events_merge	= os.path.join(self.OUT_DIR, "sports_events_merge")

		def rovi_game(self):
			three_day = datetime.now() - timedelta(days = 3)
			cursor, db = self.open_my_cursor(self.sportsdb_ip, self.sportsdb_name)
			schema = {'Gi' : 0, 'Sk' : 1}
			consume_file = os.path.join(self.sports_events_merge, "SPORTS_EVENTS_MERGE")

			for record in genericFileInterfaces.fileIterator(consume_file, schema):
				gid, sk =  record[schema['Gi']], record[schema['Sk']]

				if "RVD" in sk: continue
				query = "select id, gid, game_datetime, tournament_id from sports_games where gid = %s"
				cursor.execute(query, (gid))
				id, gid, game_datetime, tou_id = cursor.fetchall()[0]

				if game_datetime < three_day:
					continue

				self.logger.info("%s - %s" %(id, sk.replace("RV", "")))
				cursor.execute(sports_rovi_games_merge, (id, sk.replace("RV", ""), id))
			cursor.close()

		def run_main(self):
			self.rovi_game()

if __name__ == "__main__":
    vtv_task_main(MergeConsume)
