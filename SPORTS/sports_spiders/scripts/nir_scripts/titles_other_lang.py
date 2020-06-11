import sys
import os
import re
import traceback
import datetime, collections
from datetime import timedelta

from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
from data_schema import get_schema

import genericFileInterfaces
import foldFileInterface
from guidPairLoaderUtils import guidloadGuidPairList

class RoviImageLoader(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.sports_db_ip = '10.4.18.183'
        self.sports_db = 'SPORTSDB'
        self.guid_ip = '10.4.2.187'
        self.guid_db = 'GUIDMERGE'


    def set_options(self):
        self.parser.add_option('-t', '--tournament', default='', help='tournament')
        self.parser.add_option('-e', '--entity', default='', help="entity like team and tournament")

    def get_tournament_participants(self, tournament):
        print "In get_tournament_participants ..."
        self.open_cursor(self.sports_db_ip, self.sports_db)
        partispant_query = 'select participant_id from sports_tournaments_participants where tournament_id = %s and status = "active"'
        partispant_vals = (tournament)
        self.cursor.execute(partispant_query, partispant_vals)
        partispant_ids = self.cursor.fetchall()

        guid_pair_list = []
        for i in partispant_ids:
            _id = i[0]
            participant_id = i[0]
            teamid_query = 'select gid from sports_participants where id = %s'
            teamid_query_vals = (participant_id)
            self.cursor.execute(teamid_query, teamid_query_vals)
            team_id = self.cursor.fetchall()[0][0]
            guid_pair_list.append((_id, team_id))
        self.get_wiki_information(guid_pair_list)


    def get_wiki_information(self, guid_pair_list):
            self.open_cursor(self.guid_ip, self.guid_db)

            wiki_guidpair_list = []
            wikigid_qry = 'select exposed_gid from sports_wiki_merge where child_gid = %s'
            for i in guid_pair_list:
                particpant_id, team_gid = i
                wikigid_qry_vals = (team_gid)
                self.cursor.execute(wikigid_qry, wikigid_qry_vals)
                try:
                    team_wiki_gid = self.cursor.fetchall()[0][0]
                except:
                    print team_gid
                    continue
                wiki_guidpair_list.append((particpant_id, team_gid, team_wiki_gid))
            self.get_other_gid_information(wiki_guidpair_list)


    def get_other_gid_information(self, wiki_guidpair_list):
        print "In get_other_gid_information ... "
        gids_op = open('gid_pair_%s_all_teams.file' %(self.options.entity), 'w')
        self.gidMap = {}
        self.reverse_gid_map = {}
        guid_merge_file = os.path.join(os.getcwd(), 'guid_merge.list')
        guidloadGuidPairList(guid_merge_file, self.gidMap, self.reverse_gid_map)

        for i in wiki_guidpair_list:
            particpant_id, team_gid, team_wiki_gid = i
            temp = '%s#<>#%s#<>#%s#<>#%s\n'%(particpant_id, team_gid, team_wiki_gid,','.join(self.reverse_gid_map.get(team_wiki_gid, '')))
            gids_op.write(temp)

    def get_tournament_details(self, tournament):
        tou_pair_list = []
        self.open_cursor(self.sports_db_ip, self.sports_db)
        tou_qry = 'select id, gid from sports_tournaments where affiliation = "uefa"'
        self.cursor.execute(tou_qry)
        tou_rows = self.cursor.fetchall()

        for row in tou_rows:
            tou_id = row[0]
            tou_gid = row[1]
            tou_pair_list.append((tou_id, tou_gid))
        self.get_wiki_information(tou_pair_list)

    def run_main(self):
        if self.options.entity == "tournament":
            self.get_tournament_details(self.options.tournament)
        elif self.options.entity == "team":
            for tournament in self.options.tournament.split(","):
            	self.get_tournament_participants(tournament)

if __name__ == '__main__':
    vtv_task_main(RoviImageLoader)
    sys.exit( 0 )
