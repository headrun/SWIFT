#import sys , os, re, datetime, glob, ast, operator, itertools
from vtv_task import VtvTask, vtv_task_main
#from genericFileInterfaces import fileIterator, conceptIterator
from data_schema import get_schema
#import foldFileInterface
import MySQLdb

INSERT_TESTCASE = 'insert into test_cases (suite_id, record, created_at, last_modified, flag) values(%s, %s, now(), now(), %s)'

class PopulateStadiumLoc(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_ip = '10.28.218.81'
        self.db_name = 'GUIDMERGE'
        self.sports_db = '10.28.218.81'
        self.sports_dbname = 'SPORTSDB'
        self.tets_db = "DATATESTDB"
        self.out_file = open('write_testcases', 'w')
        self.stad_witout_merge = open('champ_stad_witout_merge', 'w')
        self.stadiums = {}
        self.stadiums_gids = {}
        self.duplicate_list = []
        self.stadiums_merge = {}
        self.testcases_list = []

    
    def get_seeddetails(self):
        merge_file_fields = ['Gi', 'Ti', 'Vt', 'Zl', 'Cl', 'St', 'Ct']
        data_schema = get_schema(merge_file_fields)
        for rec_values in fileIterator('stadiums_seed', data_schema):
           gid = rec_values[data_schema['Gi']]
           title = rec_values[data_schema['Ti']]
           city = rec_values[data_schema['Ct']]
           state = rec_values[data_schema['St']]
           country = rec_values[data_schema['Cl']]
           self.stadiums_gids[gid] = [title, country, state, city]

    def check_stadium_mege(self):
        self.open_cursor(self.db_ip, self.db_name)
        gid_gry = 'select exposed_gid, child_gid from sports_wiki_merge where child_gid like "%STAD%"' 
        self.cursor.execute(gid_gry)
        gids = self.cursor.fetchall()
        for gid in gids:
            wiki_gid, sports_gid = gid
            self.stadiums_merge[sports_gid] = wiki_gid

    def populate_testcases(self):
        suite_id = '121'
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="root", db="DATATESTDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        for row in self.testcases_list:
            if len(row) == 6:
                query = 'select record from test_cases where suite_id=%s and record like %s'
                val = '%' + row[0] + '#%'
                values = (suite_id, val)
                self.cursor.execute(query, values)
                data = self.cursor.fetchone()
                if data:
                    continue
                record = '#<>#'.join(row)
                print record
                values = (suite_id, record, 'active')
                self.cursor.execute(INSERT_TESTCASE, values)

                self.out_file.write('%s\n' % record)



    def get_ncaastadium(self):
        self.open_cursor(self.sports_db, self.sports_dbname)
        #query = 'select participant_id from sports_tournaments_participants where tournament_id =213'
        #query = 'select participant_id from sports_tournaments_participants where tournament_id =559'
        query = 'select participant_id from sports_tournaments_participants where tournament_id =215'
        self.cursor.execute(query) 
        data = self.cursor.fetchall()
        count = 0
        for pl_ids in data:
            pl_id = str(pl_ids[0])
            st_query = 'select stadium_id from sports_teams where participant_id = %s'
            self.cursor.execute(st_query, pl_id)
            st_id = self.cursor.fetchone()[0]

            st_gid_qry = 'select id, gid, title, location_id from sports_stadiums where id = %s' %(st_id) 
            self.cursor.execute(st_gid_qry)
            st_det = self.cursor.fetchall()
            st_id, st_gid, st_title, st_loc = st_det[0]
            if st_loc == 0:
                continue
            loc_query = 'select id, city, state, country from sports_locations where id = %s' %(st_loc)  
            self.cursor.execute(loc_query)
            loc_det = self.cursor.fetchall()
            city, state, country = loc_det[0]

            db_list = [st_title.decode('utf-8'), country.decode('utf-8'), state.decode('utf-8'), city.decode('utf-8')]
            wiki_gid = self.stadiums_merge.get(st_gid, '') 
            if self.stadiums_gids.get(wiki_gid, ''):
                values =  self.stadiums_gids.get(wiki_gid, '')
            else:
                det = [st_id, st_gid, st_title, st_loc]
                self.stad_witout_merge.write('%s\n' %det)
                continue
            if set(db_list) == set(values):
                values.insert(0, wiki_gid)
                values.insert(2, 'stadium')
                self.testcases_list.append(values)
                count += 1
            else:
                if values[-1] !=db_list[-1]:

                    if "Convocation Center" in values[0] or "WIKI" in values[0]:
                        continue

                    up_title_qry = 'update sports_stadiums set title = %s, aka=%s where id = %s limit 1'
                    up_vaules = (values[0], db_list[0], st_id)
                    self.cursor.execute(up_title_qry, up_vaules)

                

    def run_main(self):
        self.check_stadium_mege()
        self.open_cursor(self.sports_db, self.sports_dbname)
        self.get_seeddetails()
        self.get_ncaastadium()
        self.populate_testcases()




if __name__ == '__main__':
    vtv_task_main(PopulateStadiumLoc)

