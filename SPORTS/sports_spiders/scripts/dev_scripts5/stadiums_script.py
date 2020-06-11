#-*- coding: utf8 -*-
import MySQLdb
import unicodedata

STD_IDS = 'select participant_id, stadium_id from sports_teams where tournament_id =562'
TEAM_GID = 'select gid from sports_participants where id = %s'
LOC_ID = 'select title, gid, location_id from sports_stadiums where id = %s'
LOC_INFO = 'select country, state, city from sports_locations where id = %s'

class MajorStadiums:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

        self.merge_dict = {}
        self.get_merge()
        self.stadiums = open('stadiums_info', 'w')

    def get_merge(self):
        merges = open('sports_to_wiki_guid_merge.list', 'r').readlines()
        for merge in merges:
            if 'STAD' in merge.strip():
                wiki_gid, st_gid = merge.strip().split('<>')
                self.merge_dict[st_gid] = wiki_gid


    def main(self):
        self.cursor.execute(STD_IDS)
        data = self.cursor.fetchall()
        for data_ in data:
            team_id = data_[0]
            std_id = data_[1]
            self.cursor.execute(LOC_ID %(std_id))
            st_info = self.cursor.fetchall()
            loc_id = st_info[0][2]
            st_title = st_info[0][0]
            st_gid =  st_info[0][1]

            if type(st_title) == unicode:
                st_title = unicodedata.normalize('NFKD', st_title).encode('ascii','ignore')

            self.cursor.execute(LOC_INFO %(loc_id))
            loc_info = self.cursor.fetchall()
            country, state, city = loc_info[0][0], loc_info[0][1], loc_info[0][2]

            if type(city) == unicode:
                city = unicodedata.normalize('NFKD', city).encode('ascii','ignore')


            if self.merge_dict.get(st_gid, ''):
                st_wigid = self.merge_dict[st_gid]
            else:
                st_wigid = ''
            self.stadiums.write('%s,%s,%s,%s,%s\n'%(st_wigid, st_title, country, state, city)) 


if __name__ == '__main__':
    OBJ = MajorStadiums()
    OBJ.main()
