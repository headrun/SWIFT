import sys
from vtv_task import VtvTask, vtv_task_main
import MySQLdb

class DuplicateStadiums(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_ip = '10.28.218.81'
        self.db_name = 'SPORTSDB'
        #self.file_name = open('stadiums_ids.txt', 'r')
        #self.dup_list = open('existing_stadium_list', 'w+')
        #self.loc_list = open('loc_matching_stadium', 'w+')
        #self.city_std_list = open('std_city_list', 'w+')
        self.skip_list = ['stadium', 'ground', 'park', 'arena'] 


    def check_stadium(self, st_title, st_id, st_aka, loc, std_title, country, state, city):
        st_query = 'select id, title, aka, location_id from sports_stadiums where title like %s'
        name = "%" + st_title + "%"
        ext_det = (st_title, st_id, st_aka, loc)
        self.cursor.execute(st_query, name)
        data = self.cursor.fetchall()
        for data in data:
            id_, title, aka, loc_id = data
            if str(loc_id) ==0:
                continue
            loc_query = 'select city, state, country from sports_locations where id=%s'
            self.cursor.execute(loc_query, loc_id)
            loc_data = self.cursor.fetchone()
            st_city = st_state = st_country = ''
            if loc_data: 
                st_city, st_state, st_country = loc_data
            if st_id == str(id_):
                continue
            else:
                if str(loc) == str(loc_id):
                     
                    self.dup_list.write('%s\t%s\t%s\t%s\n' %(id_, title, st_id, std_title))
                elif str(loc) != str(loc_id):
                    if country == st_country:
                        if state == st_state:
                            self.loc_list.write('%s\t%s\t%s\t%s\n' %(id_, title, st_id, std_title))
                        else:
                            self.city_std_list.write('%s\t%s\t%s\t%s\n' %(id_, title, st_id, std_title)) 
        return data

    def duplicatestadiums(self):
        st_id = self.file_name
        for st_deta in st_id:
            st_ids = st_deta.split(',')
            for st_id in st_ids:
                st_id = st_id.replace('L', '').strip()
                st_query = 'select title, aka, location_id from sports_stadiums where id = %s'
                self.cursor.execute(st_query, st_id)
                data = self.cursor.fetchone()
                std_title, st_aka, loc = data
                for skip_li in self.skip_list:
                    if skip_li in std_title.lower():
                        st_title = std_title.lower().replace(skip_li, '').strip()
                    else:
                        st_title = std_title.lower()
                loc_query = 'select city, state, country from sports_locations where id = %s'
                self.cursor.execute(loc_query, loc)
                loc_det = self.cursor.fetchone()
                if loc_det:
                    city, state, country = loc_det
                else:
                    city = state = country = ''
                exist_std = self.check_stadium(st_title, st_id, st_aka, loc, std_title, country, state, city)
                    
                

    def run_main(self):
        self.open_cursor(self.db_ip, self.db_name)
        self.duplicatestadiums()




if __name__ == '__main__':
    vtv_task_main(DuplicateStadiums)

