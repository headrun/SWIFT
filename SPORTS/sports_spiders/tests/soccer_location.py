import MySQLdb

class SoccerLocation:
    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()
        self.loca_txt = open('soccer_location.txt', 'w+')

    def main(self):
        #sel_query = 'select id, continent, country, state, city, latlong, iso  from sports_locations where country in ("Germany", "France", "Italy", "Spain")'
        sel_query = 'select distinct(city) from sports_locations where city !="" and country in ("Germany", "France", "Italy", "Spain") group by city having count(*) > 1 order by count(*) desc'
        self.cursor.execute(sel_query)
        data = self.cursor.fetchall()
        for data_loc in data:
            city = data_loc[0]
            loc_query = 'select id, continent, country, state, city, latlong, iso  from sports_locations where country in ("Germany", "France", "Italy", "Spain") and city =%s'
            values = (city)
            self.cursor.execute(loc_query, values)
            data = self.cursor.fetchall()
            for data_ in data:
                id_, continent, country, state, city, latlong, iso = data_[0], data_[1], data_[2], data_[3], data_[4], data_[5], data_[6]
        
                print id_, continent, country, state, city, latlong, iso
                self.loca_txt.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(id_, continent, country, state, city, latlong, iso))
        sel_query = 'select id, continent, country, state, city, latlong, iso from sports_locations where state in (select city from sports_locations where country in ("Germany", "France", "Italy", "Spain") and state !="") and state not in ("Madrid", "Barcelona")'
        self.cursor.execute(sel_query)
        data = self.cursor.fetchall()
        for data_ in data:
            id_, continent, country, state, city, latlong, iso = data_[0], data_[1], data_[2], data_[3], data_[4], data_[5], data_[6]
            loc_query = 'select id from sports_locations where city=%s limit 1'
            values = (state)
            self.cursor.execute(loc_query, values)
            data = self.cursor.fetchall()
            if data:
                loc_id = str(data[0])
                id_ =str(id_)
                game_qry = 'update sports_games set location_id=%s where location_id=%s'
                values = (loc_id, id_)
                self.cursor.execute(game_qry, values)
                stadium_qry = 'update sports_stadiums set location_id=%s where location_id=%s'
                self.cursor.execute(stadium_qry, values) 
                part_qury = 'update sports_participants set location_id=%s where location_id=%s'
                self.cursor.execute(part_qury, values)
                tou_query = 'update sports_tournaments set location_ids=%s where location_ids=%s'
                self.cursor.execute(tou_query, values)
                del_query = 'delete from sports_locations where id = %s'
                val = (id_)
                self.cursor.execute(del_query, val)
                
                
        






if __name__ == '__main__':
    OBJ = SoccerLocation()
    OBJ.main()

