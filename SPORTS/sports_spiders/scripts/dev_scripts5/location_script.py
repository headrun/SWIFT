'''import re
import MySQLdb

CURSOR = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB").cursor()
#CURSOR = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP").cursor()
LOCATION_QRY = "select city, id, country from sports_locations where city != '' and country!='' and state!='' group by city having count(*)>1"
STADIUM_QUERY = 'update sports_stadiums set location_id=%s where location_id in (select id from sports_locations where city="%s")'
GAME_QUERY = 'update sports_games set location_id=%s where location_id in (select id from sports_locations where city="%s")'
PAR_QUERY = 'update sports_participants set location_id=%s where location_id in (select id from sports_locations where city="%s")'
TOU_QUERY = 'update sports_tournaments set location_ids=%s where location_ids in (select id from sports_locations where city="%s")'

LOC_DEl_QUERY = 'delete from sports_locations where city="%s" and id !=%s'
LOC_Q  = 'select id from sports_locations where city = "%s" and id !=%s'

class CheckLoc:

    def main(self):
        CURSOR.execute(LOCATION_QRY)
        loc_data = CURSOR.fetchall()
        for locs_data in loc_data:
            city = locs_data[0]
            id_ = locs_data[1]
            country = locs_data[2]
            if country != '':
                CURSOR.execute(STADIUM_QUERY % (id_, city))
                CURSOR.execute(GAME_QUERY % (id_, city))
                CURSOR.execute(PAR_QUERY % (id_, city))
                CURSOR.execute(TOU_QUERY % (id_, city))
                CURSOR.execute(LOC_DEl_QUERY % (city, id_))
            elif country == '':
                continue

if __name__ == '__main__':
    OBJ = CheckLoc()
    OBJ.main()'''
