import re
import MySQLdb

#CURSOR = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB").cursor()
CURSOR = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP").cursor()
LOCATION_QRY ="select distinct location_id from sports_games where location_id not in (select id from sports_locations) and location_id !=0 and stadium_id!=''"
stadium_query = "select distinct stadium_id from sports_games where location_id=%s and stadium_id!=''"
del_query= "select location_id from sports_stadiums where id=%s"
update_query = "update sports_games set location_id =%s where stadium_id =%s"
class CheckDoc:

    def main(self):
        CURSOR.execute(LOCATION_QRY)
        loc_data = CURSOR.fetchall()
        for locs_data in loc_data:
            CURSOR.execute(stadium_query %(locs_data))
            sta_ = CURSOR.fetchall()
            if len(sta_)>1:
                CURSOR.execute(del_query %(sta_[0]))
                loc= CURSOR.fetchall()
                CURSOR.execute(update_query % (str(loc[0][0]), str(sta_[0][0])))


if __name__ == '__main__':
    OBJ = CheckDoc()
    OBJ.main()

