import re
import MySQLdb

UPDATE_Q = 'update sports_locations set continent="%s" where id="%s" limit 1'


def mysql_connection():
    #conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor

class UpdateContinent:
    continent_data = open("continent.csv", "r+")

    def main(self):
        for data in self.continent_data:
            got_data = data.strip().split(',')
            if len(got_data) == 5:
                tou_id, title, loc_id, continent, country = data.split(',')
                values = (continent, loc_id)
                query = UPDATE_Q % values
            else:
                print "got_data", got_data
            if continent:
                try:
                    conn, cursor = mysql_connection()
                    cursor.execute(query)
                except:
                    print "title", title
        conn.close()



if __name__ == '__main__':
    OBJ = UpdateContinent()
    OBJ.main()
