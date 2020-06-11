import re
import MySQLdb

def mysql_connection():
    #conn = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP")
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor

class UpdateShortTitle:
    short_title = open("ncfdisplay.csv", "r+")

    def main(self):
        for data in self.short_title:
            got_data = data.split(',')
            if len(got_data) == 6:
                pl_id, name, display, db_short, short, display_title = data.split(',')
                values = (display_title.strip(), pl_id)
                query = UPDATE_Q % values
            else:
                print got_data
            if display_title:
                conn, cursor = mysql_connection()
                cursor.execute(query)
                conn.close()



if __name__ == '__main__':
    OBJ = UpdateShortTitle()
    OBJ.main()

