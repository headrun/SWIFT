import MySQLdb
import datetime

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.prod_cursor = self.conn.cursor()

        self.bkp_conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDBBKP", charset='utf8', use_unicode=True)
        self.bkp_cursor = self.bkp_conn.cursor()

    def main(self):
        query = "select id from sports_tournaments where season_end like '%0000%'"
        self.prod_cursor.execute(query)
        rows = self.prod_cursor.fetchall()
        for row in rows:
            tou_id = str(row[0])
            query = 'select season_start, season_end from sports_tournaments where id=%s' % tou_id
            self.bkp_cursor.execute(query)
            data = self.bkp_cursor.fetchall()
            if data:
                start, end = data[0]
                query = 'update sports_tournaments set season_start=%s, season_end=%s where id=%s'
                values = (str(start), str(end), tou_id)
                self.prod_cursor.execute(query, values)

if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
