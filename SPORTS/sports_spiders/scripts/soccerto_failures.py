# -*- coding: utf-8 -*-
import MySQLdb


class SoccerToFailures():
        def __init__(self):
                #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="DATATESTDB_DEV", charset='utf8', use_unicode=True)
                self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="DATATESTDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()

        def main(self):
            #sel_query = '''select id, title from sports_participants where title like "%basketball%" and title not like "%men's %" and id in (select participant_id from sports_teams where tournament_id=213)''''''
            sel_query = '''select id, record from test_cases where record like "%WIKI43016062%"'''
            self.cursor.execute(sel_query)
            data = self.cursor.fetchall()
            for data_ in data:
                id_ = str(data_[0])
                title = str(data_[1])
                title = title.replace("WIKI43016062", "TOU687")
                #up_qry = "update sports_participants set title=%s where id=%s limit 1"
                up_qry = 'update test_cases set record=%s where id = %s limit 1'
                values = (title, id_)
                self.cursor.execute(up_qry, values)

if __name__ == '__main__':
    OBJ = SoccerToFailures()
    OBJ.main()

