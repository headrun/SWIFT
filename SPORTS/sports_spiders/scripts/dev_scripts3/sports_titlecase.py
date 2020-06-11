# -*- coding: utf-8 -*-
import MySQLdb


class TitleCase:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()
        self.file_ = open('titlecase_stadiums', 'w')

    def main(self):
        query = 'select id, city from sports_locations where city!=""'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            loc_id, city = row
            if city.istitle():
                continue
            elif '/' in city:
                continue
            elif 'de' in city:
                continue
            elif 'di' in city:
                continue
            else:
                record = '%s<>%s' % (loc_id, city)
                self.file_.write('%s\n' % record)



if __name__ == '__main__':
    OBJ = TitleCase()
    OBJ.main()
