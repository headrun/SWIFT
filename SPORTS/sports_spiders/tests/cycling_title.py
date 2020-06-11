#!usr/bin/env python
import sys
import time
from urlparse import urljoin
import urllib2, httplib
import MySQLdb


def main():
    con = MySQLdb.connect(host='10.4.18.183', user='root', db='SPORTSDB')
    con.set_character_set('utf8')
    cursor = con.cursor()

    query = 'select title from sports_participants where game= "cycling" and participant_type= "player"'
    cursor.execute(query)
    data = cursor.fetchall()
    for dt in data:
        tt = dt[0]
        ott = dt[0].title()
        up =  'update sports_participants set title = "%s" where title = "%s"' % (ott, tt)
        cursor.execute(up)
if __name__ == '__main__':
    main()

