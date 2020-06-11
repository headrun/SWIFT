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

    #query = 'select title from sports_participants where game= "cycling" and participant_type= "player"'
    query = 'select display_title from sports_players where participant_id in (select id from sports_participants where game= "cycling" and participant_type= "player" ) and display_title is not NULL'
    cursor.execute(query)
    data = cursor.fetchall()
    count = 0
    for dt in data:
        tt = dt[0]
        ott = dt[0].title()
        up =  'update sports_players set display_title = "%s" where display_title = "%s"' % (ott, tt)
        count += 1
        cursor.execute(up)
        print count
if __name__ == '__main__':
    main()

