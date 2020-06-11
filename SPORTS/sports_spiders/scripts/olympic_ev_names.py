#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

UP_QRY = 'update sports_tournaments set title = %s where id=%s limit 1'

def main():

    #cursor = MySQLdb.connect(host='10.28.216.45', user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True).cursor()
    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()
    #query  = 'select id, title from sports_tournaments where subtype ="summer" and sport_id in (145 , 146 , 147, 17) and title not like "%Gymnastics%"'
    query = 'select id, title from sports_tournaments where subtype ="summer" and sport_id in (140, 141, 24) and not title like "%Canoeing%"'
    cursor.execute(query)
    data = cursor.fetchall()

    for event in data:
        id_ = event[0]
        event_title = event[1].replace('Olympics - Trampoline', 'Olympics - Trampolining Gymnastics').replace('Olympics - Rhythmic', 'Olympics - Rhythmic Gymnastics').replace('Olympics - Artistic', 'Olympics - Artistic Gymnastics').replace('Olympics - Canoe', 'Olympics - Canoeing').replace('Olympics -', 'Olympics - Canoeing')
        values = (event_title, id_)
        cursor.execute(UP_QRY, values)

    cursor.close()

if __name__ == '__main__':
    main()

