#!/usr/bin/env python
import sys 
import optparse
import re
import time
import datetime
import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.4.18.34', user="root", db="SPORTSDB_BKP").cursor()
    querey = 'select id, title from sports_tournaments where affiliation = "ioc" and subtype="summer" and title not like "%Olympics -%" and type = "event"'
    cursor.execute(querey)
    data = cursor.fetchall()
    for event in data:
        id_ = event[0]
        title = event[1]
        tt = "Olympics - " + title
        update_query = 'update sports_tournaments set title = %s, aka = %s where id = %s limit 1'
        values = (tt, title, id_)
        cursor.execute(update_query, values)

    cursor.close()

if __name__ == '__main__':
    main()

