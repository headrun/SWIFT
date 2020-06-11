#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

UP_QRY = 'update sports_types set player=%s where id=%s limit 1'

def main():

    #cursor = MySQLdb.connect(host='10.28.216.45', user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True).cursor()
    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()
    query  = "select id, title from sports_types"
    cursor.execute(query)
    data = cursor.fetchall()

    for event in data:
        id_ = event[0]
        title = event[1].lower()
        pl = ''

        if "athlet" in title:
            pl = "athlete"
        if "swimming" in title:
            pl = "swimmer"
        if "wrestling" in title:
            pl = "wrestler"
        if "boxing" in title:
            pl = "boxer<>pugilist"
        if "cricket" in title:
            pl = "cricketer"
        if "diving" in title:
            pl = "diver" 
        if "football"  in title:
            pl = "footballer"
        if "cycling" in title:
            pl = "cyclist"
        if "gymnastics" in title:
            pl = "gymnast"
        if "shooting" in title:
            pl = "shooter"
        if "rowing" in title:
            pl = "rower"
        if "running" in title:
            pl = "runner"
        if "surfing" in title:
            pl = "surfer"
        if " jump" in title and "jumping" not in title:
            pl = "athlete"

        values = (pl, id_)
        cursor.execute(UP_QRY, values)

    cursor.close()

if __name__ == '__main__':
    main()

