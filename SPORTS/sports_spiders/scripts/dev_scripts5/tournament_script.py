#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

def main():

    f = open('continent.txt', 'w')
    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    querey = 'select id, title, location_ids from sports_tournaments where type = "tournament"'
    cursor.execute(querey)
    data = cursor.fetchall()
    for tou_info in data:
        loc_id = tou_info[2]
        tou_id = tou_info[0]
        tou_title = tou_info[1]
        if "<>" in loc_id:
            loc_id = loc_id.split('<>')[0]
        if loc_id != '0':
            loc_query = 'select continent, country from sports_locations where id = %s'
            values = (loc_id)
            cursor.execute(loc_query, values)
            loc_data = cursor.fetchall()
            if loc_data:
                continent = loc_data[0][0] 
                country = loc_data[0][1]
                f.write('%s\t%s\t%s\t%s\t%s\n'%(tou_id, tou_title, loc_id, continent, country))

    cursor.close()

if __name__ == '__main__':
    main()

