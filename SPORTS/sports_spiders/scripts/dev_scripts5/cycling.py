#!/usr/bin/env python
import sys 
import optparse
import re
import time
import datetime
import MySQLdb
import urllib

def main():
    f = open('cycling_inva.txt', 'w')
    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    querey = 'select id, title, image_link from sports_participants where game="cycling" and participant_type="player"'
    cursor.execute(querey)
    data = cursor.fetchall()
    for event in data:
        id_ = event[0]
        title = event[1]
        image_link = event[2]
        d = urllib.urlopen(image_link)
        data = str(d.code)
        print data
        if data == "403":
            data_ = (id_, title, image_link)
            print data_
            f.write('%s\n'%repr(data_))
        elif data == "404":
            data_ = (id_, title, image_link)
            print data_
            f.write('%s\n'%repr(data_))

    cursor.close()

if __name__ == '__main__':
    main()

