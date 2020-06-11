#!/usr/bin/env python
import sys 
import optparse
import re
import time
import datetime
import MySQLdb
import urllib

def main():
    #f = open('ncb.txt', 'w')
    f = open('tournament.txt', 'w')
    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    querey   = 'select id, title, game from sports_tournaments where type="tournament"'
    cursor.execute(querey)
    data = cursor.fetchall()
    for data_ in data:
        id_ = str(data_[0])
        title = str(data_[1])
        game = str(data_[2])
        f.write('%s\t%s\t%s\n'%(id_, title, game))

    cursor.close()

if __name__ == '__main__':
    main()

