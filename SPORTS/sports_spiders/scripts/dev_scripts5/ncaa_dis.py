#!/usr/bin/env python
import sys 
import optparse
import re
import time
import datetime
import MySQLdb 
import urllib

def main(): 
    #f = open('ncf.txt', 'w')
    f = open('ncb_basketball.txt', 'w')
    #f = open('ncw.txt', 'w')
    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    querey = 'select participant_id, short_title, display_title from sports_teams where tournament_id =213'
    #querey = 'select participant_id, short_title, display_title from sports_teams where tournament_id =213'

    #querey = ' select participant_id, short_title, display_title from sports_teams where tournament_id =421'
    cursor.execute(querey)
    data = cursor.fetchall()
    for event in data:
        id_ = str(event[0])
        short_title = str(event[1])
        display_title = str(event[2])
        querey = 'select id, title from sports_participants where id= %s' %(id_)
        cursor.execute(querey)
        data_ = cursor.fetchall()
        team_data = str(data_[0][0]), data_[0][1]
        f.write('%s\t%s\t%s\t%s\n'%(data_[0][0], data_[0][1], str(event[1]), str(event[2])))

    cursor.close()

if __name__ == '__main__':
    main() 
