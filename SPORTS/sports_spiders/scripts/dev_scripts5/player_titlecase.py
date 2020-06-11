#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

PAR_QR = 'update sports_participants set title="%s" where id = %s limit 1'
def main():

    #cursor = MySQLdb.connect(host='10.4.15.132', user="root", db="SPORTSDB_BKP").cursor()
    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    query  = 'select id, title from sports_participants where game="tennis" and participant_type="player"'
    cursor.execute(query)
    data = cursor.fetchall()
    for pl_data in data:
        pl_id = str(pl_data[0])
        pl_name = str(pl_data[1])
        if "TBD" in pl_name or "?" in pl_name:
            continue
        pl_title = pl_data[1].title().strip()
        if pl_id:
            cursor.execute(PAR_QR %(pl_title, pl_id))


    cursor.close()

if __name__ == '__main__':
    main()

