#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

#UP_QRY = 'update sports_tournaments_groups set sport_id=%s where id=%s limit 1'

IN_QRY = "insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()"

def main():

    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSRADARDB", charset='utf8', use_unicode=True).cursor()
    #back_cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()

    query = 'select R.id, R.title, SP.id, SP.title from SPORTSDB.sports_participants SP, sports_players R where R.title=SP.title and R.sport_id=SP.sport_id and R.sport_id=5;'
    sp_cur.execute(query) 
    data = sp_cur.fetchall()
    for data_ in data:
        r_id = data_[0]
        sp_id = data_[2]
        values = (r_id, sp_id, 'player')
        sp_cur.execute(IN_QRY, values)
        

    '''query = 'select child_gid from sports_wiki_merge'
    sp_cur.execute(query)
    data = sp_cur.fetchall()
    for data_ in data:
        gid = data_[0]
        #if "PL" in gid or "TEAM" in gid:
        if "SPORT" in gid:
            #qry = 'select id from sports_participants where gid=%s'
            #qry = 'select id from sports_tournaments_groups where gid=%s'
            qry = 'select id from sports_types where gid=%s'
            values = (gid)
            back_cursor.execute(qry, values)
            data = back_cursor.fetchone()
            if not data:
                del_qry = 'delete from sports_wiki_merge where child_gid=%s limit 1'
                values = (gid)
                sp_cur.execute(del_qry, values)
                print gid'''
           

    sp_cur.close()
    #back_cursor.close()

if __name__ == '__main__':
    main() 

