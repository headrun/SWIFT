#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

#UP_QRY = 'update sports_tournaments_groups set sport_id=%s where id=%s limit 1'

def main():

    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="GUIDMERGE", charset='utf8', use_unicode=True).cursor()
    back_cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()
    #back_cursor = MySQLdb.connect(host='10.28.216.45', user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True).cursor()
    query = 'select * from sports_types'
    back_cursor.execute(query)
    data = back_cursor.fetchall()
    for data_ in data:
        id_ = data_[0]
        gid = data_[1]
        title = data_[2]
        
        if "SPORT" in gid:
            #qry = 'select id from sports_participants where gid=%s'
            #qry = 'select id from sports_tournaments_groups where gid=%s'
            qry = 'select * from sports_wiki_merge where child_gid=%s'
            values = (gid)
            sp_cur.execute(qry, values)
            data = sp_cur.fetchone()
            if not data:
                #del_qry = 'delete from sports_wiki_merge where exposed_gid=%s and child_gid=%s limit 1'
                #values = (gid, pa_gid )
                #back_cursor.execute(del_qry, values)
                print id_, gid, title


    sp_cur.close()
    back_cursor.close()

if __name__ == '__main__':
    main()

