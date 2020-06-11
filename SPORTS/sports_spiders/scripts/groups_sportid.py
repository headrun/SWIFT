#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

#UP_QRY = 'update sports_tournaments_groups set sport_id=%s where id=%s limit 1'

def main():

    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()
    #query  = "select id, participants from sports_awards_history"
    #query = 'select id, title, aka from sports_participants where modified_at like "%2016-10-29%" and not created_at like "%2016-10-29%" and aka !="" and sport_id=7'
    query = 'select id, source_key from sports_source_keys where entity_id =0 and entity_type="participant"'
    back_cursor = MySQLdb.connect(host='10.28.216.45', user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True).cursor()
   
    sp_cur.execute(query)
    data = sp_cur.fetchall()
     
    for event in data:
        id_ = event[0]
        sk  = event[1]
        if id_:
            sel_qry = 'select entity_id from sports_source_keys where entity_type="participant" and id=%s and source_key=%s'
            values = (id_, sk)
            back_cursor.execute( sel_qry, values)
            data = back_cursor.fetchone()
            if data:
                en_id = data[0]
                up_qry = 'update sports_source_keys set entity_id=%s where id =%s'
                values = (en_id, id_)
                sp_cur.execute(up_qry, values)

     
    sp_cur.close()
    back_cursor.close()

if __name__ == '__main__':
    main()   
