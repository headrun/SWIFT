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
    query = 'select * from sports_wiki_merge'
    sp_cur.execute(query)
    data = sp_cur.fetchall()
    for data_ in data:
        ex_gid= str(data_[0])
        ch_gid = str(data_[1])
        action = str(data_[2])
        modified_at = data_[3]
        #if "PL" in gid or "TEAM" in gid:
        if ex_gid:
            qry = 'select * from sports_wiki_merge where sports_gid=%s'
            values = (ch_gid)
            back_cursor.execute(qry, values)
            data = back_cursor.fetchone()
            if not data:
                qry = 'select * from sports_wiki_merge where wiki_gid=%s'
                values = (ex_gid)
                back_cursor.execute(qry, values)
                data = back_cursor.fetchone()
                if not data:
                    del_qry = 'insert into sports_wiki_merge(wiki_gid, sports_gid, action, modified_date)values(%s, %s, %s, %s)'

                    values = (ex_gid, ch_gid, action, str(modified_at))
                    back_cursor.execute(del_qry, values)
                    #print ch_gid


    sp_cur.close()
    back_cursor.close()

if __name__ == '__main__':
    main()


