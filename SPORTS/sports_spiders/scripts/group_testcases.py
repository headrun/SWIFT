#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

UP_QRY = 'update sports_tournaments_groups set sport_id=%s where id=%s limit 1'

def main():

    '''cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()'''
    dd_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="DATATESTDB", charset='utf8', use_unicode=True).cursor() 
    #sel_qry = 'select id, group_name  from sports_tournaments_groups where tournament_id =461 and group_type ="team"'
    sel_qry = 'select id, record  from test_cases where record like %s'
    values = '%'+ 'WIKI551728' + '%'
    dd_cur.execute(sel_qry, values) 
    data_ = dd_cur.fetchall() 
    for data in data_:
        id_ = data[0]
        record = data[1].replace('WIKI551728', 'WIKI39859551')
        up_qry = 'update test_cases set record=%s where id=%s limit 1' 
        values = (record, id_)
        dd_cur.execute(up_qry, values)

    dd_cur.close()


    '''cursor.execute(sel_qry)
    data = cursor.fetchall()
    for event in data:
        id_ = event[0]
        group_name = event[1]
        sel_qry = 'select id, record from test_cases where record like %s'
        group_name = "#" + group_name + "#"
        values = '%'+ group_name + '%'
        dd_cur.execute(sel_qry, values) 
        data = dd_cur.fetchone() 
        if data:
            id_ = data[0]
            record = data[1].replace('WIKI893219', 'WIKI961522')

            up_qry = 'update test_cases set record=%s where id=%s limit 1' 
            values = (record, id_)
            dd_cur.execute(up_qry, values)

    cursor.close()
    dd_cur.close()'''
    

if __name__ == '__main__':
    main()
