#!/usr/bin/env python
import sys 
import optparse
import re
import time
import datetime
import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    querey = 'select id, title from sports_tournaments where title like "%European%" and created_at like "%2015-06-10%"'
    cursor.execute(querey)
    data = cursor.fetchall()
    for event in data:
        id_ = event[0]
        text = event[1]
        text = text.replace('European Games Games', 'European Games')
        update_query = 'update sports_tournaments set title= %s where id = %s limit 1'
        values = (text, id_)
        cursor.execute(update_query, values)

    cursor.close()

if __name__ == '__main__':
    main()

