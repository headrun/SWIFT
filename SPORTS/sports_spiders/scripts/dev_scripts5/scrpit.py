#!/usr/bin/env python
import sys 
import optparse
import re
import time
import datetime
import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    querey = 'select id, field_text from sports_description where field_text like "%cricket player playing  %"'
    cursor.execute(querey)
    data = cursor.fetchall()
    for event in data:
        id_ = event[0]
        text = event[1]
        text = text.replace('cricket player playing  ', 'cricket player playing ')
        update_query = 'update sports_description set field_text = %s where id = %s'
        values = (text, id_)
        cursor.execute(update_query, values)

    cursor.close()

if __name__ == '__main__':
    main()

