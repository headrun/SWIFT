#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

GROUPS_PART = "insert ignore into sports_groups_participants (group_id, participant_id, season, created_at, modified_at) values(%s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE modified_at=now()"


def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB").cursor()
    query  = "select id, title from sports_participants where id in (select participant_id from sports_groups_participants where group_id =94)"
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        #title = event[1].replace(" men's", " women's")
        title = event[1].replace(" men's basketball", " women's softball")
        #title = event[1].replace(" men's basketball"," football")
        #title = event[1].replace(" men's basketball", " men's soccer")
        #title = event[1].replace(" men's basketball", " women's soccer")
        #title = event[1].replace(" men's basketball", " women's volleyball")
        #title = event[1].replace(" men's basketball", " baseball")
        #title = title.replace("Massachusetts", "UMass")
        pa_query = 'select id from sports_participants where title=%s'
        values = (title)
      
        cursor.execute(pa_query, values)
        data = cursor.fetchone()
        if data:
            pa_id = data[0]
            values = ('1287', pa_id, '2016')
            #values = ('1120', pa_id, '2015')
            cursor.execute(GROUPS_PART, values)		

        else:
            print title


    cursor.close()

if __name__ == '__main__':
    main()

