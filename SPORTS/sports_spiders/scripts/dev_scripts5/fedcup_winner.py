#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0', %s, '%s', '%s', %s)"


def main():

    cursor = MySQLdb.connect(host='10.4.15.132', user="root", db="SPORTSDB_BKP").cursor()
    query = "select distinct(event_id), count(*), min(created_at) from sports_games where status in ('scheduled', 'completed') and year(game_datetime) = year(now()) and game in ('football', 'hockey') and event_id in (319, 320, 1010, 900) group by event_id having count(*) > 1"

    cursor.execute(query)
    data = cursor.fetchall()
    if data:
        for event in data:
            import pdb;pdb.set_trace()
            query = 'update sports_games set status ="Hole" where event_id=%s and created_at= %s'
            values = (str(event[0]), str(event[2]))
            cursor.execute(query, values)
        



    cursor.close()

if __name__ == '__main__':
    main()
