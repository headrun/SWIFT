#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0','2020', '%s', '%s', %s) ON DUPLICATE KEY UPDATE result_sub_type='%s', result_type = '%s'"


def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB").cursor()
    query  = "select id from sports_tournaments where title like '%Tour de France%' and type = 'event'"
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        tou_id = event[0]
        game_query = 'select id, YEAR(game_datetime), status, sport_id from sports_games where event_id = %s and YEAR(game_datetime) = year(now()) and status = "completed"'
        game_values = tou_id
        cursor.execute(game_query% game_values)
        game_data = cursor.fetchall()
        if not game_data:
            continue
        id = game_data[0][0]
        year = game_data[0][1]
        status = game_data[0][2]
        game = game_data[0][3]
        _date = datetime.datetime.now().date().strftime("%Y")#.spilt('-')[0]
        if tou_id :
            
            _results = "select result_value from sports_games_results where game_id = %s and result_type = %s"
            _values = (id, 'winner')
            cursor.execute(_results, _values)
            result_value = cursor.fetchall()
            if result_value:
                 cursor.execute(TOU_RESULTS %(tou_id, 'winner', '', result_value[0][0], '', 'winner'))
    cursor.close()

if __name__ == '__main__':
    main()

