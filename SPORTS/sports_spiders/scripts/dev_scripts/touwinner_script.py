#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0', %s, '%s', '%s', %s)"


def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB").cursor()

    query  = "select tournament_id, final_event_id from sports_tournaments_finals where final_event_id in (select id from sports_tournaments where title like '% final%' and sport_id not in ('5', '3', '1', '7'))"
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        tou_id = event[0]
        event_id = event[1]
        game_query = 'select id, YEAR(game_datetime), status, sport_id from sports_games where event_id = %s and YEAR(game_datetime) = year(now()) and status = "completed"'
        game_values = event_id
        cursor.execute(game_query, game_values)
        game_data = cursor.fetchall()
        if not game_data:
            continue
        id = game_data[0][0]
        year = game_data[0][1]
        status = game_data[0][2]
        game = game_data[0][3]
        _date = datetime.datetime.now().date().strftime("%Y")
        if event_id :
            event_id = event_id
            tou_id   = tou_id
            _date = datetime.datetime.now().date().strftime("%Y")
            if game in ["cricket"] and status == "completed" and str(year) == _date:
                _results = "select result_value from sports_games_results where game_id = %s and result_type = %s"
                _values = (str(id), 'winner')
                cursor.execute(_results, _values)
                result_value = cursor.fetchall()
                cursor.execute(TOU_RESULTS %(tou_id, year, 'winner', '', result_value[0][0]))
                cursor.execute(TOU_RESULTS %(event_id, year, 'winner', '', result_value[0][0]))
    cursor.close()

if __name__ == '__main__':
    main()
