#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0', '2016', '%s', '%s', %s) ON DUPLICATE KEY UPDATE result_sub_type='%s', result_type = '%s'"


def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123",db="SPORTSDB").cursor()
    query  = " select tournament_id, event_id from sports_tournaments_events where tournament_id=2216"
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        tou_id = event[0]
        final_event_id = event[1]
        print "final_event_id", final_event_id
        game_query = 'select id, YEAR(game_datetime), status, game from sports_games where event_id = %s and YEAR(game_datetime) = year(now()) and status = "completed"'
        game_values = final_event_id
        cursor.execute(game_query, game_values)
        game_data = cursor.fetchall()
        if not game_data:
            continue
        id = game_data[0][0]
        year = game_data[0][1]
        status = game_data[0][2]
        game = game_data[0][3]
        _date = datetime.datetime.now().date().strftime("%Y")
        _query = "select id, title from sports_tournaments where id =  %s"
        _values = final_event_id
        cursor.execute(_query, _values)
        event_id_data = cursor.fetchone()
        if event_id_data :
            event_id = str(event_id_data[0])
	    group_name = str(event_id_data[1]).lower()
            print "tou_id", event_id
            _results = "select result_value from sports_games_results where game_id = %s and result_type = %s"
            _values = (id, 'winner')
            cursor.execute(_results, _values)
	
            result_value = cursor.fetchall()
	    if "Prince_Of_Lir" in result_value[0][0]:
		continue
            winner_group = group_name.replace("'s", '')
	    cursor.execute(TOU_RESULTS %(tou_id, 'winner', winner_group.lower(), result_value[0][0], winner_group.lower(), 'winner'))
	    cursor.execute(TOU_RESULTS %(event_id, 'winner', '', result_value[0][0], '', 'winner'))
    cursor.close()

if __name__ == '__main__':
    main()
