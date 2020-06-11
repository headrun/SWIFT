import sys
import optparse
import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0', '2020', %s, %s, %s) ON DUPLICATE KEY UPDATE result_sub_type=%s, result_type = %s"


def main():
    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB").cursor()
    query  = "select tournament_id, event_id from sports_tournaments_events where tournament_id in (select id from sports_tournaments where sport_id ='13' and type='tournament')"

    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        tou_id = str(event[0])
        event_id = event[1]
        game_query = 'select id, YEAR(game_datetime), status, sport_id from sports_games where event_id = %s and YEAR(game_datetime) = year(now()) and status = "completed"'
        game_values = event_id
        cursor.execute(game_query%game_values)
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
            _results = "select result_value from sports_games_results where game_id = %s and result_type = %s"
            _values = (id, 'winner')
            cursor.execute(_results, _values)
            result_value = cursor.fetchall()
            group = "select title from sports_tournaments where id = %s"
            group_values = event_id
            cursor.execute(group, group_values)
            group_name = cursor.fetchone()
            group_name =  group_name[0]
            if "Moto2" in group_name:
                winner_group = "Moto2"
            elif "Moto3" in group_name:
                winner_group = "Moto3"
            elif "MotoGP" in group_name:
                winner_group = "MotoGP"
            else:
                winner_group = ''

            if winner_group:
                cursor.execute(TOU_RESULTS , (tou_id, 'winner', winner_group, result_value[0][0], winner_group, 'winner'))
                cursor.execute(TOU_RESULTS ,(event_id, 'winner', winner_group, result_value[0][0], winner_group, 'winner'))
    cursor.close()

if __name__ == '__main__':
    main()

