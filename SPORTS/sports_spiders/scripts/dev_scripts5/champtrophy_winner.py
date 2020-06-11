import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0', %s, '%s', '%s', %s)"


def main():

    cursor = MySQLdb.connect(host='10.4.15.132', user="root", db="SPORTSDB_BKP").cursor()

    query  = " select id, tournament_id from sports_games where game_note='Final' and game_status='completed' "
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        tou_id = event[1]
        game_id = event[0]
        game_query = 'select id, YEAR(game_datetime), status, game from sports_games where event_id = %s and YEAR(game_datetime) = year(now()) and status = "completed" and id in (select entity_id from sports_source_keys where source_key not like %s and source="fedcup_tennis")'
        condition = "%" + "R" + "%" 
        game_values = final_event_id, condition
        cursor.execute(game_query, game_values)
        game_data = cursor.fetchall()
        if not game_data:
            continue
        id_ = game_data[0][0]
        year = game_data[0][1]
        status = game_data[0][2]
        game = game_data[0][3]
        _date = datetime.datetime.now().date().strftime("%Y")
        if game == "tennis" and status == "completed" and str(year) == _date:
            _results = "select result_value from sports_games_results where game_id = %s and result_type = %s"
            _values = (str(id_), 'winner')
            cursor.execute(_results, _values)
            result_value = cursor.fetchall()
            cursor.execute(TOU_RESULTS %(tou_id, year, 'winner', '', result_value[0][0]))
            cursor.execute(TOU_RESULTS %(final_event_id, year, 'winner', '', result_value[0][0]))
            cursor.execute(TOU_RESULTS %(final_event_id, year, 'winner', '', result_value[0][0]))
    cursor.close()

if __name__ == '__main__':
    main()

