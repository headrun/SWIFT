import sys
import optparse
import re
import time
import datetime
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value) values(%s, '0', '2016', %s, %s, %s) ON DUPLICATE KEY UPDATE result_sub_type=%s, result_type = %s"


def main():
    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB").cursor()
    query  = "select tournament_id, event_id from sports_tournaments_events where tournament_id in (select id from sports_tournaments where subtype='summer' and type='event' and affiliation='IOC')"

    cursor.execute(query) 
    data = cursor.fetchall()
    for event in data:
        tou_id = str(event[0])
        event_id = event[1]
        game_query = 'select id, YEAR(game_datetime), status, game from sports_games where event_id = %s and YEAR(game_datetime) = year(now()) and status = "completed"'
        game_values = event_id
        game_values = (event_id)
        cursor.execute(game_query, game_values)
        game_datas = cursor.fetchall()
        if not game_datas:
            continue
        for game_data in game_datas:
          id = game_data[0]
          year = game_data[1]
          status = game_data[2]
          game = game_data[3]
          _date = datetime.datetime.now().date().strftime("%Y")
          if event_id :
              event_id = event_id
              tou_id   = tou_id
              _results = "select participant_id from sports_games_results where game_id = %s and result_type = %s and result_value=%s"
              _values = (id, 'medal', 'gold')
              cursor.execute(_results, _values)
              result_value = cursor.fetchall()
              count = 0
              for result_val in result_value:
                  count +=1
                  if count ==1 and len(result_val) == 1:
                      result_type = "winner"
                  else:
                      result_type = "winner" + str(count)
              
                  group = "select title from sports_tournaments where id = %s"
                  group_values = event_id
                  cursor.execute(group, group_values)
                  group_name = cursor.fetchone()
                  final_even_qry = 'select tournament_id from sports_tournaments_events where event_id=%s'
                  event_ = tou_id
                  cursor.execute(final_even_qry, event_)
                  final_event_id = cursor.fetchone()
                  group_name =  group_name[0]
                  if "Athletics" in group_name:
                      winner_group = group_name.replace('Olympics - Athletics ', '').lower().strip()
                  elif "Olympics - Swimming" in group_name:
                      winner_group = group_name.replace('Olympics - Swimming ', '').lower().strip()
                  elif "Gymnastics" in group_name:
                       winner_group = group_name.split('Gymnastics')[-1].lower().strip()
                  else:
                      winner_group = group_name.split('Olympics - ')[-1].strip().split(' - ')[0].strip().lower()

                  if winner_group:
                      cursor.execute(TOU_RESULTS , (tou_id, result_type, winner_group, result_val[0], winner_group, result_type))
                      cursor.execute(TOU_RESULTS ,(event_id, result_type, winner_group, result_val[0], winner_group, result_type))
                      cursor.execute(TOU_RESULTS ,('461', result_type, winner_group, result_val[0], winner_group, result_type))
                      if final_event_id:
                          print "final_event_id", final_event_id
                          cursor.execute(TOU_RESULTS ,(final_event_id[0], result_type, winner_group, result_val[0], winner_group, result_type))
    cursor.close()

if __name__ == '__main__':
    main()
