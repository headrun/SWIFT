import sys 
import optparse
import re
import time
import datetime
import MySQLdb


SOURCE_KEY = 'insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, "%s", "%s", "%s", now(), now()) on duplicate key update modified_at = now()'


def main():
    cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    query  = 'select id, title from sports_participants where game="cycling" and participant_type ="player"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        title = str(event[1].lower())
        id_   = event[0]
        game_query = 'select participant_id from sports_players where gender ="male" and participant_id =%s'
        game_values = id_
        cursor.execute(game_query, game_values)
        game_data = cursor.fetchall()
        if not game_data:
            continue
        id_ = game_data[0][0]
        if len(title.split(' ')) == 3:
            source_key = title.split(' ')[0]+ "-"+ title.split(' ')[1]
        if len(title.split(' ')) == 4:
            source_key = title.split(' ')[0]+ "-"+ title.split(' ')[1]
        else:
            continue
        #source_key = title.replace(' ', '_').replace('-', '_')
        if id_:
            cursor.execute(SOURCE_KEY %(id_, 'participant', 'eurosport_cycling', source_key))
    cursor.close()

if __name__ == '__main__':
    main()

