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
    query  = 'select id, title from sports_participants where game="football" and participant_type ="team"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        title = str(event[1].lower())
        id_   = event[0]
        game_query = 'select entity_id, source_key from sports_source_keys where source="NFL" and entity_type="participant" and entity_id=%s'
        game_values = id_
        cursor.execute(game_query, game_values)
        game_data = cursor.fetchall()
        if not game_data:
            continue
        id_ = game_data[0][0]
        source_key = game_data[0][1]
        if id_:
            cursor.execute(SOURCE_KEY %(id_, 'participant', 'espn_nfl', source_key))
    cursor.close()

if __name__ == '__main__':
    main()

