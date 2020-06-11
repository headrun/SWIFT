#!/usr/bin/env python
import sys 
import time
import MySQLdb

INSERT_QERY = ''

def main():
    conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
    #cursor = MySQLdb.connect(host='10.4.18.183', user="root", db="SPORTSDB").cursor()
    cursor = conn.cursor()
    query  = "select id, game_datetime from sports_games where tournament_id = 240 and status= 'scheduled' and id in (select game_id from sports_games_participants where participant_id in (963)) order by game_datetime"
    cursor.execute(query)
    deta = cursor.fetchall()
    for data_ in deta:
        game_id = str(data_[0])
        game_datetime = str(data_[1])
        par_query = "select game_datetime, id from sports_games where tournament_id = 240 and status= 'scheduled' and id in (select game_id from sports_games_participants where participant_id in (963)) order by game_datetime"
        cursor.execute(par_query)
        data = cursor.fetchall()
        for par in data:
            par_id = str(par[1])
            par_date = str(par[0])
            if game_datetime == par_date:
                sk_qry ='select source_key from sports_source_keys where entity_id=%s and source="espn_nhl" and entity_type="game"'
                cursor.execute(sk_qry %(par_id))
                data = cursor.fetchone()
                pl_sk = str(data[0])
                if pl_sk:
                    query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                    values = (game_id, 'game', 'espn_nhl', pl_sk)
                    cursor.execute(query, values)

                    

    cursor.close()

if __name__ == '__main__':
    main()

