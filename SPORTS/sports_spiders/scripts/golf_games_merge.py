import genericFileInterfaces
import foldFileInterface
from datetime import datetime
import requests
import json
from pprint import pprint
import time
from data_schema import get_schema
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


UP_QRY = 'insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values ( %s,  %s, %s, now(), now()) on duplicate key update modified_at = now()'

def main():
    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSRADARDB", charset='utf8', use_unicode=True).cursor()


    sel_qry = 'select id, game_datetime, game_note, tournament_id, event_id from sports_games where sport_id=8'
    sp_cur.execute(sel_qry)
    sp_data = sp_cur.fetchall()
    for data_ in sp_data:
        Gi, Od, Ep, Ti, At = data_

        query = 'select sportsdb_id from sports_radar_merge where type="tournament" and radar_id=%s'
        tm_values = (Ti)
        sp_cur.execute(query, tm_values)
        records = sp_cur.fetchone()
        if records:
            tou_id = records[0]
        else:
            print 'missing tou', Ti
            continue

        ga_qry = 'select distinct id from SPORTSDB.sports_games where tournament_id = %s and game_datetime like %s'
        
        game_datetime = '%' + str(Od).split(' ')[0] + '%'
        oth_game_datetime = '%' + str(Od) + '%'
        game_values = (tou_id, game_datetime)
        oth_game_values = (tou_id, oth_game_datetime)

        sp_cur.execute(ga_qry, game_values)
        game_data = sp_cur.fetchall()
        if game_data and len(game_data) == 1:
            game_id = game_data[0][0]
            m_values = (Gi, game_id, 'game')
            sp_cur.execute(UP_QRY, m_values)
        else:
            
            game_values = oth_game_values
            sp_cur.execute(ga_qry, game_values)
            game_data = sp_cur.fetchall()
            if game_data and len(game_data) == 1:
                game_id = game_data[0][0]
                m_values = (Gi, game_id, 'game')
                sp_cur.execute(UP_QRY, m_values)

    sp_cur.close()

if __name__ == '__main__':
    main()

