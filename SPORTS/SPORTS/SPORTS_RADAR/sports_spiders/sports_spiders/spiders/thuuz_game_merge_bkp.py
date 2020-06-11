#!/usr/bin/env python
# -*- coding: utf-8 -*- 
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

PART_DICT = {'Ajax': 'AFC Ajax', 'Manchester United': 'Manchester United F.C.', 'Anderlecht': 'R.S.C. Anderlecht',
 'KRC Genk': 'K.R.C. Genk', 'Celta Vigo': 'Celta de Vigo', 'Besiktas': 'Besiktas J.K.', 'Lyon': 'Olympique Lyonnais',
 'Leicester City': 'Leicester City F.C.'}

SPORT_DICT = {'Basketball': 2, 'Soccer': 7, 'Tennis': 5, 'Golf': 8}

TOU_DICT = {'FRAN': 32, 'CH-UEFA2': 215, 'CHLG': 216, 'NBA': 229}

UP_QRY = 'insert into sports_thuuz_merge(thuuz_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

def clean_participants(game_note, sport_id):
    home_team = Ep.split(' at ')[-1].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
    

def main():
    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSRADARDB", charset='utf8', use_unicode=True).cursor()


#    sel_qry = 'select Gi, Ti, EP, Od, At from sports_thuuz_games where Ti in ("NBA", "FRAN", "CH-UEFA2", "CHLG")'
    sel_qry = 'select Gi, Ti, EP, Od, At, Ge from sports_thuuz_games where Ti in ("CHLG")'
    sp_cur.execute(sel_qry)
    sp_data = sp_cur.fetchall()
    for data_ in sp_data[::1]:
        Gi, Ti, Ep, Od, At , Ge = data_
        sport_id = SPORT_DICT[Ge.split('{')[0].strip()]
        home_team = Ep.split(' at ')[-1].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
        away_team = Ep.split(' at ')[0].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
        home_team = PART_DICT.get(home_team, home_team)
        away_team = PART_DICT.get(away_team, away_team)
        if 'FC Bayern M' in home_team: home_team = 'FC Bayern Munich'
        if 'FC Bayern M' in away_team: away_team = 'FC Bayern Munich'
        if 'tico de Madrid' in home_team : home_team = 'Atletico Madrid'
        if 'tico de Madrid' in away_team : away_team = 'Atletico Madrid'
        if Ti == "NBA":
            sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=2 and participant_type="team"'
        else: 
            sel_qry = 'select SP.id from SPORTSDB.sports_participants SP, SPORTSDB.sports_tournaments_participants T where T.tournament_id=%s and SP.title like %s and SP.sport_id=7 and SP.participant_type="team" and SP.id=T.participant_id'
            home_team = '%' + home_team + '%'
            away_team = '%' + away_team + '%'
        tou_id = TOU_DICT[Ti]

        if Ti == 'NBA':
            hm_values = (home_team)
        else:
            hm_values = (tou_id, home_team)
        sp_cur.execute(sel_qry, hm_values)
        hm_data = sp_cur.fetchone()
        if not hm_data:
            print home_team
            continue
        hm_id = hm_data[0]
        if Ti == 'NBA':
            aw_values = (away_team)
        else:
            aw_values = (tou_id, away_team)
        sp_cur.execute(sel_qry, aw_values)
        aw_data = sp_cur.fetchone()
        if not aw_data:
            print away_team
            continue
        aw_id = aw_data[0]
        
        ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s)'
        game_datetime = '%' + str(Od) + '%'
        game_values = (tou_id, game_datetime, hm_id, aw_id)
        sp_cur.execute(ga_qry, game_values)
        game_data = sp_cur.fetchall() 
        if game_data and len(game_data) == 1:
            game_id = game_data[0][0]
            m_values = (Gi, game_id, 'game')
            sp_cur.execute(UP_QRY, m_values)
        else:
            time_hrs = str(At).split('#')[3]
            time_min = str(At).split('#')[4]
            game_datetime = str(Od) +  " " + time_hrs + ":" + time_min
            ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s)'
            game_datetime = '%' + game_datetime + '%'
            game_values = (tou_id, game_datetime, hm_id, aw_id)
            sp_cur.execute(ga_qry, game_values)
            game_data = sp_cur.fetchall()
            if game_data and len(game_data) == 1:
                game_id = game_data[0][0]
                m_values = (Gi, game_id, 'game')
                sp_cur.execute(UP_QRY, m_values)

            

    sp_cur.close()

if __name__ == '__main__':
    main()

