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

C_DICT = {'Ilia Marchenko': 'Illya Marchenko', 'Pablo Carreno-Busta':'Pablo Carreno Busta', 'Joao (POR) Sousa': 'Joao Sousa', 
          'Roberto Bautista-Agut': 'Roberto Bautista Agut', 'Richard Berankis': 'Ricardas Berankis', 
          'Albert Ramos-Vinolas': 'Albert Ramos Vinolas', 'Yen-Hsun Lu': 'Lu Yen-hsun', 'Teymuraz Gabashvili': 'Teimuraz Gabashvili',
          'Aleksandr Dolgopolov': 'Alexandr Dolgopolov', 'Taylor Fritz': 'Taylor Harry Fritz', 'Pierre Hughes Herbert': 'Pierre-Hugues Herbert',
          'Guilherme Clesar': 'Guilherme Clezar', 'Alejandro (Col) Gonzalez': 'Alejandro Gonzalez', 'Marcelo Arevalo-Gonzalez': 'Marcelo Arevalo',
          'Adrian Menendez': 'Adrian Menendez-Maceiras', 'Yan Bai': 'Bai Yan', 'Thiago Moura Monteiro': 'Thiago Monteiro',
          'Alexandre Kudryavtsev': 'Alexander Kudryavtsev', 'Sam Groth': 'Samuel Groth', 'Di Wu': 'Wu Di', 'Xinyun Han': 'Han Xinyun',
          'Cristina-Andreea Mitu': 'Andreea Mitu', 'Kai Lin Zhang': 'Kai-Lin Zhang', 'An Sophie Mestach': 'An-Sophie Mestach',
          'Asia Muhammad': 'Asia Muhammed', 'Shuai Zhang': 'Zhang Shuai', 'Katerina Bondarenko': 'Kateryna Bondarenko',
          'Yi-Fan Xu': 'Xu Yifan', 'Yafan Wang': 'Wang Yafan', 'Tatjana Maria': '', 'Yung-Jan Chan': 'Chan Yung-jan', 
          'Ana Isabel Medina Garrigues': 'Anabel Medina Garrigues', 'Alexandra Krunic': 'Aleksandra Krunic', 'Anna-Karolina Schmiedlova': 'Anna Karolina Schmiedlova',
          'Klaudia Jans': 'Klaudia Jans-Ignacik', 'Anna-Lena Groenefeld': 'Anna-Lena Gronefeld', 'Raluca-Ioana Olaru': '', 
          'Bethanie Mattek': 'Bethanie Mattek-Sands', 'Shuai Peng': 'Peng Shuai', 'Anastassia Rodionova': 'Anastasia Rodionova',
          'Usa at the Hopman Cup': 'United States at the Hopman Cup', 'T Mirnyi': '', 'J Kontinen': '', 'Ze Zhang': 'Zhang Ze',
          'Zhe Li':  'Li Zhe', 'Hans Podlipnik': 'Hans Podlipnik-Castillo', 'Marcelo Arevalo-Gonzalez':'','Tristan Weissborn': 'Tristan-Samuel Weissborn',
          'Alex Satschko': 'Alexander Satschko', 'James Cerretini': 'James Cerretani', 'Treat Conrad Huey': 'Treat Huey',
          'Oceane Dodin': 'Dodin Oceane', 'Jia-Jing Lu': '', 'Richard Berankis': 'Ricardas Berankis', 'Chen Liang': 'Liang Chen',
          'Aisam-Ul-Hag Qureshi': 'Aisam-Ul-Haq Qureshi', 'Lourdes Dominquez Lino': 'Lourdes Dominguez Lino', 
          'Silvia Soler-Espinosa': 'Sivia Soler Espinosa', 'David Marrero Santana': 'David Marrero', 'Michelle Larcherde Brito': 'Michelle Larcher De Brito',
          'Rebecca Petersson': 'Rebecca Peterson', 'Rohan Bopanna Machanda': 'Rohan Bopanna', 'Viktoria Kamenskaya': 'Victoria Kamenskaya',
          'Cheng-Peng Hsieh': 'Hsieh Cheng-peng'}

UP_QRY = 'insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values ( %s,  %s, %s, now(), now()) on duplicate key update modified_at = now()'

def main():
    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSRADARDB", charset='utf8', use_unicode=True).cursor()


    sel_qry = 'select id, game_datetime, game_note, tournament_id, event_id from sports_games where sport_id=5'
    sp_cur.execute(sel_qry)
    sp_data = sp_cur.fetchall()
    for data_ in sp_data[::1]:
        Gi, Od, Ep, Ti, At = data_
        home_team = Ep.split(' vs. ')[-1].strip()
        away_team = Ep.split(' vs. ')[0].strip()
        home_team = C_DICT.get(home_team, home_team)
        away_team = C_DICT.get(away_team, away_team)

        if Ti == "sr:tournament:2100":
            home_team = home_team + " Davis Cup team"
            away_team = away_team + " Davis Cup team"

        if  Ti == "sr:tournament:620":
            home_team = home_team + " at the Hopman Cup"
            away_team = away_team + " at the Hopman Cup"

        sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=5'
            
        hm_values = (home_team)
        sp_cur.execute(sel_qry, hm_values)
        hm_data = sp_cur.fetchone()

        if not hm_data:
            print 'missing_home', home_team, Gi, Ep
            continue

        hm_id = hm_data[0]
        aw_values = (away_team)
        sp_cur.execute(sel_qry, aw_values)
        aw_data = sp_cur.fetchone()
        if not aw_data:
            print 'missing_away', away_team, Gi, Ep
            continue


        query = 'select sportsdb_id from sports_radar_merge where type="tournament" and radar_id=%s'
        tm_values = (Ti)
        sp_cur.execute(query, tm_values)
        records = sp_cur.fetchone()
        if records:
            tou_id = records[0]
        else:
            print 'missing tou', Ti
            continue

        aw_id = aw_data[0]
        ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s)'
        
        game_datetime = '%' + str(Od).split(' ')[0] + '%'
        oth_game_datetime = '%' + str(Od) + '%'
        game_values = (tou_id, game_datetime, hm_id, aw_id)
        oth_game_values = (tou_id, oth_game_datetime, hm_id, aw_id)

        if len(Ep.split(' vs. ')) == 4:
            home_team1 = Ep.split(' vs. ')[-2].strip()
            away_team1 = Ep.split(' vs. ')[1].strip()

            home_team1 = C_DICT.get(home_team1, home_team1)
            away_team1 = C_DICT.get(away_team1, away_team1)            

            if Ti == "sr:tournament:2100":
                home_team1 = home_team1 + " Davis Cup team"
                away_team1 = away_team1 + " Davis Cup team"

            if  Ti == "sr:tournament:620":
                home_team1 = home_team1 + " at the Hopman Cup"
                away_team1 = away_team1 + " at the Hopman Cup"

            hm1_values = (home_team1)
            sp_cur.execute(sel_qry, hm1_values)
            hm1_data = sp_cur.fetchone()

            if not hm1_data:
                print home_team1
                continue

            hm1_id = hm1_data[0]
            aw1_values = (away_team1)
            sp_cur.execute(sel_qry, aw1_values)
            aw1_data = sp_cur.fetchone()

            if not aw1_data:
                print away_team1
                continue

            aw1_id = aw1_data[0]
            ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s, %s, %s)'
            game_values = (tou_id, game_datetime, hm_id, aw_id, hm1_id, aw1_id)
            oth_game_datetime = '%' + str(Od) + '%'
            oth_game_values = (tou_id, oth_game_datetime, hm_id, aw_id, hm1_id, aw1_id)
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

