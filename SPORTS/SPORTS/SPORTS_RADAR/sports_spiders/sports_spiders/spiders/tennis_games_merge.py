import logging
from vtv_db import get_mysql_connection
from datetime import timedelta
from pprint import pprint

C_DICT = {'Ilia Marchenko': 'Illya Marchenko', 'Pablo Carreno-Busta':'Pablo Carreno Busta', 'Joao (POR) Sousa': 'Joao Sousa',
          'Roberto Bautista-Agut': 'Roberto Bautista Agut', 'Richard Berankis': 'Ricardas Berankis',
          'Albert Ramos-Vinolas': 'Albert Ramos Vinolas', 'Yen-Hsun Lu': 'Lu Yen-hsun', 'Teymuraz Gabashvili': 'Teimuraz Gabashvili',
          'Aleksandr Dolgopolov': 'Alexandr Dolgopolov', 'Pierre Hughes Herbert': 'Pierre-Hugues Herbert',
          'Guilherme Clesar': 'Guilherme Clezar', 'Alejandro (Col) Gonzalez': 'Alejandro Gonzalez', 'Marcelo Arevalo-Gonzalez': 'Marcelo Arevalo',
          'Adrian Menendez': 'Adrian Menendez-Maceiras', 'Yan Bai': 'Bai Yan', 'Thiago Moura Monteiro': 'Thiago Monteiro',
          'Alexandre Kudryavtsev': 'Alexander Kudryavtsev', 'Sam Groth': 'Samuel Groth', 'Di Wu': 'Wu Di', 'Xinyun Han': 'Han Xinyun',
          'Cristina-Andreea Mitu': 'Andreea Mitu', 'Kai Lin Zhang': 'Kai-Lin Zhang', 'An Sophie Mestach': 'An-Sophie Mestach',
          'Asia Muhammad': 'Asia Muhammed', 'Shuai Zhang': 'Zhang Shuai', 'Katerina Bondarenko': 'Kateryna Bondarenko',
          'Yi-Fan Xu': 'Xu Yifan', 'Yafan Wang': 'Wang Yafan', 'Yung-Jan Chan': 'Chan Yung-jan',
          'Ana Isabel Medina Garrigues': 'Anabel Medina Garrigues', 'Alexandra Krunic': 'Aleksandra Krunic', 'Anna-Karolina Schmiedlova': 'Anna Karolina Schmiedlova',
          'Klaudia Jans': 'Klaudia Jans-Ignacik', 'Anna-Lena Groenefeld': 'Anna-Lena Gronefeld', 'Raluca-Ioana Olaru': 'Raluca Olaru',
          'Bethanie Mattek': 'Bethanie Mattek-Sands', 'Shuai Peng': 'Peng Shuai', 'Anastassia Rodionova': 'Anastasia Rodionova',
          'Usa at the Hopman Cup': 'United States at the Hopman Cup', 'Ze Zhang': 'Zhang Ze',
          'Zhe Li':  'Li Zhe', 'Hans Podlipnik': 'Hans Podlipnik-Castillo', 'Marcelo Arevalo-Gonzalez':'Marcelo Arevalo','Tristan Weissborn': 'Tristan-Samuel Weissborn',
          'Alex Satschko': 'Alexander Satschko', 'James Cerretini': 'James Cerretani', 'Treat Conrad Huey': 'Treat Huey',
          'Oceane Dodin': 'Dodin Oceane', 'Jia-Jing Lu': 'Lu Jiajing', 'Richard Berankis': 'Ricardas Berankis', 'Chen Liang': 'Liang Chen',
          'Aisam-Ul-Hag Qureshi': 'Aisam-Ul-Haq Qureshi', 'Lourdes Dominquez Lino': 'Lourdes Dominguez Lino',
          'Silvia Soler-Espinosa': 'Sivia Soler Espinosa', 'David Marrero Santana': 'David Marrero', 'Michelle Larcherde Brito': 'Michelle Larcher De Brito',
          'Rebecca Petersson': 'Rebecca Peterson', 'Rohan Bopanna Machanda': 'Rohan Bopanna', 'Viktoria Kamenskaya': 'Victoria Kamenskaya',
          'Cheng-Peng Hsieh': 'Hsieh Cheng-peng', 'Hao-Ching Chan': 'Chan Hao-ching', 'Haerim Ahn': 'Kristie Ahn', \
          'Kristie Haerim Ahn': 'Kristie Ahn', 'Catherine Cartan Bellis': 'CiCi Bellis', 'Sesil Karatancheva': 'Sesil Karatantcheva', \
          'Lara Arrubarrena-Vecino': 'Lara Arruabarrena', 'Kourtney J Keegan': 'Kourtney Keegan', 'Carla Suarez-Navarro': 'Carla Suarez Navarro', \
          'Bianca Vanessa Andreescu': 'Bianca Andreescu', 'Lara Arruabarrena-Vecino': 'Lara Arruabarrena', 'Jackson Withrow': 'Jack Withrow', \
          'Taylor Harry Fritz': 'Taylor Fritz', 'Catherine Bellis': 'CiCi Bellis', 'Sesil Karatancheva': 'Sesil Karatantcheva', \
          'Lara Arrubarrena-Vecino': 'Lara Arruabarrena', 'Kourtney J Keegan': 'Kourtney Keegan', 'Carla Suarez-Navarro': 'Carla Suarez Navarro', \
           'Bianca Vanessa Andreescu': 'Bianca Andreescu', 'Lara Arruabarrena-Vecino': 'Lara Arruabarrena', 'Jackson Withrow': 'Jack Withrow'}

UP_QRY = 'insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values ( %s,  %s, %s, now(), now()) on duplicate key update modified_at = now()'

class SportsRadarTennisMerge:
    def __init__(self):
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger = logging.getLogger('sportsRadartennisMerge.log')
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
    def tennis_merge(self):
        sel_qry = 'select id, game_datetime, game_note, tournament_id, event_id from sports_games where sport_id=5'
        self.cursor.execute(sel_qry)
        sp_data = self.cursor.fetchall()
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

            hm_data = self.get_participant_id(home_team)
            if not hm_data:
                print 'missing_home', home_team, Gi, Ep
                continue

            hm_id = hm_data[0]
            aw_data = self.get_participant_id(away_team)
            if not aw_data:
                print 'missing_away', away_team, Gi, Ep
                continue

            query = 'select sportsdb_id from sports_radar_merge where type="tournament" and radar_id=%s'
            tm_values = (Ti)
            self.cursor.execute(query, tm_values)
            records = self.cursor.fetchone()
            if records:
                tou_id = records[0]
            else:
                print 'missing tou', Ti
                continue

            aw_id = aw_data[0]
            ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s) and status !="Hole"'
            
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

                hm1_data = self.get_participant_id(home_team1)

                if not hm1_data:
                    print home_team1
                    continue

                hm1_id = hm1_data[0]
                aw1_data = self.get_participant_id(away_team1)
                if not aw1_data: 
                    print away_team1
                    continue

                aw1_id = aw1_data[0]
                ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s, %s, %s) and Sg.status !="Hole"'
                game_values = (tou_id, game_datetime, hm_id, aw_id, hm1_id, aw1_id)
                oth_game_datetime = '%' + str(Od) + '%'
                oth_game_values = (tou_id, oth_game_datetime, hm_id, aw_id, hm1_id, aw1_id)

            self.cursor.execute(ga_qry, game_values)
            game_data = self.cursor.fetchall() 
            if game_data and len(game_data) == 1:
                game_id = game_data[0][0]
                m_values = (Gi, game_id, 'game')
                self.cursor.execute(UP_QRY, m_values)
            else:
                
                game_values = oth_game_values
                self.cursor.execute(ga_qry, game_values)
                game_data = self.cursor.fetchall()
                if game_data and len(game_data) == 1:
                    game_id = game_data[0][0]
                    m_values = (Gi, game_id, 'game')
                    self.cursor.execute(UP_QRY, m_values)


    def get_participant_id(self, team_name):
        sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=5'
        home_team = team_name
        hm_values = (home_team)
        self.cursor.execute(sel_qry, hm_values)
        hm_data = self.cursor.fetchone()
        if not hm_data:
            sel_qry = 'select id from SPORTSDB.sports_participants where aka=%s and sport_id=5'

            hm_values = (home_team)
            self.cursor.execute(sel_qry, hm_values)
            hm_data = self.cursor.fetchone()
        if not hm_data:
            home_team = home_team.split(' ')[-1] + " " + home_team.split(' ')[0]
            sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=5'
            hm_values = (home_team)
            self.cursor.execute(sel_qry, hm_values)
            hm_data = self.cursor.fetchone()
        if not hm_data:
            home_team = home_team.replace('-', ' ').strip()
            sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=5'
            hm_values = (home_team)
            self.cursor.execute(sel_qry, hm_values)
            hm_data = self.cursor.fetchone()
        return hm_data


    def run_main(self):
        self.tennis_merge()
        

if __name__ == "__main__":
    SportsRadarTennisMerge().run_main()

