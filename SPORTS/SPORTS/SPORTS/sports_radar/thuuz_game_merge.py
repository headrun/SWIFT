#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import genericFileInterfaces
import foldFileInterface
import datetime
import requests
import json
from pprint import pprint
import time
from data_schema import get_schema
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

PART_DICT = {'Ajax': 'AFC Ajax', 'Manchester United': 'Manchester United F.C.', 'Anderlecht': 'R.S.C. Anderlecht',
 'KRC Genk': 'K.R.C. Genk', 'Celta Vigo': 'Celta de Vigo', 'Besiktas': 'Besiktas J.K.', 'Lyon': 'Olympique Lyonnais',
 'Leicester City': 'Leicester City F.C.', 'Frances Tiafoe': 'Francis Tiafoe', 'Diego Schwartzman': 'Diego Sebastian Schwartzman',
 'Albert Ramos-Vinolas': 'Albert Ramos Vinolas'}

SPORT_DICT = {'Basketball': 2, 'Soccer': 7, 'Tennis': 5, 'Golf': 8, 'Football': 4, 'Baseball': 1}
TOU_DICT = {'FRAN': 32, 'CH-UEFA2': 215, 'CHLG': 216, 'NBA': 229, 'US Mens Clay Court Championship': 56, 'Grand Prix Hassan II': 20, 'Miami Open': 246,
            'Shell Houston Open': 69, 'Masters Tournament': 70, 'Trophee Hassan II': 551, 'RBC Heritage': 71, 'Shenzhen International': 1876,
            'Valero Texas Open': 102, 'Zurich Classic of New Orleans': 72, 'Volvo China Open': 530, 'Wells Fargo Championship': 74, 'THE PLAYERS Championship': 75,
            'BMW PGA Championship': 875, 'Open de Portugal': 1809, 'DEAN & DELUCA Invitational': 77, 'AT&T Byron Nelson': 73, 'The Rocco Forte Open': 526,
            'Nordea Masters': 541, 'the Memorial Tournament presented by Nationwide': 79, 'U.S. Open': 81, 'Lyoness Open': 3174, 'FedEx St. Jude Classic': 80,
            'BMW International Open': 535, 'Travelers Championship': 82, 'HNA Open de France': 534, 'Quicken Loans National': 368,
            'Dubai Duty Free Irish Open': 504, 'The Greenbrier Classic': 369,'John Deere Classic': 85,
            'Aberdeen Asset Management Scottish Open': 514,'The Open Championship': 378,
            'Barbasol Championship': 1880,'Porsche European Open': 1877,'RBC Canadian Open': 87, 'NFL': 197, 'EPL': 35, 
            'BUND': 33, 'LIGA': 29, 'SERI': 579, 'MLS': 34, 'NHL': 240,
            'Barracuda Championship': 90,'World Golf Championships-Bridgestone Invitational': 91,'PGA Championship': 92, 'NCAA': 9,
            'Fiji International': 3157}
#TOU_DICT = {'FRAN': 32, 'EPL': 35, 'BUND': 33, 'LIGA': 29, 'SERI': 579, 'MLS': 34, 'NFL': 197}
#TOU_DICT = {'NCAA': 9}

UP_QRY = 'insert into sports_thuuz_merge(thuuz_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'


class ThuuzGamesMerge():

    def __init__(self):
        self.MIS_TOUS = {}
        self.text = ''
        self.logger = initialize_timed_rotating_logger('thuuz_validator.log')

    def get_html_table(self, tou_list):
        table_data = '<br /><br /><br /><table border="1" \
                  style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>'
        table_data += '<th>%s</th><th>%s</th>' % ('Tournament', 'Sport')
        table_data += '</tr>'

        for data in tou_list:
            table_data += '<tr>'
            table_data += '<td >%s</td><td>%s</td>' % (data, tou_list[data])
            table_data += '</tr>'
        table_data += '</table>'

        return table_data

    def send_mail(self, text):
        subject    = 'New Thuuz Tournaments'
        server     = 'localhost'
        sender     = 'headrun@veveo.net'
        receivers = ['sports@headrun.com']

        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def mail_mis_tous(self, mis_tous):
        self.text += self.get_html_table(mis_tous)
        self.send_mail(self.text)

        

    def clean_participants(self, Ep, sport_id, Ge, Ti):
        home_team = Ep.split(' at ')[-1].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
        away_team = Ep.split(' at ')[0].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
        home_team = home_team.replace('Central Connecticut State', 'Central Connecticut').replace('Lehigh Hawks', 'Lehigh Mountain Hawks').replace('Virginia-Wise', 'Virginia Wise').replace('Charleston (WV)', 'Charleston Univ').replace('Minnesota State Moorhead', 'Minnesota State Univ.-Moorhead').replace('Gustavus', 'Gustavus Adolphus Golden').replace('St. Johns Johnnies', "St. John's (Minn.) Johnnies").replace('St. Thomas Tommies', "St. Thomas (Minn.) Tommies").replace('Pennsylvania', 'Penn').replace('Miami (OH)', 'Miami').replace('Miami (FL) ', 'Miami ').replace('Francis (PA)', 'Saint Francis').replace('Louisiana-Monroe', u'Louisiana–Monroe').replace('Nicholls', 'Nicholls State').replace('Delaware', "Delaware Fightin'").replace('Tennessee-Martin', 'UT Martin').replace('FIU Golden', 'FIU').replace('Anselm', 'St. Anselm').replace('North Carolina State', 'NC State').replace('Virginia Military', 'VMI').replace('Arkansas-Pine', u'Arkansas–Pine').replace('Massachusetts Minutemen', 'UMass Minutemen').replace('Brigham Young', 'BYU').replace("Delaware Fightin' State", "Delaware State Hornets").replace('Southern University', 'Southern').replace('Southern Methodist', 'SMU').replace('Mississippi Rebels', 'Ole Miss Rebels').replace('Eastern Tennessee', 'East Tennessee').replace('Louisiana-Lafayette', u'Louisiana–Lafayette').replace('Hornets Hornets', 'Hornets').replace('Boston Eagles', 'Boston College Eagles').replace('Northern Colorado', 'Northern Colorado Bears').replace('Citadel', 'The Citadel').replace('Texas-San Antonio', 'UTSA').replace('Gardner-Webb', 'Gardner–Webb').replace('Idaho Coyotes', 'Idaho Yotes').replace('Brevard College', 'Brevard').replace('Limestone College', 'Limestone')
        away_team = away_team.replace('Central Connecticut State', 'Central Connecticut').replace('Lehigh Hawks', 'Lehigh Mountain Hawks').replace('Virginia-Wise', 'Virginia Wise').replace('Charleston (WV)', 'Charleston Univ').replace('Minnesota State Moorhead', 'Minnesota State Univ.-Moorhead').replace('Gustavus', 'Gustavus Adolphus Golden').replace('St. Johns Johnnies', "St. John's (Minn.) Johnnies").replace('St. Thomas Tommies', "St. Thomas (Minn.) Tommies").replace('Pennsylvania', 'Penn').replace('Miami (OH)', 'Miami').replace('Miami (FL) ', 'Miami ').replace('Francis (PA)', 'Saint Francis').replace('Louisiana-Monroe', u'Louisiana–Monroe').replace('Nicholls', 'Nicholls State').replace('Delaware', "Delaware Fightin'").replace('Tennessee-Martin', 'UT Martin').replace('FIU Golden', 'FIU').replace('Anselm', 'St. Anselm').replace('North Carolina State', 'NC State').replace('Virginia Military', 'VMI').replace('Arkansas-Pine', u'Arkansas–Pine').replace('Massachusetts Minutemen', 'UMass Minutemen').replace('Brigham Young', 'BYU').replace("Delaware Fightin' State", "Delaware State Hornets").replace('Southern University', 'Southern').replace('Southern Methodist', 'SMU').replace('Mississippi Rebels', 'Ole Miss Rebels').replace('Eastern Tennessee', 'East Tennessee').replace('Louisiana-Lafayette', u'Louisiana–Lafayette').replace('Hornets Hornets', 'Hornets').replace('Boston Eagles', 'Boston College Eagles').replace('Northern Colorado', 'Northern Colorado Bears').replace('Citadel', 'The Citadel').replace('Texas-San Antonio', 'UTSA').replace('Gardner-Webb', 'Gardner–Webb').replace('Idaho Coyotes', 'Idaho Yotes').replace('Brevard College', 'Brevard').replace('Limestone College', 'Limestone')

        if home_team == "North Dakota":
            home_team = "North Dakota Fighting Hawks"
        if away_team == "North Dakota":
            away_team = "North Dakota Fighting Hawks"
        if 'FC Bayern M' in home_team: home_team = 'FC Bayern Munich'
        if 'FC Bayern M' in away_team: away_team = 'FC Bayern Munich'
        if 'tico de Madrid' in home_team : home_team = 'Atletico Madrid'
        if 'tico de Madrid' in away_team : away_team = 'Atletico Madrid'
        if 'Brighton & Hove FC' in home_team: home_team = 'Brighton & Hove Albion F.C.'
        if 'Brighton & Hove FC' in away_team: away_team = 'Brighton & Hove Albion F.C.'
        if 'Huddersfield Town FC' in home_team: home_team = 'Huddersfield Town A.F.C.'
        if 'Huddersfield Town FC' in away_team: away_team = 'Huddersfield Town A.F.C.'
        if 'Guingamp' in home_team: home_team = 'En Avant de Guingamp'
        if 'Guingamp' in away_team: away_team = 'En Avant de Guingamp'
        if 'Bayer Leverkusen' in home_team: home_team = 'Bayer 04 Leverkusen'
        if 'Bayer Leverkusen' in away_team: away_team = 'Bayer 04 Leverkusen'
        if 'Paris Saint-Germain FC' in home_team: home_team = 'Paris Saint-Germain F.C.'
        if 'Paris Saint-Germain FC' in away_team: away_team = 'Paris Saint-Germain F.C.'
        if 'AC Milan' in home_team: home_team = 'A.C. Milan'
        if 'AC Milan' in away_team: away_team = 'A.C. Milan'
        if 'Los Angeles Galaxy' in home_team: home_team = 'LA Galaxy'
        if 'Los Angeles Galaxy' in away_team: away_team = 'LA Galaxy'
        if 'Celta Vigo' in home_team: home_team = 'Celta de Vigo'
        if 'Celta Vigo' in away_team: away_team = 'Celta de Vigo'
        if 'SPAL' in home_team: home_team = 'S.P.A.L. 2013'
        if 'SPAL' in away_team: away_team = 'S.P.A.L. 2013'
        if 'Villarreal' in home_team: home_team = "Villarreal CF"
        if 'Villarreal' in away_team: away_team = "Villarreal CF"

        if sport_id == 5:
            if ',' in home_team: home_team = home_team.split(',')[-1].strip() + ' ' + home_team.split(',')[0].strip() 
            if ',' in away_team : away_team = away_team.split(',')[-1].strip() + ' ' + away_team.split(',')[0].strip()
        if sport_id == 4 and 'football' in Ge.lower() and Ti == "NCAA":
            home_team = home_team + " football"
            away_team = away_team + " football"
        home_team = PART_DICT.get(home_team, home_team)
        away_team = PART_DICT.get(away_team, away_team)
        return home_team, away_team

    def merge_golf_games(self, Gi, Ti, Od, cur):
        temp_check_list_1 = [Od + datetime.timedelta(days = 1) for i in range(4)]
        temp_check_list_2 = [Od - datetime.timedelta(days = 1) for i in range(4)]
        date_list = temp_check_list_1 + temp_check_list_2
        tou_id = TOU_DICT.get(Ti,'')
        if not tou_id:
            self.MIS_TOUS[Ti] = 'golf'
            return
        game_id_qry = 'select id from SPORTSDB.sports_games where tournament_id = %s and game_datetime like %s'
        vals = (tou_id, '%'+str(Od)+'%')
        cur.execute(game_id_qry, vals)
        try:
            game_id = cur.fetchone()[0]
        except:
            game_id = ''
        if game_id:
            cur.execute(UP_QRY, (Gi, game_id, 'game'))
        else:
            for i in list(set(date_list)):
                cur.execute(game_id_qry, (tou_id, '%'+str(i)+'%'))
                try:
                    game_id = cur.fetchone()[0]
                except:
                    game_id = ''
                if game_id:
                    cur.execute(UP_QRY, (Gi, game_id, 'game'))

    def main(self):
        sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSRADARDB", charset='utf8', use_unicode=True).cursor()


        sel_qry = 'select Gi, Ti, EP, Od, At, Ge from sports_thuuz_games'
        sp_cur.execute(sel_qry)
        sp_data = sp_cur.fetchall()
        for data_ in sp_data[::1]:
            Gi, Ti, Ep, Od, At , Ge = data_
            day_list = [Od + datetime.timedelta(days = 1), Od - datetime.timedelta(days = 1)]
            sport_id = SPORT_DICT[Ge.split('{')[0].strip()]
            if sport_id == 8:
                self.merge_golf_games(Gi, Ti, Od, sp_cur)
                continue
            home_team, away_team = self.clean_participants(Ep, sport_id, Ge, Ti)
            if Ti == "NBA":
                sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=2 and participant_type="team"'
            elif Ti == "NCAA" and sport_id == 4:
                sel_qry = 'select id from SPORTSDB.sports_participants where title=%s and sport_id=4 and participant_type="team"'
            else: 
                sel_qry = 'select SP.id from SPORTSDB.sports_participants SP, SPORTSDB.sports_tournaments_participants T where T.tournament_id=%s and SP.title like %s and SP.sport_id=%s and SP.participant_type=%s and SP.id=T.participant_id'
                home_team = '%' + home_team + '%'
                away_team = '%' + away_team + '%'
            tou_id = TOU_DICT.get(Ti, '')
            if not tou_id: continue
            if tou_id in (56, 20, 246):
                _type = 'player'
            else: _type = 'team'

            if Ti == 'NBA' or Ti == "NCAA":
                hm_values = (home_team)
            else:
                hm_values = (tou_id, home_team, sport_id, _type)
            sp_cur.execute(sel_qry, hm_values)
            hm_data = sp_cur.fetchone()
            if not hm_data:
                print home_team+ ','+ str(tou_id)
                continue
            hm_id = str(hm_data[0])+'<>1'
            if Ti == 'NBA' or Ti == "NCAA":
                aw_values = (away_team)
            else:
                aw_values = (tou_id, away_team, sport_id, _type)
            sp_cur.execute(sel_qry, aw_values)
            aw_data = sp_cur.fetchone()
            if not aw_data:
                print away_team+ ','+ str(tou_id)
                continue
            aw_id = str(aw_data[0])+'<>0'
            ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and concat(Sgp.participant_id,"<>",Sgp.is_home) in (%s, %s) and Sg.status !="Hole"'
            game_datetime = '%' + str(Od) + '%'
            game_values = (tou_id, game_datetime, hm_id, aw_id)
            sp_cur.execute(ga_qry, game_values)
            game_data = sp_cur.fetchall() 
            if not game_data:
                ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s) and Sg.status !="Hole"'
                sp_cur.execute(ga_qry, game_values)
                game_data = sp_cur.fetchall()
            if not game_data and Ti == "NCAA" and sport_id ==4:
                ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id in (%s, %s, %s) and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s) and Sg.status !="Hole"'
                hm_id = str(hm_data[0])
                aw_id = str(aw_data[0])
                game_values = ('9', '1072', '1073', game_datetime, hm_id, aw_id)
                sp_cur.execute(ga_qry, game_values)
                game_data = sp_cur.fetchall()

            
            if game_data and len(game_data) == 1:
                game_id = game_data[0][0]
                m_values = (Gi, game_id, 'game')
                sp_cur.execute(UP_QRY, m_values)
            else:
                for _date in day_list:
                    ga_qry = 'select distinct Sg.id  from SPORTSDB.sports_games Sg, SPORTSDB.sports_games_participants Sgp where Sgp.game_id = Sg.id and Sg.tournament_id = %s and Sg.game_datetime like %s and Sgp.participant_id in (%s, %s) and Sg.status !="Hole"'

                    game_date = '%' + str(_date) + '%'
                    gm_vals = (tou_id, game_date, hm_id, aw_id)
                    sp_cur.execute(ga_qry, gm_vals)
                    game_data = sp_cur.fetchall()
                    if game_data and len(game_data) == 1:
                        game_id = game_data[0][0]
                        m_values = (Gi, game_id, 'game')
                        sp_cur.execute(UP_QRY, m_values)
                        break
                '''
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
                '''
                

        sp_cur.close()
        if self.MIS_TOUS:
            self.mail_mis_tous(self.MIS_TOUS)

if __name__ == '__main__':
    ThuuzGamesMerge().main()   
