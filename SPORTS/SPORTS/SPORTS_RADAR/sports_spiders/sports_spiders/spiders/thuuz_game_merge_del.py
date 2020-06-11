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
import MySQLdb, re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
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
          'Taylor Harry Fritz': 'Taylor Fritz', 'Catherine Bellis': 'CiCi Bellis'}

PART_DICT = {'Ajax': 'AFC Ajax', 'Manchester United': 'Manchester United F.C.', 'Anderlecht': 'R.S.C. Anderlecht',
             'KRC Genk': 'K.R.C. Genk', 'Celta Vigo': 'Celta de Vigo', 'Besiktas': 'Besiktas J.K.', 'Lyon': 'Olympique Lyonnais',
             'Leicester City': 'Leicester City F.C.', 'Frances Tiafoe': 'Francis Tiafoe', 'Diego Schwartzman': 'Diego Sebastian Schwartzman',
             'Albert Ramos-Vinolas': 'Albert Ramos Vinolas', 'Taylor University Trojans': 'Taylor Trojans', 'Walsh University Cavaliers': 'Walsh Cavaliers', \
             'Trinity International (IL) Trojans': 'Trinity International Trojans', 'Southwestern College Moundbuilders': 'Southwestern Moundbuilders', \
             'Franklin College Grizzlies': 'Franklin Grizzlies', \
             'Guilford College Quakers': 'Guilford Quakers', 'C.D Tiburones Rojos de Veracruz': 'Tiburones Rojos de Veracruz', \
             'C.A. Monarcas Morelia':'Monarcas Morelia', 'UANL Tigres': 'Tigres UANL', 'Pumas UNAM': 'Club Universidad Nacional', \
             'C.A. Monarcas Morelia': 'Monarcas Morelia', 'Cruz Azul F.C.': 'Cruz Azul', \
             'Coritiba FBC': 'Coritiba Foot Ball Club', 'AA Ponte Preta': 'Associacao Atletica Ponte Preta', 'SE Palmeiras': 'Sociedade Esportiva Palmeiras', \
            u'Atlético Goianiense': u'Atlético Clube Goianiense', 'CR Flamengo': 'Clube de Regatas do Flamengo', 'Fluminense FC': 'Fluminense FC', \
            'Cruzeiro EC': 'Cruzeiro Esporte Clube', 'SC Corinthians Paulista': 'Sport Club Corinthians Paulista', 'CA Paranaense': u'Clube Atlético Paranaense', \
            'Chapecoense AF': u'Associação Chapecoense de Futebol', u'Grêmio FB Porto Alegrense': u'Grêmio Foot-Ball Porto Alegrense', \
            'North Carolina Wilmington Seahawks': 'UNC Wilmington Seahawks', "Delaware Fightin Blue Hens": "Delaware Fightin' Blue Hens", \
            "Slavia Prague": "SK Slavia Prague", "AEK Larnaka":"AEK Larnaca", "Fenerbahce SK":"Fenerbahce S.K."}

SPORT_DICT = {'Basketball': 2, 'Soccer': 7, 'Tennis': 5, 'Golf': 8, 'Football': 4, 'Baseball': 1, 'Hockey': 3, 'Race': 10}
#SPORT_DICT = {'Baseball': 1}
"""TOU_DICT = {'FRAN': 32, 'CH-UEFA2': 215, 'CHLG': 216, 'NBA': 229, 'US Mens Clay Court Championship': 56, 'Grand Prix Hassan II': 20, 'Miami Open': 246,
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
            'Fiji International': 3157,'Wyndham Championship': 93,
            'THE NORTHERN TRUST': 94,'Paul Lawrie Match Play': 1878,'Made In Denmark': 1251,'Czech Masters': 1252, 'MLB': 88, 'NCAA': 9,
            'BMW Championship': 96,'Omega European Masters': 540,'KLM Open': 523,'Dell Technologies Championship': 95, 'MLB': 88, 'TOUR Championship': 97,
            'Portugal Masters': 536,'British Masters': 1899 ,'Presidents Cup': 101,'Alfred Dunhill Links Championship': 515 ,
            'Safeway Open': 104, 'CIMB Classic': 372 ,'Italian Open': 519, 'Andalucia Valderrama Masters': 8652,
            'World Golf Championships-HSBC Champions': 509, 'Shriners Hospitals for Children Open': 103, 'Sanderson Farms Championship': 99,
            'Turkish Airlines Open': 904, 'Nedbank Golf Challenge': 1243, 'OHL Classic at Mayakoba': 62, 'The CJ Cup at Nine Bridges': 9572, 'The RSM Classic': 371,
            'DP World Tour Championship Dubai': 518, 'MWC': 954, 'LIGAMX': 586, 'BRA1': 573, 'UBS Hong Kong Open': 548,
            'Australian PGA Championship': 2992, 'Hero World Challenge': 539, 'AfrAsia Bank Mauritius Open': 1956, 'Joburg Open': 507,
            'BMW SA Open': 543, 'Sentry Tournament of Champions': 366,'Sony Open in Hawaii': 55, 'Abu Dhabi HSBC Championship': 528, 'EURASIA CUP presented by DRB-HICOM': 1244, 
            'CareerBuilder Challenge': 57, 'Farmers Insurance Open': 58, 'Omega Dubai Desert Classic': 527,'Maybank Championship': 8649, 'Waste Management Phoenix Open': 59,'AT&T Pebble Beach Pro-Am': 60,
            'Puerto Rico Open': 367,'Genesis Open': 61, 'The Honda Classic': 64, 'Commercial Bank Qatar Masters': 506, 'Tshwane Open': 547, 'Arnold Palmer Invitational presented by Mastercard': 66, 'Valspar Championship': 65, 'Hero Indian Open': 1875, 'World Golf Championships-Mexico Championship': 68, 'Arnold Palmer Invitational': 66, 'PGA European Tour': 218, 'open de españa': 505, 'Corales Puntacana Resort & Club Championship': 3371, 'Houston Open': 2, 'Fort Worth Invitational': 77,'Open de España': 505, 'Rocco Forte Sicilian Open': 526,
            'DUBAI DUTY FREE IRISH OPEN': 504, "KPMG Women's PGA Championship": 123, 'A Military Tribute at The Greenbrier': 369, 'Shot Clock Masters': 1869, 'HNA OPEN DE FRANCE': 534, 'NBO Oman Open': 9588, "World Golf Championships-Dell Technologies Match Play": 63, "ABERDEEN STANDARD INVESTMENTS SCOTTISH OPEN": 514, "Open de España": 505}"""
TOU_DICT = {'FRAN': 32, 'EPL': 35, 'BUND': 33, 'LIGA': 29, 'SERI': 579, 'MLS': 34, 'NFL': 197, 'NHL':240, 'CH-UEFA2': 215, 'CHLG': 216, 'NBA': 229, 'MLB': 88, 'NCAA': 9}
#TOU_DICT = {'MLB': 88}

UP_QRY = 'insert into sports_thuuz_merge(thuuz_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'


class ThuuzGamesMergedel():

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
        #receivers = ['sports@headrun.com']
        receivers = ['sraju@headrun.com']

        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def mail_mis_tous(self, mis_tous):
        self.text += self.get_html_table(mis_tous)
        self.send_mail(self.text)

        

    def clean_participants(self, Ep, sport_id, Ge, Ti):
        home_team = Ep.split(' at ')[-1].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
        away_team = Ep.split(' at ')[0].strip().replace('St. ', '').replace(' SG', '').replace('Rennes', 'Rennais').strip()
        home_team = home_team.replace("Thomas (TX)", "St. Thomas (TX)").replace('Mountain Lions Concord', 'Concord Mountain Lions').replace('Concordia University-Paul', 'Concordia, St. Paul').replace('Bonaventure', 'St. Bonaventure').replace('College of Charleston', 'Charleston').replace('Virginia Commonwealth', 'VCU').replace('Central Connecticut State', 'Central Connecticut').replace('Lehigh Hawks', 'Lehigh Mountain Hawks').replace('Virginia-Wise', 'Virginia Wise').replace('Charleston (WV)', 'Charleston Univ').replace('Minnesota State Moorhead', 'Minnesota State Univ.-Moorhead').replace('Gustavus', 'Gustavus Adolphus Golden').replace('St. Johns Johnnies', "St. John's (Minn.) Johnnies").replace('St. Thomas Tommies', "St. Thomas (Minn.) Tommies").replace('Pennsylvania', 'Penn').replace('Miami (OH)', 'Miami').replace('Miami (FL) ', 'Miami ').replace('Francis (PA)', 'Saint Francis').replace('Louisiana-Monroe', u'Louisiana–Monroe').replace('Nicholls', 'Nicholls State').replace('Delaware', "Delaware Fightin'").replace('Tennessee-Martin', 'UT Martin').replace('FIU Golden', 'FIU').replace('Anselm', 'St. Anselm').replace('North Carolina State', 'NC State').replace('Virginia Military', 'VMI').replace('Arkansas-Pine', u'Arkansas–Pine').replace('Massachusetts Minutemen', 'UMass Minutemen').replace('Brigham Young', 'BYU').replace("Delaware Fightin' State", "Delaware State Hornets").replace('Southern University', 'Southern').replace('Southern Methodist', 'SMU').replace('Mississippi Rebels', 'Ole Miss Rebels').replace('Eastern Tennessee', 'East Tennessee').replace('Louisiana-Lafayette', u'Louisiana–Lafayette').replace('Hornets Hornets', 'Hornets').replace('Boston Eagles', 'Boston College Eagles').replace('Northern Colorado', 'Northern Colorado Bears').replace('Citadel', 'The Citadel').replace('Texas-San Antonio', 'UTSA').replace('Gardner-Webb', 'Gardner–Webb').replace('Idaho Coyotes', 'Idaho Yotes').replace('Brevard College', 'Brevard').replace('Limestone College', 'Limestone').replace('FK Qarabag', 'Qarabag FK').replace('Apoel Nicosia', 'APOEL FC').replace('Sporting Lisbon', 'Sporting Clube de Portugal').replace('Sporting Braga', 'S.C. Braga').replace('Partizan Belgrade', 'FK Partizan').replace('İstanbul Başakşehir FK', 'İstanbul Başakşehir F.K.').replace('Dynamo Kiev', 'FC Dynamo Kyiv').replace('FC Copenhagen', 'F.C. Copenhagen').replace('Slavia Prague', 'SK Slavia Prague').replace('Steaua Bucharest', 'FC Steaua Bucuresti').replace('Zenit St Petersburg', 'FC Zenit').replace('Crvena Zvezda', 'Red Star Belgrade').replace('SV Zulte Waregem', 'S.V. Zulte Waregem').replace('Vitória SC de Guimarães', 'Vitória S.C.').replace('American University', 'American').replace('Blue Mountain College', 'Blue Mountain').replace('Vikings Augustana (IL)', 'Augustana (IL) Vikings').replace('Valley University', 'Valley').replace('California Riverside', 'UC Riverside').replace("Utah Runnin' Utes", 'Utah Utes').replace('Charleston', 'College of Charleston').replace('Morgan State Golden Bear', 'Morgan State Bear').replace("Mount Mary's", "Mount St. Mary's").replace("Cal State Bakersfield", "CSU Bakersfield").replace('UC-Irvine', 'UC Irvine').replace('Loyola (MD)', 'Loyola').replace('Nebraska Omaha Mavericks', 'Omaha Mavericks').replace('Texas Rio Grande Valley', u'Texas–Rio Grande Valley Vaqueros').replace('Fighting Sioux', 'Fighting Hawks').replace('University of Maryland Baltimore County', 'UMBC Retrievers').replace('City College of New York', 'CCNY').replace("John's Red Storm", "St. John's Red Storm").replace("Joseph's (PA) Hawks", "Saint Joseph's Hawks").replace("Panthers Kentucky Wesleyan", "Kentucky Wesleyan Panthers").replace('Texas-Arlington', u'Texas–Arlington').replace("Peter's Peacocks", "Saint Peter's Peacocks").replace("Shore", "Shore Hawks").replace('Bears Bears', 'Bears').replace('North Carolina Greensboro', 'UNC Greensboro').replace('IPFW Mastodons', 'Fort Wayne Mastodons').replace('Congo DR', 'DR Congo').replace(u'Vitória Futebol Clube', u'Esporte Clube Vitória').replace(u'Los Angeles Football Club', 'Los Angeles FC')


        away_team = away_team.replace("Thomas (TX)", "St. Thomas (TX)").replace('Mountain Lions Concord', 'Concord Mountain Lions').replace('Concordia University-Paul', 'Concordia, St. Paul').replace('Bonaventure', 'St. Bonaventure').replace('College of Charleston', 'Charleston').replace('Virginia Commonwealth', 'VCU').replace('Central Connecticut State', 'Central Connecticut').replace('Lehigh Hawks', 'Lehigh Mountain Hawks').replace('Virginia-Wise', 'Virginia Wise').replace('Charleston (WV)', 'Charleston Univ').replace('Minnesota State Moorhead', 'Minnesota State Univ.-Moorhead').replace('Gustavus', 'Gustavus Adolphus Golden').replace('St. Johns Johnnies', "St. John's (Minn.) Johnnies").replace('St. Thomas Tommies', "St. Thomas (Minn.) Tommies").replace('Pennsylvania', 'Penn').replace('Miami (OH)', 'Miami').replace('Miami (FL) ', 'Miami ').replace('Francis (PA)', 'Saint Francis').replace('Louisiana-Monroe', u'Louisiana–Monroe').replace('Nicholls', 'Nicholls State').replace('Delaware', "Delaware Fightin'").replace('Tennessee-Martin', 'UT Martin').replace('FIU Golden', 'FIU').replace('Anselm', 'St. Anselm').replace('North Carolina State', 'NC State').replace('Virginia Military', 'VMI').replace('Arkansas-Pine', u'Arkansas–Pine').replace('Massachusetts Minutemen', 'UMass Minutemen').replace('Brigham Young', 'BYU').replace("Delaware Fightin' State", "Delaware State Hornets").replace('Southern University', 'Southern').replace('Southern Methodist', 'SMU').replace('Mississippi Rebels', 'Ole Miss Rebels').replace('Eastern Tennessee', 'East Tennessee').replace('Louisiana-Lafayette', u'Louisiana–Lafayette').replace('Hornets Hornets', 'Hornets').replace('Boston Eagles', 'Boston College Eagles').replace('Northern Colorado', 'Northern Colorado Bears').replace('Citadel', 'The Citadel').replace('Texas-San Antonio', 'UTSA').replace('Gardner-Webb', 'Gardner–Webb').replace('Idaho Coyotes', 'Idaho Yotes').replace('Brevard College', 'Brevard').replace('Limestone College', 'Limestone').replace('FK Qarabag', 'Qarabag FK').replace('Apoel Nicosia', 'APOEL FC').replace('Sporting Lisbon', 'Sporting Clube de Portugal').replace('Sporting Braga', 'S.C. Braga').replace('Partizan Belgrade', 'FK Partizan').replace('İstanbul Başakşehir FK', 'İstanbul Başakşehir F.K.').replace('Dynamo Kiev', 'FC Dynamo Kyiv').replace('FC Copenhagen', 'F.C. Copenhagen').replace('Slavia Prague', 'SK Slavia Prague').replace('Steaua Bucharest', 'FC Steaua Bucuresti').replace('Zenit St Petersburg', 'FC Zenit').replace('Crvena Zvezda', 'Red Star Belgrade').replace('SV Zulte Waregem', 'S.V. Zulte Waregem').replace('Vitória SC de Guimarães', 'Vitória S.C.').replace('American University', 'American').replace('Blue Mountain College', 'Blue Mountain').replace('Vikings Augustana (IL)', 'Augustana (IL) Vikings').replace('Valley University', 'Valley').replace('California Riverside', 'UC Riverside').replace("Utah Runnin' Utes", 'Utah Utes').replace('Charleston', 'College of Charleston').replace('Morgan State Golden Bear', 'Morgan State Bear').replace("Mount Mary's", "Mount St. Mary's").replace("Cal State Bakersfield", "CSU Bakersfield").replace('UC-Irvine', 'UC Irvine').replace('Loyola (MD)', 'Loyola').replace('Nebraska Omaha Mavericks', 'Omaha Mavericks').replace('Texas Rio Grande Valley', u'Texas–Rio Grande Valley Vaqueros').replace('Fighting Sioux', 'Fighting Hawks').replace('University of Maryland Baltimore County', 'UMBC Retrievers').replace('City College of New York', 'CCNY').replace("John's Red Storm", "St. John's Red Storm").replace("Joseph's (PA) Hawks", "Saint Joseph's Hawks").replace("Panthers Kentucky Wesleyan", "Kentucky Wesleyan Panthers").replace('Texas-Arlington', u'Texas–Arlington').replace("Peter's Peacocks", "Saint Peter's Peacocks").replace("Shore", "Shore Hawks").replace('Bears Bears', 'Bears').replace('North Carolina Greensboro', 'UNC Greensboro').replace('IPFW Mastodons', 'Fort Wayne Mastodons').replace('Congo DR', 'DR Congo').replace(u'Vitória Futebol Clube', u'Esporte Clube Vitória').replace(u'Los Angeles Football Club', 'Los Angeles FC')



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
            home_team = C_DICT.get(home_team, home_team)
            away_team = C_DICT.get(away_team, away_team)

        if  'Yen-Hsun Lu' in home_team: home_team = "Lu Yen-hsun"
        if  'Yen-Hsun Lu' in away_team: away_team = "Lu Yen-hsun"

        if 'Stanislas Wawrinka' in home_team: home_team = "Stan Wawrinka"
        if 'Stanislas Wawrinka' in away_team: away_team = "Stan Wawrinka"

            
        home_team = PART_DICT.get(home_team, home_team)
        away_team = PART_DICT.get(away_team, away_team)

        if sport_id == 4 and 'football' in Ge.lower() and Ti == "NCAA":
            home_team = home_team + " football"
            away_team = away_team + " football"

        if sport_id == 2 and 'basketball' in Ge.lower() and Ti == "NCAA":
            home_team = home_team + " Men's Basketball"
            away_team = away_team + " Men's Basketball"
            home_team = home_team.replace("men Men's", 'men')
            away_team = away_team.replace("men Men's", 'men')

        return home_team, away_team

    def merge_golf_games(self, Gi, Ti, Od, cur):
        temp_check_list_1 = [Od + datetime.timedelta(days = 1) for i in range(4)]
        temp_check_list_2 = [Od - datetime.timedelta(days = 1) for i in range(4)]
        date_list = temp_check_list_1 + temp_check_list_2
        tou_id = TOU_DICT.get(Ti,'')
        if not tou_id:
            self.MIS_TOUS[Ti] = 'golf'
            return
        game_id_qry = 'select id from SPORTSDB.sports_games where tournament_id = %s and game_datetime like %s and status !="Hole"'
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
        sel_qry = 'select thuuz_id from sports_thuuz_merge  group by thuuz_id having count(*) > 1'
        sp_cur.execute(sel_qry)
        sp_data = sp_cur.fetchall()
        for data_ in sp_data:
            sel_qry1 = 'select sportsdb_id from sports_thuuz_merge where thuuz_id=%s'
            values_thuuz = (data_)
            sp_cur.execute(sel_qry1 % values_thuuz)
            spd_id = sp_cur.fetchall()
            for db_id in spd_id:
                sel_db_qry = 'select id,game_datetime,status from SPORTSDB.sports_games where id = %s'
                values_db = (db_id)
                sp_cur.execute(sel_db_qry % values_db)
                db_date = sp_cur.fetchall()
                for d_da in db_date:
                    db_id = d_da[0]
                    import datetime
                    db_date1 = str(d_da[1].date())
                    status_ = d_da[2]
                    if status_ =='Hole':
                        del_qry = 'delete from sports_thuuz_merge where thuuz_id=%s and sportsdb_id=%s'
                        values_del = (data_[0], db_id)
                        sp_cur.execute(del_qry % values_del)
                sel_thuzz = 'select Od from sports_thuuz_games where Sk=%s'
                sp_cur.execute(sel_thuzz % data_)
                thuuz_date = sp_cur.fetchall()
                thh_da1 = str(thuuz_date[0][0])
                if thh_da1 == db_date1:
                    continue
                else:
                    del_qry = 'delete from sports_thuuz_merge where thuuz_id=%s and sportsdb_id=%s'
                    values_del = (data_[0], db_id)
                    print thh_da1,db_date1,data_[0]
                    sp_cur.execute(del_qry % values_del)


        sp_cur.close()
        if self.MIS_TOUS:
            self.mail_mis_tous(self.MIS_TOUS)

if __name__ == '__main__':
    ThuuzGamesMergedel().main()   
