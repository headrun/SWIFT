from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, get_md5
import datetime
import MySQLdb

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_images set description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'

UP_IMG_QRY = 'update sports_images_mapping set is_primary=%s where entity_id=%s and image_id=%s and entity_type=%s limit 1'

SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

TEAMS_DICT = {'Carpi': "Carpi FC 1909", "Juventus": "Juventus F.C.", "Atalanta": "Atalanta B.C.", \
        "Napoli": "S.S.C. Napoli", "Sampdoria": "U.C. Sampdoria",  "Verona": "Hellas Verona FC", \
        "Lazio": "S.S. Lazio", "Roma": "A.S. Roma", "Genoa": "Genoa C.F.C.", "Turin": "Torino F.C.", \
        "Inter": "Inter Milan", "Milan": "A.C. Milan", "Sparta Prague": "Sparta Prague", \
        "B. Munich": "FC Bayern Munich", "Palermo": "U.S. Citta di Palermo", "At. Madrid":"Atletico Madrid", \
        "Borussia":"Borussia Monchengladbach", "Bremen": "SV Werder Bremen", "Stuttgart": "VfB Stuttgart", \
        "Liverpool":"Liverpool F.C.", "St.-Germain": "Saint-Germain F.C.", "Wolfsburg": "VfL Wolfsburg", \
        'Villarreal':"Villarreal CF", "E. Frankfurt": "Eintracht Frankfurt", "Hannover": "Hannover 96", \
        'Chelsea':"Chelsea F.C.", "Sassuolo": "US Sassuolo Calcio",
        'Man City':"Manchester City F.C.", "AC Milan": "A.C. Milan",
        'Southampton':"Southampton F.C.",
        'Tottenham Hotspur':"Tottenham Hotspur F.C.",
        'Real Madrid':"Real Madrid C.F.",
        'Manchester United':"Manchester United F.C.",
        'Manchester City':"Manchester City F.C.",
        'Shakhtar':'FC Shakhtar Donetsk',
        'FC Sevilla':'Sevilla FC',
        'AS Roma':'A.S. Roma',  \
        "Paris St.-Germain": "Paris Saint-Germain F.C.", \
        "Ol. Marseille": "Olympique de Marseille", \
        "Sheff Wed": "Sheffield Wednesday", \
        "Hull": "Hull City A.F.C.", \
        "Barcelona": "FC Barcelona", \
        "Nuremberg": "1. FC Nurnberg", \
        "Dortmund": "Borussia Dortmund", \
        "Crystal Palace": "Crystal Palace FC", \
        "Man Utd": "Manchester United F.C.", \
        "Nantes": "FC Nantes", \
        "Deportivo": "Deportivo de La Coruna", \
        "Granada": "Granada CF", \
        "Brighton": "Brighton & Hove Albion F.C.", \
        "Bournemouth": "A.F.C. Bournemouth", \
        "Derby": "Derby County", \
        "Nancy": "AS Nancy", \
        "Espanyol": "RCD Espanyol", \
        "Valencia": "Valencia CF", \
        "Levante": "Levante UD", \
        "Chievo": "A.C. Chievo Verona", \
        "Everton": "Everton F.C.", \
        "Sunderland": "Sunderland A.F.C.", \
        "Norwich": "Norwich City F.C.", \
        "Watford": "Watford F.C.", \
        "Monaco": "AS Monaco FC", \
        "Ol. Lyon": "Olympique Lyonnais", \
        "West Ham": "West Ham United F.C.", \
        "Ingolstadt": "FC Ingolstadt 04", \
        "West Bromwich": "West Bromwich Albion F.C.", \
        "Newcastle": "Newcastle United F.C.", \
        "Tottenham": "Tottenham Hotspur F.C.", \
        "Arsenal": "Arsenal F.C.", \
        "Aston Villa": "Aston Villa F.C.", \
        "Swansea": "Swansea City A.F.C.", \
        "Leicester": "Leicester City F.C.", \
        "Stoke": "Stoke City F.C.", \
        "Nice": "OGC Nice", "Crotone": "F.C. Crotone", \
        "Nantes": "FC Nantes", \
        "Ajaccio": "AC Ajaccio", \
        "M'gladbach": "Borussia Monchengladbach", \
        "Betis": "Real Betis", \
        "Real Sociedad": "Real Sociedad", \
        "Vallecano": "Rayo Vallecano", \
        "Bologna": "Bologna F.C. 1909",  \
        "Leverkusen": "Bayer 04 Leverkusen", "FC Basel": "FC Basel", "At. Bilbao": "Athletic Bilbao", \
        "Reims": "Stade de Reims", "Getafe": "Getafe CF", "Troyes": "Troyes AC",  "Mainz 05": "1. FSV Mainz 05", \
        "Las Palmas": "Las Palmas", "PSV Eindhoven": "PSV Eindhoven", "Rennes": "Stade Rennais F.C.", \
        "Cologne": "1. FC Koln", "Sporting": "Sporting Clube de Portugal", "Arouca": "FC Arouca", \
        "GFCO Ajaccio": "Ajaccio GFCO", "Bordeaux": "FC Girondins de Bordeaux", "Empoli": "Empoli F.C.", \
        "Udinese": "Udinese Calcio", "Benfica": "S.L. Benfica", "Guingamp": "EA Guingamp", \
        "Hoffenheim": "TSG 1899 Hoffenheim", "Eibar": "Eibar", "Schalke 04": "FC Schalke 04", \
        "Caen": "Stade Malherbe Caen", "Darmstadt": "SV Darmstadt 98", "Lorient": "FC Lorient", \
        "Hertha": "Hertha BSC", "Hamburg": "Hamburger SV", "Lille": "Lille OSC", "Gijon": "Sporting Gijon", \
        "Malaga": "Malaga CF", "Fiorentina":"ACF Fiorentina", "Middlesbrough": "Middlesbrough F.C.", \
        "Charlton": "Charlton Athletic F.C.", "Burnley": "Burnley", "Iceland": "Iceland national football team", \
        "Norway": "Norway national football team", "Germany": "Germany national football team", \
        "Slovakia": "Slovakia national football team"}


class SoccerAPIActionImages(VTVSpider):
    name = "soccer_action_images"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/%s/%s/actionshots/events/%s/manifest.xml?api_key=%s'

    image_url = 'https://api.sportradar.us/soccer-images-p3/reuters%s?api_key=%s'

    apis_dict = {
                 'bundesliga': {'api-key': 'pr253kju82fm9964ypgk6zg2', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 #'epl': {'api-key': 'bnspjq4ngenjrs5dh4z373mn', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'la-liga': {'api-key': '6ecu2yybawrzgpm49srmwmww', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'}, 
                 'serie-a': {'api-key': 'crsdhq4ckunjgeqpax5q487q', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'ligue-1': {'api-key': 'tmxeze4tfmffdxrnp2sz28xh', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'}}



    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.images_file = open('soccer_image_details', 'w+')
        self.without_game_id = open('without_game_id', 'w+')
        self.pl_exists = open('soccer_pl_exists_images', 'w+') 
        self.player_images = []
        self.word_frequency={}
        self.data=[]
        self.f = open("wordfrequency.txt",'w')
        self.fteam = open("teamnames.txt",'a')

    def start_requests(self):
        now = datetime.datetime.now()
        for i in range(0, 10):
            _date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
            #_date = '2016/10/15'
            _game_date = (now - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            #_game_date = '2016-10-15'
            for key, values in self.apis_dict.iteritems():
                api_key = values['api-key']
                version = values['version']
                sport = values['sport']
                provider = values['provider']
                req_url = self.url % (sport, version, provider, key, _date, api_key)
                yield Request(req_url.replace('https', 'http'), self.parse,  \
                meta={'game': key, 'version': version, \
                'api-key': api_key, 'provider': provider, 'sport': sport, 'gamedate': _game_date, 'proxy':'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game       = response.meta['sport']
        version   = response.meta['version']
        api_key   = response.meta['api-key']
        provider  = response.meta['provider']
        sport       = response.meta['sport']
        game_date = response.meta['gamedate']
        nodes     = get_nodes(sel, '//assetlist/asset')

        for node in nodes:
            title      = extract_data(node, './title/text()')
            description  = extract_data(node, './/description//text()'). \
                    replace('amp;', '').replace('\n', '').strip()

            '''if ' v ' not in title or '-' not in title:
                 title = extract_data(node, './refs/ref[@type="event"]/@name')'''
            game_id = ''
            pl_id = ''
            is_primary = '0'
            team_id = ''
            '''if title:
                game_id = self.parse_participants(title, game_date, game)
                if game_id:
                       self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary)'''


            teamnames  = node.xpath('.//refs//ref[@type="organization"]//@name').extract()
            image_link = node.xpath('.//links/link/@href').extract()
            for data in teamnames:
                team_ = TEAMS_DICT.get(data, '')
                if not team_:
                    print data
                    team = data
                else:
                    team = team_

                query  =  "select id from sports_participants where title ='%s' and sport_id='7' and participant_type='team'"%(team)
                self.cursor.execute(query)
                res_query  =  self.cursor.fetchall()
                if res_query:
                    res_id  =  res_query[0][0]
                    team_id = res_id
                    print team_id
                    game_id = ''
                    pl_id = ''
                    is_primary = '0'
                    self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary)

                    players_team  =  "select player_id from sports_roster where team_id='%s'" %res_id
                    self.cursor.execute(players_team)
                    players_id  =  self.cursor.fetchall()
                    if players_id:
                        for each_id in players_id:
                            query  =  "select id, title from sports_participants where participant_type='player' and id='%s' and sport_id='7'" %each_id[0]
                            self.cursor.execute(query)
                            for player in self.cursor.fetchall():
                                pl_id= player[0]
                                player_name  =  player[1]
                                if player_name in title:
                                    game_id = ''
                                    team_id = ''
                                    is_primary = '0'
                                    self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary)
                                if player_name in description:
                                    game_id = ''
                                    team_id = ''
                                    is_primary = '0'
                                    self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary)


                                
        def parse_participants(self, title, game_date, game):
            participants = title
            if 'Football Soccer' in title: title = title.split('Football Soccer')[-1].strip('- ')
            if '-' in title: title = title.split('-')[0].strip()
            if '(' in title: title = title.split('(')[0].strip()
            if " v " in title:
                participants = title.split(' v ')
            

            game_list = []
            for par in participants:
                    if "At. Madrid" in par: par = "Atletico Madrid"
                    if "B. Munich" in par: par = "FC Bayern Munich"
                    if "Borussia" in par: par = "Borussia Monchengladbach"
                    if "Liverpool" in par: par = "Liverpool F.C."
                    if 'Villarreal' in par: par = "Villarreal CF"
                    if 'Chelsea' in par: par = "Chelsea F.C."
                    if 'Man City' in par: par = "Manchester City F.C."
                    if 'Southampton' in par: par = "Southampton F.C."
                    if 'Tottenham Hotspur' in par: par = "Tottenham Hotspur F.C."
                    if 'Real Madrid' in par: par = "Real Madrid C.F."
                    if 'Manchester United' in par: par = "Manchester United F.C."
                    if 'Manchester City' in par: par = "Manchester City F.C."
                    if 'Shakhtar' in par: par = 'FC Shakhtar Donetsk'    
                    if 'FC Sevilla' in par: par = 'Sevilla FC'
                    if 'AS Roma' in par: par = 'A.S. Roma'

                    pa_qury = 'select id from sports_participants where title=%s and sport_id=%s'
                    values = (par, '7')
                    self.cursor.execute(pa_qury, values)
                    data = self.cursor.fetchone()
                    if not data:
                        pa_query = 'select id from sports_participants where title like %s and sport_id=%s and participant_type="team"'
                        par = '%'+ par + '%'
                        values = (par, '7')
                        self.cursor.execute(pa_query, values)
                        data = self.cursor.fetchone()

                        if data:
                            pa_id = data[0]
                            game_query = 'select game_id from sports_games_participants where participant_id =%s and game_id in (select id from sports_games where sport_id = %s and game_datetime like %s and status !="Hole")'
                            game_date_ = '%' + game_date + '%'
                            values = (pa_id, '7', game_date_)
                            self.cursor.execute(game_query, values)
                            data = self.cursor.fetchone()
                            if data:
                                data = str(data[0])
                                game_list.append(data)
                            else:
                                data = ''

                    if len(game_list) == 2:
                        if game_list[0] == game_list[1]:
                            data = game_list[0]
                        else:
                            data = ''
                    return data


    def populate_images(self, node, game_id, game, version, api_key, pl_id, team_id, is_primary):
        images = get_nodes(node, './links/link')
        image_created = extract_data(node, './@created')
        image_updated = extract_data(node, './@updated')
        description  = extract_data(node, './/description//text()'). \
        replace('amp;', '').replace('\n', '').strip()

        for image_node in images:
            pl_image = extract_data(image_node, './@href')
            height = extract_data(image_node, './@height')
            width  = extract_data(image_node, './@width')
            image = self.image_url %  (pl_image, api_key)
            image_type = 'actionshots'
            image_sk = get_md5(image)
            query = 'select id from sports_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
                if pl_id and count:
                    query = 'select id from sports_images_mapping where image_id=%s and entity_type="player" and entity_id=%s'
                    pl_values = (count, pl_id)
                    self.cursor.execute(query, pl_values)
                    im_id = self.cursor.fetchone()
                    if im_id:
                        im_id = str(im_id[0])
                        im_values = (is_primary, pl_id, count, 'player')
                        self.cursor.execute(UP_IMG_QRY, im_values)
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_images' and TABLE_SCHEMA='SPORTSDB'")

                count = str(self.cursor.fetchone()[0])
            values = (image_sk, image, image_type, height, width, description, image_created, image_updated)
            self.cursor.execute(IMAGE_QUERY, values)

            right_values = (count, '2')
            self.cursor.execute(SP_IMG_RIGHT_ID, right_values)

            if game_id:
                values = (game_id, 'game', count, is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)
            if pl_id:
                values = (pl_id, 'player', count, is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)
                if team_id:
                    values = (team_id, 'team', count, is_primary)
                    self.cursor.execute(IMG_MAP_QUERY, values)
            

