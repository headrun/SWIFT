import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data, get_sport_id
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals



INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, "%s", "%s", "%s", now(), now())'

PL_QRY = 'select id from sports_participants where title = "%s" and sport_id = "3"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'

PL_RADAR = 'insert into sports_radar_players(participant_id, source, source_key, title, aka, sport_id, height, weight, participant_type, position, college, school, participant_style, birth_place, birth_date, debut, jersey_number, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())on duplicate key update modified_at = now()'

ROLES_DICT = {'C': 'Center', 'G': 'Guard', 'T': 'Tackle',
              'QB': 'Quarterback', 'RB': 'Running back',
              'WR': 'Wide receiver', 'TE': 'Tight end',
              'DT': 'Defensive tackle', 'DE': 'Defensive end',
              'MLB': 'Middle linebacker', 'OLB': 'Outside linebacker',
              'CB': 'Cornerback', 'S': 'Safety', 'K': 'Kicker',
              'H': 'Holder', 'LS': 'Long snapper', 'D': 'Defenceman',
              'P': 'Punter', 'PR': 'Punt returner', 'KR': 'Kick returner',
              'FB': 'Fullback', 'HB': 'Halfback', 'ILB': 'Inside linebacker',
              'TB': 'Tailback', 'RG': 'Right guard', 'LG': 'Left guard',
              'RT': 'Right tackle', 'LT': 'Left tackle', 'NG': 'Nose guard',
              'DL': 'Defensive line', 'FS': 'Free safety', 'LB': 'Linebacker',
              'NT': 'Defensive tackle', 'DB': 'Defensive back', 'PK': 'Placekicker',
              'SS': 'Strong safety', 'OG': 'Offensive guard',
              'OT': 'Offensive tackle', 'OL': 'Offensive line',
              'SAF': 'Safety', 'LW': "Left wing", 'RW': "Right wing"}


API_DICT = {'COL': '4415ce44-0f24-11e2-8525-18a905767e44', \
            'STL': '441660ea-0f24-11e2-8525-18a905767e44', \
            'CHI': '4416272f-0f24-11e2-8525-18a905767e44', \
            'MIN': '4416091c-0f24-11e2-8525-18a905767e44', \
            'DAL': '44157522-0f24-11e2-8525-18a905767e44', \
            'NSH': '441643b7-0f24-11e2-8525-18a905767e44', \
            'WPG': '44180e55-0f24-11e2-8525-18a905767e44', \
            'ANA': '441862de-0f24-11e2-8525-18a905767e44', \
            'SJ' : '44155909-0f24-11e2-8525-18a905767e44', \
            'LA' : '44151f7a-0f24-11e2-8525-18a905767e44', \
            'ARI': '44153da1-0f24-11e2-8525-18a905767e44', \
            'VAN': '4415b0a7-0f24-11e2-8525-18a905767e44', \
            'CGY': '44159241-0f24-11e2-8525-18a905767e44', \
            'EDM': '4415ea6c-0f24-11e2-8525-18a905767e44', \
            'BOS': '4416ba1a-0f24-11e2-8525-18a905767e44', \
            'MTL': '441713b7-0f24-11e2-8525-18a905767e44', \
            'TB' : '4417d3cb-0f24-11e2-8525-18a905767e44', \
            'DET': '44169bb9-0f24-11e2-8525-18a905767e44', \
            'TOR': '441730a9-0f24-11e2-8525-18a905767e44', \
            'OTT': '4416f5e2-0f24-11e2-8525-18a905767e44', \
            'FLA': '4418464d-0f24-11e2-8525-18a905767e44', \
            'BUF': '4416d559-0f24-11e2-8525-18a905767e44', \
            'PIT': '4417b7d7-0f24-11e2-8525-18a905767e44', \
            'NYR': '441781b9-0f24-11e2-8525-18a905767e44', \
            'CBJ': '44167db4-0f24-11e2-8525-18a905767e44', \
            'PHI': '44179d47-0f24-11e2-8525-18a905767e44', \
            'WSH': '4417eede-0f24-11e2-8525-18a905767e44', \
            'CAR': '44182a9d-0f24-11e2-8525-18a905767e44', \
            'NJ' : '44174b0c-0f24-11e2-8525-18a905767e44', \
            'NYI': '441766b9-0f24-11e2-8525-18a905767e44'}


class NHLRadarJson(VTVSpider):
    name = 'radar_nhljson'
    #url = 'https://api.sportradar.us/nhl-t3/teams/%s/profile.json?api_key=6nhy2rwyw7fdxkcum7d5xjrn'
    url = 'https://api.sportradar.us/nhl-p3/teams/%s/profile.json?api_key=fc4ndqfgxbeb7kzzzvkx6wtx'

    def __init__(self):
        self.conn        = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        #self.conn        = MySQLdb.connect(host="10.28.216.45", user="veveo",passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
        self.cursor      = self.conn.cursor()
        self.nhl_exists_file = open('nhl_exists_file', 'w')
        self.nhl_missed_file = open('nhl_missed_file', 'w')
        self.schema      = {'nhl_sk'        : 0,       'radar_sk'      : 1,
                            'full_name'     : 2,       'position'      : 3,
                            'status'        : 4,       'short_title'   : 5,
                            'jersey_number' : 6,       'height'        : 7,
                            'height'        : 7,       'weight'        : 8,
                            'birthdate'     : 9,       'birthcity'     : 10,
                            'birthstate'    : 11,      'birthcountry'  : 12,
                            'pro_debut'     : 13}
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def check_sk(self, sk):
        query = 'select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s'
        values = ('radar', 'participant', sk)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        if data:
            return True
        else:
            return False

    def start_requests(self):
        for api_key in API_DICT.values():
            top_url = self.url % api_key
            yield Request(top_url.replace('https', 'http'), self.parse, meta={'proxy':'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse(self, response):
        data = json.loads(response.body)
        players = data.get('players', '')

        for node in players:
            nhl_sk        = ''
            radar_sk      = node.get('id', '')
            full_name     = node.get('full_name', '')
            if full_name == "Steve Santini":
                full_name = "Steven Santini"
            if full_name == "Alex DeBrincat":
                full_name = "Alexander Debrincat"
            if full_name == "Matthew Lorito":
                full_name = "Matt Lorito"
            if full_name == "P.A. Parenteau":
                full_name = "Pierre Parenteau"
            if full_name == "Alex Ovechkin":
                full_name = "Alexander Ovechkin"
            if full_name == "Aleksander Barkov, Jr.":
                full_name = "Aleksander Barkov"
            if full_name == "Mikka Salomaki":
                full_name = "Miikka Salomaki"
            if full_name == "Mike Cammalleri":
                full_name = "Michael Cammalleri"
            if full_name == "Mattias Janmark":
                full_name = "Mattias Janmark-Nylen"
            if full_name == "T.J. Brodie":
                full_name = "TJ Brodie"
            if full_name == "Micheal Ferland":
                full_name = "Michael Ferland"
            if full_name == "Dan Girardi":
                full_name = "Daniel Girardi"
            if full_name == "Alex Burmistrov":
                full_name = "Alexander Burmistrov"
            if full_name == "Ben Chiarot":
                full_name = "Ben Chiaroat"
            if full_name == "Kris Letang":
                full_name = "Kristopher Letang"
            if full_name == "Christopher Higgins":
                full_name = "Chris Higgins"
            if full_name == "Rob Scuderi":
                full_name = "Robert Scuderi"


            position_      = node.get('primary_position', '')
            if position_:
                position = ROLES_DICT.get(position_, '')
                if not position:
                        position = position_
            status        = node.get('status', '')
            jersey_number = node.get('jersey_number', '')
            height        = node.get('height', '')
            weight        = node.get('weight', '')
            birthdate     = node.get('birthdate', '')
            birthcity     = node.get('birthcity', '')
            birthstate    = node.get('birthstate', '')
            birthcountry  = node.get('birthcountry', '')
            birth_place   = node.get('birth_place', '').replace(',,', ',').strip()
            pro_debut     = node.get('pro_debut', '')
            college       = node.get('college', '')
            school        = node.get('school', '')

            if birthdate:
                site_birt_date = str(datetime.datetime.strptime(birthdate, "%Y-%m-%d"))
            else:
                birthdate = "0000-00-00"

            pl_exists = self.check_sk(radar_sk)
           
            pl_id         = self.get_plid(site_birt_date, full_name)
            if pl_id:
                sports_id = get_sport_id("ice hockey")
                pl_values = (pl_id, "radar", radar_sk, full_name, "", sports_id, height, weight, "player", position, college, school, "", birth_place, birthdate, pro_debut, jersey_number)
                #self.cursor.execute(PL_RADAR, pl_values) 
            if pl_exists == True:
                continue

            if not pl_id:
                record = [nhl_sk, radar_sk, full_name, position, status, jersey_number, height, weight, birthdate, birthcity, birthstate, birthcountry, pro_debut]
                self.nhl_missed_file.write('%s\n' % record)
            else:
                self.cursor.execute(INSERT_SK %(pl_id, "participant", "radar", radar_sk))


    def get_plid(self, site_birt_date, full_name):
        self.cursor.execute(PL_QRY %(full_name))
        data = self.cursor.fetchone()
        if data:
            pl_id = data[0]
            if full_name == "Mike Green":
                pl_id = "2905"
            if full_name == "Erik Gustafsson":
                pl_id = "105627"
            self.cursor.execute(PL_DOB_QRY %(pl_id))
            birth_date = self.cursor.fetchone()
            if birth_date:
                db_birth_date = str(birth_date[0])
            if db_birth_date == site_birt_date or full_name in ['Erik Haula', 'Morgan Rielly', 'Tyler Randell', 'Mikhail Grabovski', 'Jyrki Jokipakka', 'John Klingberg', 'Radek Faksa', 'Brett Pesce', 'Phillip Di Giuseppe', 'J.T. Brown', 'Jonathan Marchessault', 'Greg Pateryn', 'Darnell Nurse', 'Laurent Dauphin', 'David Dziurzynski', 'Mark Stone']:
                pl_id = pl_id
            else:
                pl_id = ''
        else:
            pl_id = ''
        return pl_id


    def spider_closed(self):
        spider_stats = self.crawler.stats.get_stats()
        start_time   = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)

        query = "insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
        values = (self.name, start_time, finish_time, '', '', json.dumps(spider_stats))
        self.cursor.execute(query,values)
