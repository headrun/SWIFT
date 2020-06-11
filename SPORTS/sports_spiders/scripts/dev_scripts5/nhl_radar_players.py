import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
import datetime

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, "%s", "%s", "%s", now(), now())'

PL_QRY = 'select id from sports_participants where title = "%s" and game = "hockey"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'


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
        self.conn        = MySQLdb.connect(host='10.4.18.34', user='root', db= 'SPORTSDB_RADAR')
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
            yield Request(top_url, self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        players = data.get('players', '')

        for node in players:
            nhl_sk        = ''
            radar_sk      = node.get('id', '')
            full_name     = node.get('full_name', '')
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


            position      = node.get('position', '')
            status        = node.get('status', '')
            jersey_number = node.get('jersey_number', '')
            height        = node.get('height', '')
            weight        = node.get('weight', '')
            birthdate     = node.get('birthdate', '')
            birthcity     = node.get('birthcity', '')
            birthstate    = node.get('birthstate', '')
            birthcountry  = node.get('birthcountry', '')
            pro_debut     = node.get('pro_debut', '')

            if birthdate:
                site_birt_date = str(datetime.datetime.strptime(birthdate, "%Y-%m-%d"))
            else:
                birthdate = "0000-00-00"

            pl_exists = self.check_sk(radar_sk)
            if pl_exists == True:
                continue

            pl_id         = self.get_plid(site_birt_date, full_name)
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

