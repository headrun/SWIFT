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

class CricketRadar(VTVSpider):
    name = 'cricket_radar'

    def start_requests(self):
        teams_url = 'https://api.sportradar.us/cricket-p1/teams/hierarchy.xml?api_key=9b2psy3r9a2w2tee64jv56qz'
        yield Request(teams_url, callback = self.parse_one)

    def __init__(self):
        self.conn        = MySQLdb.connect(host='10.4.18.183', user='root', db= 'SPORTSDB')
        self.cursor      = self.conn.cursor()
        self.url = 'https://api.sportradar.us/cricket-p1/teams/%s/profile.xml?api_key=9b2psy3r9a2w2tee64jv56qz'
        self.nhl_exists_file = open('cricket_exists_file', 'w')
        self.nhl_missed_file = open('cricket_missed_file', 'w')
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

    def parse_one(self, response):
        sel = Selector(response)
        team_ids = sel.xpath('//team/@id').extract()
        print team_ids
        import pdb; pdb.set_trace()

    def parse_teams(self, response):
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

