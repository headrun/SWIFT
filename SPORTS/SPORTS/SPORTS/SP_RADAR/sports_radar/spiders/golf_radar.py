import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
import datetime

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, "%s", "%s", "%s", now(), now())'

PL_QRY = 'select id from sports_participants where title = "%s" and game = "golf"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'

TOUR_LIST = ['2016', '2015', '2014', '2013', '2012']

class GolfRadarJson(VTVSpider):
    name = 'radar_golfjson'
    #url = 'https://api.sportradar.us/golf-t1/profiles/pga/%s/players/profiles.json?api_key=m74qbkhc9sqdey9dw2nmhurz'
    url = 'https://api.sportradar.us/golf-rt1/profiles/pga/%s/players/profiles.json?api_key=85j55dzmkh8eddq3wcymkzj5'

    def __init__(self):
        self.conn        = MySQLdb.connect(host='10.4.18.183', user='root', db= 'SPORTSDB')
        self.cursor      = self.conn.cursor()
        self.golf_exists_file = open('golf_exists_file', 'w')
        self.golf_missed_file = open('golf_missed_file', 'w')

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
        for api_key in TOUR_LIST:
            top_url = self.url % api_key
            yield Request(top_url, self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        players = data.get('players', '')
        for node in players:
            radar_sk      = node.get('id', '')
            first_name    = node.get('first_name', '')
            last_name     = node.get('last_name', '')
            height        = node.get('height', '')
            weight        = node.get('weight', '')
            birthdate     = node.get('birthday', '')
            birth_place   = node.get('birth_place', '')
            full_name = first_name + " " + last_name

            if birthdate:
                site_birt_date = str(datetime.datetime.strptime(birthdate, "%Y-%m-%d"))
            else:
                site_birt_date = "0000-00-00"

            pl_exists = self.check_sk(radar_sk)
            if pl_exists == True:
                continue

            pl_id         = self.get_plid(site_birt_date, full_name)
            if not pl_id:
                record = [radar_sk, full_name, height, weight, birthdate]
                self.golf_missed_file.write('%s\n' % record)
            else:
                self.cursor.execute(INSERT_SK %(pl_id, "participant", "radar", radar_sk))

    def get_plid(self, site_birt_date, full_name):
        self.cursor.execute(PL_QRY %(full_name))
        data = self.cursor.fetchone()
        if data:
            pl_id = data[0]
            self.cursor.execute(PL_DOB_QRY %(pl_id))
            birth_date = self.cursor.fetchone()
            if birth_date:
                db_birth_date = str(birth_date[0])
            if db_birth_date == site_birt_date:
                pl_id = pl_id
            elif db_birth_date != site_birt_date and len(data) == 1:
                pl_id = pl_id
            else:
                pl_id = ''
        else:
            pl_id = ''
        return pl_id


