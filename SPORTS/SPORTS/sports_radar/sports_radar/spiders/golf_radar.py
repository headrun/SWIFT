import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals


INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, "%s", "%s", "%s", now(), now())'

PL_QRY = 'select id from sports_participants where title = "%s" and sport_id = "8"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'

TOUR_LIST = ['2016', '2015', '2014', '2013', '2012']

class GolfRadarJson(VTVSpider):
    name = 'radar_golfjson'
    #url = 'https://api.sportradar.us/golf-t1/profiles/pga/%s/players/profiles.json?api_key=m74qbkhc9sqdey9dw2nmhurz'
    url = 'https://api.sportradar.us/golf-rt1/profiles/pga/%s/players/profiles.json?api_key=85j55dzmkh8eddq3wcymkzj5'

    def __init__(self):
        self.conn        = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor      = self.conn.cursor()
        self.golf_exists_file = open('golf_exists_file', 'w')
        self.golf_missed_file = open('golf_missed_file', 'w')
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
        for api_key in TOUR_LIST:
            top_url = self.url % api_key
            yield Request(top_url.replace('https', 'http'), self.parse, meta={'proxy':'http://game-dynamics-proxy-dev-0-489291303.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

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

    def spider_closed(self):
        spider_stats = self.crawler.stats.get_stats()
        start_time   = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)

        query = "insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
        values = (self.name, start_time, finish_time, '', '', json.dumps(spider_stats))
        self.cursor.execute(query,values)
