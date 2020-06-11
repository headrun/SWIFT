import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals




SK_QUERY = 'select entity_id from sports_source_keys where source="radar" and entity_type="participant" and source_key=%s'

INSERT_SK = 'insert into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

API_DICT = ['583ecb8f-fb46-11e1-82cb-f4ce4684ea4c', '583ec8d4-fb46-11e1-82cb-f4ce4684ea4c', '583ecea6-fb46-11e1-82cb-f4ce4684ea4c', '583ec97e-fb46-11e1-82cb-f4ce4684ea4c', '583ed157-fb46-11e1-82cb-f4ce4684ea4c', '583ecda6-fb46-11e1-82cb-f4ce4684ea4c', '583ec9d6-fb46-11e1-82cb-f4ce4684ea4c', '583eccfa-fb46-11e1-82cb-f4ce4684ea4c', '583ec87d-fb46-11e1-82cb-f4ce4684ea4c', '583ec70e-fb46-11e1-82cb-f4ce4684ea4c', '583ec5fd-fb46-11e1-82cb-f4ce4684ea4c', '583ecefd-fb46-11e1-82cb-f4ce4684ea4c', '583ec773-fb46-11e1-82cb-f4ce4684ea4c', '583ec7cd-fb46-11e1-82cb-f4ce4684ea4c','583ec928-fb46-11e1-82cb-f4ce4684ea4c', '583eca88-fb46-11e1-82cb-f4ce4684ea4c', '583ecb3a-fb46-11e1-82cb-f4ce4684ea4c', '583ecf50-fb46-11e1-82cb-f4ce4684ea4c', '583ecd4f-fb46-11e1-82cb-f4ce4684ea4c', '583ecc9a-fb46-11e1-82cb-f4ce4684ea4c', '583ed056-fb46-11e1-82cb-f4ce4684ea4c', '583ecfff-fb46-11e1-82cb-f4ce4684ea4c', '583ed102-fb46-11e1-82cb-f4ce4684ea4c', '583ece50-fb46-11e1-82cb-f4ce4684ea4c', '583eca2f-fb46-11e1-82cb-f4ce4684ea4c', '583ec825-fb46-11e1-82cb-f4ce4684ea4c', '583ecdfb-fb46-11e1-82cb-f4ce4684ea4c', '583ecfa8-fb46-11e1-82cb-f4ce4684ea4c', '583ed0ac-fb46-11e1-82cb-f4ce4684ea4c', '583ecae2-fb46-11e1-82cb-f4ce4684ea4c']

class NBARadar(VTVSpider):
    name = 'radar_nba'
    url = 'https://api.sportradar.us/nba-t3/teams/%s/profile.json?api_key=v93rc99k72gmrd89c8bq9332'
    p_url = 'https://api.sportradar.us/nba-p3/teams/%s/profile.json?api_key=4gjmrrsbpsuryecdsdazxyuj'

    def __init__(self):
        self.conn        = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor      = self.conn.cursor()
        self.missed_file = open('pl_missed_file_nba', 'w')
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def start_requests(self):
        for api_key in API_DICT:
            top_url = self.p_url % api_key
            yield Request(top_url, self.parse, meta= {'api_key': api_key})

    def parse(self, response):
        data = json.loads(response.body)
        players = data.get('players', '')
        for node in players:
            mlb_sk        = node.get('mlbam_id', '')
            radar_sk      = node.get('id', '')
            full_name     = node.get('full_name', '')
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
            pl_id         = self.get_sk(full_name, birthdate, radar_sk)
            if not pl_id:
                record = [mlb_sk, radar_sk, full_name, position, status, jersey_number, height, weight, birthdate, birthcity, birthstate, birthcountry, pro_debut]
                self.missed_file.write('%s\n' % record)

    def get_sk(self, name, birthdate, radar_sk):
        self.cursor.execute(SK_QUERY, radar_sk)
        data = self.cursor.fetchone()
        if not data:
            query = 'select sp.id from sports_participants sp, sports_players pl where sp.id=pl.participant_id and title= %s and birth_date=%s'
            self.cursor.execute(query, (name, birthdate))
            pl_id = self.cursor.fetchone()
            if pl_id:
                pl_id = str(pl_id[0])
                values = (pl_id, 'participant', 'radar', radar_sk)
                self.cursor.execute(INSERT_SK, values)
        else:
            pl_id = data[0]
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


