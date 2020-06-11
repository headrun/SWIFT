from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.http import Request
import json
import MySQLdb
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals


TOUR_LIST = ['2016', '2015', '2014', '2013', '2012']

class NASCARRadar(VTVSpider):
    name = 'radar_nascar'
    start_urls = []
    url = 'https://api.sportradar.us/nascar-p3/%s/%s/drivers/list.json?api_key=33ra3kxy6f2esqxak8zgwrrm'

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.file_  = open('nasacr_players_radar', 'a+')
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def start_requests(self):
        for nascar_series in ['sc', 'xf', 'cw']:
            for year in TOUR_LIST:
                req_url = self.url % (nascar_series, year)
                yield Request(req_url.replace('https', 'http'), self.parse, meta={'proxy':'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse(self, response):
        data = json.loads(response.body)
        drivers = data['drivers']
        for driver in drivers:
            radar_sk = driver['id']
            name = driver['full_name']
            birthday = driver.get('birthday', '')
            query = 'select entity_id from sports_source_keys where source="radar" and source_key=%s'
            self.cursor.execute(query, radar_sk)
            data = self.cursor.fetchone()
            if not data:
                query = 'select id from sports_participants where sport_id="10" and title=%s'
                self.cursor.execute(query, name)
                pl_id = self.cursor.fetchall()
                pl_ids = [str(id_[0]) for id_ in pl_id]
                if len(pl_ids) == 1:
                    query = 'insert into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())'
                    values = (pl_ids[0], 'participant', 'radar', radar_sk)
                    self.cursor.execute(query, values)
                else:
                    record = [name, birthday, radar_sk]
                    self.file_.write('%s\n' % record)

    def spider_closed(self):
        spider_stats = self.crawler.stats.get_stats()
        start_time   = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)

        query = "insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
        values = (self.name, start_time, finish_time, '', '', json.dumps(spider_stats))
        self.cursor.execute(query,values)
