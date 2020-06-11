from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.http import Request
import json
import MySQLdb

TOUR_LIST = ['2016', '2015', '2014', '2013', '2012']

class NASCARRadar(VTVSpider):
    name = 'radar_nascar'
    start_urls = []
    url = 'https://api.sportradar.us/nascar-p3/%s/%s/drivers/list.json?api_key=33ra3kxy6f2esqxak8zgwrrm'

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.4.18.183', db='SPORTSDB', user='root')
        self.cursor = self.conn.cursor()
        self.file_  = open('nasacr_players_radar', 'a+')

    def start_requests(self):
        for nascar_series in ['sc', 'xf', 'cw']:
            for year in TOUR_LIST:
                req_url = self.url % (nascar_series, year)
                yield Request(req_url, self.parse)

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
                query = 'select id from sports_participants where game="auto racing" and title=%s'
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
