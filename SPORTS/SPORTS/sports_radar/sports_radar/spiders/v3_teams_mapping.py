from scrapy.http import Request
from vtvspider import VTVSpider
import json
import MySQLdb
from datetime import datetime
import time

SEL_QRY = 'select sportsdb_id from sports_radar_merge where radar_id=%s and type="team"'
IN_QRY = "insert into sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, 'team', now(), now()) on duplicate key update modified_at=now()"
IN_EN_QRY = "insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"
class V3TeamsMapping(VTVSpider):
    name = 'v3_teams_mapping'
    start_urls = [ 'https://api.sportradar.us/soccer-p3/eu/na/teams/v2_v3_id_mappings.json?api_key=xzuefb5xj5294amryqu828gk']
    start_urls = ['https://api.sportradar.us/soccer-p3/am/en/teams/v2_v3_id_mappings.json?api_key=eah2zymdzsdne76xm5uc9rvp']

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        j_data  = json.loads(response.body)
        for json_data in j_data['team_mappings']:
            v2_id         = json_data['v2_id']
            v3_id         = json_data['v3_id']
            values = (v2_id)
            self.cursor.execute(SEL_QRY, values)
            data =  self.cursor.fetchone()
            if data:
                sportsdb_id = data[0]
                v3_values = (v3_id, sportsdb_id)
                self.cursor.execute(IN_QRY, v3_values)
                en_values = (v2_id, 'team', 'reference_id', v3_id)
                self.cursor.execute(IN_EN_QRY, en_values)
            else:
                print v2_id, v3_id
