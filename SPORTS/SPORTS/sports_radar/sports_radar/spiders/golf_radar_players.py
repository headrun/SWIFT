import json
import MySQLdb
import logging
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from vtv_db import get_mysql_connection

INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s, debut = %s'


class GolfSpider(Spider):
    name = "golf_radar_player"
    start_urls = ["https://api.sportradar.us/golf-p2/schedule/pga/2017/players/profiles.json?api_key=36c62wf2dmztgvfe2nt3qr9v"]

    def __init__(self):
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger  = logging.getLogger('sportsRadar.log')
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')

    def parse(self, response):
        sel = Selector(response)
    
        json_data   = json.loads(response.body)
        players     = json_data['players']
        for player in players:
            player_id   = player['id']           
            first_name  = player['first_name']   
            last_name   = player['last_name']    
            height      = player.get('height', '')
            weight      = player.get('weight', '')
            birthday    = player.get('birthday', '')
            country     = player.get('country', '')      
            residence   = player.get('residence',  '')
            birth_place = player.get('birth_place', '')
            college     = player.get('college', '')
            turned_pro  = player.get('turned_pro', '')
            updated_at  = player.get('updated', '')
            sport_id    = 8
            if turned_pro:
                turned_pro = str(turned_pro) + "-01-01"
            print "*"*40
            values = (player_id, "%s %s" %(first_name, last_name), sport_id, first_name, 
                      last_name, '', response.url, turned_pro, country, '', '', '',
                      height, weight, birthday, birth_place, college, '', '',
                      '', updated_at, sport_id, turned_pro)
            self.cursor.execute(INT_PL, values)
        self.cursor.close()
        self.conn.close()
