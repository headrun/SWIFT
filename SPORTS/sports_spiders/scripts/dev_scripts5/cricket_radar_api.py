from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import datetime
import MySQLdb
import genericFileInterfaces

PL_QRY = 'select id from sports_participants where title = "%s" and game = "cricket"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'


class CricketRadar(VTVSpider):
    name = "cricket_radar"
    start_urls = ['https://api.sportradar.us/nfl-ot1/teams/97354895-8c77-4fd4-a860-32e62ea7382a/profile.xml?api_key=ru43ezr53p9jdsbg9r2tmvyr']      
    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_RADAR")
        self.cursor = self.conn.cursor()
        self.player_exists_file = open('player_exist', 'w')
        self.player_not_exists_file = open('birtdate_not_matched', 'w')

        self.schema = { 'pl_name'    : 0, 'jersey'   : 1, 'last_name' : 2, \
                        'first_name' : 3, 'abbr_name': 4, 'preferred_name' : 5, \
                        'birth_date' : 6, 'weight'   : 7, 'height' : 8, \
                        'pl_id'      : 9, 'position' : 10, 'birth_place': 11, \
                        'high_school': 12, 'college' : 13, 'college_conf': 14, \
                        'rookie_year': 15
                            }

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        nodes = get_nodes(sel, '//players/player')
        for node in nodes:
            pl_name = extract_data(node, './@name')

