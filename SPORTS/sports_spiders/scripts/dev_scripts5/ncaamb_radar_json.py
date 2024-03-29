from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import json
import datetime
import MySQLdb
import genericFileInterfaces

PL_QRY = 'select id from sports_participants where title = "%s" and game = "basketball"'


def add_source_key(entity_id, _id):
    if _id and entity_id:
        conn, cursor = mysql_connection()
        query = "insert into sports_radar_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now())"
        values = (entity_id, 'participant', 'radar', _id)

        cursor.execute(query, values)
        conn.close()



class NCAAMBRadarJson(VTVSpider):
    name = "ncaamb_radar"
    start_urls = ['http://developer.sportradar.us/files/ncaamb_v3_standings_example.xml']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_RADAR")
        self.cursor = self.conn.cursor()
        self.player_exists_file = open('ncb_player_exist', 'w')
        self.player_not_exists_file = open('ncb_not_matched', 'w')
        self.player2_file = open('ncb_pl2_exist', 'w')
        self.team_list = []

    def add_source_key(self, entity_id, _id):
        if _id and entity_id:
            query = "insert into sports_radar_source_keys (entity_id, entity_type, \
                    source, source_key, created_at, modified_at) \
                    values(%s, %s, %s, %s, now(), now())"
            values = (entity_id, 'participant', 'radar', _id)
            self.cursor.execute(query, values)


    def check_sk(self, sk):
        query = 'select entity_id from sports_radar_source_keys where source=%s and entity_type=%s and source_key=%s'
        values = ('radar', 'participant', sk)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        if data:
            return True
        else:
            return False


    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        nodes = get_nodes(sel, '//league//season//conference//team')
        for node in nodes:
            team_id = extract_data(node, './@id')
            self.team_list.append(team_id)

        for team_id in self.team_list:
            url = 'https://api.sportradar.us/ncaamb-t3/teams/%s/profile.json?api_key=ssxrq93mvx72ytvbza457wd7' %(team_id)
            yield Request(url, callback=self.parse_next)


    def parse_next(self, response):
        data = json.loads(response.body)
        players = data.get('players')
        if players:
            for player in players:
                radar_sk = player.get('id')
                full_name = player.get('full_name')
                first_name = player.get('first_name')
                last_name = player.get('last_name')
                abbr_name = player.get('abbr_name')
                height = player.get('height')
                weight = player.get('weight')
                position = player.get('position')
                birth_place = player.get('birth_place')

                pl_exists = self.check_sk(radar_sk)
                if pl_exists == True:
                    continue
                entity_id = self.get_plid(full_name)

                if entity_id:
                    self.add_source_key(str(entity_id), radar_sk)
                else:
                    record = {'full_name': full_name, 'birth_place': birth_place, 'weight': weight, 'height': height}
                    self.player_not_exists_file.write('%s\n' % record)


    def get_plid(self, full_name):
        self.cursor.execute(PL_QRY % full_name)
        data = self.cursor.fetchone()
        if data:
            if len(data) == 1:
                pl_id = data[0]
            elif len(data)>1:
                pl_id = ''
                self.player2_file.write('%s\n' % data)
            else:
                pl_id = ''
        else:
            pl_id = ''
        return pl_id

