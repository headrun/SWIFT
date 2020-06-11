from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import json
import datetime
import MySQLdb
import genericFileInterfaces

PL_QRY = 'select id from sports_participants where title = "%s" and game = "football"'


class NCAAFBRadarJson(VTVSpider):
    name = "ncaafb_radar"
    start_urls = ['https://api.sportradar.us/ncaafb-t1/teams/fcs/hierarchy.json?api_key=negspb5zmtxbyz2jmrafe7wg', \
                 'https://api.sportradar.us/ncaafb-t1/teams/fbs/hierarchy.json?api_key=negspb5zmtxbyz2jmrafe7wg']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_RADAR")
        self.cursor = self.conn.cursor()
        self.player_exists_file = open('ncf_player_exist', 'w')
        self.player_not_exists_file = open('ncf_not_matched', 'w')
        self.player2_file = open('ncf_pl2_exist', 'w')
        self.team_list = []
        self.schema = { 'pl_name'    : 0, 'jersey'   : 1, 'last_name' : 2, \
                        'first_name' : 3, 'abbr_name': 4, 'high_school' : 5, \
                        'weight'     : 6, 'height'   : 7, 'pl_id' : 8, \
                        'position'   : 9, 'birth_place' : 10}

    def add_source_key(self, entity_id, _id):
        if _id and entity_id:
            query = "insert into sports_source_keys (entity_id, entity_type, \
                    source, source_key, created_at, modified_at) \
                    values(%s, %s, %s, %s, now(), now())"
            values = (entity_id, 'participant', 'radar', _id)
            self.cursor.execute(query, values)


    def check_sk(self, sk):
        query = 'select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s'
        values = ('radar', 'participant', sk)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        if data:
            return True
        else:
            return False


    def parse(self, response):
        data = json.loads(response.body)
        conferences = data['conferences']
        if "/ncaafb-t1/teams/fcs/" in response.url:
            for conference in conferences:
                teams = conference.get('teams')
                if teams:
                    for team in teams:
                        team_id = team['id']
                        team_ft_name = team['market']
                        team_lt_name = team['name']
                        self.team_list.append(team_id)

        if "/ncaafb-t1/teams/fbs/" in response.url:
            for conference in conferences:
                divisions = conference.get('subdivisions')
                if divisions:
                    for division in divisions:
                        teams = division['teams']
                        for team in teams:
                            team_id = team['id']
                            team_ft_name = team['market']
                            team_lt_name = team['name']
                            self.team_list.append(team_id)

        for team_id in self.team_list:
            url = 'https://api.sportradar.us/ncaafb-t1/teams/%s/roster.json?api_key=negspb5zmtxbyz2jmrafe7wg' %(team_id)
            yield Request(url, callback=self.parse_next)

    def parse_next(self, response):
        data = json.loads(response.body)
        players = data['players']
        for player in players:
            radar_sk = player.get('id')
            full_name = player.get('name_full')
            name_first = player.get('name_first')
            name_last = player.get('name_last')
            name_abbr = player.get('name_abbr')
            birth_place = player.get('birth_place')
            high_school = player.get('high_school')
            height = player.get('height')
            weight = player.get('weight')
            position = player.get('position')
            jersey_number = player.get('jersey_number')
            status = player.get('status')
            experience = player.get('experience')


            record  = [full_name, jersey_number, name_last, name_first, name_abbr, \
                       high_school, weight, height, radar_sk, position, \
                       birth_place]


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

