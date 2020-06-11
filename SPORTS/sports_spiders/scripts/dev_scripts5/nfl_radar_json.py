from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import json
import datetime
import MySQLdb
import genericFileInterfaces

PL_QRY = 'select id from sports_participants where title = "%s" and game = "football"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'


class NFLRadarJSON(VTVSpider):
    name = "radar_api_images"
    start_urls = ['https://api.sportradar.us/nfl-t1/teams/hierarchy.json?api_key=ru43ezr53p9jdsbg9r2tmvyr']
    team_api = 'http://api.sportradar.us/nfl-t1/teams/%s/roster.json?api_key=ru43ezr53p9jdsbg9r2tmvyr'

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_RADAR")
        self.cursor = self.conn.cursor()
        self.player_exists_file = open('player_exist', 'w')
        self.player_not_exists_file = open('birtdate_not_matched', 'w')
        self.team_not_exists_file = open('team_not_exit', 'w')

        self.schema = { 'pl_name'    : 0, 'jersey'   : 1, 'last_name' : 2, \
                        'first_name' : 3, 'abbr_name': 4, 'birth_date' : 5, \
                        'weight'     : 6, 'height'   : 7, 'pl_id' : 8, \
                        'position'   : 9, 'birth_place' : 10}

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

    def get_team_id(self, id_):
        query = 'select entity_id from sports_source_keys where source= "NFL" and source_key=%s'
        values = (id_)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        if data:
            team_id = data
        else:
            team_id = ''
        return team_id

    def parse(self, response):
        data = json.loads(response.body)
        conferences = data['conferences']
        for conference in conferences:
            divisions = conference['divisions']
            for division in divisions:
                teams =  division['teams']
                for team in teams:
                    team_id = team['id'].lower()
                    team_ft_name = team['market']
                    team_lt_name = team['name']
                    tm_exists = self.check_sk(team_id)
                    if tm_exists == True:
                        continue
                    tm_entity_id = self.get_team_id(team_id)

                    if tm_entity_id:
                        self.add_source_key(str(tm_entity_id[0]), str(team_id.upper()))
                    else:
                        data = {'team_sk': team_id, 'name': team_ft_name + " " + team_lt_name}
                        self.team_not_exists_file.write('%s\n' % data)

                    req_url = self.team_api % team_id
                    yield Request(req_url, self.parse_next)


    def parse_next(self, response):
        return
        '''self.schema = {}
        data = json.loads(response.body)
        players = data['players']
        for player in players:
            player_sk = player.get('id', '')
            pl_name = player.get('name_full', '')
            abbr_name = player.get('name_abbr', '')
            last_name = player.get('name_last', '')
            first_name = player.get('name_first', '')
            position = player.get('position', '')
            birth_date = player.get('birthdate', '')
            weight = player.get('weight', '')
            height = player.get('height', '')
            birth_place = player.get('birth_place', '')
            jersey = player.get('jersey_number', '')
            if birth_date:
                site_birt_date = str(datetime.datetime.strptime(birth_date, "%Y-%m-%d"))
            else:
                site_birt_date = ''

            record  = [pl_name, jersey, last_name, first_name, abbr_name, \
                       birth_date, weight, height, player_sk, position, \
                       birth_place]

            pl_exists = self.check_sk(player_sk)
            if pl_exists == True:
                continue
            entity_id = self.get_plid(pl_name, site_birt_date, first_name, last_name)

            if entity_id:
                self.add_source_key(str(entity_id), player_sk)
            else:
                data = {'player_sk': player_sk, 'name': pl_name, 'birth_date': birth_date}
                self.player_not_exists_file.write('%s\n' % data)'''

    def get_plid(self, pl_name, site_birt_date, first_name, last_name):
        self.cursor.execute(PL_QRY % pl_name)
        data = self.cursor.fetchone()
        if not data:
            name = first_name + ' ' + last_name
            self.cursor.execute(PL_QRY % name)
            data = self.cursor.fetchone()
        if data:
            pl_id = data[0]
            self.cursor.execute(PL_DOB_QRY % pl_id)
            birth_date = self.cursor.fetchone()
            if birth_date:
                db_birth_date = str(birth_date[0])
            else:
                db_birth_date = ''
            if db_birth_date == site_birt_date:
                pl_id = pl_id
            else:
                pl_id = pl_id
        else:
            pl_id = ''
        return pl_id

