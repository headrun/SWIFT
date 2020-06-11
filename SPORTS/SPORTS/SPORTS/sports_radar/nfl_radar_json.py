# 2017.08.12 09:05:35 UTC
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import json
import datetime
import MySQLdb
import genericFileInterfaces
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals



PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title=%s and P.game="football" and P.id=PL.participant_id and PL.birth_date=%s'
PL_AKA_QUERY = 'select P.id from sports_participants P, sports_players PL where P.aka=%s and P.game="football" and P.id=PL.participant_id and PL.birth_date=%s'

class NFLRadarJSON(VTVSpider):
    name = 'nflradar_api_images'
    start_urls = ['https://api.sportradar.us/nfl-rt1/teams/hierarchy.json?api_key=f593kvk9569c8yztm8h576nu']
    team_api = 'http://api.sportradar.us/nfl-rt1/teams/%s/roster.json?api_key=f593kvk9569c8yztm8h576nu'

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.player_exists_file = open('player_exist', 'w')
        self.player_not_exists_file = open('nfl_birtdate_not_matched', 'w')
        self.schema = {'pl_name': 0,
         'jersey': 1,
         'last_name': 2,
         'first_name': 3,
         'abbr_name': 4,
         'birth_date': 5,
         'weight': 6,
         'height': 7,
         'pl_id': 8,
         'position': 9,
         'birth_place': 10}
        dispatcher.connect(self.spider_closed, signals.spider_closed)


    def add_source_key(self, entity_id, _id):
        if _id and entity_id:
            query = 'insert into sports_source_keys (entity_id, entity_type,                     source, source_key, created_at, modified_at)                     values(%s, %s, %s, %s, now(), now())'
            values = (entity_id,
             'participant',
             'radar',
             _id)
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
        for conference in conferences:
            divisions = conference['divisions']
            for division in divisions:
                teams = division['teams']
                for team in teams:
                    team_id = team['id'].lower()
                    req_url = self.team_api % team_id
                    yield Request(req_url, self.parse_next)

    def parse_next(self, response):
        self.schema = {}
        data = json.loads(response.body)
        players = data['players']
        for player in players:
            player_sk = player.get('id', '')
            pl_name = player.get('name_full', '')
            if pl_name == 'Cap Capi':
                pl_name = 'Nordly Capi'
            if pl_name == 'Daniel Herron':
                pl_name = 'Dan Herron'
            if pl_name == 'Terrance Magee':
                pl_name = 'Terrence Magee'
            if pl_name == 'Chris Reed':
                pl_name = 'Christopher Reed'
            if pl_name == 'Cam Lawrence':
                pl_name = 'Cameron Lawrence'
            if pl_name == 'TJ Carrie':
                pl_name = 'T. J. Carrie'
            if pl_name == 'Mitch Bell':
                pl_name = 'Mitchell Bell'
            if pl_name == 'Bradley Bars':
                pl_name = 'Brad Bars'
            if pl_name == 'Owa Odighizuwa':
                pl_name = 'Owamagbe Odighizuwa'
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
                site_birt_date = str(datetime.datetime.strptime(birth_date, '%Y-%m-%d'))
            else:
                site_birt_date = ''
            record = [pl_name,
             jersey,
             last_name,
             first_name,
             abbr_name,
             birth_date,
             weight,
             height,
             player_sk,
             position,
             birth_place]
            pl_exists = self.check_sk(player_sk)
            if pl_exists == True:
                continue
            entity_id = self.get_plid(pl_name, site_birt_date, first_name, last_name)
            if entity_id:
                self.add_source_key(str(entity_id), player_sk)
            else:
                data = {'player_sk': player_sk,
                 'name': pl_name,
                 'birth_date': birth_date}
                self.player_not_exists_file.write('%s\n' % data)

    def get_plid(self, pl_name, site_birt_date, first_name, last_name):
        values = (pl_name, site_birt_date)
        self.cursor.execute(PL_NAME_QUERY, values)
        data = self.cursor.fetchone()
        if not data:
            self.cursor.execute(PL_AKA_QUERY, values)
            data = self.cursor.fetchone()
            if not data:
                name = first_name + ' ' + last_name
                values = (name, site_birt_date)
                self.cursor.execute(PL_NAME_QUERY, values)
                data = self.cursor.fetchone()
        if data:
            pl_id = data[0]
        else:
            pl_id = ''
        return pl_id



    def spider_closed(self):
        spider_stats = self.crawler.stats.get_stats()
        start_time = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)
        query = 'insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())'
        values = (self.name, start_time, finish_time, '','', json.dumps(spider_stats))
        self.cursor.execute(query, values)
