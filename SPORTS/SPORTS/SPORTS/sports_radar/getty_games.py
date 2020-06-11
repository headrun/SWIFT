from vtvspider import VTVSpider, get_tzinfo, \
    extract_list_data, get_nodes, extract_data, get_utc_time
from scrapy.http import Request
import re
import datetime
from scrapy.selector import Selector
import json
import MySQLdb

INS_QUERY = "insert into getty_games(getty_event_id, name, date, sub_header, league, created_at, modified_at) values(%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_INS_QUERY = "insert into getty_players(getty_player_id, name, sub_header, league, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_MAP_QRY = "insert into getty_players_mapping(entity_id, entity_type, getty_player_id, is_primary, created_at,modified_at)values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"

class getty_games(VTVSpider):
    name = "getty_games_meta"
    start_urls = []

    def __init__(self):
        self.conn = MySQLdb.connect(host = "10.28.218.81", user = "veveo", passwd  = "veveo123", db = "GETTY_IMAGES", charset = 'utf8', use_unicode = True)        
        self.cursor = self.conn.cursor()

    def start_requests(self):
        seasons = {'Ligue 1':['ligue+1'],
                    'Premier League':['epl'],
                    'UEFA Europa League':['UEFA Europa League'],
                    'UEFA Champions League':['UEFA Champions League'],
                    'La Liga':['La Liga'],
                    'Bundesliga':['Bundesliga'],
                    'Serie A':['Serie A','Serie A italy']
                    }
        for k, words in seasons.iteritems():
            for v in words:
                #top_url = "http://www.gettyimages.in/search/facets/events?assettype=image&excludenudity=true&family=editorial&phrase=%s&sort=mostpopular&editorialproducts=sport"%v
                top_url = "http://www.gettyimages.in/search/facets/specificpeople?assettype=image&excludenudity=true&family=editorial&phrase=%s&sort=best"%v

                yield Request(top_url, self.parse, meta={'league':k})

    def parse(self, response):
        data = json.loads(response.body)
        events = data.get('refinements','')
        league = response.meta['league']
        for each in events:
            year = each.get('year', '')
            game_datetime = each.get('date', '')
            sub_header  = each.get('subHeader', '')
            game_note   = each.get('name', '')
            game_id     = each.get('id', '')
            values      = (game_id, game_note, game_datetime, sub_header, league)
            #self.cursor.execute(INS_QUERY, values)
            pl_values = (game_id, game_note,sub_header, league)
            self.cursor.execute(PL_INS_QUERY, pl_values)
            entity_id = self.get_player_id(game_note, league)
            if entity_id:
                values = (entity_id, 'player', game_id, '1')
                self.cursor.execute(PL_MAP_QRY, values)


    def get_player_id(self, pl_name, league):
        pl_name = pl_name.split(' - ')[0].strip()
        pl_name = '%' + pl_name + '%'
        sel_qry = 'select id from SPORTSDB.sports_participants where sport_id="7" and participant_type="player" and title like %s'
        values = (pl_name)
        self.cursor.execute(sel_qry, values)
        data = self.cursor.fetchall()
        if not data:
            sel_qry = 'select id from SPORTSDB.sports_participants where sport_id="7" and participant_type="player" and aka like %s'
            values = (pl_name)
            self.cursor.execute(sel_qry, values)
            data = self.cursor.fetchall()
        entity_id = ''
        if len(data) == 1:
            entity_id= data[0][0]
        else:
            for data_ in data:
                pl_id = data_[0]
                qry = 'select R.player_id, R.team_id from sports_roster R, sports_tournaments_participants TP, sports_tournaments T where TP.participant_id=R.team_id and T.title=%s and T.id=TP.tournament_id and R.player_id=%s and R.status="active"'
                values = (league, pl_id)
                self.cursor.execute(qry, values)
                pl_data = self.cursor.fetchall()
                if pl_data:
                    entity_id = pl_data[0][0]
            print data, pl_name
        return entity_id
