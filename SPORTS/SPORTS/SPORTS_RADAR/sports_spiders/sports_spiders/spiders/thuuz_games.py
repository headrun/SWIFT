from vtvspider import VTVSpider, log
from sports_spiders.items import SportsSetupItem
from scrapy.spider import Spider
from scrapy.http import Request
import json
import MySQLdb
from datetime import datetime
import logging

GAMES_QRY = 'insert into sports_thuuz_games(Gi, Sk, Ti, Ep, Vt, Ot, Ll, Du, Ge, De, Od, Tg, Ci, At, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), Ti = %s, Ep= %s, De=%s, Od=%s, At=%s'

GENRE_DICT = {'basketball' : 'RVG2647', 'soccer' : 'RVG2906', 'tennis':'', 'golf':'', 'football': '', 'hockey': '', 'baseball': '', 'race': ''}

SEL_QRY = 'select sportsdb_id from sports_radar_merge where radar_id=%s'

INST_QRY = 'insert into sports_thuuz_merge(thuuz_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INT_ENT = 'INSERT IGNORE INTO sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

games_list = {'mlb':'baseball.mlb','nba':'basketball.nba','nhl':'hockey.nhl','fran':'soccer.fran',\
            'nfl':'football.nfl','epl':'soccer.epl','seri':'soccer.seri','liga':'soccer.liga','bund':'soccer.bund','mls':'soccer.mls',\
            'euro':'soccer.euro', 'chlg':'soccer.chlg', 'ncaa_football':'football.ncaa', 'ncaa_basketball':'basketball.ncaa'}

class ThuuzAPIGames(VTVSpider):
    name = 'thuuzapi_gamesmeta'
    start_urls = []

    def __init__(self, *args, **kwargs):
        #super(ThuuzAPIGames, self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.spider_type = kwargs.get('spider_type')

    def start_requests(self):
        #games_list = ['soccer.fran', 'basketball.nba', 'soccer.euro', 'soccer.ch-uefa2', 'soccer.chlg', 'tennis.atp', 'golf.pga', 'golf.euro', 'football.nfl', 'football.ncaa', 'basketball.ncaa', 'soccer.epl', 'soccer.seri', 'soccer.liga', 'soccer.bund', 'soccer.mls', 'hockey.nhl', 'baseball.mlb', 'tennis.wta', 'race.nsc1', 'race.nsc2', 'race.nsct', 'soccer.mwc', 'soccer.bra1', 'soccer.ligamx']
        url = 'http://api.thuuz.com/2.4/games?auth_code=14bfa6bb838386c8&type=normal&status=7&days=14&sport_leagues=%s'
        if self.spider_type in games_list.keys():
            league = self.spider_type
            value = games_list[league]
            if value:
                xurl = url %(value)
                yield Request(xurl, callback = self.parse)
    @log

    def parse(self, response):
        try:
            j_data  = json.loads(response.body)
            for json_data in j_data['ratings']:
                game_id         = str(json_data['id'])
                home_team       = json_data['home_team']
                away_team       = json_data['away_team']
                game_datetime   = json_data['date']
                sport           = json_data['sport']
                league          = json_data['league'].upper()
                try:
                    radar_id        = json_data['external_ids']['sportsdata'].get('game', '')
                except:
                    radar_id = ''
                if sport == 'golf' or sport == 'tennis':
                    league = json_data['title']
                
                game_datetime   = datetime.strptime(game_datetime, "%Y-%m-%d %H:%M:%S")
                game_date       = game_datetime.date
                genre           = "%s{%s}" %(sport.capitalize(), GENRE_DICT[sport])

                ci = "Ci: %s" %game_id
                at = "At: T: %s#4500<>Li:" %(game_datetime.strftime("%d#%m#%Y#%H#%M"))
                tg = "Tg: LIVE"

                print game_id
                game_values = (game_id, game_id, league, "%s at %s" %(away_team, home_team),
                       "tvvideo", "Other", "english", "150", "%s{eng}" %genre,
                       "%s at %s" %(away_team, home_team), game_date().strftime("%Y-%m-%d"), "game", ci, at, league,  "%s at %s" %(away_team, home_team),  "%s at %s" %(away_team, home_team), game_date().strftime("%Y-%m-%d"), at)
                self.cursor.execute(GAMES_QRY , game_values)

                """if radar_id:
                    en_values = (game_id, 'game', 'thuuz_radar_id', radar_id)
                    self.cursor.execute(INT_ENT, en_values)
                    radar_values = (radar_id)
                    self.cursor.execute(SEL_QRY, radar_values)
                    sports_data = self.cursor.fetchone()
                    if sports_data:
                        merge_values = (game_id, sports_data[0], 'game')
                        self.cursor.execute(INST_QRY, merge_values)"""
        
        except:
            print "json data is not loads "+ response.body
