from scrapy.http import Request
from vtvspider import VTVSpider
import json
import MySQLdb
from datetime import datetime
import time

GAMES_QRY = 'insert into sports_thuuz_games(Gi, Sk, Ti, Ep, Vt, Ot, Ll, Du, Ge, De, Od, Tg, Ci, At, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), Ti = %s'

GENRE_DICT = {'basketball' : 'RVG2647', 'soccer' : 'RVG2906', 'tennis':'', 'golf':'', 'football': '', 'hockey': '', 'baseball': ''}

SEL_QRY = 'select sportsdb_id from sports_radar_merge where radar_id=%s'

INST_QRY = 'insert into sports_thuuz_merge(thuuz_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INT_ENT = 'INSERT IGNORE INTO sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'


class ThuuzAPIGames(VTVSpider):
    name = 'thuuzapi_gamesmeta'
    start_urls = [ 'http://api.thuuz.com/2.4/games?auth_code=14bfa6bb838386c8&type=normal&status=7&days=28&sport_leagues=soccer.fran,basketball.nba,soccer.euro,soccer.ch-uefa2,soccer.chlg,tennis.atp,golf.pga,golf.euro,football.nfl,football.ncaa,basketball.ncaa,soccer.epl,soccer.seri,soccer.liga,soccer.bund,soccer.mls,hockey.nhl,baseball.mlb']

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
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
            if sport == 'golf':
                league = json_data['title']
            
            game_datetime   = datetime.strptime(game_datetime, "%Y-%m-%d %H:%M:%S")
            game_date       = game_datetime.date
            genre           = "%s{%s}" %(sport.capitalize(), GENRE_DICT[sport])

            ci = "Ci: %s" %game_id
            at = "At: T: %s#4500<>Li:" %(game_datetime.strftime("%d#%m#%Y#%H#%M"))
            tg = "Tg: LIVE"

            game_values = (game_id, game_id, league, "%s at %s" %(away_team, home_team),
                   "tvvideo", "Other", "english", "150", "%s{eng}" %genre,
                   "%s at %s" %(away_team, home_team), game_date().strftime("%Y-%m-%d"), "game", ci, at, league)
            self.cursor.execute(GAMES_QRY, game_values)

            if radar_id:
                en_values = (game_id, 'game', 'thuuz_radar_id', radar_id)
                self.cursor.execute(INT_ENT, en_values)
                radar_values = (radar_id)
                self.cursor.execute(SEL_QRY, radar_values)
                sports_data = self.cursor.fetchone()
                if sports_data:
                    merge_values = (game_id, sports_data[0], 'game')
                    self.cursor.execute(INST_QRY, merge_values)         
