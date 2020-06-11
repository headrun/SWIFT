import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from vtv_db import get_mysql_connection

LEAGUES_DICT = {
                'mlb' : ("p5", "e7gv4ntqzhyrrs5fn4qje6vr", ['REG'], '1'),
                'nhl' : ("p3", "fc4ndqfgxbeb7kzzzvkx6wtx", ['REG'], '3'),
                'nba' : ("p3", "4gjmrrsbpsuryecdsdazxyuj", ['PRE', 'REG'], '2')
                }
class NHLGames(Spider):
    name = "nhl_games"
    start_urls = []

    def __init__(self):
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger = logging.getLogger('sportsRadar.log')
    
    def start_requests(self):
        url = "https://api.sportradar.us/%s-%s/games/2016/%s/schedule.json?api_key=%s"
        for league, values in LEAGUES_DICT.iteritems():
            version, api_key, season, sport_id = values
            for sea in season:
                xurl = url % (league, version, sea, api_key)
                yield Request(xurl.replace('https', 'http'), callback = self.parse, meta = {'league' : league, 'season' : sea, 'sport_id': sport_id, 'proxy':'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})
    
    def parse(self, response):
        cursor, conn = get_mysql_connection(self.logger, self.db_ip, self.db_name, cursorclass='', user = 'veveo', passwd='veveo123')
        json_response = json.loads(response.body)
        if response.meta['league']=='mlb':
            games = json_response['league']["season"]['games']
        else:
            games = json_response['games']
        for game in games:
            game_datetime   = game['scheduled']
            game_id         = game['id']
            away_id         = game['away']['id']
            home_id         = game['home']['id']
            game_status     = game['status']
            stadium_id      = game['venue']['id']
            sport_id        = response.meta['sport_id']
            game_note       = ''
            tou_id          = json_response['league']['id']

            channels = ''
            if game.has_key("broadcast"):
                channels = game['broadcast'].get('network','')
            
            query = 'insert into sports_games(id, game_datetime, sport_id, game_note, tournament_id, status, channels, radio, online, reference_url, event_id, location_id, stadium_id, time_unknown, tzinfo, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), status=%s'
            values = (game_id, game_datetime, sport_id, game_note, tou_id, game_status,
                      channels, '', '', response.url, '', '', stadium_id, '', '', game_status)
            cursor.execute(query, values)

            for par in ['away', 'home']:
                is_home = 0
                if par == "home": is_home = 1
                par = game[par]['id']
                par_query = 'insert into sports_games_participants(game_id, participant_id, is_home, group_number, created_at, modified_at) values ( %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                values = (game_id, par, is_home, 0)
                cursor.execute(par_query, values)

        cursor.close()
        conn.close()
