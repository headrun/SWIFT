import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
import datetime
import MySQLdb

true = True
false = False
null = ''

LEAGUES_DICT = {
                'mlb' : ("p5", "e7gv4ntqzhyrrs5fn4qje6vr", ['REG', 'PRE', 'PST'], '1'),
                'nhl' : ("p3", "fc4ndqfgxbeb7kzzzvkx6wtx", ['REG', 'PRE', 'PST'], '3'),
                'nba' : ("p3", "4gjmrrsbpsuryecdsdazxyuj", ['PRE', 'REG', 'PST'], '2'),
                'nfl': ("ot1", "ru43ezr53p9jdsbg9r2tmvyr", ['PRE', 'REG', 'PST'], '4')
                }

UP_QRY = 'update sports_teams set id=%s where id=%s and tournament_id="NFL" limit 1'
UP_GM_QRY = 'update sports_games_participants set participant_id=%s where participant_id=%s and game_id=%s limit 1'
UP_ROSTER = 'update sports_roster set team_id=%s where team_id=%s'
UP_MERGE = 'update sports_radar_merge set radar_id=%s where radar_id=%s limit 1'
UP_DRAFT = 'update sports_players_draft set team_id=%s where team_id=%s'

cur_year = datetime.datetime.now().year
YEAR_LIST = [cur_year - 1, cur_year, cur_year + 1]

class MajorRadarGames(Spider):
    name = "major_radar_games"
    start_urls = []

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd='veveo123', db="SPORTSRADARDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
 
    def start_requests(self):
        url = "https://api.sportradar.us/%s-%s/games/%s/%s/schedule.json?api_key=%s"
        for league, values in LEAGUES_DICT.iteritems():
            version, api_key, season, sport_id = values
            for sea in season:
                for _year in YEAR_LIST:
                    xurl = url % (league, version, str(_year),sea, api_key)
                    yield Request(xurl, callback = self.parse, meta = {'league' : league, 'season' : sea, 'sport_id': sport_id})
    
    def parse(self, response):
        json_response = json.loads(response.body)

        record = SportsSetupItem()
        record['participants'] = {}

        if response.meta['league']=='mlb':
            games = json_response['league']["season"]['games']
        elif response.meta['league'] in ['nhl', 'nba']:
            games = json_response['games']
        elif response.meta['league']=="nfl":
            weeks = json_response['weeks']
            for week in weeks:
                games = week['games']
                for game in games:
                    game_datetime   = game['scheduled']
                    game_id         = game['id']
                    away_alias      = game['away']['alias']
                    home_alias      = game['home']['alias']
                    game_status     = game['status']
                    stadium_id      = game['venue']['id']
                    sport_id        = response.meta['sport_id']
                    game_note       = ''
                    tou_id          = "NFL"
                    home_id         = game['home']['id']
                    away_id         = game['away']['id']
                    channels = ''
                    if game.has_key("broadcast"):
                        channels = game['broadcast'].get('network','')

                    record['game_datetime']     = game_datetime
                    record['sport_id']          = sport_id
                    record['game_status']       = game_status
                    record['participant_type']  = "team"
                    record['tournament']        = tou_id
                    record['reference_url']     = response.url
                    record['result']            = {}
                    record['source_key']        = game_id 
                    record['rich_data'] = {'location': {'stadium': stadium_id}, 'channels' : str(channels)}
                    record['participants'] = { home_id: ('1',''), away_id: ('0','')}
                    yield record

                    home_tm_values = (home_id, home_alias)
                    away_tm_values = (away_id, away_alias)
                    '''
                    home_gm_values = (home_id, home_alias, game_id)
                    away_game_values = (away_id, away_alias, game_id)
                    self.cursor.execute(UP_QRY, home_tm_values)
                    self.cursor.execute(UP_QRY, away_tm_values)
                    self.cursor.execute(UP_GM_QRY, home_gm_values)
                    self.cursor.execute(UP_GM_QRY, away_game_values)
                    self.cursor.execute(UP_ROSTER, home_tm_values)
                    self.cursor.execute(UP_ROSTER, away_tm_values)
                    self.cursor.execute(UP_MERGE, home_tm_values)
                    self.cursor.execute(UP_MERGE, away_tm_values)
                    self.cursor.execute(UP_DRAFT, away_tm_values)
                    self.cursor.execute(UP_DRAFT, home_tm_values)'''

        if response.meta['league'] !="nfl":
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

                record['game_datetime']     = game_datetime
                record['sport_id']          = sport_id
                record['game_status']       = game_status
                record['participant_type']  = "team"
                record['tournament']        = tou_id
                record['reference_url']     = response.url
                record['result']            = {}
                record['source_key']        = game_id
                record['rich_data'] = {'location': {'stadium': stadium_id}, 'channels' : str(channels)}
                record['participants'] = { home_id: ('1',''), away_id: ('0','')}
                yield record

