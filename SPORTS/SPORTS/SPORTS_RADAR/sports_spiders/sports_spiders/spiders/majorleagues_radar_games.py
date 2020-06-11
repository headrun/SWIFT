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

TEAM_DICT = {'MIN': '33405046-04ee-4058-a950-d606f8c30852', 'MIA': '4809ecb0-abd3-451d-9c4a-92a90b83ca06', 'CAR': 'f14bf5cc-9a82-4a38-bc15-d39f75ed5314', 'ATL': 'e6aa13a4-0055-48a9-bc41-be28dc106929', 'DET': 'c5a59daa-53a7-4de0-851f-fb12be893e9e', 'CIN': 'ad4ae08f-d808-42d5-a1e6-e9bc4e34d123', 'NYJ': '5fee86ae-74ab-4bdd-8416-42a9dd9964f3', 'DEN': 'ce92bd47-93d5-4fe9-ada4-0fc681e6caa0', 'BAL': 'ebd87119-b331-4469-9ea6-d51fe3ce2f1c', 'NYG': '04aa1c9d-66da-489d-b16a-1dee3f2eec4d', 'OAK': '1c1cec48-6352-4556-b789-35304c1a6ae1', 'TEN': 'd26a1ca5-722d-4274-8f97-c92e49c96315', 'LA': '2eff2a03-54d4-46ba-890e-2bc3925548f3', 'DAL': 'e627eec7-bbae-4fa4-8e73-8e1d6bc5c060', 'NE': '97354895-8c77-4fd4-a860-32e62ea7382a', 'SEA': '3d08af9e-c767-4f88-a7dc-b920c6d2b4a8', 'CHI': '7b112545-38e6-483c-a55c-96cf6ee49cb8', 'PIT': 'cb2f9f1f-ac67-424e-9e72-1475cb0ed398', 'NFC': '892cf7f3-46ab-4a45-8824-8afbd84924a9', 'CLE': 'd5a2eb42-8065-4174-ab79-0a6fa820e35e', 'TB': '4254d319-1bc7-4f81-b4ab-b5e6f3402b69', 'HO': '82d2d380-3834-4938-835f-aec541e5ece7', 'GB': 'a20471b4-a8d9-40c7-95ad-90cc30e46932', 'WAS': '22052ff7-c065-42ee-bc8f-c4691c50e624', 'JAC': 'f7ddd7fa-0bae-4f90-bc8e-669e4d6cf2de', 'KC': '6680d28d-d4d2-49f6-aace-5292d3ec02c2', 'PHI': '386bdbf9-9eea-4869-bb9a-274b0bc66e80', 'BUF': '768c92aa-75ff-4a43-bcc0-f2798c2e1724', 'NO': '0d855753-ea21-4953-89f9-0e20aff9eb73', 'LAC': '1f6dcffb-9823-43cd-9ff4-e7a8466749b5', 'IND': '82cf9565-6eb9-4f01-bdbd-5aa0d472fcd9', 'ARI': 'de760528-1dc0-416a-a978-b510d20692ff', 'SF': 'f0e724b0-4cbf-495a-be47-013907608da9', 'SD': '9dbb9060-ba0f-4920-829e-16d4d9246b5d', 'HOU': '82d2d380-3834-4938-835f-aec541e5ece7'}


LEAGUES_DICT = {
                'mlb' : ("p5", "e7gv4ntqzhyrrs5fn4qje6vr", ['REG', 'PRE', 'PST'], '1'),
                'nhl' : ("p3", "fc4ndqfgxbeb7kzzzvkx6wtx", ['REG', 'PRE', 'PST'], '3'),
                'nba' : ("p3", "4gjmrrsbpsuryecdsdazxyuj", ['PRE', 'REG', 'PST'], '2'),
                'nfl': ("rt1", "f593kvk9569c8yztm8h576nu", ['PRE', 'REG', 'PST'], '4')}

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
        url = "http://api.sportradar.us/%s-%s/games/%s/%s/schedule.json?api_key=%s"
        for league, values in LEAGUES_DICT.iteritems():
            version, api_key, season, sport_id = values 
            for sea in season:
                for _year in YEAR_LIST:
                    xurl = url % (league, version, str(_year),sea, api_key)
                    if 'nfl' in xurl:
                        xurl = xurl.replace('games/', '')
                    yield Request(xurl.replace('https', 'http'), callback = self.parse, meta = {'league' : league, 'season' : sea, 'sport_id': sport_id, 'proxy': 'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})
    
    def parse(self, response):
        json_response = json.loads(response.body)

        record = SportsSetupItem()
        record['participants'] = {}
        if response.meta['league']=='mlb':
            try:
                games = json_response['league']["season"]['games']
            except:
                games = json_response.get('games', '')
        elif response.meta['league'] in ['nhl', 'nba']:
            games = json_response['games']
        elif response.meta['league']=="nfl":
            weeks = json_response['weeks']
            season_type = json_response['type']
            for week in weeks:
                games = week['games']
                week_id = week['id']
                week_number = week['number']
                for game in games:
                    game_datetime   = game['scheduled']
                    game_id         = game['id']
                    game_status     = game['status']
                    stadium_id      = game['venue']['id']
                    sport_id        = response.meta['sport_id']
                    game_note       = ''
                    tou_id          = "NFL"


                    try:
                        away_alias      = game['away']['alias']
                        home_alias      = game['home']['alias']
                        home_id         = game['home']['id']
                        away_id         = game['away']['id']

                    except:
                        home_alias       = game['home']
                        away_alias       = game['away']

                        home_id = TEAM_DICT[home_alias]
                        away_id = TEAM_DICT[away_alias]

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
                    record['week_id']           = week_id
                    record['week_number']       = week_number
                    record['season_type']       = season_type
                    record['rich_data'] = {'location': {'stadium': stadium_id}, 'channels' : str(channels)}
                    record['participants'] = { home_alias: ('1',''), away_alias: ('0','')}
                    yield record

                    '''home_tm_values = (home_alias, home_id)
                    away_tm_values = (away_alias, away_id)
                    
                    home_gm_values = (home_alias, home_id, game_id)
                    away_game_values = (away_alias, away_id, game_id)
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

