import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem

true = True
false = False
null = ''

TOU_URL = 'https://api.sportradar.us/golf-p2/summary/pga/2017/tournaments/%s/summary.json?api_key=36c62wf2dmztgvfe2nt3qr9v'

class GolfRadarGames(Spider):
    name = "golf_radar_games"
    start_urls = ['https://api.sportradar.us/golf-p2/schedule/pga/2017/tournaments/schedule.json?api_key=36c62wf2dmztgvfe2nt3qr9v']
    def parse(self, response):
        data = eval(response.body)
        tournament = data['tournaments']
        for tou in tournament:
            id_        = tou['id']
            name       = tou['name']
            start_date = tou['start_date']
            end_date   = tou['end_date']

            record = SportsSetupItem()
            stadium_id   = tou['venue']['id']
            stadium_name = tou['venue']['name']
            address      =  tou.get('venue', '').get('address', '')
            capacity     =  tou.get('venue', '').get('capacity', '')
            city         =  tou.get('venue', '').get('city', '')
            state        =  tou.get('venue', '').get('state', '')
            country      = tou.get('venue', '').get('country', '')
            zip_code     = tou.get('venue', '').get('zipcode', '')
            time_zone    = tou.get('venue', '').get('time_zone', '')
            surface      = tou.get('venue', '').get('surface', '')
            type_        = tou.get('venue', '').get('type', '')

            if start_date > '2017-02-02':
                record['game_datetime']     = start_date
                record['sport_id']          = 8
                record['game_status']       = 'scheduled'
                record['participant_type']  = "player"
                record['tournament']        = id_
                record['reference_url']     = response.url
                record['result']            = {}
                record['source_key']        = id_
                record['rich_data'] = {'location': {'stadium': stadium_id, 'city': city, 'state': state, 'country': country}}
                record['participants'] = {}
                yield record
         
            #tou_url = TOU_URL % id_ 
            #yield Request(tou_url, callback=self.parse_games, dont_filter=True)

    def parse_games(self, response):
        game = eval(response.body)
        record = SportsSetupItem()
        participants = {}
        city = state = country = ''
        game_datetime   = game['start_date']
        game_id         = game['id']
        game_status     = game.get('status', '')
        stadium_id      = game['venue']['id']
        sport_id        = '8'
        game_note       = ''
        tou_id          = game['id']
        city            = game.get('venue').get('city', '')
        state           = game.get('venue').get('state', '')
        country         = game.get('venue').get('country', '')

        channels = ''
        if game.has_key("broadcast"):
            channels = game['broadcast'].get('network','')
        players = game.get('field', '')
        for player in players:
            player_id = player['id']
            player_name = player['first_name'] + " " + player['last_name']
            participants[player_id]         = (0, player_name)

        record['game_datetime']     = game_datetime
        record['sport_id']          = sport_id
        record['game_status']       = game_status
        record['participant_type']  = "player"
        record['tournament']        = tou_id
        record['reference_url']     = response.url
        record['result']            = {}
        record['source_key']        = game_id
        record['rich_data'] = {'location': {'stadium': stadium_id, 'city': city, 'state': state, 'country': country}, 'channels' : str(channels)}
        record['participants'] = participants
        yield record

