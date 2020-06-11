import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5

true = True
false = False
null = ''



class NCAAFBGames(Spider):
    name = "ncaafb_games"
    start_urls = ['https://api.sportradar.us/ncaafb-rt1/2017/REG/schedule.json?api_key=2u6h2nxweuzvwtnrbwjej7aw', \
                'https://api.sportradar.us/ncaafb-rt1/2017/PST/schedule.json?api_key=2u6h2nxweuzvwtnrbwjej7aw']   
 
    def parse(self, response):
        sel = Selector(response)
        json_response = json.loads(response.body)
        weeks = json_response['weeks']
        season_type = json_response['type']
        sports_id='4'
        season_year = json_response['season'] 
        for week in weeks:
            games = week['games']
            week_id = week['id']
            week_number = week['number']
            for game in games:
                game_datetime   = game['scheduled']
                game_id         = game['id']
                game_status     = game['status']
                try:
                    stadium_id      = game['venue']['id']
                    venue_country   = game['venue']['country']
                    venue_state     = game['venue']['state']
                    venue_city      = game['venue']['city']
                except:
                    venue_state = ''
                    venue_city = ''
                    venue_country = ''
                    stadium_id = ''
                game_note       = ''

                home_id       = game['home']
                away_id       = game['away']


                channels = ''
                if game.has_key("broadcast"):
                    channels = game['broadcast'].get('network','')

                
                record = SportsSetupItem()
                record['participants'] = {}
        
                record['game']              = 'american football'
                record['game_datetime']     = game_datetime
                record['sport_id']          = sports_id
                record['game_status']       = game_status
                record['participant_type']  = "team"
                record['tournament']        = 'ncaafb'
                record['reference_url']     = response.url
                record['result']            = {}
                record['source_key']        = game_id
                record['season']            = season_year
                record['season_type']       = season_type
                record['week_number']       = week_number
                record['week_id']           = week_id
                record['rich_data'] = {'location': {'stadium': stadium_id,'country':venue_country,'state':venue_state,'city':venue_city}}
                record['participants'] = { home_id: ('1',''), away_id: ('0','')}
                yield record

