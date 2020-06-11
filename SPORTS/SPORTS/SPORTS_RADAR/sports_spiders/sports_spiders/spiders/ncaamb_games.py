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


class NCAAMBGames(Spider):
    name = "ncaamb_games"
    start_urls = ['https://api.sportradar.us/ncaamb-p3/games/2017/REG/schedule.xml?api_key=znq3h3jd57kuf9qmjc9pr3mp', \
                    'https://api.sportradar.us/ncaamb-p3/games/2017/PST/schedule.xml?api_key=znq3h3jd57kuf9qmjc9pr3mp']   
 
    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()

        sports_id='2'
        
        league_nodes = get_nodes(sel,'//league')
        for l_node in league_nodes:
            l_id = extract_data(l_node,'./@id')
            l_name = extract_data(l_node,'./@name')
            l_alias = extract_data(l_node,'./@alias')
 
            season_id = extract_data(l_node,'./season-schedule/@id')
            season_year = extract_data(l_node,'./season-schedule/@year')
            season_type = extract_data(l_node,'./season-schedule/@type')


            game_nodes = get_nodes(l_node,'.//games/game')
            for g_node in game_nodes:
                game_id = extract_data(g_node,'./@id')
                game_status = extract_data(g_node,'./@status')
                game_coverage = extract_data(g_node,'./@coverage')
                game_scheduled = extract_data(g_node,'./@scheduled')
                home_points = extract_data(g_node,'./@home_points')
                away_points = extract_data(g_node,'./@away_points')
    
                
                venue_id = extract_data(g_node,'./venue/@id')
                venue_name = extract_data(g_node,'./venue/@name')
                venue_capacity = extract_data(g_node,'./venue/@capacity')
                venue_address = extract_data(g_node,'./venue/@address')
                venue_city  = extract_data(g_node,'./venue/@city')
                venue_state = extract_data(g_node,'./venue/@state')
                venue_zip =  extract_data(g_node,'./venue/@zip')
                venue_country = extract_data(g_node,'./venue/@country')
                
                home_name = extract_data(g_node,'./home/@name')
                home_id = extract_data(g_node,'./home/@id')
                home_alias = extract_data(g_node,'./home/@alias')

                away_name = extract_data(g_node,'./away/@name')
                away_id = extract_data(g_node,'./away/@id')
                away_alias = extract_data(g_node,'./away/@alias')

                record = SportsSetupItem()
                record['participants'] = {}
       
                game_note       = ''
                record['game']              = 'basketball'
                record['game_datetime']     = game_scheduled
                record['sport_id']          = sports_id
                record['game_status']       = game_status
                record['participant_type']  = "team"
                record['tournament']        = 'ncaamb'
                record['reference_url']     = response.url
                record['result']            = {}
                record['source_key']        = game_id
                record['season']            = season_year
                record['season_type']       = season_type
                record['rich_data'] = {'location': {'stadium': venue_id,'country':venue_country,'state':venue_state,'city':venue_city}}
                record['participants'] = { home_id: ('1',''), away_id: ('0','')}
                yield record

