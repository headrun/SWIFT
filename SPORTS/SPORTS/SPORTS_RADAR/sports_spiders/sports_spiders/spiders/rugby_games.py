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



class RugbyGames(Spider):
    name = "rugby_games"
    start_urls = ['https://api.sportradar.us/rugby-p1/2016/schedule.xml?api_key=8zp2sjv9ra28tct2rzfqh73a']
    
    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()

        #schedule_nodes = get_nodes(sel, '//matches/match_id')
        schedule_nodes = get_nodes(sel, '//matches/match')
        for sch_node in schedule_nodes:
            match_id = extract_data(sch_node,'./@id')
            match_scheduled = extract_data(sch_node,'./@scheduled')
            match_coverage = extract_data(sch_node,'./@coverage')
            match_status = extract_data(sch_node,'./@status')

            league_id = extract_data(sch_node,'./league/@id')
            league_name = extract_data(sch_node,'./league/@name')

            sports_id=''
            sport_id_dict = {'Rugby Union Sevens':'167','Rugby League':'12','Rugby Union':'11'}
            for i,j in sport_id_dict.iteritems():
                if i==league_name:
                    sports_id = j

            
            tournament_id = extract_data(sch_node,'./tournament/@id')
            tournament_name =extract_data(sch_node,'./tournament/@name')
            season_id = extract_data(sch_node,'./season/@id')
            season_name =  extract_data(sch_node,'./season/@name')
            season_year = extract_data(sch_node,'./season/@year')
            season_start =  extract_data(sch_node,'./season/@start')
            season_end = extract_data(sch_node,'./season/@end')
        
            home_id = extract_data(sch_node,'./home/@id')
            home_name = extract_data(sch_node,'./home/@name')
            home_alias = extract_data(sch_node,'./home/@alias')
            away_id = extract_data(sch_node,'./away/@id')
            away_name =extract_data(sch_node,'./away/@name')
            away_alias = extract_data(sch_node,'./away/@alias')
            venue_id = extract_data(sch_node,'./venue/@id')

 
            record = SportsSetupItem()
            record['participants'] = {}
    
            game_note       = ''
            record['game_datetime']     = match_scheduled
            record['sport_id']          = sports_id
            record['game_status']       = match_status
            record['participant_type']  = "team"
            record['tournament']        = tournament_id
            record['reference_url']     = response.url
            record['result']            = {}
            record['source_key']        = match_id
            record['season']            = season_year
            record['rich_data'] = {'location': {'stadium': venue_id}}
            record['participants'] = { home_id: ('1',''), away_id: ('0','')}
            yield record

