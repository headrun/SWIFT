import json
import logging
import datetime
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from vtv_db import get_mysql_connection
from datetime import timedelta
from sports_spiders.items import SportsSetupItem

true = True
false = False
null = ''


class SoccerRadarGames(Spider):
    name = "soccer_radar_games"
    start_urls = []

    apis_dict = {'na': 'kpwbhzaxkpxhfgp3zdaqe2xw', 
                'eu': '72vxpdus7zy22a93kcnhjqct'}
    #apis_dict = {'eu': '72vxpdus7zy22a93kcnhjqct'}

    def __init__(self):
        self.logger  = logging.getLogger('sportsRadar.log')
        self.start_urls = ['https://api.sportradar.us/soccer-p2/na/matches/schedule.xml?api_key=kpwbhzaxkpxhfgp3zdaqe2xw', 'https://api.sportradar.us/soccer-p2/eu/matches/schedule.xml?api_key=72vxpdus7zy22a93kcnhjqct']
        self.start_urls.extend(self.get_past_games_urls())

    def get_past_games_urls(self):
        url_list = []
        now = datetime.datetime.now()
        url = "http://api.sportradar.us/soccer-p2/%s/matches/%s/boxscore.xml?api_key=%s"
        for i in range(0, 30):

            _date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
            for key, values in self.apis_dict.iteritems():
                type_ = key
                value = values
                main_url  = url % (type_, _date, value)
                url_list.append(main_url)
        return url_list 
    
    def parse(self, response):
        hdoc = Selector(response)
        hdoc.remove_namespaces()
        record = SportsSetupItem()
        games = hdoc.xpath('//matches//match')
        for game in games:
            game_datetime   = "".join(game.xpath('./@scheduled').extract())
            game_id         = "".join(game.xpath('./@id').extract())
            away_id         = "".join(game.xpath('.//away/@id').extract())
            home_id         = "".join(game.xpath('.//home/@id').extract())
            game_status     = "".join(game.xpath('./@status').extract())
            stadium_id      = "".join(game.xpath('.//venue/@id').extract())
            sport_id        = 7 #response.meta['sport_id']
            game_note       = ''
            tou_id          = "".join(game.xpath('.//tournament_group/@id').extract())
            ref_id          = "".join(game.xpath('./@reference_id').extract())
           
            #game_datetime = datetime.strptime(game_datetime, "%Y-%m-%dT%H:%M:%SZ") 

            record['game_datetime']     = game_datetime
            record['sport_id']          = sport_id
            record['game_status']       = game_status
            record['participant_type']  = "team"
            record['tournament']        = tou_id
            record['reference_url']     = response.url
            record['result']            = {}
            record['source_key']        = game_id
            record['reference_id']      = ref_id
            record['rich_data'] = {'location': {'stadium': stadium_id}}
            record['participants'] = { home_id: ('1',''), away_id: ('0','')}
            yield record

