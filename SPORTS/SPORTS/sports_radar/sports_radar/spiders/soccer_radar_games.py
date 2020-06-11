import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from vtv_db import get_mysql_connection
from datetime import datetime
from datetime import timedelta

class NHLGames(Spider):
    name = "soccer_radar_games"
    start_urls = []

    def __init__(self):
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger  = logging.getLogger('sportsRadar.log')
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        self.start_urls = ['https://api.sportradar.us/soccer-p2/eu/matches/schedule.xml?api_key=72vxpdus7zy22a93kcnhjqct']
        self.start_urls.extend(self.get_past_games_urls())

    def get_past_games_urls(self):
        url_list = []
        url = "http://api.sportradar.us/soccer-p2/eu/matches/%s/%s/%s/boxscore.xml?api_key=72vxpdus7zy22a93kcnhjqct"
        for i in range(0, 60):
            game_datetime = datetime.now() - timedelta(days=1)
            main_url  = url % (game_datetime.year, game_datetime.month, game_datetime.day)
            url_list.append(main_url)
        return url_list 
    
    def parse(self, response):
        hdoc = Selector(response)
        hdoc.remove_namespaces()

        games = hdoc.xpath('//matches//match')
        for game in games:
            game_datetime   = "".join(game.xpath('./@scheduled').extract())
            game_id         = "".join(game.xpath('./@id').extract())
            away_id         = "".join(game.xpath('.//away/@id').extract())
            home_id         = "".join(game.xpath('.//home/@id').extract())
            game_status     = "".join(game.xpath('.//status').extract())
            stadium_id      = "".join(game.xpath('.//venue/@id').extract())
            sport_id        = 7 #response.meta['sport_id']
            game_note       = ''
            tou_id          = "".join(game.xpath('.//tournament/@id').extract())

            pars = {'home' : home_id, 'away' : away_id}
           
            game_datetime = datetime.strptime(game_datetime, "%Y-%m-%dT%H:%M:%SZ") 
            query = 'insert into sports_games(id, game_datetime, sport_id, game_note, tournament_id, status, channels, radio, online, reference_url, event_id, location_id, stadium_id, time_unknown, tzinfo, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), status=%s'
            values = (game_id, game_datetime, sport_id, game_note, tou_id, game_status,
                      '', '', '', response.url, '', '', stadium_id, '', '', game_status)
            self.cursor.execute(query, values)

            for key, value in pars.iteritems():
                if key == "home": is_home = 1
                else: is_home = 0
                par_query = 'insert into sports_games_participants(game_id, participant_id, is_home, group_number, created_at, modified_at) values ( %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                values = (game_id, value, is_home, 0)
                self.cursor.execute(par_query, values)
        self.cursor.close()
        self.conn.close()
