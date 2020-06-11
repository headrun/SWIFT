import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from vtv_db import get_mysql_connection
from datetime import datetime

class TennisGames(Spider):
    name = "sports_radar_tnns"
    start_urls = ['http://api.sportradar.us/tennis-t2/en/tournaments/sr:tournament:2591/schedule.json?api_key=yybj7e5ma9j5ebvrucn9bpye']

    def __init__(self):
        self.db_name = "SPORTSRADARDB"
        self.db_ip   = "10.28.218.81"
        self.logger = logging.getLogger('sportsRadar.log')

    def parse(self, response):
        json_data = json.loads(response.body)
        cursor, conn = get_mysql_connection(self.logger, self.db_ip, self.db_name, cursorclass='', user = 'veveo', passwd='veveo123')
        tou_main_id = json_data['tournament']['id']
        tou_main_name = json_data['tournament']['name']

        tou_games = json_data['sport_events']
        for game in tou_games:
            game_id = game['id']
            game_status = game['status']
            game_datetime = game['scheduled']
            game_type       = game['tournament_round'].get('type', '')
            game_season_id = game['season']['id']
            game_season_start = game['season']['start_date']
            game_season_end = game['season']['end_date']
            print tou_main_id
            print tou_main_name
            print game_id
            print game_datetime
            import pdb; pdb.set_trace()
            sport_id        = 5
            game_note       = ''
            stadium_id = ''

            game_datetime = datetime.strptime(game_datetime, "%Y-%m-%dT%H:%M:%SZ")
            query = 'insert into sports_games(id, game_datetime, sport_id, game_note, tournament_id, status, channels, radio, online, reference_url, event_id, location_id, stadium_id, time_unknown, tzinfo, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), status=%s'
            values = (game_id, game_datetime, sport_id, game_note, tou_main_id, game_status,
                      '', '', '', response.url, '', '', stadium_id, '', '', game_status)
            #cursor.execute(query, values)

            for part in game['competitors']:
                part_id = part['id']
                if part['qualifier'] == "home": is_home = 1
                else: is_home = 0
                par_query = 'insert into sports_games_participants(game_id, participant_id, is_home, group_number, created_at, modified_at) values ( %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                values = (game_id, part_id, is_home, 0)
                #cursor.execute(par_query, values)
        cursor.close()
        conn.close()

