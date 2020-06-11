import json
import logging
from scrapy.selector import Selector
from scrapy.spider import Spider
from scrapy.http import Request
from vtv_db import get_mysql_connection
import datetime
from sports_spiders.items import SportsSetupItem
from vtvspider import VTVSpider, get_nodes, extract_data 

TOU_lIST = ['sr:tournament:620', 'sr:tournament:660', 'sr:tournament:764', 'sr:tournament:1997', 'sr:tournament:2555', 'sr:tournament:2557', 'sr:tournament:2559', 'sr:tournament:2561', 'sr:tournament:2563', 'sr:tournament:2567', 'sr:tournament:2569', 'sr:tournament:2571', 'sr:tournament:2573', 'sr:tournament:2575', 'sr:tournament:2579', 'sr:tournament:2581', 'sr:tournament:2583', 'sr:tournament:2587', 'sr:tournament:2591', 'sr:tournament:2593', 'sr:tournament:2595', 'sr:tournament:2597', 'sr:tournament:2599', 'sr:tournament:2577', 'sr:tournament:2553', 'sr:tournament:2565', 'sr:tournament:2589', 'sr:tournament:2100', 'sr:tournament:2102']

#TOU_lIST = ['sr:tournament:2589', 'sr:tournament:2591', 'sr:tournament:2593', 'sr:tournament:2595', 'sr:tournament:2597', 'sr:tournament:2599']

URL = 'http://api.sportradar.us/tennis-t2/en/tournaments/%s/schedule.json?api_key=yybj7e5ma9j5ebvrucn9bpye'

DATE_CHECKLIST = [datetime.datetime.now().date() + datetime.timedelta(days = i) for i in range(30)]
_today = datetime.datetime.now().date()

class TennisGames(VTVSpider):
    name = "sports_radar_tnns"
    start_urls = ['http://api.sportradar.us/tennis-t2/en/tournaments.xml?api_key=yybj7e5ma9j5ebvrucn9bpye']

    def __init__(self):
        self.logger = logging.getLogger('sportsRadar.log')

    def validate_date(self, start_date, end_date, tou_id):
        start_date = datetime.datetime.strptime(start_date,'%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date,'%Y-%m-%d').date()
        if _today >= start_date and _today <= end_date:
            return True
        elif start_date in DATE_CHECKLIST:
            return True
        else: return False

 
    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        data = get_nodes(sel, '//tournament')
        for rd_data in data:
            id_  = extract_data(rd_data, './@id')
            tou_start_date = extract_data(rd_data, './current_season/@start_date')
            tou_end_date = extract_data(rd_data, './current_season/@end_date')
            temp_check = self.validate_date(tou_start_date, tou_end_date, id_)
            if id_ not in TOU_lIST:
                continue
            if temp_check:
                print extract_data(rd_data, './@name')
                link = URL %id_ 
                yield Request(link.replace('https', 'http'), callback=self.parse_next, meta = {'proxy': 'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse_next(self, response):
        json_data = json.loads(response.body)
        tou_main_id = json_data['tournament']['id']
        tou_main_name = json_data['tournament']['name']
        tou_games = json_data['sport_events']
        record = SportsSetupItem()
        for game in tou_games:
            game_id = game['id']
            game_status = game['status']
            try:
                tou_par_id = game['tournament']['parent_id']
                event_id = tou_main_id
            except:
                tou_par_id = tou_main_id
                event_id = ''
            game_datetime = game['scheduled']
            #game_type       = game['tournament_round'].get('type', '')
            game_season_id = game['season']['id']
            game_season_start = game['season']['start_date']
            game_season_end = game['season']['end_date']
            sport_id        = 5
            game_note       = ''
            try:
                stadium_id = game['venue']['id']
            except:
                stadium_id = ''
            record['participants'] = {}
            counter = 0
            group_count = 0

            players = game['competitors']
            playe_name_list = []
            if "Double" in tou_main_name:
                for player in players:
                    qualifier = player['qualifier']
                    pl_datas = player['players']
                    for data in pl_datas:
                        player_id = data['id']
                        player_name = data['name']
                        if "," in player_name:
                            
                            player_name = player_name.split(',')[-1].strip() + " " + player_name.split(',')[0].strip()
                        if qualifier == "home":
                            group_count = 1
                        else:
                            group_count = 2
                        playe_name_list.append(player_name)
                        record['participants'][player_id] = (group_count, '') 
            else:    
                if len(players) == 4:
                    group_count = 1
                for player in players:
                    if len(players) == 4:
                        counter += 1
                    if counter > 2:
                        group_count = 2
                    player_name = player['name']
                    if "," in player_name:

                        player_name = player_name.split(',')[-1].strip() + " " + player_name.split(',')[0].strip()
                    playe_name_list.append(player_name) 
                    player = player['id']
                    record['participants'][player] = (group_count, '')  
            game_note = " vs. ".join(playe_name_list)
            record['game_datetime']     = game_datetime
            record['sport_id']          = sport_id
            record['game_status']       = game_status
            record['participant_type']  = "player"
            record['tournament']        = tou_par_id
            record['game']              = "tennis"
            record['reference_url']     = response.url
            record['result']            = {}
            record['source_key']        = game_id
            record['event']             = event_id
            record['rich_data'] = {'location': {'stadium': stadium_id}, 'game_note': game_note}
            yield record



