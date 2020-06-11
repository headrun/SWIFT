import datetime
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, \
extract_data, extract_list_data, get_nodes, get_utc_time
from vtvspider_dev import get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

true = True
false = False
null = ''
final_result_dict = {}

class Rydercup(VTVSpider):
    name = "ryder_cup"
    start_urls = ['http://www.pgatour.com/data/r/468/rcup_summary.json']

    def parse(self, response):
        hxs = Selector(response)
        raw_data = response.body
        data = eval(raw_data)
        participant = []
        participants = {}
        results = {}
        record = SportsSetupItem()
        if data:
            import pdb;pdb.set_trace()
            nodes = data.get("sports-scores", "").get("golf-tournament", "").get("golfer-results", "")
            for node in nodes:
                plf1_name = node.get("team-one", "").get("golfer-one-first-name", "")
                plf2_name = node.get("team-one", "").get("golfer-one-last-name", "")
                player1_sk = plf1_name + "-" +plf2_name
                player1_name = plf1_name + ' ' + plf2_name
                participants[player1_sk] = (0, player1_name)
                plf3_name = node.get("team-one", "").get("golfer-two-first-name", '')
                plf4_name = node.get("team-one", "").get("golfer-two-last-name", '')
                player2_sk = plf3_name+ "-" +plf4_name
                player2_name = plf3_name+  ' ' + plf4_name
                participants[player2_sk] = (0, player2_name)
                tm2pl1 = node.get("team-two", "").get("golfer-one-first-name", '')
                tm2pl2 = node.get("team-two", "").get("golfer-one-last-name", "")
                tm2player1_sk = tm2pl1 + "-" + tm2pl2
                tm2player1_name = tm2pl1 + ' ' + tm2pl2
                participants[tm2player1_sk] = (0, tm2player1_name)
                tm2pl3 = node.get("team-two", "").get("golfer-two-first-name", '')
                tm2pl4 = node.get("team-two", "").get("golfer-two-last-name", '')
                tm2player2_sk = tm2pl3 + "-" + tm2pl4
                tm2player2_name = tm2pl3 + ' ' +tm2pl4
                participants[tm2player2_sk] = (0, tm2player2_name)
                points = node.get("match-lead", "").get("details")
                team1_points = points.split('and')[0].strip()
                team2_points  = points.split('and')[-1].strip()
                team_id = node.get("match-lead", "").get("id")
                event_type = node.get("cup-type", "").get("name")
                player1_points = player2_points = player3_points = player4_points = ''
                player1_id =  player2_id = player3_id = player4_id =''
                player1_score = player2_score = player3_score =  player4_score = ''
                if "1" in team_id:
                    player1_points = "1"
                    player2_points = "0"
                    player3_points = "1"
                    player4_points = "0"
                elif "2" in team_id:
                    player2_points =  "1"
                    player1_points =  "0"
                    player3_points = "0"
                    player4_points = "1"
                else:
                    player1_points = "0.5"
                    player2_points = "0.5"
                    player3_points ="0.5" 
                    player4_points = "0.5"
                if "Foursome" in event_type or "Four Ball" in event_type:
                    final_result_dict[player1_sk] = player1_points
                    final_result_dict[tm2player1_sk] =  player2_points
                    final_result_dict[player2_sk] =  player3_points
                    final_result_dict[tm2player2_sk] =  player4_points
                    for k, v in final_result_dict.iteritems():
                        if player1_sk in k:
                            player1_score = float(v) + float(player1_points)
                        elif tm2player1_sk in k:
                            player2_score = float(v) + float(player2_points)
                        elif player2_sk in k:
                            player3_score = float(v) + float(player3_points)
                        elif tm2player2_sk in k:
                            player4_score = float(v) + float(player4_points)
                    final_result_dict.update({player1_sk: player1_score, \
                                         tm2player1_sk: player2_score, player2_sk: player3_score, tm2player2_sk: player4_score})
                    results[player1_sk] = {'final': player1_score}
                    results[tm2player1_sk] = {'final': player2_score}
                    results[player2_sk] = {'final': player3_score}
                    results[tm2player2_sk] = {'final': player4_score}
                    record['result'] = results
                elif "Singles" in event_type:
                    final_result_dict[player1_sk] = player1_points
                    final_result_dict[tm2player1_sk] =  player2_points
                    for k, v in final_result_dict.iteritems():
                        if player1_sk in k:
                            player1_score = float(v) + float(player1_points)
                        elif tm2player1_sk in k:
                            player2_score = float(v) + float(player2_points)
                    final_result_dict.update({player1_sk: player1_score, \
                                tm2player1_sk: player2_score})
                    results[player1_sk] = {'final': player1_score}
                    results[tm2player1_sk] = {'final': player2_score}
                    record['result'] = results
                tz_info = get_tzinfo(city = "Chaska", country="USA")
                record['affiliation'] = "euro"
                record['source'] = "pga_golf"
                record['participant_type']  = 'player'
                record['game_status'] = "completed"
                record['tournament'] = "Ryder Cup"
                record['participants'] = participants
                record['game'] = "golf"
                record['rich_data'] = {}
                location = {'city': "Chaska", 'state': "Minnesota", "country": "USA", 'stadium': "Hazeltine National Golf Club"}
                record['rich_data'] = {'location': location}
                record['reference_url'] = response.url
                record['time_unknown'] = 1
                record['tz_info'] = tz_info
                #record['result'].setdefault('0', {}) ['super_game'] = "Ryder_Cup_Sep_26_Sep_28"
                if "Singles" in event_type:
                    record['event'] = "Ryder Cup Singles"
                    record['source_key'] = "Ryder_Cup_Singles_02_Sep_02"
                    record['game_datetime'] = "2016-10-02 04:00:00"
                    yield record 
                elif "Foursome" in event_type:
                    record['event'] = "Ryder Cup Foursomes"
                    record['source_key'] = "Ryder_Cup_Foursomes_30_Sep_09"
                    record['game_datetime'] = "2016-09-30 04:00:00"
                    yield record
                elif "Four Ball" in event_type:
                    record['event'] = "Ryder Cup Fourball"
                    record['source_key'] = "Ryder_Cup_FourBall_30_Sep_09"
                    record['game_datetime'] = "2016-09-30 04:00:00"
                    yield record
