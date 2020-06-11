import time, datetime
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import extract_data, extract_list_data, \
get_nodes, get_utc_time, get_tzinfo
from vtvspider_new import VTVSpider


class CBABasketball(VTVSpider):
    name = "cba_basketball"
    allowed_domains = ['www.sportstats.com']
    start_urls = []

    def start_requests(self):
        top_urls = {}
        top_lis  = ["IjoxMX0=/", "IjoxMn0=/", "IjoxMH0=/", "Ijo5fQ==/", \
                    "Ijo4fQ==/", "Ijo3fQ==/", "Ijo2fQ==/", "Ijo1fQ==/", \
                    "Ijo0fQ==/", "IjozfQ==/", "IjoxfQ==/", "IjowfQ==/", \
                    "IjoyfQ==/"]

        sch_list = ["ciOjB9/", "ciOjF9/", "ciOjJ9/", "ciOjN9/", "ciOjR9/", "ciOjV9/", "ciOjZ9/",  \
                    "ciOjd9/", "ciOjh9/", "ciOjl9/", "ciOjEwfQ==/", "ciOjExfQ==/", "ciOjEyfQ==/"]

        sch_dict = {'regular': 'http://www.sportstats.com/ajax-table-pagination/h/978546ab082954d522df4aae0141878b/tzc/Mg==/d/eyI0M2RkIjpbM10sImNlNWIiOls1Ml0sIjU1ZWIiOltbIjI1OTkxIl1dLCJmZWFhIjpbMTQ0NjUyNjkyMF0sImFjYzQiOlt0cnVlXSwiYTZjYSI6W3RydWVdLCJjIjoiU1NfRGF0YV9XcmFwcGVyRXZlbnRzIn0=/v/eyI5NmVmIjpbdHJ1ZV0sIjk1ZjkiOlt0cnVlXSwicCI6MzAsImMiOiJTU19WaWV3X0V2ZW50VGFibGVQcmVtYXRjaCIsImRiM2UiOiJuZXh0TWF0Y2hlcyIsIjJkMT%s', 
                    'play_offs': 'http://www.sportstats.com/ajax-table-pagination/h/08f5ae9c2be1099141953297204586e1/tzc/MQ==/d/eyI0M2RkIjpbM10sImNlNWIiOls1Ml0sIjU1ZWIiOltbIjI1OTkxIl1dLCJmZWFhIjpbMTQ1NTY4ODUwMF0sImFjYzQiOlt0cnVlXSwiYTZjYSI6W3RydWVdLCJjIjoiU1NfRGF0YV9XcmFwcGVyRXZlbnRzIn0=/v/eyI5NmVmIjpbdHJ1ZV0sIjk1ZjkiOlt0cnVlXSwicCI6MzAsImMiOiJTU19WaWV3X0V2ZW50VGFibGVQcmVtYXRjaCIsImRiM2UiOiJuZXh0TWF0Y2hlcyIsIjJkMT%s'}

        ref_dict = {'play_offs': 'http://www.sportstats.com/ajax-results/t/0/46673/play-offs-46673-180/1/pos_68/3/52/25991/46673/0/30/0/1/', \
                    'regular': 'http://www.sportstats.com/ajax-results/t/0/46672/main-46672-116/1/pos_68/3/52/25991/46672/0/30/0/1/'}

        if self.spider_type == "scores":
            for key, value in ref_dict.iteritems():
                if "play_offs" in key:
                        event_name = key
                        top_urls[value] = event_name
                elif "regular" in key:
                        event_name = key
                        top_urls[value] = event_name
            for url, event_name in top_urls.iteritems():
                yield Request(url, callback=self.parse, \
                        meta = {'event_name' :  event_name})

        if self.spider_type == "schedules":
            for key, value in sch_dict.iteritems():
                if "regular" in key:
                    for url in sch_list:
                        urls = url
                        event_name = key
                        top_urls[value % (url)] = event_name
                if 'play_offs' in key:
                    for url in sch_list:
                        urls = url
                        event_name = key
                        top_urls[value % (url)] = event_name
            for url, event_name in top_urls.iteritems():
                yield Request(url, callback=self.parse, meta = {'event_name' : event_name})

    def parse(self, response):
        event_name = response.meta['event_name']
        hxs   = Selector(response)
        nodes_ = get_nodes(hxs, '//table//tbody//tr')
        for node in nodes_:
            ref_id = extract_list_data(node, './/td//@xteid')
            if ref_id:
                ref_id = ref_id[0]
            else:
                continue
            team_home = extract_data(node, "./td[contains(@class, 'table-home')]/a/strong/text()")
            if not team_home:
                team_home = extract_data(node, "./td[contains(@class, 'table-home')]/a/text()")
                if not team_home:
                    team_home = extract_list_data(node, ".//td[contains(@class, 'table-home')]/a/text()")[0]
            team_away = extract_data(node, "./td[contains(@class, 'table-away')]/a/strong/text()")
            if not team_away:
                team_away = extract_list_data(node, ".//td[contains(@class, 'table-away')]/a/text()")[0]
            ref_det = team_home.lower().replace(' ', '-') + '-' + \
            team_away.lower().replace(' ', '-') + '-' + \
            ref_id.replace('\\', '').replace('"', '').replace('/', '')
            ref ="http://www.sportstats.com/basketball/china/cba/" + ref_det
            yield Request(ref, callback= self.parse_next, \
                          meta = {'home_team': team_home, \
                          'away_team': team_away, \
                          'event_name': event_name})

    def parse_next(self, response):
        record = SportsSetupItem()
        hxs   = Selector(response)
        event_name = response.meta['event_name']
        event_det = extract_data(hxs, '//div[@class="bread"]//text()')
        if "Play Offs" in event_det:
            event_name = "play_offs"
        game_id = response.url.split('cba/')[-1].replace('/', '').strip()
        home_id = extract_data(hxs, '//div[@class="event-header-wrapper"]//div[@class="event-header-home"]//a//text()').lower()
        away_id =  extract_data(hxs, '//div[@class="event-header-wrapper"]//div[@class="event-header-away"]//a//text()').lower()
        game_details     = extract_list_data(hxs, '//div[@class="event-header-wrapper"]//div[@class="full"]/p//text()')
        if len(game_details) == 3:
            game_date        = game_details[2].split(',')
            game_datetime    = game_date[1].strip() +game_date[-1].replace('+', '')
        else:
            game_date        = game_details[1].split(',')
            game_datetime    = game_date[1].strip() +game_date[-1].replace('+', '')
        pattern = "%d %b %Y %H:%M"
        game_datetime  = get_utc_time (game_datetime, pattern, 'Asia/Shanghai')
        game_status = extract_data(hxs, '//div[@class="event-header-score"]//span//text()')

        if game_status == "-":
            game_status = "scheduled"
        else:
            game_status = "completed"

        if "play_offs" in event_name:
            record['event'] = "CBA Playoffs"
        else:
            record['event'] = "CBA Regular Season"

        record['source'] = 'sportstats_basketbal'
        record['source_key'] = game_id
        record['tournament'] = 'Chinese Basketball Association'
        record['reference_url'] = response.url
        record['game_datetime'] = game_datetime
        record['time_unknown'] = 0
        record['affiliation'] = 'cba'
        record['participants'] = {home_id: (1, ''), away_id: (0, '')}
        record['game'] = "basketball"
        record['participant_type'] = "team"
        record['game_status'] = game_status
        record['tz_info'] = get_tzinfo(country = "China")
        if self.spider_type == "schedules" and game_status == "scheduled":
            record['result'] = {}
            yield record

        if self.spider_type == "scores":
            team_total_score = extract_data(hxs, '//div[@class="event-header-wrapper"]//div[@class="event-header-score"]//span/text()')
            home_final_score = team_total_score.split(' - ')[0]
            away_final_score = team_total_score.split(' - ')[-1]
            game_scores      = game_details[0].replace('(', '').replace(')', '')
            home_q1, away_q1 = game_scores.split(',')[0].split('-')
            home_q2, away_q2 = game_scores.split(',')[1].split('-')
            home_q3, away_q3 = game_scores.split(',')[2].split('-')
            home_q4, away_q4 =  game_scores.split(',')[3].split('-')
            home_ot, away_ot = '', ''
            if len(game_scores.split(',')) >4:
                home_ot, away_ot = game_scores.split(',')[4].split('-')

            if (int(away_final_score) > int(home_final_score)):
                winner = away_id
            elif int(away_final_score) < int(home_final_score):
                winner = home_id
            else:
                winner = ''
            game_score = home_final_score + '-' + away_final_score
            if home_ot:
                game_score = game_score + '(OT)'

            record['tz_info'] = get_tzinfo(country = "China")
            record['result'] = {'0': {'winner': winner, 'score': game_score}, \
                                home_id: {'Q1': home_q1, 'Q2': home_q2.strip(), \
                                'Q3': home_q3.strip(), 'Q4': home_q4.strip(), 'OT1': home_ot, \
                                'final': home_final_score}, \
                                away_id: { 'Q1': away_q1, 'Q2': away_q2, \
                                'Q3': away_q3, 'Q4': away_q4, 'OT1': away_ot, \
                                'final': away_final_score}}
            yield record
