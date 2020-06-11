from vtvspider import VTVSpider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import extract_data, \
get_nodes, get_utc_time, get_tzinfo


class HockeyChampTrophy(VTVSpider):
    name = 'hockey_champtrophy'
    start_urls = ['http://events.fih.ch/new/competition/347/matches']


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="row "]//table[@class="match_schedule"]//tr//td//a')
        for node in nodes:
            game_id = extract_data(node, './/@href')
            game_url = "http://events.fih.ch" + game_id
            print game_url
            yield Request(game_url, callback=self.parse_details, meta = {'game_id': game_id})

    def parse_details(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        game_date = extract_data(hxs, '//table[@class="match_details"]/tr/td[contains(text(), "-")]/text()'). \
                replace('5th - 8th', '').strip()
        game_time = extract_data(hxs, '//table[@class="match_details"]/tr/td[contains(text(), ":")]/text()')
        game_datetime = game_date + " " + game_time
        game_datetime = get_utc_time(game_datetime, "%Y-%m-%d %H:%M", 'Asia/Kolkata')
        game_note = extract_data(hxs, '//table[@class="match_details"]/tr/td[4]/text()')
        nodes = get_nodes(hxs, '//table[@class="match_scoreline"]//tr')
        for node in nodes:
            home_so = ''
            away_so = ''
            home_sk = extract_data(node, './/th[1]/a/text()').strip().lower()
            away_sk = extract_data(node, './/th[3]/a/text()').strip().lower()
            scores_ = extract_data(node, './/th[2]//text()').strip()
            if not home_sk:
                home_sk = "tbd1"
            if not away_sk:
                away_sk = "tbd2"
            if "Upcoming" in scores_ or "Warmup" in scores_:
                home_score = ''
                away_scores = ''
            else:
                scores =  scores_.split('- ')
                home_score = scores[0].strip()
                away_score = scores[1]
                away_scores = away_score.replace('Official', '').replace('Upcoming', ''). \
                            replace('2nd Period', '').replace('1st Period', '').replace('Final', '').\
                             replace('4th Period', '').replace('3rd Period', '')
                if "(" in away_scores:
                    home_score = scores_.split('(')[0].split('-')[0].strip()
                    away_scores = scores_.split('(')[0].split('-')[1].strip()
                    home_so = scores_.split('(')[1].split('-')[0].strip()
                    away_sos = scores_.split('(')[1].split('-')[1].strip().replace('SO)', '')
                    away_so  = away_sos.replace('Official', '')
            participants = {home_sk: (1, home_sk), away_sk: (0, away_sk)}
        record['source_key'] = response.meta['game_id']
        record['game'] = 'field hockey'
        record['source'] = 'field_hockey'
        record['participant_type'] = 'team'
        record['rich_data'] = {}
        record['participants'] = participants
        record['participant_type'] = 'team'
        record['event'] = ''
        record['tournament'] = "Hockey Champions Trophy"
        record['affiliation'] = 'fih'
        record['reference_url'] = response.url
        record['game_datetime'] = game_datetime
        record['time_unknown'] = 0
        record['tz_info'] = get_tzinfo(city = "Bhubaneswar")
        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(country = "India")
        record['rich_data'] = {'location': {'city': "Bhubaneswar", 'state': "Odisha", \
                                    'country': "India", \
                                    'stadium': 'Kalinga Stadium'}, 'game_note': game_note}
        winner = ''
        if "Upcoming" in scores_ or "Warmup" in scores_:
            record['result'] = {}
            record['game_status']  = "scheduled"
            yield record
        elif "Period" in away_score:
            record['game_status']  = "ongoing"
            results = {'0' : {'score' : home_score + "-" + away_scores, 'winner':''}, \
                     home_sk : {'final' : home_score},
                    away_sk : {'final' : away_scores}}
            record['result'] = results
            yield record
        elif "Official" in away_score or "Final" in away_score:
            if home_score > away_scores:
                winner = home_sk
            elif home_score < away_scores:
                winner = away_sk
            else:
                winner = ''
            record['game_status']  = "completed"
        elif "Official" in away_sos:
            record['game_status']  = "completed"
            if home_so > away_so:
                winner = home_sk
            elif home_so < away_so:
                winner = away_sk
            else:
                winner = ''
        results = {'0' : {'score' : home_score + "-" + away_scores, 'winner': winner }, \
                            home_sk : {'final' : home_score, 'SO': home_so},
                            away_sk : {'final' : away_scores, 'SO': away_so}
                              }
        record['result'] = results
        yield record
