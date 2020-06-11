import time
import datetime
import re
import urllib2
from vtvspider_dev import VTVSpider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import extract_data, extract_list_data, get_nodes, log, get_utc_time


class FieldHockey(VTVSpider):
    name = 'fieldhockey'
    start_urls = ['http://www.rabobankhockeyworldcup2014.com/schedule']

    def replace_values(self, data):
        data = data.replace('null', '""')
        return data

    def parse(self, response):
        hxs = Selector(response)
        now = datetime.datetime.now()
        import pdb;pdb.set_trace()
        nodes = get_nodes(hxs, '//div[@id="playing-schedule"]//div[@class="schedule"]//tr[@class="table__row table__row--has-link"]')
        for node in nodes:
            record = SportsSetupItem()
            gender = extract_data(node, './@data-gender')
            game_note = extract_data(node, './@data-group')
            if "Group" in game_note:
                game_note = 'First Round'
            team_a = extract_data(node, './td[3]//div[@class="country"]//p[contains(@class,"country__name")]/text()')
            team_b = extract_data(node, './td[5]//div[@class="country"]//p[contains(@class,"country__name")]/text()')
            if gender.lower() == 'women':
                team_a_sk = team_a.replace(' ', '_') + '_women'
                team_b_sk = team_b.replace(' ', '_') + '_women'
            else:
                team_a_sk = team_a.replace(' ', '_')
                team_b_sk = team_b.replace(' ', '_')
            event = ''
            if "Semifinal" in game_note and "women" in gender.lower():
                event = "Hockey Women's World Cup Semifinal"
                tournament = "Hockey Women's World Cup"
            elif "Final" in game_note and "women" in gender.lower():
                 event = "Hockey Women's world cup Final"
                 tournament = "Hockey Women's World Cup"
            elif "women" in gender.lower():
                 tournament = "Hockey Women's World Cup"

            elif "Semifinal" in game_note and "men" in gender.lower():
                event = "Hockey World Cup Semifinal"
                tournament = "Hockey World Cup"
            elif "Final" in game_note and "men" in gender.lower():
                event = "Hockey World Cup Final"
                tournament = "Hockey World Cup"
            elif  "men" in gender.lower():
                tournament = "Hockey World Cup"

            participants = {team_a_sk : (1, team_a), team_b_sk : (0, team_b)}
            stadium = extract_data(node, './td[6]/text()')

            score_link = extract_data(node, './@data-href')
            source_key = "".join(re.findall(r'match/(\d+)/live', score_link))

            record['source_key'] = source_key
            record['game'] = 'field hockey'
            record['source'] = 'field_hockey'
            record['participant_type'] = 'team'
            record['rich_data'] = {}
            record['rich_data']['game_note'] = game_note
            record['rich_data']['stadium'] = stadium
            record['participants'] = participants
            record['participant_type'] = 'team'
            record['event'] = event
            record['tournament'] = tournament
            record['affiliation'] = 'fih'
            record['reference_url'] = score_link

            game_date = extract_data(node, './@data-date')
            game_time = extract_data(node, './td[4]/time/text()').replace('CET', '').strip()
            if not '-' in game_time and self.spider_type== 'schedules':
                game_datetime = game_date + ' ' + game_time
                gmt_game_datetime = get_utc_time(game_datetime, '%Y-%m-%d %H:%M', 'CET')
                status = 'scheduled'
                record['game_datetime'] = gmt_game_datetime
                record['game_status'] = status
                #import pdb; pdb.set_trace()
                yield record

            elif '-' in game_time and score_link and self.spider_type == 'scores':
                gmt_game_datetime = get_utc_time(game_date, '%Y-%m-%d', 'CET')
                scores = game_time
                final_scores = ''
                if "(" not in scores:
                    scores = game_time.split('-')
                    results = {'0'       : {'score' : game_time},\
                           team_a_sk : {'final' : scores[0], 'H1' : '0'},
                           team_b_sk : {'final' : scores[1], 'H1' : '0'}}
                if "(" in scores:
                    scores = scores.split('(')
                    final_scores = scores[0].split('-')
                    shoot_out = scores[1].split(' ')
                    shoot_out_scores = shoot_out[0].split('-')
                    results = {'0'       : {'score' : scores[0].strip()},\
                              team_a_sk : {'final' : final_scores[0], 'H1' : '0', 'SO' : shoot_out_scores[0]},
                              team_b_sk : {'final' : final_scores[1], 'H1' : '0', 'SO' : shoot_out_scores[1]}
                              }

                status_url = urllib2.urlopen(score_link)
                game_status = status_url.geturl()
                if "live" in game_status:
                    status = 'ongoing'
                elif "archive" in game_status and not final_scores:
                    status = 'completed'
                    if int(scores[0]) > int(scores[1]):
                        results['0']['winner'] = team_a_sk
                    elif int(scores[0]) < int(scores[1]):
                        results['0']['winner'] = team_b_sk
                elif "archive" in game_status and final_scores:
                    status = 'completed'
                    if int(shoot_out_scores[0]) > int(shoot_out_scores[1]):
                         results['0']['winner'] = team_a_sk
                    elif int(shoot_out_scores[0]) < int(shoot_out_scores[1]):
                        results['0']['winner'] = team_b_sk
                record['game_datetime'] = gmt_game_datetime
                record['game_status'] = status
                record['result'] = results
                score_link = "http://wkhockey-apiproxy-2093249819.eu-west-1.elb.amazonaws.com/api/matchactionlist_extended/sportid/108/match/%s/language/2.json" %source_key
                yield Request(score_link, callback=self.parse_match_details, meta={'record' : record})


    def parse_match_details(self, response):
        import pdb;pdb.set_trace()
        hxs = Selector(response)
        result = response.meta['record']['result']
        body = self.replace_values(response.body)
        details = eval(body)

        for detail in details:
            period = detail.get('Period', '')
            home_away = str(detail.get('HomeOrAway', ''))
            score1 = detail.get('Score1', '')
            score2 = detail.get('Score2', '')
            team = detail.get('Team', '').replace(' ', '_')

            for key in result.keys():
                if "women" in key:
                    team = team +  "_women"
                    break

            if period.lower() == 'first half':
                if home_away == '1' and score1:
                    result[team]['H1'] = str(score1)
                elif home_away == '-1' and score2:
                    result[team]['H1'] = str(score2)

        response.meta['record']['result'] = result
        yield response.meta['record']
