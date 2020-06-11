import datetime
import time
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, \
extract_data, get_nodes, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


true = True
false = False
null = ''

record  = SportsSetupItem()


class WorldTourTennis(VTVSpider):
    name ="worldtour_tennis"
    allowed_domains = ['atpworldtour.com']
    start_urls = []

    def start_requests(self):
        next_week_days = []
        print next_week_days
        date_time = datetime.datetime.now()
        if self.spider_type == "scores":
            top_url = 'http://m.atpworldtour.com/Scores/Scores-and-Stats.aspx?Completed=1&EventId=0605&Date=%s'
            for i in range(0, 9):
                next_week_days.append((date_time - datetime.timedelta(days=i)).strftime('%d.%m.%Y'))
                for wday in next_week_days:
                    link = top_url % wday
                yield Request(link, callback=self.parse_scores, meta={})
        else:
            top_url = "http://m.atpworldtour.com/Mobile/Scores/Schedule.aspx?EventId=605&Year=2014&Seq=8"
            yield Request(top_url, callback=self.parse_schedules, meta={})

    def parse_scores(self, response):
        hxs = Selector(response)
        game_date = response.url.split('&Date')[-1].replace('=', '')
        record['rich_data'] = {}
        rounds = extract_data(hxs, '//td[@class="completedMatchTitle"]//text()')
        nodes = get_nodes(hxs, '//table[@class="completedMatchesModule"]//tr//td[@class="completedMatchRecord rowAlt"]')
        for node in nodes:
            players = extract_list_data(node, './/a[contains(@href, "Tennis/Players")]//@href')
            scores = extract_data(node, './/span[@class="scores"]//text()').split(' ')
            game_note = extract_data(node, '..//preceding-sibling::tr/td[@class="completedMatchTitle"]//text()').strip()
            players = [pl.split('/')[-1].replace('.aspx', '').lower() for pl in players]
            game_datetime = get_utc_time(game_date, '%d.%m.%Y', 'GMT')
            date_sk = game_datetime.split(' ')[0]
            record['participants'] = {}
            record['result'] = {}
            self.generate_scores(scores, players)
            counter = 0
            group_count = 0
            if len(players) == 4:
                group_count = 1
            for player in players:
                if len(players) == 4:
                    counter += 1
                if counter > 2:
                    group_count = 2
                record['participants'][player] = (group_count, '')
            source_key = players
            source_key.sort()
            source_key.append(date_sk.replace('-', '_'))
            record['source_key'] = '_'.join(source_key)
            record['game'] = "tennis"
            record['reference_url'] = response.url
            record['source'] = "worldtour_tennis"
            record['tournament'] = "ATP World Tour Finals"
            record['participant_type'] = "player"
            record['event'] = ''
            record['rich_data'] =  {'channels': '',
                                'location': {'city': '',
                                'country': 'England',
                                'state': 'London',
                                'stadium': ''}}
            record['rich_data'] ['game_note'] = game_note
            record['game_status'] = "completed"
            record['game_datetime'] = game_datetime
            record['time_unknown'] = 1
            record['tz_info'] = get_tzinfo(city = "London")
            record['affiliation'] = "atp"
            import pdb;pdb.set_trace()
            yield record
    def parse_schedules(self, response):
        hxs = Selector(response)
        game_date = extract_data(hxs, '//td[@class="completedCurr"]//text()')
        nodes = get_nodes(hxs, '//table[@class="scheduleCourts"]//tr[td[a[contains(@href, "Tennis/Players")]]]')
        for node in nodes:
            players = extract_list_data(node, './td/a[contains(@href, "Tennis/Players")]//text()')
            game_time = extract_data(node, '..//preceding-sibling::tr/td[@class="scheduleCourtTitle"]/strong/text()').replace('noon', 'pm')
            game_datetime = game_date + ' ' + game_time
            players = [pl.replace(' ', '-').lower() for pl in players]
            game_datetime = get_utc_time(game_datetime, '%d.%m.%Y %H:%M pm', 'GMT')
            date_sk = game_datetime.split(' ')[0]
            record['participants'] = {}
            record['result'] = {}
            counter = 0
            group_count = 0
            if len(players) == 4:
                group_count = 1
            for player in players:
                if len(players) == 4:
                    counter += 1
                if counter > 2:
                    group_count = 2
                record['participants'][player] = (group_count, '')
            source_key = players
            source_key.sort()
            source_key.append(date_sk.replace('-', '_'))
            record['source_key'] = '_'.join(source_key)
            record['game'] = "tennis"
            record['reference_url'] = response.url
            record['source'] = "worldtour_tennis"
            record['tournament'] = "ATP World Tour Finals"
            record['participant_type'] = "player"
            record['event'] = ''
            record['source'] = "worldtour_tennis"
            record['rich_data'] =  {'channels': '',
                                'location': {'city': '',
                                'country': 'England',
                                'state': 'London',
                                'stadium': ''}}
            record['rich_data'] ['game_note'] = 'Semifinals'
            record['game_status'] = "scheduled"
            record['game_datetime'] = game_datetime
            record['affiliation'] = "atp"
            record['time_unknown'] =0
            record['tz_info'] = get_tzinfo(city = "London")

            yield record

    def generate_scores(self, scores, players)  :
        counter = home_final = away_final = 0
        print scores
        for score in scores:
            counter += 1
            home_score = score[0]

            away_score = score[1:]
            if '-' in score:
                home_score = score[:2]
                away_score = score[3]
            extras = ''
            if '(' in away_score:
                extras = away_score.split('(')[-1].strip(')')
                away_score = away_score.split('(')[0]
                if home_score == "7":
                    extras1 = "7"
                    extras2 = extras
                else:
                    extras2 = "7"
                    extras1 = extras

            if home_score > away_score:
                home_final += 1
            elif home_score < away_score:
                away_final += 1

            if len(players) == 2:
                record['result'].setdefault(players[0], {})['S%s' %counter] = home_score
                record['result'].setdefault(players[1], {})['S%s' %counter] = away_score
                if extras:
                     record['result'][players[0]]['T%s' %counter] = extras1
                     record['result'][players[1]]['T%s' %counter] = extras2
            elif len(players) == 4:
                record['result'].setdefault(players[0], {})['S%s' %counter] = home_score
                record['result'].setdefault(players[1], {})['S%s' %counter] = home_score
                record['result'].setdefault(players[2], {})['S%s' %counter] = away_score
                record['result'].setdefault(players[3], {})['S%s' %counter] = away_score
                if extras:
                    record['result'][players[0]]['T%s' %counter] = extras1
                    record['result'][players[1]]['T%s' %counter] = extras1
                    record['result'][players[2]]['T%s' %counter] = extras2
                    record['result'][players[3]]['T%s' %counter] = extras2
        if len(players) == 2:
            record['result'][players[0]]['final'] = home_final
            record['result'][players[1]]['final'] = away_final
            if str(home_final) > str(away_final):
                winner = players[0]
            elif str(home_final) < str(away_final):
                winner = players[1]
            else:
                winner = ''
            record['result'].setdefault('0', {})['winner'] = [winner]
        if len(players) == 4:
            record['result'][players[0]]['final'] = home_final
            record['result'][players[1]]['final'] = home_final
            record['result'][players[2]]['final'] = away_final
            record['result'][players[3]]['final'] = away_final
            if str(home_final) > str(away_final):
                winner = players[0]
                winner2 = players[1]
            elif str(home_final) < str(away_final):
                winner = players[2]
                winner2 = players[3]
            record['result'].setdefault('0', {})['winner'] = [winner]
            record['result'].setdefault('0', {})['winner'].append(winner2)
        score = ' - '.join([str(home_final), str(away_final)])
        record['result'].setdefault('0', {})['score'] = score
