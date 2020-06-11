import time
import datetime
from datetime import timedelta
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, extract_data, extract_list_data
from vtvspider_new import get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import sys
import pytz

true = True
false = False
null = ''

accept_titles = ["women's", "men's", "mixed doubles"]
events_dict = {'Round 1': 'First Round', 'Round 2': 'Second Round', \
               'Round 3': 'Third Round', 'Round 4': 'Fourth Round', 'Quarterfinals': 'Quarterfinals',\
               'Semifinals': 'Semifinals', 'Final': 'Final'}
status_dict = {'': 'scheduled', 'In Progress': 'ongoing', 'To Finish': 'completed',\
               'Canceled': 'cancelled', 'Postponed': 'postponed', 'walkover': 'walkover',\
               'Suspended': 'cancelled', 'Complete': 'completed', 'Cancelled': 'cancelled'}

class FrenchOpen(VTVSpider):
    name    = "french_open_tennis"
    allowed_domains = ['rolandgarros.com']
    start_urls = []

    date_time = datetime.datetime.now()
    next_week_days = []
    for i in range(0, 2):
        next_week_days.append((date_time + datetime.timedelta(days=i)).strftime('%Y-%m-%d'))
    for i in range(1, 2):
        next_week_days.append((date_time - datetime.timedelta(days=i)).strftime('%Y-%m-%d'))

    xml_link   = 'http://www.rolandgarros.com/en_FR/xml/gen/scoreboard/draws/%s.xml'

    def start_requests(self):
        if self.spider_type == 'draws':
            url = 'http://www.rolandgarros.com/en_FR/draws/index.html'
        else:
            url = 'http://www.rolandgarros.com/en_FR/scores/schedule/'
        yield Request(url, callback = self.parse, meta = {})

    def parse(self,response):
        hxs = Selector(response)
        self.record = SportsSetupItem()
        self.record['game'] = "tennis"
        self.record['result'] = {}
        self.record['source'] = "frenchopen_tennis"
        self.record['tournament'] = "French Open"
        self.record['participant_type'] = "player"
        self.record['reference_url'] = response.url
        self.record['tz_info'] = get_tzinfo(city= 'Paris')
        self.record['rich_data'] =  {'channels': '',
                                     'location': {'city': 'Paris',
                                     'country': 'France',
                                     'continent': 'Europe',
                                     'state': 'Paris',
                                     'stadium': 'Stade Roland Garros'}}
        self.record['rich_data'] ['game_note']= ''

        main_xpath = '//div[@id="round_selector"]//select[@id="%sSelector"]/option[contains(@value, "%s")]'
        if self.spider_type == 'draws':
            qual_nodes = get_nodes(hxs, main_xpath % ('draw', 'draws'))
            for node_ in qual_nodes:
                link = extract_data(node_, './@value')
                skip_link = link.split('=')[-1]
                if skip_link not in ['MD', 'MS', 'WS', 'WD', 'XD']:
                    continue
                if 'http' not in link:
                    req_link = self.xml_link % (skip_link)
                print req_link
                yield Request(req_link, self.parse_draws, meta={'date': 'Friday, 29 May', 'event_id': skip_link})

        elif self.spider_type == 'qualifying':
            qual_nodes = get_nodes(hxs, main_xpath % ('sched', 'schedule'))
            for node_ in qual_nodes:
                link = extract_data(node_, './@value')
                date = extract_data(node_, './text()').split(':')[-1].strip()
                if link:
                    link = response.url + link
                    yield Request(link, self.parse_details, meta={'date': date}, dont_filter=True)

        elif self.spider_type == 'schedules':
            qual_nodes = get_nodes(hxs, main_xpath % ('sched', 'schedule'))
            for node_ in qual_nodes:
                link = extract_data(node_, './@value')
                date = extract_data(node_, './text()')
                if 'qual' in date.lower() or 'choose' in date.lower():
                    continue
                if link:
                    date = date.split(':')[-1].strip()
                    link = response.url + link
                    yield Request(link, self.parse_details, meta={'date': date}, dont_filter=True)

    def parse_draws(self, response):
        hxs = Selector(response)
        event = ''
        affiliation = ''
        event_dict = {'64': {'wta': "French Open Women's Singles First Round",
                             'atp': "French Open Women's Singles First Round"},
                      '32': {'wta': "French Open Women's Singles second Round",
                             'atp': "French Open men's Singles second Round",},
                      '16': {'wta': "French Open Women's Singles third Round",
                             'atp': "French Open men's Singles third Round"},
                      '8':  {'wta': "French Open Women's Singles fourth Round",
                             'atp': "French Open men's Singles fourth Round"},
                      '4': {'wta': "French Open Women's Singles Quarterfinals",
                            'atp': "French Open men's Singles Quarterfinals"},
                      '2': {'wta': "French Open Women's Singles Semifinals",
                            'atp': "French Open men's Singles Semifinals"},
                      '1': {'wta': "French Open Women's Singles Final",
                            'atp': "French Open men's Singles Final"}}

        mixed_doubles_events = {'16': "French Open Mixed Doubles First Round",
                                '8' : "French Open Mixed Doubles Second Round",
                                '4' : "French Open Mixed Doubles Quarterfinals",
                                '2' : "French Open Mixed Doubles Semifinals",
                                '1' : "French Open Mixed Doubles Final"}

        doubles_event_dict = {'32': {'wta': "French Open Women's Doubles First Round",
                                     'atp': "French Open Men's Doubles First Round"},
                              '16': {'wta': "French Open Women's Doubles Second Round",
                                     'atp': "French Open Men's Doubles Second Round"},
                              '8': {'wta': "French Open Women's Doubles Third Round",
                                    'atp': "French Open Men's Doubles Third Round"},
                              '4': {'wta': "French Open Women's Doubles Quarterfinals",
                                    'atp': "French Open Men's Doubles Quarterfinals"},
                              '2': {'wta': "French Open Women's Doubles Semifinals",
                                    'atp': "French Open Men's Doubles Semifinals"},
                              '1': {'wta': "French Open Women's Doubles Final",
                                    'atp': "French Open Men's Doubles Final"}}


        if response.meta['event_id'] == 'WD':
            event = "French Open Women's Doubles First Round"
            affiliation = 'wta'
        elif response.meta['event_id'] == 'WS':
            affiliation = 'wta'
        elif response.meta['event_id'] == 'MS':
            affiliation = 'atp'
        elif response.meta['event_id'] == 'MD':
            affiliation = 'atp'
            event = "French Open Men's Doubles First Round"
        elif response.meta['event_id'] == 'XD':
            affiliation = 'atp_wta'
            event = "French Open Mixed Doubles First Round"

        nodes = get_nodes(hxs, '//draw/match')
        for ind, node in enumerate(nodes):
            if response.meta['event_id'] == 'WS' and ind < 64:
                continue
            if response.meta['event_id'] == 'MS' and ind < 64:
                continue
            if response.meta['event_id'] in ['WS', 'MS']:
                if ind <= 95:
                    event = event_dict['32'][affiliation]
                elif ind <= 111:
                    event = event_dict['16'][affiliation]
                elif ind <= 119:
                    event = event_dict['8'][affiliation]
                elif ind <= 123:
                    event = event_dict['4'][affiliation]
                elif ind <= 125:
                    event = event_dict['2'][affiliation]
                else:
                    event = event_dict['1'][affiliation]

            if response.meta['event_id'] == 'XD':
                if ind <= 15:
                    event = mixed_doubles_events['16']
                elif ind <= 23:
                    event = mixed_doubles_events['8']
                elif ind <= 27:
                    event = mixed_doubles_events['4']
                elif ind <= 29:
                    event = mixed_doubles_events['2']
                else:
                    event = mixed_doubles_events['1']
            elif response.meta['event_id'] in ['MD', 'WD']:
                if ind <= 31:
                    event = doubles_event_dict['32'][affiliation]
                elif ind <= 47:
                    event = doubles_event_dict['16'][affiliation]
                elif ind <= 55:
                    event = doubles_event_dict['8'][affiliation]
                elif ind <= 59:
                    event = doubles_event_dict['4'][affiliation]
                elif ind <= 61:
                    event = doubles_event_dict['2'][affiliation]
                else:
                    event = doubles_event_dict['1'][affiliation]
            game_id = extract_data(node, './@id')
            pl_one  = extract_data(node, './@P1A').replace('wta', '').replace('atp', '')
            pl_two  = extract_data(node, './@P1B').replace('wta', '').replace('atp', '')
            pl_three = extract_data(node, './@P2A').replace('wta', '').replace('atp', '')
            pl_four = extract_data(node, './@P2B').replace('wta', '').replace('atp', '')
            game_datetime = self.get_game_datetime(response.meta['date'], '11:00 AM')
            if pl_one == '' and pl_three == '' and pl_two == '' and pl_four == '':
                continue
            if pl_one == '' and pl_three == '':
                continue
            if pl_two and pl_four:
                players = self.get_players([pl_one, pl_two, pl_three, pl_four])
                sk = self.get_source_key([pl_one, pl_two, pl_three, pl_four], event)
            else:
                if pl_one == '':
                    pl_one = 'qualifier'
                elif pl_three == '':
                    pl_one = 'qualifier'
                players = self.get_players([pl_one, pl_three])
                sk = self.get_source_key([pl_one, pl_three], event)
            status  = extract_data(node, './mStatus')
            status  = status_dict.get(status, '')
            if not status:
                continue

            self.record['game_status']   = status
            self.record['event']         = event
            self.record['affiliation']   = affiliation
            self.record['game_datetime'] = game_datetime
            self.record['time_unknown']  = '1'
            self.record['participants']  = players
            self.record['source_key']    = sk
            yield self.record

    def get_source_key(self, players, event):
        event = event.replace("'", '')
        source_key = players
        source_key.sort()
        source_key.append(event.replace(' ', '_').lower())
        sk = '_'.join(source_key)

        return sk

    def get_game_datetime(self, game_dt, game_time):
        if game_time == '':
            game_datetime = '2015 ' + game_dt
            pattern = "%Y %A, %d %B"
        else:
            game_datetime = "2015 " + game_dt + " " + game_time.lower()
            pattern = "%Y %A, %d %B %I:%M %p"
        tz_info = 'CET'
        game_datetime = get_utc_time(game_datetime, pattern, tz_info)

        return game_datetime

    def get_players(self, players):
        counter = 0
        group_count = 0
        participants = {}
        if len(players) == 4:
            group_count = 1
        for player in players:
            if player == '':
                player = 'qualifier'
            if len(players) == 4:
                counter += 1
            if counter > 2:
                group_count = 2
            participants[player] = (group_count, '')

        return participants

    def parse_details(self,response):
        hxs = Selector(response)
        scores = {}
        court_nodes = get_nodes(hxs, '//div[@class="court"]')
        self.record['reference_url'] = response.url
        for court in court_nodes:
            court_name = extract_data(court, './/div[@class="courtName"]/div/text()')
            if 'chatrier' in court_name.lower():
                stadium = 'Court Philippe Chatrier'
            elif 'suzanne' in court_name.lower():
                stadium = 'Court Suzanne Lenglen'
            else:
                stadium = "Stade Roland Garros"
            game_time = extract_data(court, './/div[@class="courtName"]/div/span[@class="startDate en"]/text()')
            nodes = get_nodes(court, './/div[@class="match"]')
            for node in nodes:
                game_datetime = self.get_game_datetime(response.meta['date'], game_time)
                date_sk = game_datetime.split(' ')[0]

                self.record['game_datetime'] = game_datetime
                self.record['result'] = {}

                teams = get_nodes(node, './div//div[contains(@class, "teams")]')
                for team in teams:
                    winner_status = extract_data(team, './/span[@class="active"]/text()')
                    if "def." in winner_status:
                        winner = extract_list_data(teams, './/span[@class="playerName active"]/a/@href')
                        winner2 = extract_list_data(team, './div[@class="versus"]//preceding-sibling::span/a/@href')
                        winner = [win.split('/')[-1].replace('.html', '').replace('atp', '').replace('wta', '').strip() for win in winner]
                        self.record['result'].setdefault('0', {})['winner'] = winner
                        if winner2:
                            winner2 = winner2[0].split('/')[-1].replace('.html', '').replace('atp', '').replace('wta', '')
                            self.record['result'].setdefault('0', {})['winner'].append(winner2)

                players = extract_list_data(teams, './/a[contains(@href, "/en_FR/players/overview/")]/@href')
                players = [pl.split('/')[-1].replace('.html', '').replace('atp', '').replace('wta', '') for pl in players]
                self.record['participants'] = self.get_players(players)

                event = extract_data(node, './/div[@class="matchHeader"]/span[1]/text()')
                if 'wheelchair' in event.lower():
                    continue
                if not event:
                    continue
                if "Girls' " in event or "Boys' " in event or "Men's Legends" in event or "Women's Legends" in event or "WC Doubles" in event or "WC Singles" in event:
                    continue
                if '\n' in event:
                    event = event.split('\n')[-1].strip()
                game_note = ''
                if 'qualifications' in event.lower():
                    game_note = event.split(' - ')[1].strip()
                    event = "French Open" + " " + event.split(' - ')[0].strip()
                else:
                    event = "French Open" + " " + event.split(' - ')[0] + " " + event.split(' - ')[1]
                for key, value in events_dict.iteritems():
                    event = event.replace(key, value)
                if "Women's Singles" in event or "Women's Doubles" in event: self.record['affiliation'] = "wta"
                elif "Men's Singles" in event or "Men's Doubles" in event: self.record['affiliation'] = "atp"
                elif "Mixed" in event: self.record['affiliation'] = "atp_wta"

                status_text  = extract_data(node, './/div[@class="matchHeader"]/span[2]/text()').strip()
                status = status_text.split('\n\n')

                if 'To Finish' in status_text:
                    game_status = 'To Finish'
                elif 'Walkover\n\nComplete' in status_text:
                    game_status = "walkover"
                elif 'to finish' in extract_data(node, './/div[@class="matchScore"]/text()').lower():
                    game_status = 'To Finish'
                else:
                    game_status = status[-1]

                self.record['game_status'] = status_dict[game_status]

                if self.record['game_status'] == "ongoing":
                    ongoing_link = "http://www.rolandgarros.com/en_FR/xml/gen/sumScores/sumScores_jsonp.json"
                    yield Request(ongoing_link, callback = self.parse_ongoing, meta ={'record': self.record, 'players': players, 'date_sk': date_sk})#, ROBOTSTXT_OBEY=0)

                if self.record['game_status'] == 'completed' and players:
                    scores = extract_data(node, './/div[@class="matchScore"]/text()').replace('To Finish', '').strip().replace('[','').replace(']', '').split(' ')
                    self.generate_scores(scores, players)

                self.record['source_key'] = self.get_source_key(players, event)
                if self.record['source_key'] == "d643_mc10_french_open_mens_singles_semifinal" and "schedule18.html" in response.url:
                    continue
                print self.record['source_key']

                self.record['reference_url'] = response.url
                self.record['event'] = event
                self.record['rich_data'] ['game_note'] = game_note
                if date_sk in self.next_week_days:
                    yield self.record

    def parse_ongoing(self,response):
        raw_data = response.body
        #hxs = Selector(response)
        _players = response.meta['players']
        record = response.meta['record']
        date_sk = response.meta['date_sk']
        record['result'] = {}
        raw_data = eval(raw_data.strip().replace('ssb_callback', '').replace(';', ''))

        nodes = raw_data.keys()
        for node in nodes:
            data = raw_data[node].get('match', [])
            for _data in data:
                teams = _data.get('team', [])
                players = []
                for team in teams:
                    player_sk = team.get('playerIdA', '').replace('atp', '').replace('wta', '')
                    set1 = team.get('set1', '')
                    set2 = team.get('set2', '')
                    set3 = team.get('set3', '')
                    set4 = team.get('set4', '')
                    set5 = team.get('set5', '')
                    tb1  = team.get('tb1', '')
                    tb2  = team.get('tb2', '')
                    tb3  = team.get('tb3', '')
                    tb4  = team.get('tb4', '')
                    tb5  = team.get('tb5', '')
                    if not record['result'].has_key(player_sk):  record['result'][player_sk] = {}
                    record['result'][player_sk]['S1'] = set1
                    record['result'][player_sk]['S2'] = set2
                    record['result'][player_sk]['S3'] = set3
                    record['result'][player_sk]['S4'] = set4
                    record['result'][player_sk]['S5'] = set5
                    record['result'][player_sk]['T1'] = tb1
                    record['result'][player_sk]['T2'] = tb2
                    record['result'][player_sk]['T3'] = tb3
                    record['result'][player_sk]['T4'] = tb4
                    record['result'][player_sk]['T5'] = tb5
                players.append(player_sk)
                players.sort()
                players.append(date_sk.replace('-', '_'))
                self.record['source_key'] = '_'.join(players)
                print  record['result']
                yield record

    def generate_scores(self, scores, players):
        if players > 4:
            players = players[:4]
        counter = home_final = away_final = 0
        for score in scores:
            counter += 1
            if '-' in score:
                score = score.split('-')
                home_score = score[0]
                away_score = score[1]
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
                    self.record['result'].setdefault(players[0], {})['S%s' %counter] = home_score
                    self.record['result'].setdefault(players[1], {})['S%s' %counter] = away_score
                    if extras:
                         self.record['result'][players[0]]['T%s' %counter] = extras1
                         self.record['result'][players[1]]['T%s' %counter] = extras2
                elif len(players) == 4:
                    self.record['result'].setdefault(players[0], {})['S%s' %counter] = home_score
                    self.record['result'].setdefault(players[1], {})['S%s' %counter] = home_score
                    self.record['result'].setdefault(players[2], {})['S%s' %counter] = away_score
                    self.record['result'].setdefault(players[3], {})['S%s' %counter] = away_score
                    if extras:
                        self.record['result'][players[0]]['T%s' %counter] = extras1
                        self.record['result'][players[1]]['T%s' %counter] = extras1
                        self.record['result'][players[2]]['T%s' %counter] = extras2
                        self.record['result'][players[3]]['T%s' %counter] = extras2
        if len(players) == 2:
             self.record['result'][players[0]]['final'] = home_final
             self.record['result'][players[1]]['final'] = away_final
        if len(players) == 4:
            self.record['result'][players[0]]['final'] = home_final
            self.record['result'][players[1]]['final'] = home_final
            self.record['result'][players[2]]['final'] = away_final
            self.record['result'][players[3]]['final'] = away_final

        score = ' - '.join([str(home_final), str(away_final)])
        self.record['result'].setdefault('0', {})['score'] = score
