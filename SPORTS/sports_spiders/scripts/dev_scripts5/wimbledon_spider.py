import datetime
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re

true = True
false = False
null = ''

EVENTS_DICT = {"Ladies'": "Women's", "Gentlemen'S": "Men's", "Quarter-finals": "Quarterfinals"}

STATUS_DICT = {'': 'scheduled', 'In Progress': 'ongoing', 'To Finish': 'completed',
               'Cancelled': 'cancelled', 'Postponed': 'postponed', 'walkover': 'walkover', \
               'Complete': 'completed'}
AFF_DICT    = {"Women's Singles": "wta", "Women's Doubles": "wta", \
               "Men's Singles": "atp", "Men's Doubles": "atp", "Mixed": "atp_wta"}

EVE_SKIP_LIST = ["Girls' ", "Boys' ", "Men's Legends", "Women's Legends", "WC Doubles", "WC Singles", \
                 "Invitation", "Inv.", "Snr.", "Wheelchair"]

EVENTS_TBD = {"Wimbledon Men's Singles First Round": \
              ['/ms/r1s1.html', '/ms/r1s2.html', '/ms/r1s3.html', '/ms/r1s4.html'], \
              "Wimbledon Men's Singles Second Round": ['/ms/r2s1.html', '/ms/r2s2.html'], \
              "Wimbledon Men's Singles Third Round": ['/ms/r3s1.html', '/ms/r3s1.html'], \
              "Wimbledon Men's Singles Fourth Round" : ['/ms/r4s1.html'], \
              "Wimbledon Men's Singles Quarterfinals": ['/ms/r5s1.html'], \
              "Wimbledon Men's Singles Semifinals": ['/ms/r6s1.html'], \
              "Wimbledon Men's Singles Final": ['/ms/r7s1.html'], \
              "Wimbledon Women's Singles First Round": \
              ['/ws/r1s1.html', '/ws/r1s2.html', '/ws/r1s3.html', '/ws/r1s4.html'], \
              "Wimbledon Women's Singles Second Round": ['/ws/r2s1.html', '/ws/r2s2.html'], \
              "Wimbledon Women's Singles Third Round": ['/ws/r3s1.html', '/ws/r3s1.html'], \
              "Wimbledon Women's Singles Fourth Round": ['/ws/r4s1.html'], \
              "Wimbledon Women's Singles Quarterfinals": ['/ws/r5s1.html'], \
              "Wimbledon Women's Singles Semifinals": ['/ws/r6s1.html'], \
              "Wimbledon Women's Singles Final": ['/ws/r7s1.html'],\
              "Wimbledon Women's Doubles First Round": ['/wd/r1s1.html', '/wd/r1s2.html'], \
              "Wimbledon Women's Doubles Second Round": ['/wd/r2s1.html'], \
              "Wimbledon Women's Doubles Third Round": ['/wd/r3s1.html'], \
              "Wimbledon Women's Doubles Quarterfinals": ['/wd/r4s1.html'], \
              "Wimbledon Women's Doubles Semifinals": ['/wd/r5s1.html'], \
              "Wimbledon Women's Doubles Final": ['/wd/r6s1.html'], \
              "Wimbledon Men's Doubles First Round": ['/md/r1s2.html', '/md/r1s1.html'], \
              "Wimbledon Men's Doubles Second Round": ['/md/r2s1.html'], \
              "Wimbledon Men's Doubles Third Round": ['/md/r3s1.html'], \
              "Wimbledon Men's Doubles Quarterfinals": ['/md/r4s1.html'], \
              "Wimbledon Men's Doubles Semifinals": ['/md/r5s1.html'], \
              "Wimbledon Men's Doubles Final": ['/md/r6s1.html'], \
              "Wimbledon Mixed Doubles First Round": ['/xd/r1s2.html', '/xd/r1s1.html'], \
              "Wimbledon Mixed Doubles Second Round": ['/xd/r2s1.html'], \
              "Wimbledon Mixed Doubles Third Round": ['/xd/r3s1.html'], \
              "Wimbledon Mixed Doubles Quarterfinals": ['/xd/r4s1.html'], \
              "Wimbledon Mixed Doubles Semifinals": ['/xd/r5s1.html'], \
              "Wimbledon Mixed Doubles Final": ['/xd/r6s1.html']}

def get_player_sk(reference):
    replace_string = ['.html', 'atp', 'wta', 'unknown']
    reference = reference.split('/')[-1]
    for data in replace_string:
        if data == "unknown":
           reference = reference.replace(data, 'tbd1')
        else:
            reference = reference.replace(data, '')
    return reference

class Wimbledon(VTVSpider):

    name    = "wimbledon_tennis"
    allowed_domains = ['wimbledon.com']
    draws_dict = {}
    start_urls = []

    def start_requests(self):
        reference = "http://www.wimbledon.com/en_GB/scores/schedule/"
        yield Request(reference, callback = self.parse, meta = {})

    def draws(self, response):
        hxs = Selector(response)
        nodes = extract_list_data(hxs, '//div[@id="cmatch"]/ul/li/a[not(contains(text(), "Qualifying")) and not(contains(text(), "Boys")) \
                                                     and not(contains(text(), "Girls")) and not(contains(text(), "Wheelchair")) and not(contains(@href, ".pdf"))]/@href')
        last_node = nodes[-1]
        terminal_crawl = False
        for node in nodes:
            domain = "http://www.wimbledon.com"
            url = domain + node
            if node == last_node:
                terminal_crawl = True
            yield Request(url, callback = self.rounds_info, meta = {'terminal_crawl': terminal_crawl})

    def rounds_info(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="navigator"]')[0]
        nodes = get_nodes(nodes, './/div[contains(@class, "column")]/a')
        last_node = nodes[-1]
        for node in nodes:
            terminal_crawl = False
            if response.meta['terminal_crawl'] and node == last_node:
                terminal_crawl = True
            link = extract_data(node, './@href')
            reference = response.url.replace('index.html', link)
            event_link = reference.split('draws')[-1]
            for key, value in EVENTS_TBD.iteritems():
                if event_link not in value:
                    continue
                event = key
                yield Request(reference, callback = self.collect_draws, \
                meta = {'link': link, 'event': event, 'terminal_crawl': terminal_crawl})

    def collect_draws(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[contains(@id, "drawTable")]//table[contains(@id, "match")]')
        for node in nodes:
            record = SportsSetupItem()
            record['participants'] = {}
            player_sk = extract_list_data(node, './/a[contains(@href, "/en_GB/players/overview/")]/@href')
            player_text = extract_list_data(node, './/a[contains(@href, "/en_GB/players/overview/")]/text()')
            player_sk = [get_player_sk(pl) for pl in player_sk]
            if ['tbd1', 'tbd1'] == player_sk or ['tbd1', 'tbd1', 'tbd1', 'tbd1'] == player_sk or "Bye" in player_text:
                continue
            counter = 0
            group_count = 0
            if len(player_sk) == 4:
                group_count = 1
            for players_sk in player_sk:
                if len(player_sk) == 4:
                    counter += 1
                if counter > 2:
                    group_count = 2
                record['participants'][players_sk] = (group_count, '')

            for key, value in AFF_DICT.iteritems():
                if key in response.meta['event']:
                    record['affiliation'] = value
                    break

            record['game_datetime'] = datetime.datetime.now().date().strftime('%Y-%m-%d')
            player_sk.sort()
            temp_dict = {'source_key': '_'.join(['_'.join(player_sk), record['game_datetime'].replace('-', '_')]),
                         'game': 'tennis', 'reference_url': response.url,
                         'source': 'wimbledon_tennis', 'tournament': 'Wimbledon',
                         'participant_type': 'player',
                         'game_datetime': record['game_datetime'],  \
                         'game_status': 'constructed', 'event': response.meta['event']}
            for key, value in temp_dict.iteritems():
                record[key] = value
            record['rich_data'] =  {'channels': '',
                                        'location': {'city': 'Wimbledon',
                                        'country': 'UK',
                                        'continent': 'Europe',
                                        'state': 'London',
                                        'stadium': 'All England Lawn Tennis and Croquet Club'}}
            self.draws_dict['_'.join(player_sk[:2])] = record

        if response.meta['terminal_crawl']:
            url = 'http://www.wimbledon.com/en_GB/scores/schedule/'
            yield Request(url, callback = self.parse, meta = {})

    def parse(self, response):
        import pdb;pdb.set_trace()
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[contains(@id, "Days")]//ul//li/a')
        year  = extract_data(hxs, '//div[@id="wimMasthead"]//img[@class="tagline"]/@alt')
        year = year.split(' ')
        if year:
            year = year[-1]
        else:
            year = str(datetime.datetime.now().year)
        last_node = nodes[-1]
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            url = extract_data(node, './@href')
            data = extract_data(node, './@title').split('Day')
            date = data[1].strip()
            date = "".join(re.findall('\s(.*day\s\d+\d+\s.*)', date))
            if not date:
                date = "".join(re.findall('\d (.*\s.*)', data[1].strip()))
            url = response.url + url
            yield Request(url, callback = self.parse_details, \
            meta ={'date': date, 'year': year, 'terminal_crawl': terminal_crawl})

    def parse_details(self, response):
        hxs = Selector(response)
        scores = {}
        court_nodes = get_nodes(hxs, '//div[@class="court"]')
        for court in court_nodes:
            stadium = extract_data(court, './/div[@class="courtName"]/text()')
            game_note = extract_list_data(court, './/div[@class="courtName"]/text()')[0] \
            .replace('R: ', '')
            if "Centre" in stadium:
                game_time = stadium.split('Court')[-1]
                if game_time == '1:00pm':
                    game_time = '01:00pm'
                else:
                    game_time
            elif "Court1:" not in stadium:
                game_time = "".join(re.findall('.*Court.*(\d+\d+:\d+\d+.*)', stadium))
            elif "Court1:" in stadium:
                game_time = "".join(re.findall('.*Court(\d+:\d+\d+.*)', stadium))
            elif "tba" in stadium.lower():
                game_time = '05:30pm'
                game_note = ''
            if "To be Arranged" in game_note:
                game_note = ''
            nodes = get_nodes(court, './/div[@class="match"]')
            for node in nodes:
                self.record = SportsSetupItem()
                self.record['rich_data'] = {}
                self.record['result'] = {}

                game_datetime = "2015" + " " + response.meta['date']
                if game_time:
                    game_datetime += " " + game_time.lower()
                    pattern = "%Y %A %d %B %I:%M%p"
                    time_unknown = 0
                else:
                    pattern = "%Y %A %d %B"
                    time_unknown = 1
                game_datetime = get_utc_time(game_datetime, pattern, 'UTC')
                date_sk = game_datetime.split(' ')[0]

                self.record['game_datetime'] = game_datetime
                teams = get_nodes(node, './/div[@class="names content"]')
                winner_status = extract_data(node, './/div[@class="versus content"]//text()')

                if "defeat" in winner_status:
                    winner = extract_list_data(node, './/preceding-sibling::a/@href')
                    winner2 = extract_list_data(node, './/preceding-sibling::span/a/@href')
                    if len(winner) == 4:
                        winner1 = get_player_sk(winner[0])
                        winner2 = get_player_sk(winner[1])
                    else:
                        winner1 = get_player_sk(winner[0])
                    self.record['result'].setdefault('0', {})['winner'] = [winner1]
                    if winner2:
                        self.record['result'].setdefault('0', {})['winner'].append(winner2)

                players = extract_list_data(teams, './/a[contains(@href, "/en_GB/players/overview/")]/@href')
                players = [get_player_sk(pl) for pl in players]
                self.record['participants'] = {}
                counter = 0
                group_count = 0
                if len(players) == 4:
                    group_count = 1
                for player in players:
                    if len(players) == 4:
                        counter += 1
                    if counter > 2:
                        group_count = 2
                    self.record['participants'][player] = (group_count, '')
                event = extract_data(node, './/div[contains(@class, "matchHeader")]/text()')
                if "Qualifying" in event:
                    game_note = event.split('-')[-1].strip().title()
                event = event.replace(' -', '').replace('Tournament', '').title()
                event = "Wimbledon %s" % event

                event_skip = False
                for eve in EVE_SKIP_LIST:
                    if eve in event:
                        event_skip = True
                        break
                if event_skip:
                    continue
                for key, value in EVENTS_DICT.iteritems():
                    event = event.replace(key, value).replace('-', '')
                if "Qualifying" in event:
                    event = event.replace("Women's Qualifying", "Qualifications Women's"). \
                    replace("Men's Qualifying", "Qualifications Men's"). \
                    replace('First Round', '').replace('Second Round', ''). \
                    replace('Third Round', '').strip()

                for key, value in AFF_DICT.iteritems():
                    if key in event:
                        self.record['affiliation'] = value
                        break
                if event == "Wimbledon Men's Singles":
                    continue
                status_text  = extract_data(node, './/div[@class="matchScore"]//text()').strip()

                status = status_text.split('\n\n')
                status_ = extract_data(node, './/div[@class="matchStatus header"]//text()').strip()
                if 'To Finish' in status_text.title():
                    game_status = 'To Finish'
                elif 'Walkover\n\nComplete' in status_text:
                    game_status = "walkover"
                else:
                    game_status = status[-1]

                if "Walkover" in status_:
                    game_status = "walkover"
                if "Postponed" in status_:
                    game_status = "Postponed"
                if "To Finish" in status_:
                    game_status = "To Finish"

                self.record['game_status'] = STATUS_DICT[game_status]
                match_states = extract_data(node, './/div[@class="matchStatus header"]//text()')
                if self.record['game_status'] == "completed" and players:
                    for i in ['Ret.', '[', ']', 'To Finish ']:
                        scores = match_states.replace(i, '')
                    scores = scores.split(' ')
                    self.generate_scores(scores, players)

                source_key = players
                source_key.sort()
                self.draws_dict.pop('_'.join(source_key), 0)
                source_key.append(date_sk.replace('-', '_'))
                self.record['source_key'] = '_'.join(source_key)
                if self.record['source_key'] in ['312536_319600_2015_06_30', '312336_316959_2015_06_30'] and "/schedule/schedule9.html" in response.url:
                    continue
                if self.record['source_key'] in ['c977_i186_2015_07_03'] and "/schedule/schedule12.html" in response.url:
                    continue
                if self.record['source_key']  == 'a678_d643_2015_07_06' and "/schedule/schedule15.html" in response.url:
                    continue

                self.record['game'] = "tennis"
                self.record['reference_url'] = response.url
                self.record['source'] = "wimbledon_tennis"
                self.record['tournament'] = "Wimbledon"
                self.record['participant_type'] = "player"
                self.record['event'] = event
                self.record['rich_data'] =  {'channels': '', 'game_note': game_note,
                                            'location': {'city': 'Wimbledon',
                                            'country': 'UK',
                                            'continent': 'Europe',
                                            'state': 'London',
                                            'stadium': 'All England Lawn Tennis and Croquet Club'}}
                self.record['tz_info'] = get_tzinfo(country = 'United Kingdom', game_datetime = game_datetime)
                self.record['time_unknown'] = time_unknown
                days = []
                yday  = (datetime.datetime.now().date()- datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                days.append(yday)
                today =  datetime.datetime.now().date().strftime("%Y-%m-%d")
                days.append(today)
                tomo =  (datetime.datetime.now().date()+ datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                days.append(tomo)
                gametime = date_sk
                if gametime in days:
                    yield self.record
        if response.meta['terminal_crawl']:
            for draws in self.draws_dict.values():
                yield draws

    def generate_scores(self, scores, players):
        counter = home_final = away_final = 0
        result = self.record['result']
        for score in scores:
            counter += 1
            if '-' not in score:
                continue

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

            if int(home_score) > int(away_score):
                home_final += 1
            elif int(home_score) < int(away_score):
                away_final += 1

            if len(players) == 2:
                result.setdefault(players[0], {})['S%s' % counter] = home_score
                result.setdefault(players[1], {})['S%s' % counter] = away_score
                if extras:
                    result[players[0]]['T%s' % counter] = extras1
                    result[players[1]]['T%s' % counter] = extras2
            elif len(players) == 4:
                result.setdefault(players[0], {})['S%s' % counter] = home_score
                result.setdefault(players[1], {})['S%s' % counter] = home_score
                result.setdefault(players[2], {})['S%s' % counter] = away_score
                result.setdefault(players[3], {})['S%s' % counter] = away_score
                if extras:
                    result[players[0]]['T%s' % counter] = extras1
                    result[players[1]]['T%s' % counter] = extras1
                    result[players[2]]['T%s' % counter] = extras2
                    result[players[3]]['T%s' % counter] = extras2
        if len(players) == 2:
            result[players[0]]['final'] = home_final
            result[players[1]]['final'] = away_final
        if len(players) == 4:
            result[players[0]]['final'] = home_final
            result[players[1]]['final'] = home_final
            result[players[2]]['final'] = away_final
            result[players[3]]['final'] = away_final

        score = ' - '.join([str(home_final), str(away_final)])
        self.record['result'].setdefault('0', {})['score'] = score
