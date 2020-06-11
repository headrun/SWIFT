import datetime
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, \
extract_data, extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

true = True
false = False
null = ''


STATUS_DICT = {'': 'scheduled', 'In Progress': 'ongoing', 'To Finish': 'completed', \
               'Cancelled': 'cancelled', 'Postponed': 'postponed', 'walkover': 'walkover', \
               'Suspended': 'cancelled', 'Complete': 'completed'}
AFF_DICT    = {"Women's Singles": "wta", "Women's Doubles": "wta", \
               "Men's Singles": "atp", "Men's Doubles": "atp", "Mixed": "atp_wta"}

EVE_SKIP_LIST = ["Girls' ", "Boys' ", "Men's Legends", \
                 "Women's Legends", "WC Doubles", "WC Singles", \
                 "Invitation", "Inv.", "Snr.", "Wheelchair", "Juniors", \
                 "Men's Champions", "Men's Collegiate", \
                 "Women's Collegiate", "Exhibition", "Junior", "Champions"]

EVENTS_TBD = {"U.S. Open Men's Singles First Round": \
              ['/ms/r1s1.html', '/ms/r1s2.html', '/ms/r1s3.html', '/ms/r1s4.html'], \
              "U.S. Open Men's Singles Second Round": ['/ms/r2s1.html', '/ms/r2s2.html'], \
              "U.S. Open Men's Singles Third Round": ['/ms/r3s1.html', '/ms/r3s1.html'], \
              "U.S. Open Men's Singles Fourth Round" : ['/ms/r4s1.html'], \
              "U.S. Open Men's Singles Quarterfinals": ['/ms/r5s1.html'], \
              "U.S. Open Men's Singles Semifinals": ['/ms/r6s1.html'], \
              "U.S. Open Men's Singles Final": ['/ms/r7s1.html'], \
              "U.S. Open Women's Singles First Round": \
              ['/ws/r1s1.html', '/ws/r1s2.html', '/ws/r1s3.html', '/ws/r1s4.html'], \
              "U.S. Open Women's Singles Second Round": ['/ws/r2s1.html', '/ws/r2s2.html'], \
              "U.S. Open Women's Singles Third Round": ['/ws/r3s1.html', '/ws/r3s1.html'], \
              "U.S. Open Women's Singles Fourth Round": ['/ws/r4s1.html'], \
              "U.S. Open Women's Singles Quarterfinals": ['/ws/r5s1.html'], \
              "U.S. Open Women's Singles Semifinals": ['/ws/r6s1.html'], \
              "U.S. Open Women's Singles Final": ['/ws/r7s1.html'],\
              "U.S. Open Women's Doubles First Round": ['/wd/r1s1.html', '/wd/r1s2.html'], \
              "U.S. Open Women's Doubles Second Round": ['/wd/r2s1.html'], \
              "U.S. Open Women's Doubles Third Round": ['/wd/r3s1.html'], \
              "U.S. Open Women's Doubles Quarterfinals": ['/wd/r4s1.html'], \
              "U.S. Open Women's Doubles Semifinals": ['/wd/r5s1.html'], \
              "U.S. Open Women's Doubles Final": ['/wd/r6s1.html'], \
              "U.S. Open Men's Doubles First Round": ['/md/r1s2.html', '/md/r1s1.html'], \
              "U.S. Open Men's Doubles Second Round": ['/md/r2s1.html'], \
              "U.S. Open Men's Doubles Third Round": ['/md/r3s1.html'], \
              "U.S. Open Men's Doubles Quarterfinals": ['/md/r4s1.html'], \
              "U.S. Open Men's Doubles Semifinals": ['/md/r5s1.html'], \
              "U.S. Open Men's Doubles Final": ['/md/r6s1.html'], \
              "U.S. Open Mixed Doubles First Round": ['/xd/r1s2.html', '/xd/r1s1.html'], \
              "U.S. Open Mixed Doubles Second Round": ['/xd/r2s1.html'], \
              "U.S. Open Mixed Doubles Third Round": ['/xd/r3s1.html'], \
              "U.S. Open Mixed Doubles Quarterfinals": ['/xd/r4s1.html'], \
              "U.S. Open Mixed Doubles Semifinals": ['/xd/r5s1.html'], \
              "U.S. Open Mixed Doubles Final": ['/xd/r6s1.html']}



def get_player_sk(reference):
    replace_string = ['.html', 'unknown']
    reference = reference.split('/')[-1]
    for data in replace_string:
        if data == "unknown":
           reference = reference.replace(data, 'tbd1')
        else:
            reference = reference.replace(data, '')
    return reference

class UsopenTennis(VTVSpider):
    name = "usopen_spider"
    draws_dict = {}
    start_urls = []

    def start_requests(self):
        #reference = 'http://www.usopen.org/en_US/scores/draws/index.html'
        reference =  'http://www.usopen.org/en_US/scores/schedule/'
        #yield Request(reference, callback = self.draws, meta = {})
        yield Request(reference, callback = self.parse, meta = {})

    def draws(self, response):
        hxs = Selector(response)
        nodes = extract_list_data (hxs, '//div[@id="nav_top"]//ul//li[@class="nav_draws"]//div//ul//li \
                //a[not(contains(text(), "Invitational")) and not(contains(text(), "Qualifying")) \
                and not(contains(text(), "Juniors")) and not(contains(text(), "Wheelchair")) \
                and not(contains(text(), "Printable")) and not(contains(text(), "Bracket"))]/@href')
        last_node = nodes[-1]
        terminal_crawl = False
        for node in nodes:
            domain = "http://www.usopen.org"
            url = domain + node
            url = url.replace('?promo=subnav', '')
            if node == last_node:
                terminal_crawl = True
            yield Request(url, callback = self.rounds_info, \
                meta = {'terminal_crawl': terminal_crawl})
    def rounds_info(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="navigator"]')
        nodes = get_nodes(nodes, './/div[contains(@class, "column")]//a')
        last_node = nodes[-1]
        for node in nodes:
            terminal_crawl = False
            if response.meta['terminal_crawl'] and node == last_node:
                terminal_crawl = True
            link = extract_data(node, './/@href').replace('?promo=subnav', '')
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
            player_sk = extract_list_data(node, './/a[contains(@href, "/en_US/players/overview/")]/@href')
            player_text = extract_list_data(node, './/a[contains(@href, "/en_US/players/overview/")]/text()')
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
            source_key = '_'.join(['_'.join(player_sk), record['game_datetime'].replace('-', '_')])
            temp_dict = {'source_key': source_key,
                         'game': 'tennis',
                         'reference_url': response.url,
                         'source': 'usopen_tennis',
                         'tournament': 'U.S. Open (Tennis)',
                         'participant_type': 'player',
                         'game_datetime': record['game_datetime'],
                         'game_status': 'constructed',
                         'event': response.meta['event']}
            for key, value in temp_dict.iteritems():
                record[key] = value
            record['rich_data'] =  {'channels': '',
                                        'location': {'city': 'New York City',
                                        'country': 'USA',
                                        'state': 'New York'},
                                        'stadium': 'USTA Billie Jean King National Tennis Center'}
            self.draws_dict['_'.join(player_sk[:2])] = record
            if response.meta['terminal_crawl'] == False:
                url = 'http://www.usopen.org/en_US/scores/schedule/'
                yield Request(url, callback = self.parse, meta = {})
    def parse(self, response):
        hxs = Selector(response)
        #nodes = get_nodes(hxs, '//div[@id="tournDays"]//ul//li/a')
        nodes = get_nodes(hxs, '//div[contains(@id, "Days")]//ul//li/a')
        year = extract_data(hxs, '//div[@id="datesGraphic"]/text()')
        year = year.split(', ')
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
            date = extract_data(node, './@title')
            url = response.url + url
            yield Request(url, callback = self.parse_details, \
            meta ={'date': date, 'year': year, 'terminal_crawl': terminal_crawl})

    def parse_details(self, response):
        hxs = Selector(response)
        scores = {}
        court_nodes = get_nodes(hxs, '//table[@class="court"]')
        for court in court_nodes:
            game_time = extract_list_data(court, './/td[@class="courtName"]//text()')
            game_note = game_time[0].replace('R: ', '')
            game_time = game_time[-1]
            if game_time == '1:00pm':
                game_time = '01:00pm'
            elif game_time == '7:00pm':
                game_time = '07:00pm'
            else:
                game_time = game_time
            if "tba" in game_time.lower():
                game_time = '05:30pm'
            nodes = get_nodes(court, './/td[2]/table')
            for node in nodes:
                self.record = SportsSetupItem()
                self.record['rich_data'] = {}
                self.record['result'] = {}

                game_datetime = response.meta['year'] + " " + response.meta['date']
                if game_time:
                    game_datetime += " " + game_time.lower()
                    pattern = "%Y %A, %B %d %I:%M %p"
                    time_unknown = 0
                else:
                    pattern = "%Y %A %d %B"
                    time_unknown = 1
                game_datetime = get_utc_time(game_datetime, pattern, 'US/Eastern')
                date_sk = game_datetime.split(' ')[0]

                self.record['game_datetime'] = game_datetime
                teams = get_nodes(node, './tr//td[@class="teams"]')
                winner_status = extract_data(teams, './div[@class="versus"]/text()')

                if "def." in winner_status:
                    winner = extract_list_data(teams, './div[@class="versus"]//preceding-sibling::a/@href')
                    winner2 = extract_list_data(teams, './div[@class="versus"]//preceding-sibling::span/a/@href')
                    if len(winner) == 2:
                        winner1 = get_player_sk(winner[0])
                        winner2 = get_player_sk(winner[1])
                    else:
                        winner1 = get_player_sk(winner[0])
                    self.record['result'].setdefault('0', {})['winner'] = [winner1]
                    if winner2:
                        self.record['result'].setdefault('0', {})['winner'].append(winner2)

                players = extract_list_data(teams, './/a[contains(@href, "/en_US/players/overview/")]/@href')
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

                event = extract_data(node, './/td[@class="matchHeader"]/text()')
                if "Qualifying" in event:
                    game_note = event.split('-')[-1].strip().title()
                    game_note = game_note.replace('Round 1', 'First Round').replace('Round 2', 'Second Round'). \
                                replace('Round 3', 'Third Round').replace('Round 4', 'Fourth Round')
                event = event.replace(' -', '').replace('Round 1', 'First Round').replace('Round 2', 'Second Round'). \
                        replace('Round 3', 'Third Round').replace('Round 4', 'Fourth Round')
                event = "U.S. Open %s" % event
                event_skip = False
                for eve in EVE_SKIP_LIST:
                    if eve in event:
                        event_skip = True
                        break
                if event_skip:
                    continue
                for key, value in AFF_DICT.iteritems():
                    if key in event:
                        self.record['affiliation'] = value
                        break
                if "Qualifying" in event:
                    event = event.replace("Women's Qualifying", "Qualifications Women's"). \
                    replace("Men's Qualifying", "Qualifications Men's"). \
                    replace('First Round', '').replace('Second Round', ''). \
                    replace('Third Round', '').strip()


                status_text  = extract_data(node, './/td[@class="matchScore"]//text()').strip()
                status = status_text.split('\n\n')

                if 'To Finish' in status_text:
                    game_status = 'To Finish'
                elif 'Walkover\n\nComplete' in status_text:
                    game_status = "walkover"
                else:
                    game_status = status[-1]

                self.record['game_status'] = STATUS_DICT[game_status]

                if self.record['game_status'] == 'completed' and players:
                    for i in ['To Finish', '[', ']']:
                        scores = status[0].replace(i, '')
                        scores = scores.split(' ')
                        self.generate_scores(scores, players, event)

                source_key = players
                source_key.sort()
                self.draws_dict.pop('_'.join(source_key), 0)
                source_key.append(date_sk.replace('-', '_'))
                self.record['source_key'] = '_'.join(source_key)

                self.record['game'] = "tennis"
                self.record['reference_url'] = response.url
                self.record['source'] = "usopen_tennis"
                self.record['tournament'] = "U.S. Open (Tennis)"
                self.record['participant_type'] = "player"
                self.record['event'] = event

                self.record['rich_data'] =  {'channels': '', 'game_note': game_note,
                                            'location': {'city': 'New York City',
                                            'country': 'USA',
                                            'state': 'New York',
                                            'stadium': 'USTA Billie Jean King National Tennis Center'}}
                self.record['tz_info'] = get_tzinfo(city = 'New York City', country = 'USA', game_datetime = game_datetime)
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
                if not draws['participants']:
                    return
                yield draws

    def generate_scores(self, scores, players, event):
        counter = home_final = away_final = 0
        result = self.record['result']
        for score in scores:
            counter += 1
            if '-' not in score:
                continue

            score = score.split('-')
            home_score = score[0].replace('[', '')
            away_score = score[1].replace('[', '')
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
            if "U.S. Open Mixed Doubles Final" in event:
                result[players[0]]['final'] = 2
                result[players[1]]['final'] = 2
                result[players[2]]['final'] = 1
                result[players[2]]['final'] = 1
            else:

                result[players[0]]['final'] = home_final
                result[players[1]]['final'] = home_final
                result[players[2]]['final'] = away_final
                result[players[3]]['final'] = away_final

        score = ' - '.join([str(home_final), str(away_final)])
        self.record['result'].setdefault('0', {})['score'] = score

