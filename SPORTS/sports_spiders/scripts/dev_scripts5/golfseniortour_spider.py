import re
import datetime
import time
from vtvspider import VTVSpider, extract_data, get_nodes, get_utc_time
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import get_tzinfo


RECORD = SportsSetupItem()

REPLACE_STD_SPL_DICT = {' at ': ' ', ' &amp; ': ' ', 'GC': 'Golf Club', \
                        'CC': 'Country Club', \
                        'TPC': 'Tournament Players Club', \
                        'G&amp;CC': 'Golf and Country Club', \
                        'GCC': 'Golf and Country Club', \
                        'G&amp;': 'Golf and ', 'TBC,.': 'TBC'}

REPLACE_STATE_DICT = {'Hong Kong': 'Hong Kong', 'Gauteng': 'Gauteng'}

CTRY_ACC_STATE = {'United Arab Emirates': 'United Arab Emirates', \
                  'Italy': 'Italy', 'South Africa': 'South Africa' , \
                  'Hong Kong': 'China', 'Qatar': 'Qatar', \
                  'Morocco': 'Morocco', 'Malaysia': 'Malaysia', \
                  'China': 'China', 'Spain': 'Spain', 'Sweden': 'Sweden', \
                  'Austria': 'Austria', \
                  'Republic of Ireland': 'Republic of Ireland', \
                  'Germany': 'Germany', 'France': 'France', \
                  'Scotland': 'Scotland', 'England': 'England', \
                  'Denmark': 'Denmark', 'Switzerland': 'Switzerland', \
                  'The Netherlands': 'Netherlands', 'Wales': 'Wales', \
                  'Portugal': 'Portugal', 'Australia': 'Australia', \
                  'Turkey': 'Turkey'}

IGNORE_WORDS = ('.', ',', 'TBC')

REPLACE_TOU_NAME = {'SSE Scottish Senior Open': 'Scottish Seniors Open', \
                    'Travis Perkins Masters' : 'Travis Perkins plc Senior Masters', \
                    'WINSTONgolf  Senior Open' : 'WINSTONgolf Senior Open'
                    }


IGNORE_TOURNAMENTS = ['US Senior PGA Championship presented by KitchenAid', \
                      'ISPS Handa PGA Seniors Championship', \
                      'Bad Ragaz PGA Seniors Open', \
                      'US Senior Open Championship', \
                      'The Senior Open Championship Presented by Rolex']



def ignore_tournaments(tou_name):
    tou_name = get_refined_tou_name(tou_name)
    for tournament in IGNORE_TOURNAMENTS:
        if tournament in tou_name.lower():
            res = True
            return res

def get_refined_tou_name(tou_name):
    for key, value in REPLACE_TOU_NAME.iteritems():
        if key in tou_name:
            tou_name = tou_name.replace(key, value).strip()
    return tou_name

def get_refined_stadium(stadium):
    for key, value in REPLACE_STD_SPL_DICT.iteritems():
        if key in stadium:
            stadium = stadium.replace(key, value).strip()
    if '(' in stadium:
        stadium_extra = (re.findall(r'\(.*\)', stadium))
        stadium = stadium.replace(stadium_extra, '').strip()
    return stadium

def get_refined_locations(stadium, city, state, country):
    for i in IGNORE_WORDS:
        if i in stadium:
            stadium = stadium.replace(i, '').strip()
        if i in city:
            city = city.replace(i, '').strip()
        if i in state:
            state = state.replace(i, '').strip()
        if i in country:
            country = country.replace(i, '').strip()
    return stadium, city, state, country

def get_city_state_ctry(venue):
    venue_ = venue.split(',')
    if len(venue_) == 2:
        country = ''
        city  = venue_[0].strip()
        state = REPLACE_STATE_DICT.get(venue_[-1].strip())
        if not state:
            state = ''
            country = venue_[-1].strip()
    elif len(venue_) == 3:
        city = venue_[0].replace('&amp; ', '').strip()
        state = venue_[1].strip()
        country = venue_[-1].strip()
    elif len(venue_) == 4:
        city = venue_[1].strip()
        state = venue_[2].strip()
        country = venue_[-1].strip()
    else:
        country = venue.replace(',', '').strip()
        city, state = '', ''
    return city, state, country

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position
    return pos

def get_start_end_dates(_date, year):
    if _date and year:
        start_end_date = _date + ' ' + year
        final_date = (datetime.datetime(*time.strptime(start_end_date.\
                      strip(), '%b %d %Y',)[0:6])).date()
    return final_date

def get_source_key(tou_name, tou_date):
    modified_tou_name = '_'.join(i.strip() for i in tou_name.strip().\
                        split('-')).replace(' ', '_').strip()
    modified_tou_date = '_'.join(i.strip() for i in tou_date.strip().\
                        split('-')).replace(' ', '_').strip()
    tou_sk = modified_tou_name + '_' + modified_tou_date
    return tou_sk

def get_game_datetime(tou_date, year):
    game_dates = [i.strip() for i in tou_date.split('-') if i]
    if len(game_dates) == 2:
        start_date, end_date = game_dates
        game_datetime = start_date + ' %s' % year
    elif len(game_dates) >= 1:
        game_datetime = game_dates[0] + ' %s' % year
    else:
        game_datetime = ''

    if game_datetime:
        game_datetime = get_utc_time(game_datetime, '%b %d %Y', 'US/Eastern')

    return game_datetime

MAIN_URL = 'http://www.europeantour.com'
class GolfSeniorTour(VTVSpider):
    name = "seniortour_spider"
    allowed_domains = []
    start_urls = ['http://www.europeantour.com/seniortour/tournament/index.html']
    winner_id = ''
    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//tbody[@id = "includeSchedule"]\
                /tr[contains(@class,"bg") or contains(@class,"bmwpa")]')
        for node in nodes[1:]:
            winner_name = extract_data(node, \
                         './/td[@class="tdName"]/a/text()').strip()
            self.winner_id = "".join(re.findall(r'playerid=(\d+)', extract_data\
                        (node, './/td[@class="tdName"]/a/@href'))).strip()
            year = extract_data(node, '//div[@id="#divContainer"]/\
                    div[@id="column1"]//select[@id="season"]/\
                    option[@selected="Yes"]/@value').strip()
            tou_link = extract_data(node, './td[@class="tdName"]/ul/li/a/@href').strip()
            if tou_link and 'http' not in tou_link:
                tou_link = MAIN_URL + tou_link
            tou_name = extract_data(node, './td[@class="tdName"]/ul/li/a/text()').strip()
            if tou_name:
                tou_name = get_refined_tou_name(tou_name)
            if ('US Senior PGA Championship presented by KitchenAid' == tou_name or \
                'ISPS Handa PGA Seniors Championship' == tou_name or \
                'Bad Ragaz PGA Seniors Open' == tou_name or \
                'US Senior Open Championship' == tou_name or \
                'The Senior Open Championship Presented by Rolex' == tou_name):
                continue
            else:
                tou_name = tou_name
            tou_venue = extract_data(node, './td[@class="tdName"]/ul/li[2]/text()')
            stadium = tou_venue.split(',')[0].strip()
            stadium = get_refined_stadium(stadium)
            start_date = extract_data(node, './td[2]/text()')
            end_date = extract_data(node, './td[3]/text()')
            if not start_date or not end_date:
                continue
            venue = ",".join(tou_venue.split(',')[1:]).strip()
            city, state, country = get_city_state_ctry(venue)
            if not country:
                country = CTRY_ACC_STATE.get(state)
            stadium, city, state, country = get_refined_locations(stadium, \
                                            city, state, country)

            if not tou_link:
                continue

            tz_info = get_tzinfo(city = city)
            RECORD['source']  = 'euro_golf'
            RECORD['affiliation'] = 'euro'
            RECORD['participant_type'] = 'player'
            RECORD['game'] = 'golf'
            RECORD['season'] = year
            RECORD['event'] = ''
            RECORD['time_unknown'] = 1
            RECORD['tz_info'] = tz_info

            data_record = {'start_url': response.url, 'tou_name': tou_name, \
                           'start_date': start_date, \
                           'end_date': end_date, 'winner_id': self.winner_id, \
                           'winner_name': winner_name, \
                           'year': year, 'stadium': stadium, \
                           'country': country, 'city' : city, 'state': state}

            yield Request(tou_link, callback = self.parse_next, \
                          meta = {'data_record': data_record})

    def parse_next(self, response):
        hxs = Selector(response)
        data_record = response.meta['data_record']
        tou_year = extract_data(hxs, \
                   '//div[@class="centerBox"]//div[@id="tourTournSubDates"]/text()').split('-')[-1].split(' ')[-1]
        if not tou_year:
            tou_year = "2014"
        start_date  = get_start_end_dates(data_record['start_date'], tou_year)
        end_date = get_start_end_dates(data_record['end_date'], tou_year)
        tou_date = start_date.strftime('%b %-d') +' - '+ end_date.strftime('%b %-d')
        today_date = datetime.datetime.utcnow().date()
        if start_date <= today_date and today_date <= end_date:
            status = "ongoing"
        elif end_date < today_date:
            status = "completed"
        else:
            status = "scheduled"
        previous_date = datetime.timedelta(-1)
        new_date = today_date + previous_date
        lb_link = extract_data(hxs, '//li[contains(@id,"ml2_to_")]/a[contains(@href,"leaderboard")]/@href').strip() or \
                  extract_data(hxs, '//li[contains(@id,"ml2_mp_")]/a[contains(@href,"leaderboard")]/@href').strip()
        lb_link = MAIN_URL + lb_link
        if lb_link and 'http' not in lb_link:
            lb_link = MAIN_URL + lb_link
        game_datetime = get_game_datetime(data_record['start_date'], tou_year)

        data_record.update({'game_status': status, \
                            'tou_date': tou_date, \
                            'start_date': game_datetime, \
                            'end_date': end_date})

        if lb_link:
            yield Request(lb_link, callback = self.parse_next_second, meta = {'data_record': data_record})

    def parse_next_second(self, response):
        hxs = Selector(response)
        data_record = response.meta['data_record']
        status = data_record['game_status']
        tou_name = data_record['tou_name']
        data_record.update({'start_url': response.url})

        leader_board = extract_data(hxs, '//script[contains(text(), "_results.html")]')
        if leader_board:
            l_b_link = "".join((re.findall(r'get(.*_results.html)', leader_board))).split('("')[1]
        else:
            l_b_link = ''


        if (status == "completed" or status == "ongoing"):
            if l_b_link:
                l_b_link =  MAIN_URL + l_b_link
                yield Request(l_b_link, callback = self.parse_details, \
                              meta = {'data_record': data_record})
            else:
                leader_board = l_b_link = ''
                leader_board = extract_data(hxs, '//script[contains(text(), "_leaderboard_v2.html")]')
                if leader_board:
                    l_b_link =  MAIN_URL + \
                                        "".join((re.findall\
                                        (r'get(.*_leaderboard_v2.html)',\
                                        leader_board))).split('("')[1]
                    if l_b_link:
                        yield Request(l_b_link, \
                                      callback = self.parse_ongoing, \
                                      meta = {'data_record': data_record})


        elif status == "scheduled" and self.spider_type == "schedules":
            RECORD['rich_data'] = {"channels": '', \
                               "game_note": '', \
                               "stadium": data_record['stadium'], 'location': {\
                               "country": data_record['country'], \
                               "city" : data_record['city'], \
                               "state": data_record['state']}}

            RECORD['result'] = {}
            RECORD['participants'] = {}
            RECORD['reference_url'] = response.url
            RECORD['game_datetime'] = data_record['start_date']
            RECORD['tournament'] = tou_name
            RECORD['location_info'] = data_record['country']
            RECORD['game_status'] = status
            RECORD['source_key'] = get_source_key(tou_name, \
                                    data_record['tou_date'])
            yield RECORD
        else:
            leader_board = extract_data(hxs, '//script[contains(text(), "_leaderboard_v2.html")]')
            if leader_board:
                leader_board_link =  MAIN_URL + \
                "".join(re.findall(r'get(.*_leaderboard_v2.html)', leader_board)).split('("')[1]

                if leader_board_link:
                    yield Request(leader_board_link, \
                                  callback = self.parse_details, \
                                  meta = {'data_record': data_record})
    def parse_details(self, response):
        result_final = {}
        players = {}
        hxs = Selector(response)
        data_record = response.meta['data_record']
        status = data_record['game_status']
        tou_sk = get_source_key(data_record['tou_name'], \
                    data_record['tou_date'])
        RECORD['rich_data'] = {"channels": '', \
                               "game_note": '', \
                               "stadium": data_record['stadium'], 'location': {\
                               "country": data_record['country'], \
                               "city" : data_record['city'], \
                               "state": data_record['state']}}
        RECORD['source_key'] = tou_sk
        RECORD['reference_url'] = data_record['start_url']
        RECORD['game_datetime'] = data_record['start_date']
        RECORD['game_status'] = status

        self.winner_id = data_record['winner_id']
        nodes = get_nodes(hxs, '//div[@id="leaderboardTable"]//ul[@class="dataRow"]')
        for node in nodes:
            position = extract_data(node, './li[@class="pos"]/span/text()')
            pl_pos = get_position(position).replace('\n', '')
            player_name = extract_data(node, \
                          './li[@class="hname2"]/span/a/text()') or \
                          extract_data(node, \
                          './/a[@title="Player Profile"]/text()')
            player_name = ' '.join(i.capitalize().strip() for i in player_name.split() if i)
            if '(AM)' in player_name:
                player_name = player_name.replace('(AM)', '')
            else:
                player_name = player_name
            player_id = "".join(re.findall(r'playerid=(\d+)/', \
                        extract_data\
                        (node, './li[@class="hname2"]/span/a/@href')\
                        or extract_data\
                        (node, './/a[@title="Player Profile"]/@href')))
            to_par = extract_data(node, './/li[@class="topar"]/span/text()')
            round1 = extract_data(node, './/li[@class="r1"]/span/text()')
            round2 = extract_data(node, './/li[@class="r2"]/span/text()')
            round3 = extract_data(node, './/li[@class="r3"]/span/text()')
            round4 = extract_data(node, './/li[@class="r4"]/span/text()')
            total = extract_data(node, './/li[@class="total"]/span/text()')
            if status == "completed":
                winner = self.winner_id
            else:
                winner = ''
            result = {'0': {'winner': winner}, \
                      player_id: {'final': total, 'position': pl_pos, \
                      'R1': round1, 'R2': round2, 'R3': round3, 'R4': round4, \
                      'TO PAR': to_par}}
            players.update({player_id: (0, player_name)})
            result_final.update(result)
        RECORD['participants'] = players
        RECORD['result'] = result_final
        RECORD['tournament'] = data_record['tou_name']
        yield RECORD

    def parse_ongoing(self, response):
        print response.url
        result_final = {}
        players = {}
        hxs = Selector(response)
        data_record = response.meta['data_record']
        status = data_record['game_status']
        tou_sk = get_source_key(data_record['tou_name'], \
                 data_record['tou_date'])
        RECORD['rich_data'] = {"channels": '', \
                               "game_note": '', \
                               "stadium": data_record['stadium'], \
                               "country": data_record['country']}
        RECORD['source_key'] = tou_sk
        RECORD['reference_url'] = data_record['start_url']
        RECORD['game_datetime'] = data_record['start_date']
        RECORD['game_status'] = status

        if status == 'ongoing':
            game_note = extract_data(hxs, '//div[@class="c"]/\
                        div[contains(@style, "width: 25%")]/text()').\
                        replace('Status:', '').replace(',', ' - ').strip()
            if ',' in game_note:
                _round, num = re.findall('([A-Za-z]+)(\d+)', game_note.split('-')[0].strip())[0]
                current_round = _round + ' ' + num + ' - ' + game_note.split('-')[1].strip()
            elif 'Round' in game_note:
                _round, num = re.findall('([A-Za-z]+)(\d+)', game_note.split('-')[0].strip())[0]
                current_round = _round + ' ' + num + ' - ' + game_note.split('-')[1].strip()
            elif len(game_note.split(' ')) == 1:
                current_round = game_note.strip()
            else:
                one, two, _round, num = re.findall('([A-Za-z]+) ([a-z]+) ([A-Za-z]+)(\d+)', game_note)
                current_round = one + ' ' + two + ' ' + _round + ' ' + num
            if 'Suspended' in game_note:
                current_round = 'Suspended'
            current_round = current_round
        else:
            current_round = ''
        RECORD['rich_data'].update({'game_note': game_note})
        nodes = get_nodes(hxs, '//table[@id="lbl"]//tr[not(contains(@id,"boardPromo"))]')
        for node in nodes:
            position = extract_data(node, './/td[@class="b"]/text()')
            pl_pos = get_position(position).replace('\n', '')
            player_name = extract_data(node, './/td[@class="nm"]/div[@class="nm"]/text()')
            player_id = extract_data(node, './@id')
            round1 = extract_data(node, './/td[@class="rnd "][1]/text()') or \
                     extract_data(node, './/td[@class="rnd"][1]/text()')
            round2 = extract_data(node, './/td[@class="rnd "][2]/text()') or \
                     extract_data(node, './/td[@class="rnd"][2]/text()')
            round3 = extract_data(node, './/td[@class="rnd "][3]/text()') or \
                     extract_data(node, './/td[@class="rnd"][3]/text()')
            round4 = extract_data(node, './/td[@class="rnd "][4]/text()') or \
                     extract_data(node, './/td[@class="rnd"][4]/text()')
            to_par = extract_data(node, './/td[10]/text()')
            total = extract_data(node, './/td[15]/text()')
            print total
            hole = extract_data(node, './/td[9]/text()')
            round1 = round1.replace('-', '')
            round2 = round2.replace('-', '')
            round3 = round3.replace('-', '')
            round4 = round4.replace('-', '')
            total = total.replace('-', '')
            if status == "completed":
                winner = self.winner_id
            else:
                winner = ''
            result = {'0': {'winner': winner}, \
                          player_id: {'final': total, 'position': pl_pos, \
                          'R1': round1, 'R2': round2, 'R3': round3, 'R4': round4, \
                          'TO PAR': to_par, 'TeeTime': hole}}
            result_final.update(result)
            players.update({player_id: (0, player_name)})

        RECORD['participants'] = players
        RECORD['result'] = result_final
        RECORD['tournament'] = data_record['tou_name']
        import pdb;pdb.set_trace()

        yield RECORD
