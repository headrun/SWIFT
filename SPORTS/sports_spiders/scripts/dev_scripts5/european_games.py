#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import datetime
import time
from vtvspider_new import VTVSpider, extract_data, get_nodes, get_utc_time
from vtvspider_new import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

record = SportsSetupItem()

REPLACE_STD_SPL_DICT = {' at ': ' ', ' &amp; ': ' ', 'GC': 'Golf Club', \
                        'CC': 'Country Club', \
                        'TPC': 'Tournament Players Club', \
                        'G&amp;CC': 'Golf and Country Club', \
                        'G&CC': 'Golf and Country Club', \
                        'GCC': 'Golf and Country Club', \
                        'G&amp;': 'Golf and ', 'TBC,.': 'TBC', \
                        'G&Country': 'Golf & Country'}

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
REPLACE_TOU_WORDS = {', Hosted by the Sergio Garcia Foundation': '', \
                        'Real Club Valderrama ': '', \
                        'ISPS HANDA ': ''}

REPLACE_TOU_NAME = {'alfred dunhill championship': 'Alfred Dunhill Championship',
                    'australian pga championship': 'Australian PGA Championship',
                    'nedbank golf challenge': 'Nedbank Golf Challenge',
                    'the bmw sa open hosted by city of ekurhuleni': 'South African Open',
                    'abu dhabi hsbc golf championship': 'Abu Dhabi HSBC Golf Championship',
                    'qatar masters': 'Qatar Masters',
                    'dubai desert classic': 'Dubai Desert Classic',
                    'tshwane open': 'Tshwane Open',
                    'maybank championship malaysia': 'Malaysian Open',
                    'thailand classic': 'Thailand Classic',
                    'hassan ii': 'Hassan II Golf Trophy',
                    'bmw pga championship': 'BMW PGA Championship',
                    'nordea masters': 'Scandinavian Masters',
                    'lyoness open': 'Austrian Open',
                    'bmw international open': 'BMW International Open',
                    'aam scottish open': 'Scottish Open',
                    '145th open championship': 'The Open Championship',
                    'european masters': 'Omega European Masters',
                    'italian championship': 'Italian Open',
                    'porsche european open': 'European Open (golf)',
                    'british masters supported by sky sports': 'British Masters',
                    'irish open': 'Irish Open',
                    'open de france': 'Open de France',
                    'saltire energy': 'Saltire Energy Paul Lawrie Matchplay', \
                    }

IGNORE_TOURNAMENTS = ['isps handa world cup of golf', \
                      'wgc - cadillac championship', 'tbc', \
                      'us pga championship', \
                      '143rd open championship', 'us open', \
                      'masters tournament', \
                      'eurasia cup presented by',
                      'wgc - bridgestone invitational',
                      "olympic" , 'ryder',
                      'wgc - hsbc champions',
                      'hsbc champions',
                      'cadillac', 'bridgestone',
                      'masters', 'dell match play']


ONGOING_RESULT = """.//td[@class="rnd "][%s]/text()"""

COMPLETED_RESULT = """.//li[@class="%s"]/span/text()"""

def get_source_key(tou_name, tou_date, year):
    modified_tou_name = '_'.join(i.strip() for i in tou_name.strip().\
                         split('-')).replace(' ', '_').strip()
    modified_tou_date = '_'.join(i.strip() for i in tou_date.strip().\
                         split('-')).replace(' ', '_').strip()
    tou_sk = modified_tou_name + "_" + year + '_' + modified_tou_date
    return tou_sk


def ignore_tournaments(tou_name):
    tou_name = get_refined_tou_name(tou_name)
    for tournament in IGNORE_TOURNAMENTS:
        if tournament in tou_name.lower():
            res = True
            return res

def get_refined_tou_name(tou_name):
    for key, value in REPLACE_TOU_NAME.iteritems():
        if key in tou_name.lower():
            tou_name = value
    for key, value in REPLACE_TOU_WORDS.iteritems():
        if key in tou_name:
            tou_name = tou_name.replace(key, value)
    return tou_name

def get_refined_stadium(stadium):
    for key, value in REPLACE_STD_SPL_DICT.iteritems():
        if key in stadium:
            stadium = stadium.replace(key, value)
    if '(' in stadium:
        stadium_extra = (re.findall(r'\(.*\)', stadium))
        stadium = stadium.replace(stadium_extra, '')
    return stadium

def get_refined_locations(stadium, city, state, country):
    for i in IGNORE_WORDS:
        if i in stadium:
            stadium = stadium.replace(i, '').strip()
        if i in city:
            city =  city.replace(i, '').strip()
        if i in state:
            state = state.replace(i, '').strip()
        if i in country:
            country = country.replace(i, '').strip()
    return stadium, city, state, country

def get_city_state_ctry(venue):
    venue_ = venue.split(',')
    if len(venue_) == 2:
        country = ''
        city  = venue_[0]
        state = REPLACE_STATE_DICT.get(venue_[-1])
        if not state:
            state = ''
            country = venue_[-1]
    elif len(venue_) == 3:
        city = venue_[0].replace('&amp; ', '').strip()
        state = venue_[1]
        country = venue_[-1]
    elif len(venue_) == 4:
        city = venue_[1]
        state = venue_[2]
        country = venue_[-1]
    else:
        country = venue.replace(',', '').strip()
        city, state = '', ''
    if city == "Waterkloof":
        city = "Pretoria"
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
        game_t = time.strftime("%Y-%m-%d %H:%M:%S", final_date.timetuple())
    return game_t, final_date

def get_richdata(channels, game_note, stadium, loc):
    rich_data = {"channels": '', "game_note": '', "stadium": '', "country": ''}
    if (channels or game_note or stadium or loc):
        rich_data = {"channels": channels, "game_note": game_note, \
                     "stadium": stadium, 'location': loc}
    return rich_data

def get_result(winner_id, player_id, total, pl_pos, round1, round2, round3, round4, to_par):
    result = {'0': {'winner': winner_id}, \
              player_id: {'final': total, 'position': pl_pos, \
              'R1': round1, 'R2': round2, 'R3': round3, 'R4': round4, \
              'TO PAR': to_par}}
    return result


CHANNELS = GAME_NOTE = ''

LEADER_BOARD = '//li[contains(@id,"%s")]/a[contains(@href,"leaderboard")]/@href'

MAIN_URL = "http://www.europeantour.com"

class EuropeanTour(VTVSpider):
    name = "european_games"
    start_urls = ['http://www.europeantour.com/europeantour/tournament/index.html']

    next_week_days = []
    date_time = datetime.datetime.now()
    next_week_days.append('2015-12-03')
    for i in range(0, 7):
        next_week_days.append((date_time - datetime.timedelta(days=i)).strftime('%Y-%m-%d'))

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//tbody[@id = "includeSchedule"]\
                /tr[contains(@class,"bg") or contains(@class,"bmwpa")]')
        for node in nodes:
            winner_name = extract_data(node, './/td[@class="tdName"]/a/text()')
            winner_id = "".join(re.findall(r'playerid=(\d+)', extract_data(node, './/td[@class="tdName"]/a/@href'))).strip()
            year = extract_data(node, '//div[@id="#divContainer"]/div[@id="column1"]//select[@id="season"]/option[@selected="Yes"]/@value').strip()
            tou_link = extract_data(node, './td[@class="tdName"]/ul/li/a/@href').strip()
            if tou_link and 'http' not in tou_link:
                tou_link = MAIN_URL + tou_link
            t_name = extract_data(node, './td[@class="tdName"]/ul/li/a/text()').strip()
            tou_name = get_refined_tou_name(t_name)
            tou_venue = extract_data(node, './td[@class="tdName"]/ul/li[2]/text()')
            stadium = tou_venue.split(',')[0].strip()
            stadium = get_refined_stadium(stadium)
            start_date = extract_data(node, './td[2]/text()')
            end_date = extract_data(node, './td[3]/text()')
            if not start_date or not end_date:
                continue

            res = ignore_tournaments(tou_name)
            if not 'Mauritius' in tou_name:
                if res:
                    continue
            venue = ",".join(tou_venue.split(',')[1:]).strip()
            city = state = country = ''
            city, state, country = get_city_state_ctry(venue)
            if not country:
                country = CTRY_ACC_STATE.get(state)
            stadium, city, state, country = get_refined_locations(stadium, city, state, country)
            loc = {'city': city, 'state': state, 'country': country}
            if not tou_link:
                continue

            record['source'] = 'euro_golf'
            record['affiliation'] = 'euro'
            record['participant_type'] = 'player'
            record['game'] = 'golf'
            record['season'] = year
            record['event'] = ''

            data_record = {'start_url': response.url, 'tou_name': tou_name, \
                           'start_date': start_date, \
                           'end_date': end_date, 'winner_id': winner_id, \
                           'winner_name': winner_name, \
                           'year': year, 'stadium': stadium, \
                           'loc': loc}

            yield Request(tou_link, callback = self.parse_next, meta = {'data_record': data_record})

    def parse_next(self, response):
        hxs = Selector(response)
        data_record = response.meta['data_record']
        tou_year = extract_data(hxs, \
                   '//div[@class="centerBox"]//div[@id="tourTournSubDates"]/text()').split('-')[-1].split(' ')[-1]
        if not tou_year:
            print 'not tou_year'
        start_date, start_dt  = get_start_end_dates(data_record['start_date'], tou_year)
        end_date, end_dt = get_start_end_dates(data_record['end_date'], tou_year)
        tou_date = start_dt.strftime('%b %-d') + ' - ' + end_dt.strftime('%b %-d')
        today_date = datetime.datetime.utcnow().date()
        game_datetime = get_utc_time(start_date, '%Y-%m-%d %H:%M:%S', 'US/Eastern')

        lb_link = extract_data(hxs, LEADER_BOARD % "ml2_to_").strip() or \
                  extract_data(hxs, LEADER_BOARD % "ml2_mp_").strip()

        tz_info = get_tzinfo(city = data_record['loc']['city'], game_datetime = game_datetime)
        if not tz_info:
            tz_info = get_tzinfo(city = data_record['loc']['country'], game_datetime = game_datetime)
        lb_link = MAIN_URL + lb_link

        if lb_link and 'http' not in lb_link:
            lb_link = MAIN_URL + lb_link
        if start_dt <= today_date and today_date <= end_dt:
            status = "ongoing"
        elif end_dt < today_date:
            print game_datetime.split(' ')[0]
            print data_record['tou_name']
            status = "completed"
        else:
            status = "scheduled"
        if status == 'scheduled' and self.spider_type == 'schedules':
            record['game_status'] = status
            record['rich_data'] = get_richdata(CHANNELS, GAME_NOTE, data_record['stadium'], data_record['loc'])
            record['source_key'] = get_source_key(data_record['tou_name'], tou_date, tou_year)
            record['result'] = {}
            record['participants'] = {}
            record['reference_url'] = response.url
            record['game_datetime'] = game_datetime
            record['location_info'] = data_record['loc']
            record['tournament'] = get_refined_tou_name(data_record['tou_name'])
            record['time_unknown'] = 1
            record['tz_info'] = tz_info
            yield record

        data_record.update({'game_status': status, \
                            'tou_date': tou_date, \
                            'start_date': game_datetime, \
                            'end_date': end_date, 'tou_year': tou_year,
                            'tz_info': tz_info})
        if "leaderboard" in lb_link and "Seve Trophy" in data_record['tou_name']:
            yield Request(lb_link, callback = self.parse_sevegolftrophy, meta = {'data_record': data_record})
        elif status == "ongoing" and self.spider_type == 'ongoing':
            yield Request(lb_link, callback = self.parse_next_second, meta = {'data_record': data_record})
        elif game_datetime.split(' ')[0] in self.next_week_days and self.spider_type == 'scores':
            if status == "completed":
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
            leader_board = ''
        leader_board = extract_data(hxs, '//script[contains(text(), "_leaderboard_v2.html")]')
        if leader_board:
            l_b_link = "".join((re.findall(r'get(.*_leaderboard_v2.html)', leader_board))).split('("')[1]

        if status == "completed" and self.spider_type == 'scores':
            if l_b_link:
                l_b_link = MAIN_URL + l_b_link
                yield Request(l_b_link, callback = self.parse_details, meta = {'data_record': data_record})
        elif status == "ongoing" and self.spider_type == 'ongoing':
            if l_b_link:
                l_b_link = MAIN_URL + l_b_link
                yield Request(l_b_link, callback = self.parse_ongoing, meta = {'data_record': data_record})

    def parse_sevegolftrophy(self, response):
        hxs = Selector(response)
        data_record = response.meta['data_record']
        status = data_record['game_status']
        record['game_status'] = status
        team_1_name = extract_data(hxs, '//div[@class="overallStandings"]//div[@class="teamRight"]/div[@class="name"]/text()')
        team_2_name = extract_data(hxs, '//div[@class="overallStandings"]//div[@class="teamLeft"]/div[@class="name"]/text()')
        nodes = get_nodes(hxs, '//div[@class="overallStandings"]//div[contains(@class, "team")]')
        players = {}
        result_final = {}

        for node in nodes:
            team_name   = extract_data(node, './div[@class="name"]/text()')
            team_score  = extract_data(node, './div[@class="points"]/text()')
            players[team_name] = {'to_par': team_score, \
                            'tscore': team_score, 'name': team_name}

        t1_score = players[team_1_name]['tscore']
        t2_score = players[team_2_name]['tscore']
        if int(t1_score) > int(t2_score):
            winner = team_1_name.lower()
            players[team_1_name].update({'pos': "T1"})
            players[team_2_name].update({'pos': "T2"})
        elif int(t1_score) == int(t2_score):
            winner = ''
            players[team_1_name].update({'pos': "T1"})
            players[team_2_name].update({'pos': "T2"})
        else:
            winner = team_2_name.lower()
            players[team_2_name].update({'pos': "T1"})
            players[team_1_name].update({'pos': "T2"})

        if status == 'completed':
            winner = winner
        else:
            winner = ''
        record['participants'] = players
        record['rich_data'] = get_richdata(CHANNELS, GAME_NOTE, data_record['stadium'], data_record['loc'])
        record['location_info'] = data_record['loc']
        tou_sk = get_source_key(data_record['tou_name'], data_record['tou_date'], data_record['tou_year'])
        record['source_key'] = tou_sk
        record['reference_url'] = data_record['start_url']
        record['game_datetime'] = data_record['start_date']
        record['tournament'] = get_refined_tou_name(data_record['tou_name'])
        record['time_unknown'] = 1
        record['result'] = players.update({'0': {'winner': data_record['winner_id']}})
        yield record
    def parse_details(self, response):
        hxs = Selector(response)
        data_record = response.meta['data_record']
        tou_name = data_record['tou_name']
        players = {}
        result_final = {}
        tou_sk = get_source_key(data_record['tou_name'], data_record['tou_date'], data_record['tou_year'])

        record['rich_data'] = get_richdata(CHANNELS, GAME_NOTE, data_record['stadium'], data_record['loc'])
        record['source_key'] = tou_sk
        record['reference_url'] = data_record['start_url']
        record['game_datetime'] = data_record['start_date']
        record['location_info'] = data_record['loc']
        record['game_status'] = data_record['game_status']
        record['tz_info'] = data_record['tz_info']

        winner_id = data_record['winner_id']
        nodes = get_nodes(hxs, '//div[@id="leaderboardTable"]//ul[@class="dataRow"]')
        if not nodes:
            nodes = get_nodes(hxs, '//div[@id="leaderboardTable"]//ul[contains(@class, "dataRow")]')
        count = 0
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
                        extract_data(node, './li[@class="hname2"]/span/a/@href')\
                        or extract_data\
                        (node, './/a[@title="Player Profile"]/@href')))
            to_par = extract_data(node, COMPLETED_RESULT % "topar")
            round1 = extract_data(node, COMPLETED_RESULT % "r1")
            round2 = extract_data(node, COMPLETED_RESULT % "r2")
            round3 = extract_data(node, COMPLETED_RESULT % "r3")
            round4 = extract_data(node, COMPLETED_RESULT % "r4")
            total = extract_data(node, COMPLETED_RESULT % "total")
            players.update({player_id: (0, player_name)})
            result_ = get_result(winner_id, player_id, total, pl_pos, round1, round2, round3, round4, to_par)
            result_final.update(result_)
        record['time_unknown'] = 1
        record['participants'] = players
        record['result'] = result_final
        record['tournament'] = get_refined_tou_name(tou_name)
        yield record

    def parse_ongoing(self, response):
        hxs = Selector(response)
        data_record = response.meta['data_record']
        status = data_record['game_status']
        winner_id = data_record['winner_id']
        tou_sk = get_source_key(data_record['tou_name'], data_record['tou_date'], data_record['tou_year'])
        players = {}
        result = {}

        if status == 'ongoing':
            game_note = extract_data(hxs, '//div[@class="c"]/div[contains(@style, "width: 25%")]/text()').replace('Status:', '').replace(',', ' - ').strip()
            if ',' in game_note:
                _round, num = re.findall('([A-Za-z]+)(\d+)', game_note.split('-')[0].strip())[0]
                current_round = _round + ' ' + num + ' - ' + game_note.split('-')[1].strip()
            elif 'Round' in game_note:
                _round, num = re.findall('([A-Za-z]+)(\d+)', game_note.split('-')[0].strip())[0]
                current_round = _round + ' ' + num + ' - ' + game_note.split('-')[1].strip()
            elif len(game_note.split(' ')) == 1:
                current_round = game_note.strip()
            else:
                cur_rn_list = re.findall('([A-Za-z]+) ([a-z]+) ([A-Za-z]+)(\d+)', game_note)
                if cur_rn_list:
                    one, two, _round, num = re.findall('([A-Za-z]+) ([a-z]+) ([A-Za-z]+)(\d+)', game_note)
                    current_round = one + ' ' + two + ' ' + _round + ' ' + num
            if 'Suspended' in game_note:
                current_round = 'Suspended'
            current_round = current_round
        else:
            current_round = ''
        nodes = get_nodes(hxs, '//table[@id="lbl"]//tr[not(contains(@id,"boardPromo"))]')
        for node in nodes:
            position = extract_data(node, './/td[@class="b"]/text()')
            if not position:
                continue
            pl_pos = get_position(position).replace('\n', '')
            player_name = extract_data(node, './/td[@class="nm"]/div[@class="nm"]/text()')
            player_id = extract_data(node, './@id')
            round1 = extract_data(node, ONGOING_RESULT % '1')
            round2 = extract_data(node, ONGOING_RESULT % '2')
            round3 = extract_data(node, ONGOING_RESULT % '3')
            round4 = extract_data(node, ONGOING_RESULT % '4')
            to_par = extract_data(node, './/td[9]/text()')
            total = extract_data(node, './/td[16]/text()')
            players.update({player_id: (0, player_name)})
            result = get_result(winner_id, player_id, total, pl_pos, round1, round2, round3, round4, to_par)
            result_final.update(result)

        record['participants'] = players
        record['result'] = result_final
        record['tournament'] = get_refined_tou_name(data_record['tou_name'])
        record['location_info'] = data_record['loc']
        record['rich_data'] = get_richdata(CHANNELS, GAME_NOTE, data_record['stadium'], data_record['loc'])
        record['source_key'] = tou_sk
        record['reference_url'] = data_record['start_url']
        record['game_datetime'] = data_record['start_date']
        record['game_status'] = status
        record['time_unknown'] = 1
        record['tz_info'] = data_record['tz_info']
        yield record
