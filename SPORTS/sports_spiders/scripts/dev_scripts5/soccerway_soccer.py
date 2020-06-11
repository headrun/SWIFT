# -*- coding: utf-8 -*-
from pytz import *
import re
import time
import json
import datetime
import urllib
from datetime import timedelta
from vtvspider_dev import VTVSpider, extract_data, extract_list_data
from vtvspider_new import get_nodes, get_tzinfo, get_utc_time
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

days = []
_now = datetime.datetime.now().date()
yday  = (_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
days.append(yday)
today =  _now.strftime("%Y-%m-%d")
days.append(today)
tomo =  (_now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
days.append(tomo)


def get_gmt_time(date_val, pattern):
    t =  datetime.datetime.utcnow()
    utc = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
    t =  datetime.datetime.now()
    now = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
    timedelta = utc - now
    date_val = urllib.quote(date_val)
    date_val = urllib.unquote(date_val)
    date_val = date_val.replace('%A0', ' ').replace('Noon', '12:00 PM')
    et = time.strptime(date_val, pattern)#'%Y %a, %b %d  %H:%M %p ET')
    et_dt = datetime.datetime(et.tm_year, et.tm_mon, et.tm_mday, et.tm_hour, et.tm_min, et.tm_sec)
    gmt = et_dt+timedelta
    gmt = gmt.strftime('%Y-%m-%d %H:%M:%S')
    return gmt

STADIUM_DICT = {'allianz-arena': 'Allianz Arena',
                'audi-sportpark': 'Audi Sportpark',
                'stadion im borussia-park': 'Borussia-Park',
                'hdi-arena': 'Niedersachsenstadion',
                'volksparkstadion': 'Imtech Arena',
                'mercedes-benz-arena': 'Mercedes-Benz Arena',
                'signal-iduna-park': 'Westfalenstadion'}

CITY_DICT  = {'frankfurt am main': 'Frankfurt',
              u'k\xf6ln': 'Cologne', u'm\xfcnchen': 'Munich',
              'newcastle-upon-tyne': 'Newcastle upon Tyne'}

class SoccerwaySoccer(VTVSpider):
    name                = "soccerway_soccer"
    start_urls          = ['http://int.soccerway.com/competitions/club-domestic/?ICID=TN_03_01']
    dont_filter         = True

    leagues_list    = []
    leagues         = {'Argentina' : 'argentina/primera-division',
                        'Chile'     : 'chile/primera-division',
                        'Bolivia'   : 'bolivia/lfpb',
                        'Uruguay'   : 'uruguay/primera-division',
                        'Brazil'    : 'brazil/serie-a',
                        'Venezuela' : 'venezuela/primera-division',
                        'China PR'  : 'national/china-pr/csl',
                        'Peru'      : 'national/peru/primera-division',
                        'England'   : 'national/england/premier-league/',
                        'Spain'     : '/national/spain/primera-division/',
                        'Germany'   : '/national/germany/bundesliga/',
                        'Italy'     : '/national/italy/serie-a/',
                        'France'    : '/national/france/ligue-1/',
                        'Scotland'  : '/national/scotland/premier-league/',
                        'Wales'     : '/national/wales/premier-league/',
                        'Northern Ireland': '/national/northern-ireland/ifa-premiership/'}
    leagues     = {
                    'Norway': '/eliteserien/',
                    'Sweden': '/allsvenskan/',
                    'Finland': '/veikkausliiga/',
                    'Iceland': '/urvalsdeild/',
                    'Denmark': '/denmark/superliga/'}


    eng_leagues = {'League Cup'   : 'national/england/league-cup/',
                'Premier League'   : 'national/england/premier-league/'}

    eng_tou_dict     = {'League Cup'     : 'English League Cup',
                       'Premier League' : 'English Premier League'}

    tou_dict        = {'Argentina' : 'Argentine Primera Division',
                        'Chile'     : 'Chilean Primera Division',
                        'Uruguay'   : 'Uruguan Primera Division',
                        'Bolivia'   : 'Liga de Futbol Profesional Boliviano',
                        'Brazil'    : 'Campeonato Brasileiro Serie A',
                        'Venezuela' : 'Venezuelan Primera Division',
                        'China PR'  : 'Chinese Super League',
                        'Peru'      : 'Peruvian Primera Division',
                        'England'   : 'English League Cup',
                        'Spain'     : 'Spanish Liga',
                        'Germany'   : 'German Bundesliga',
                        'Italy'     : 'Italian Serie A',
                        'France'    : 'French Ligue 1',
                        'Scotland'  : 'Scottish Premier League',
                        'Wales'     : 'Welsh Premier League',
                        'Northern Ireland': 'NIFL Premiership'}
    tou_dict    = {
                    'Norway': 'Tippeligaen',
                    'Sweden': 'Allsvenskan',
                    'Finland': 'Veikkausliiga',
                    'Iceland': 'Urvalsdeild',
                    'Denmark': 'Danish Superliga'}

    next_page_link  = '''http://int.soccerway.com/a/block_competition_matches_summary?block_id=%s&callback_params={"page":%s,"bookmaker_urls":[],"block_service_id":"competition_summary_block_competitionmatchessummary","round_id":%s,"outgroup":false,"view":2}&action=changePage&params={"page":%s}'''


    def parse(self, response):
        hdoc = Selector(response)
        leagues = get_nodes(hdoc, '//li[@class="expandable "]/div[@class="row"]/a')

        for league in leagues:
            league_name = extract_data(league, './text()')

            if league_name in self.leagues.keys():
                league_link = extract_data(league, './@href')

                if league_link:
                    league_link = "http://int.soccerway.com" + league_link
                    yield Request(league_link, callback = self.parse_next,
                                  meta = {'next_page' : 0, 'check':0,
                                          'other_page': 0})

    def parser_gametime(self, epoch):
        if self.spider_type == 'scores':
            T               = time.gmtime(float(epoch[0]))
            timedelta       = datetime.timedelta(hours=4)
            et_dt           = datetime.datetime(T.tm_year, T.tm_mon, T.tm_mday, T.tm_hour, T.tm_min, T.tm_sec)
            game_date       = (et_dt + timedelta).strftime('%Y-%m-%d %H:%M:%S')
        else:
            T                = time.strftime('%Y-%m-%d',  time.gmtime(int(epoch[0])))
            game_date        = get_gmt_time(T, '%Y-%m-%d')

        return game_date

    def parse_next(self, response):
        hdoc    = Selector(response)
        tou_link = ''
        if 'final' in response.url:
            url = extract_data(hdoc, '//a[contains(text(), "Group stage")]/@href')
            if url:
                url = "http://www.soccerway.com" + url
                yield Request(url, self.parse_next, meta = {  'next_page' : 0, 'check':0, 'other_page': 1})
        nodes   = get_nodes(hdoc, '//tr[contains(@class,"expanded    match no-date-repetition")]')
        if not response.meta['other_page']:
            title = extract_data(hdoc, '//div[@class="small-column"]//h2//text()')
            links = get_nodes(hdoc, '//div[@class="content plain "]//ul[@class="left-tree"]//li')

            for link in links:
                url     = extract_data(link, './a/@href')
                if title == 'England':
                    header = extract_data(hdoc, '//h1/text()')
                    if self.eng_leagues[header] in url:
                        url     = "http://www.soccerway.com" + url
                        tou_link = self.eng_leagues[header].replace('national', '')
                        yield Request(url, callback = self.parse_next,
                                    meta = {  'next_page' : 0, 'check':0,
                                              'other_page': 1,
                                              'tou_link'  : tou_link})
                else:
                    if self.leagues[title] in url:
                        url     = "http://www.soccerway.com" + url
                        tou_link  = self.leagues[title].replace('national', '')
                        yield Request(url, callback = self.parse_next,
                                  meta = {  'next_page' : 0, 'check':0,
                                            'other_page': 1,
                                            'tou_link'  : tou_link})

        elif response.meta['check']:
            lines           = response.body.strip().replace("\\", "").replace('\"', "").split(" ")
            has_next_page   = re.findall('"has_next_page":"(\d+)', response.body)
            round_id        = re.findall('"round_id":"(\d+)', response.body)
            prev_next_page  = re.findall('"has_previous_page":"(\d+)', response.body)

            for line in lines:
                L = re.findall('href=(/matches.*/)', line)

                if L:
                    if response.meta.get('tou_link', '') in L[0]:
                        url = "http://int.soccerway.com" + L[0]
                        yield Request(url, callback = self.parse_scores,
                                      meta = {'details' : 0})

            round_id        = round_id[0]
            prev_page_num   = ''
            next_page_num   = ''
            if has_next_page and self.spider_type == 'schedules':
                prev_page_num   = int(response.meta['next_page']) + 1
                next_id         = 'page_competition_1_block_competition_matches_summary_6'
                next_page_num   = response.meta['prev_page'] + 1
                next_page_link  = self.next_page_link % (next_id, prev_page_num, round_id, next_page_num)

                yield Request(next_page_link, callback = self.parse_next,
                              meta = {  'check'     : 1,
                                        'next_page' : prev_page_num,
                                        'prev_page' : next_page_num,
                                        'other_page': 1})
            if prev_next_page:
                next_id         = 'page_competition_1_block_competition_matches_summary_6'
                if response.meta.get('page', ''):
                    prev_page = response.meta.get('page', '')
                    page_no       = response.meta.get('next_page_', '')
                else:
                    res_data = json.loads(response.body)
                    page_no  = res_data['commands'][2]['parameters']['params']['page']
                    prev_page = int(page_no) + 1
                next_page  = int(page_no) - 1
                next_page_link  = self.next_page_link % (next_id, prev_page, round_id, page_no)
                yield Request(next_page_link, callback = self.parse_next,
                            meta = {'check'     : 1,
                                    'page'      : page_no,
                                    'next_page_' : next_page,
                                    'other_page': 1,
                                    'next_page' : prev_page_num,
                                    'prev_page' : next_page_num})


        else:
            if '1st-round' in response.url:
                game_note = 'First Round'
            else:
                game_note = ''

            if not nodes:
                nodes   = get_nodes(hdoc, '//tr[contains(@class,"  match no-date-repetition")]')
            for node in nodes:
                game_note = ''
                if '/fa-cup/' in response.url:
                    game_note = self.get_game_note(response.url)
                if extract_data(node, './preceding-sibling::tr[1]/td[@class="aggr"]/text()'):
                    aggr = extract_data(node, './preceding-sibling::tr[1]/td[@class="aggr"]/text()')
                    if aggr:
                        game_note = 'Leg 1'
                    else:
                        game_note = 'Leg 2'
                game_link       = extract_data(node, './/td[contains(@class,"score-time")]/a/@href')
                g_time          = extract_list_data(node, './@data-timestamp')
                game_date       = self.parser_gametime(g_time)
                #if "00:00:00" in game_date or "04:00:00" in game_date or "05:00:00" in game_date:
                    #time_unknown = 1
                #else:
                    #time_unknown = ''
                home_link       = extract_data(node, './/td[contains(@class, "team team-a ")]/a/@href')
                if not home_link:
                    home_link       = extract_data(node, './/td[contains(@class, "team team-a ")]/a/@href')
                away_link       = extract_data(node, './/td[contains(@class, "team team-b ")]/a/@href')

                if not away_link:
                    away_link = extract_data(node, './/td[contains(@class, "team team-b ")]/a/@href')
                if not (home_link or away_link):
                    continue
                home_id         = home_link.split('/')[-2]
                away_id         = away_link.split('/')[-2]

                if game_link:
                    game_link = "http://int.soccerway.com" + game_link
                    date_compare = game_date.split(' ')[0]
                    if date_compare in days and self.spider_type == 'scores':
                        yield Request(game_link, callback = self.parse_scores,
                                  meta = {'home_id'   : home_id,
                                          'away_id'   : away_id,
                                          'details'   : 1,
                                          'game_note': game_note})
                    elif self.spider_type == 'schedules':
                        yield Request(game_link, callback = self.parse_scores,
                                meta = {'home_id'   : home_id,
                                        'away_id'   : away_id,
                                        'details'   : 1,
                                        'game_note': game_note})

        next_page_link = extract_data(hdoc, '//span[@class="nav_description"]//a[@class="previous "]//text()')
        if next_page_link and tou_link in response.url:
            if not "callback_params" in response.url:
                round_id = response.url.split('/')[-2].replace('r','')
            else : round_id = response.meta['round_id']

            next_id = extract_data(hdoc, '//div[@class="pagination match-pagination"]//a[@rel="previous"]/@id')

            if not next_id:
                next_id = response.meta['next_id']
            else:
                next_id = next_id.replace('_next','')

            next_page_text = extract_data(hdoc, '//div[@class="pagination match-pagination"]//a[contains(@id,"previous")]/@class')
            if not next_page_text:
                next_page_text = response.meta['prev_page_text']

            if not response.meta['next_page']:
                next_page_num = response.meta['next_page'] - 1
                prev_page_num = response.meta['next_page']

            next_page_link = self.next_page_link % (next_id, prev_page_num, round_id, next_page_num)
            yield Request(next_page_link, callback = self.parse_next,
                          meta = {'check'    : 1, 'next_page'   : "1",
                                  'prev_page': 0, 'other_page'  : 1})

    def parse_scores(self, response):
        import pdb;pdb.set_trace()
        time_unknown = ''
        hdoc = Selector(response)
        game_dt = extract_list_data(hdoc, '//div[@class="container middle"]//dd//span[@class="timestamp"]/text()')
        if len(game_dt) == 2:
            game_dt = ' '.join(game_dt)
            game_datetime = get_utc_time(game_dt, '%d %B %Y %H:%M', 'CET')
        else:
            game_datetime = extract_list_data(hdoc, '//div[@class="container middle"]//span[@class="timestamp"]/@data-value')
            game_datetime = self.parser_gametime(game_datetime)
            if "00:00:00" in game_datetime or "04:00:00" in game_datetime or "05:00:00" in game_datetime:
                time_unknown = 1

        if not response.meta['details']:
            home_id = extract_data(hdoc, '//div[contains(@class, "block_match_info-wrapper")]//div[@class="content  "]//div[@class="container left"]//h3[@class="thick"]//a/@href')
            away_id = extract_data(hdoc, '//div[contains(@class, "block_match_info-wrapper")]//div[@class="content  "]//div[@class="container right"]//h3[@class="thick"]//a/@href')
            home_id = re.findall('/(\d+)/', home_id)[0]
            away_id = re.findall('/(\d+)/', away_id)[0]
        else:
            home_id         = response.meta['home_id']
            away_id         = response.meta['away_id']

        home_scores = {}
        away_scores = {}
        link        = response.url

        winner = final_score = ''
        is_tie, game, event_name = False, "soccer", ''

        game_sk         = link.rsplit('/')[-2]

        division_name   = extract_data(hdoc, '//div[@class="block  clearfix block_competition_left_tree-wrapper"]//h2/text()')
        league          = extract_data(hdoc, '//div[@class="container middle"]//div[@class="details clearfix"]/dl/dt[contains(text(), "Competition")]/following-sibling::dd/a/text()')
        half_time_scores = extract_data(hdoc, '//div[@class="container middle"]//div[@class="details clearfix"]/dl/dt[contains(text(), "Half-time")]/following-sibling::dd[1]/text()')
        if division_name == 'England':
            tournament = self.eng_tou_dict[league]
        elif division_name:
            tournament = self.tou_dict[division_name]
        if division_name == "China PR":
            affiliation = "afc"
            division_name = "China"
        else:
            affiliation = "club-football"

        if half_time_scores:
            home_half_time_score    = half_time_scores.split(' - ')[0]
            away_half_time_score    = half_time_scores.split(' - ')[1]
        else:
            home_half_time_score    = '0'
            away_half_time_score    = '0'

        venue = extract_data(hdoc, '//div[@class="container middle"]//div[@class="details clearfix"]/dl/dt[contains(text(), "Venue")]/following-sibling::dd/a/text()')
        location_details, tz_info = self.get_location_info(venue, division_name)
        if "(" in venue:
            venue = venue.split('(')[0].strip()

        if STADIUM_DICT.get(venue.lower(), ''):
            venue = STADIUM_DICT.get(venue, '')

        #home team scores
        home_yellow_card, home_red_card, home_goal_stats  = [], [], []
        home_game_stats = get_nodes(hdoc, '//div[@class="container left"]/table[contains(@class,"playerstats lineups")]//td[@class="player large-link"]/following-sibling::td[@class="bookings"]/span')

        for home_game_stat in home_game_stats:
            player_name     = extract_data(home_game_stat, './/../..//a/text()')
            cards           = extract_data(home_game_stat, './img/@src')

            if "/YC.png" in cards:
                y_card = cards
                home_yellow_card.append(y_card)
            elif "/RC.png" in cards:
                r_card = cards[0]
                home_red_card.append(r_card)
            elif "events/G.png" in cards or "events/PG.png" in cards:
                goal_score_time     = extract_data(home_game_stat, './text()')
                home_goal_scorer    = (goal_score_time[0], player_name[0])
                home_goal_stats.append(home_goal_scorer)

        home_yellow_cards   = len(home_yellow_card)
        home_red_cards      = len(home_red_card)

        #away team scores
        away_yellow_card  = away_red_card = away_goal_stats = []

        away_game_stats = get_nodes(hdoc, '//div[@class="container right"]/table[contains(@class,"playerstats lineups")]//td[@class="player large-link"]/following-sibling::td[@class="bookings"]/span')

        for away_game_stat in away_game_stats:
            away_player_name    = extract_data(away_game_stat, './/../..//a/text()')
            cards               = extract_data(away_game_stat, './img/@src')

            if cards:
                cards = cards[0]
                if "/YC.png" in cards:
                    away_y_card = cards[0]
                    away_yellow_card.append(away_y_card)
                elif "/RC.png" in cards or "/Y2C.png" in cards:
                    away_r_card = cards
                    away_red_card.append(away_r_card)
                elif "events/G.png" in cards or "events/PG.png" in cards:
                    away_goal_score_time    = extract_data(away_game_stat, './text()')
                    away_goal_scorer        = (away_goal_score_time[0], away_player_name[0])
                    away_goal_stats.append(away_goal_scorer)

        away_yellow_cards   = len(away_yellow_card)
        away_red_cards      = len(away_red_card)
        away_goal_stats     = away_goal_stats
        home_scores         = {'yellow_cards': home_yellow_cards,
                              'red_cards': home_red_cards}

        away_scores         = {'yellow_cards': away_yellow_cards,
                              'red_cards': away_red_cards}

        if half_time_scores:
            home_scores['H1'] = home_half_time_score
            away_scores['H1'] = away_half_time_score

        if response.meta.get('game_note', ''):
            game_note = response.meta.get('game_note', '')
        else:
            game_note = ''

        score = extract_data(hdoc, '//div[@class="container middle"]/h3[@class="thick scoretime "]/text()')
        sportssetupitem                         = SportsSetupItem()
        sportssetupitem['affiliation'] = affiliation
        sportssetupitem['source']               = "soccerway_soccer"
        sportssetupitem['game']                 = game
        sportssetupitem['reference_url']        = response.url
        location                                = {}
        location['location']                    = location_details
        location['stadium']                     = venue.strip()
        location['game_note']                   = game_note
        sportssetupitem['rich_data']            = location
        sportssetupitem['event']                = event_name
        sportssetupitem['participant_type']     = "team"
        sportssetupitem['tournament']           = tournament
        sportssetupitem['tz_info']              = tz_info
        sportssetupitem['time_unknown']         = time_unknown
        sportssetupitem['source_key']           = game_sk

        if score.strip() == '' and self.spider_type == 'schedules':
            status  = "scheduled"
            sportssetupitem['game_status']          = status
            participants                            = {home_id : (1, ''), away_id : (0, '')}
            sportssetupitem['participants']         = participants
            sportssetupitem['game_datetime']        = game_datetime
            yield sportssetupitem

        elif '-' in score.strip() and self.spider_type == 'scores':
            status                  = "completed"
            home_score              = score.split(' - ')[0]
            away_score              = score.split(' - ')[1]
            home_scores['final']    = home_score.strip()
            away_scores['final']    = away_score.strip()

            HG = int(home_score)
            AG = int(away_score)

            if int(HG)>int(AG):
                home_win = 1
                away_win = 0
            elif int(HG)<int(AG):
                home_win = 0
                away_win = 1
            elif int(HG)==int(AG):
                home_win = 1
                away_win = 1
                is_tie = True

            if home_win and away_win:
                winner = ''
            elif home_win:
                winner = home_id
            elif away_win:
                winner = away_id

            sportssetupitem['game_status']     = status
            sportssetupitem['game_datetime']   = game_datetime
            participants                       = {home_id : (1, ''), away_id : (0, '')}
            sportssetupitem['participants']    = participants
            result                             = {'0' : {'winner' : winner, 'score' : score.strip()},
                                                  home_id : home_scores, away_id : away_scores}
            sportssetupitem['result']          = result

            date_compare = game_datetime.split(' ')[0]
            if date_compare in days and status == 'completed' and self.spider_type == 'scores':
                yield sportssetupitem

    def get_location_info(self, venue, country):
        match_venue = venue
        location_details = {}
        city = ""
        state = ''
        tz_info = ""

        venue       = ''.join(venue.split('(')[0].replace('', ''))
        place       = re.findall('\\((.*)\\)', match_venue)
        if place:
            place   = place[0].split(",")
            if len(place) == 2:
                city, state = place[0].strip(), place[1].strip()
            elif len(place) == 1:
                city = place[0].strip()
            if "(" in city:
                city = city.split('(')[-1].strip().replace(')', '').encode('utf-8')
            if CITY_DICT.get(city.lower(), ""):
                city = CITY_DICT.get(city.lower(), '')
            location_details = {'country'   : country.strip(),
                                'city'      : city,
                                'state'     : state}
        if city:
            tz_info = get_tzinfo(city = city)
        if not tz_info and country:
            if country == 'England':
                country = 'United Kingdom'
            tz_info = get_tzinfo(country = country.encode('utf-8') , city= city.encode('utf-8'))

        return location_details, tz_info

    def get_game_note(self, url):
        game_note = ''
        game_note_dict = {'extra-preliminary-round': 'Extra Preliminary Round',
                          'preliminary-round': 'Preliminary Round'}
        for key, value in game_note_dict.iteritems():
            if key in url:
                game_note = value
        return game_note
