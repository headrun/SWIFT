# coding=utf8
# -*- coding: utf8 -*-
# vim: set fileencoding=utf8 :
import re
import time
import datetime
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data, log, get_utc_time, get_tzinfo

def get_tournament(data):
    tou_dict = {'Major League Soccer' : 'MLS Soccer', 'Primera A de Ecuador' : 'Ecuadorian Serie A',\
                'Primera División de Paraguay' : 'Paraguan Primera Division', \
                'Primera Division de Paraguay' : 'Paraguan Primera Division', \
                'Primera Profesional de Peru' : 'Peruvian Primera Division', \
                'English FA Community Shield' : 'FA Community Shield', \
                'German Super Cup': 'DFL-Supercup', 'Fútbol Profesional Colombiano' : 'Futbol Profesional Colombiano',
                'English League Two': 'Football League Two', 'English League One': 'Football League One',
                'South African Premiership': "Premier Soccer League", "Japanese J League": "J. League Division 1",
                'German 2. Bundesliga': "German Bundesliga 2", "Mexican Liga MX": "Liga MX",
                "Primera Division de Honduras": "Honduras Primera Division",
                "Italian Serie B": "Serie B", "French Ligue 2": "Ligue 2",
                "Dutch Eerste Divisie": "Eerste Divisie", 
                "Scottish League Two": "Scottish Football League Third Division", \
                "Scottish League One": "Scottish Football League Second Division", \
                "Primera División de Costa Rica": "Costa Rican Primera Division"}
    tou = tou_dict.get(data, '')
    if tou:
        return tou
    else:
        return data

def encode_data(data, val="utf-8"):
    try:
        return ''.join([chr(ord(x)) for x in data]).encode(val)
    except ValueError:
        return data.encode(val)

def clean_text(data):
    data = data.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').strip()
    data = data.rstrip().lstrip().strip()
    return data

def get_location_details(data):
    string = data.split(',')
    if len(string) == 4:
        string = string[:2] + [string[2] + string[3]]
        stadium, city, country = tuple(string)
    elif len(string) == 3:
        stadium, city, country = tuple(string)
    elif len(string) == 2:
        stadium, city = tuple(string)
        country = ''
    elif len(string) == 1:
        stadium = string[0]
        country = city = ''
    return stadium.strip(), city.strip(), country.strip()

def get_game_status(data):
    status = {'ft' : 'completed', 'ht' : 'ongoing', ':' : 'scheduled', 'canc' : 'cancelled', 'off' : 'cancelled',\
              'posp' : 'postponed', 'post' : 'postponed', 'aban' : 'cancelled', 'aet' : 'completed', \
              'susp' : 'suspended', 'p - p' : 'postponed', 'gmt' : 'scheduled'}
    for key, status in status.iteritems():
        if key in data:
            return status

    status = ''
    return status

class ESPNSoccer(VTVSpider):
    name = 'espnsoccer'
    allowed_domains = ['www.espnfc.com', 'espn.go.com']
    start_urls = []

    def start_requests(self):
        urls = []
        required_tournaments = [i.strip() for i in open('allowed_soccer_tournaments.txt', 'r').readlines()]
        games  = {'soccer': 'http://soccernet.espn.go.com/scores?date=%s'}
        next_week_days = []

        if "schedule" in self.spider_type:
            for i in range(0, 400):
                next_week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        else:
            for i in range(0,20):
                next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))

        for game, top_url in games.iteritems():
            for wday in next_week_days:
                url = top_url %wday
                url =  'http://soccernet.espn.go.com/scores?date=20150117'
                yield Request(url, callback=self.parse, meta = {'game' : game, 'leagues_list' : required_tournaments})

    def parse(self, response):
        sel = Selector(response)
        leagues = get_nodes(sel, '//div[@id="score-leagues"]/div[@class="score-league"]')
        for league in leagues:
            league_name = clean_text(extract_data(league, './h4/a/text()'))

            if "Relegation" in league_name:
                league_name = league_name.replace('Playoffs', '').strip()

            league_name = encode_data(league_name)
            if "Scottish League One" in league_name:
                print league_name
                league_link = extract_data(league, './h4/a/@href').strip()

                if league_link and "http" not in league_link:
                    league_link = "http://www.espnfc.com" + league_link
                    print league_link
                yield Request(league_link, callback=self.game_participants, meta = {'league_name' : league_name})

    def game_participants(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//div[@class="scores"]/div[@class="score-box"]/div[contains(@class, "full")]')
        for node in nodes:
            record = SportsSetupItem()
            game_id = extract_data(node, './@data-gameid') 

            status = extract_data(node, './/div[@class="game-info"]/span[contains(@class, "time")]/text()')
            if status:
                status = get_game_status(status.lower())

            if status != 'scheduled':
                date = response.url.split('date=')[1]
                game_datetime = get_utc_time(date, '%Y%m%d', "GMT")
            else:
                date_time = extract_data(node, './/div[@class="game-info"]/span[contains(@class, "time")]/@data-time').split('.')[0].replace('T', ' ')
                game_datetime = date_time
#                game_datetime = get_utc_time(date_time, "%Y-%m-%dT%H:%M:%S", "")

            league_name = get_tournament(response.meta['league_name'])

            hm_tm_nm = extract_data(node, './/div[@class="team-names"]/div[@class="team-name"][1]/span/text()')
            aw_tm_nm = extract_data(node, './/div[@class="team-names"]/div[@class="team-name"][2]/span/text()')

            event_name = ''
            if "MLS All-Stars" in hm_tm_nm or "MLS All-Stars" in aw_tm_nm:
                event_name = "MLS All-Star Game"

            record['tournament'] = league_name
            record['event'] = event_name
            record['game_datetime'] = game_datetime
            record['game_status'] = status
            record['source_key'] = game_id
            record['source'] = 'espn_soccer'
            record['participant_type'] = 'team'
            record['affiliation'] = 'club-football'

            gamecast_link = "http://www.espnfc.com/gamecast/%s/gamecast.html" % game_id
            yield Request(gamecast_link, callback=self.game_scores_indetail, meta = {'record' : record})

    def game_scores_indetail(self, response):
        sel = Selector(response)
        rich_data = {}
        stadium = state = country = ''
        match_details = get_nodes(sel, '//section[contains(@class, "amecast-match")]')

        aw_tm_nm = extract_data(match_details, './/div[@class="matchup"]/div[@class="team away"]//p[contains(@class,"team-name")]/a/text()')
        aw_tm_id = extract_data(match_details, './/div[@class="matchup"]/div[@class="team away"]/@id').split('-')[1]
        hm_tm_nm = extract_data(match_details, './/div[@class="matchup"]/div[@class="team home"]//p[contains(@class,"team-name")]/a/text()')
        hm_tm_id = extract_data(match_details, './/div[@class="matchup"]/div[@class="team home"]/@id').split('-')[1]
        if hm_tm_id == "7082":
            hm_tm_id = "TEAM7082"
        elif aw_tm_id == "7082":
            aw_tm_id = "TEAM7082"
        if hm_tm_id == "5326":
            hm_tm_id = "TEAM5326"
        elif aw_tm_id == "5326":
            aw_tm_id = "TEAM5326"

        if hm_tm_id == "5323":
            hm_tm_id = "TEAM5323"
        elif aw_tm_id == "5323":
            aw_tm_id = "TEAM5323"
        if hm_tm_id == "6990":
            hm_tm_id = "TEAM6990"
        elif aw_tm_id == "6990":
            aw_tm_id = "TEAM6990"



        participants = {hm_tm_id : (1, hm_tm_nm), aw_tm_id : (0, aw_tm_nm)}

        location_details = extract_data(match_details, './div[@class="match-details"]/p[2]/text()')
        if location_details:
            stadium, city, country = get_location_details(location_details)

        rich_data['stadium'] = stadium
        rich_data['country'] = country
        rich_data['city'] = city

        tzinfo = ''
        if city:
            tzinfo = get_tzinfo(city = city)
        elif country:
            tzinfo = get_tzinfo(country = country)

        winner = hm_scr = aw_scr = scores = ''
        if self.spider_type != 'scheduled':
            scores = extract_data(match_details, './/div[@class="matchup"]//div[@class="score-time"]//p[@class="score"]/text()')
            if scores and '-' in scores:
                hm_scr = scores.split('-')[1].strip()
                aw_scr = scores.split('-')[0].strip()

        if response.meta['record']['game_status'] == 'completed':
            if int(hm_scr) > int(aw_scr):
                winner = hm_tm_id
            elif int(hm_scr) < int(aw_scr):
                winner = aw_tm_id

        results = {'0' :{'winner' : winner, 'score' : scores}, hm_tm_id : {'final' : hm_scr, 'H1' : ''}, aw_tm_id : {'final' : aw_scr, 'H1' : ''}}

        away_half_goals = get_nodes(match_details, './/div[@class="matchup"]/div[@class="team away"]//ul[@class="goal-scorers"]/li')
        aw_hg_sc  = 0
        for aw_hg in away_half_goals:
            gtime = extract_data(aw_hg, './strong/text()').split("'")[0]
            if gtime and int(gtime) <= 45:
                aw_hg_sc += 1

        home_half_goals = get_nodes(match_details, './/div[@class="matchup"]/div[@class="team home"]//ul[@class="goal-scorers"]/li')
        hm_hg_sc = 0
        for hm_hg in home_half_goals:
            gtime = extract_data(hm_hg, './strong/text()').split("'")[0]
            if gtime and int(gtime) <= 45:
                hm_hg_sc += 1

        results = {'0' :{'winner' : winner, 'score' : scores}, hm_tm_id : {'final' : hm_scr, 'H1' : hm_hg_sc}, aw_tm_id : {'final' : aw_scr, 'H1' : aw_hg_sc}}
        response.meta['record']['participants'] = participants
        response.meta['record']['result'] = results
        response.meta['record']['rich_data'] = rich_data
        response.meta['record']['game'] = 'soccer'
        response.meta['record']['reference_url'] = response.url
        response.meta['record']['tz_info'] = tzinfo
        import pdb;pdb.set_trace()
        yield response.meta['record']

