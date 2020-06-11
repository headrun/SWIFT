# -*- coding: utf-8 -*-
from vtvspider_new import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest
from scrapy_spiders_dev.items import SportsSetupItem
import re


COUNTRY_DICT = {'Las Vegas': ['Las Vegas', '', 'Nevada'], \
                    'Beijing': ['Beijing', '', 'China'], \
                    'California': ['', 'California', 'USA'], \
                    'New York': ['New York', 'New York', 'USA'], \
                    'Washington': ['Washington', '', 'USA'], \
                    'Macao': ['Macau', '', 'China'], \
                    'Republica Dominicana': ['', '', 'Dominican Republic'], \
                    'Finlandia': ['', '', 'Finland']
                    }


def get_location(venue):
    loc             = {}
    if len(venue.split(',')) == 2:
        _details    = venue.split(',')
        stadium     = ""
        city        = ",".join(_details).split(',')[0].strip()
        country     = ",".join(_details).split(',')[1].strip()
        state       = ''
    elif len(venue.split(',')) ==3 :
        _details    = venue.split(',')
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[-1].strip()
        if country == city:
            country = ''
        state       = ''
    elif len(venue.split(',')) == 1:
        country     = venue
        city = state = stadium = ''
    else:
        stadium = city = country = state = ''

    det = COUNTRY_DICT.get(country, [])
    if det:
        city = det[0]
        state = det[1]
        country = det[2]

    if city == "Coliseo Roberto Clemente San Juan":
        city = "San Juan"
        country = "Puerto Rico"
        stadium = "Roberto Clemente Coliseum"

    if "Mexico City" in stadium:
        stadium = "Mexico City Arena"
    if "Dynamo Palace Of Sports" in stadium:
        stadium = "Dynamo Sports Palace"

    loc = {'city' : city, 'state' : state, \
                 'country' : country.replace('United Kingdom', 'UK'). \
                 replace('Usa', 'USA').replace('United States', 'USA'). \
                replace('Rusia', 'Russia'), 'stadium' : stadium}

    return loc


class WBASpider(VTVSpider):
    name = "wba_spider"
    allowed_domains = []
    start_urls = []

    def start_requests(self):
        if self.spider_type == "scores":
            top_url = "http://www.wbanews.com/wba-fights-master-scores"
            yield Request(top_url, callback = self.parse_scores)

        elif self.spider_type == "schedules":
            top_url = "http://www.wbanews.com/schedule-of-wba-title-fights"
            yield Request(top_url, callback = self.parse_schedules)


    def parse_scores(self, response):
        sel  = Selector(response)
        yr_nodes = get_nodes(sel, '//link[@rel="archives"]')

        for node in yr_nodes:
            yr_ = extract_data(node, './/@href')
            year = yr_.split('/')[-2]
            month_ = yr_.split('/')[-1]

            if '2014' in year or '2015' in year:
                yield FormRequest(url="http://www.wbanews.com/index.php?page_id=4094", method = "POST",
                                formdata={'smonth': month_, 'syear': year, 'submit': 'buscar'},
                            callback=self.parse_details, meta={"ref_url": response.url})


    def parse_details(self, response):
        sel  = Selector(response)
        game_nodes = get_nodes(sel, '//div[@id="master_scores"]//table')

        for game_node in game_nodes:
            url = extract_data(game_node, './/td//a[contains(@href, "scores-analysis")]//@href')

            if not url:
                continue

            loc_info = extract_list_data(game_node, './/tr[@class="ranking-dark"]//td//text()')[0].strip().title()
            loc = get_location(loc_info)
            event = extract_list_data(game_node, './/tr[@class="ranking-clear"]//td//text()')[0].strip()
            winner = extract_data(game_node, './/tr//td//a[@class="winner"]//@href').strip()
            winner = "".join(re.findall(r'boxer=(\d+)', winner))
            pl_names = extract_list_data(game_node, './/tr//td//a[contains(@href, "&boxer")]//@href')
            home_pl = "".join(re.findall(r'boxer=(\d+)', pl_names[0]))
            away_pl = "".join(re.findall(r'boxer=(\d+)', pl_names[1]))

            if "http" not in url:
                url = "http://www.wbanews.com"  + url

            yield Request(url, callback = self.parse_scores_details, \
                meta = {'loc': loc, 'winner': winner, 'home_pl': home_pl, \
                'away_pl': away_pl, 'event': event, 'loc_info': loc_info})

    def parse_scores_details(self, response):
        sel  = Selector(response)
        record = SportsSetupItem()

        winner = response.meta['winner']
        loc = response.meta['loc']
        home_pl = response.meta['home_pl']
        away_pl = response.meta['away_pl']
        event = response.meta['event'].replace("FEMALE", "Women's"). \
        replace('(UNIFICATION)', '').replace('WBA/IBF/WBO', '').strip(). \
        replace('/WBA', '').replace('TITLE', '').strip().replace('WORLD', ''). \
        replace('(INTERIM)', ''). replace('WBA', ''). \
        replace('(VACANT)', ''). replace('/WBC', ''). \
        replace('/WBO', '').replace('/IBF', '').replace('  ', ' ').strip()
        game_scores = extract_list_data(sel, '//table//tr//td//b[contains(text(), "THEORICAL SCORE")]//following-sibling::b//text()')
        home_scores = game_scores[0]
        away_scores = game_scores[-1]
        game_date = extract_data(sel, '//div[@id="master_scores"]//h4//text()')
        game_date = game_date.split(' - ')[-1].strip()
        game_datetime = get_utc_time(game_date, '%Y-%m-%d', 'US/Eastern')

        game_id = game_datetime.split(' ')[0] + "_" + home_pl + "_" + away_pl
        game_id = game_id.replace('-', '_').strip()

        event = "WBA - " + event.replace(' CHAMPIONSHIP', '').title().replace('Super Super', 'Super').strip()
        event = event.replace('Super Welterweight', 'Light middleweight').replace('Super Lightweight', 'Light welterweight').strip()
        if event == "WBA - Minimum":
            event = "WBA - Minimumweight"
        if event == "WBA - Cruiserweight":
            event =  "WBA - Cruiserweight (boxing)"

        record['affiliation'] = "wba"
        record['event'] = event
        record['game'] = "boxing"
        record['participant_type'] = "player"
        record['source'] = "wba_boxing"
        record['time_unknown'] = '1'
        record['tournament'] = "World Boxing Association"
        record['game_datetime'] = game_datetime
        record['source_key'] = game_id
        record['game_status'] = "completed"
        record['reference_url'] = response.url
        record['participants'] = { home_pl: ('1',''), away_pl: ('0','')}
        record['rich_data'] = {'location': loc}
        tz_info = get_tzinfo(city = loc['city'], game_datetime = game_datetime)
        record['tz_info'] = tz_info

        if not tz_info:
            record['tz_info'] = get_tzinfo(country = loc['country'], \
                        game_datetime = game_datetime)
        record['result'] = {'0': {'score': home_scores + " - " + away_scores, 'winner': winner},
                            home_pl: {'final': home_scores}, away_pl: {'final': away_scores}}
        import pdb;pdb.set_trace()
        yield record




    def parse_schedules(self, response):
        sel  = Selector(response)
        nodes = get_nodes(sel, '//table[@id="schedule"]')
        record = SportsSetupItem()

        for node in nodes:
            event = extract_list_data(node, './/td[@class="schedule-title"]//text()')[-1].strip()
            event = "WBA - " + event.replace(' CHAMPIONSHIP', '').replace("FEMALE", "Women's").replace('  ', ' ').strip()
            if event == "WBA - MIDDLEWEIGHT SUPER":
                event = "WBA - SUPER MIDDLEWEIGHT"
            date_ = extract_data(node, './/td[contains(text(), ",")]//text()') 
            game_date = date_.split('-')[0].strip()
            loc = date_.split('-')[-1].strip()
            pl_name = extract_list_data(node, './/td[@class="schedule-names"]//a//@href') 
            home_pl = "".join(re.findall(r'boxer=(\d+)', pl_name[0]))
            away_pl = "".join(re.findall(r'boxer=(\d+)', pl_name[1]))
            city = state = country = stadium = ''
            if len(loc.split(',')) == 2:
                city = loc.split(',')[-1].strip()
                stadium = loc.split(',')[0].strip()

            elif len(loc.split(',')) ==3:
                stadium = loc.split(',')[0].strip()
                city = loc.split(',')[1].strip() 
                state = loc.split(',')[-1].strip()

            game_datetime = get_utc_time(game_date, '%B %d, %Y', 'US/Eastern')
            game_id = game_datetime.split(' ')[0] + "_" + home_pl + "_" + away_pl
            game_id = game_id.replace('-', '_').strip()

            record['affiliation'] = "wba"
            record['event'] = event
            record['game'] = "boxing"
            record['participant_type'] = "player"
            record['source'] = "wba_boxing"
            record['time_unknown'] = '1'
            record['tournament'] = "World Boxing Association"
            record['game_datetime'] = game_datetime
            record['source_key'] = game_id
            record['game_status'] = "scheduled"
            record['reference_url'] = response.url
            record['participants'] = { home_pl: ('1',''), away_pl: ('0','')}
            record['rich_data'] = {'location': {'city': city, 'country': country, \
                                               'state': state, \
                                               'stadium': stadium}}
            tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
            record['tz_info'] = tz_info
            record['result'] = {}

            if not tz_info:
                record['tz_info'] = get_tzinfo(country = country, \
                            game_datetime = game_datetime)
            import pdb;pdb.set_trace()
            yield record
