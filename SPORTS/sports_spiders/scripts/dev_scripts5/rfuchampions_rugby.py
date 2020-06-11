from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
import datetime

STADIUM_CITY_DICT = {'moseley': ['Billesley Common', '', 'England', '', 'Birmingham'], \
                     'bristol-rugby': ['Ashton Gate Stadium', '', 'England', '', 'Bristol'], \
                     'jersey': ['St. Peter', '', 'England', 'Saint Peter', 'Jersey'], \
                     'worcester': ['Sixways Stadium', '', 'England', '', 'Worcester'], \
                     'yorkshire-carnegie': ['Headingley Stadium', '', 'England', '', 'Leeds'], \
                     'cornish-pirates': ['Mennaye Field', '', 'England', '', 'Cornwall'], \
                     'bedford-blues': ['Goldington Road', '', 'England', '', 'Bedford'], \
                     'london-scottish': ['Athletic Ground, Richmond', '', 'England', '', 'London'], \
                     'nottingham': ['Lady Bay Sports Ground', '', 'England', '', 'Nottingham'], \
                     'doncaster-knights': ['Castle Park rugby stadium', '', 'England', '', 'Doncaster'], \
                     'plymouth-albion': ['The Brickfield', '', 'England', '', 'Devonport'], \
                     'rotherham-titans': ['Clifton Lane', '', 'England', '', 'Rotherham']}

DATE_ = ['December', 'November', 'October', 'September']

class RFUChampions(VTVSpider):
    name = "rfuchampions_spider"
    allowed_domains = ["englandrugby.com"]
    start_urls = ['http://www.englandrugby.com/fixtures-and-results/competitions/greene-king-ipa-championship/']
    record = SportsSetupItem()


    def parse(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        if  self.spider_type == "scores":
            nodes = get_nodes(hxs, '//div[@class="tabs-content"]//div[@class="content active"]//div[@class="row item"]')
            for node in nodes:
                home_sk    = extract_data(node, './/div[contains(@class, "text-right")]//a//@href')
                home_sk    = home_sk.split('/')[-1]
                away_sk    = extract_data(node, './/div[contains(@class, "text-left")]//a//@href')
                away_sk    = away_sk.split('/')[-1]
                game_date  = extract_list_data(node, './/preceding-sibling::div[@class="row fixturedate"]//text()')[-1]
                game_date  = game_date.replace('th ', ' ').replace('st ', ' ').replace('rd ', ' ').replace('nd ', ' ')
                game_time  = extract_data(node, './/div[@class="fr-result small-2 medium-2 large-2"]//span//text()')
                DATES_     = game_date.split(' ')[-1].strip()
                if DATES_ in DATE_:
                    year = "2015"
                else:
                    year = "2016"
                if ":" not in game_time and "TBC" not in game_time:
                    home_final  = game_time.split('-')[0].strip()
                    away_final  = game_time.split('-')[1].strip()
                    final_score = game_time
                    status = "completed"
                    game_datetime = game_date + " " + year
                    pattern = '%A %d %B %Y'
                    time_unknown = 1
                else:
                    continue
                stadium  = extract_data(node, './/div[@class="fr-result small-2 medium-2 large-2"]//div//text()')
                game_datetime = get_utc_time(game_datetime, pattern, 'UTC')
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
                final_stadium = STADIUM_CITY_DICT.get(home_sk, [])
                if final_stadium:
                    stadium  = final_stadium[0]
                    continent = final_stadium[1]
                    country   = final_stadium[2]
                    state     = final_stadium[3]
                    city      = final_stadium[4]
                else:
                    stadium   = stadium
                    continent = ''
                    country   = ''
                    state     = ''
                    city      =  ''

                record['affiliation']   = 'irb'
                record['game_datetime'] = game_datetime
                record['game']          = 'rugby'
                record['source']        = 'england_rugby'
                record['game_status']   = status
                record['tournament']    = "RFU Championship"
                record['event']         = ''
                record['participant_type'] = "team"
                record['source_key']    = game_sk
                record['time_unknown']  = time_unknown
                record['reference_url'] = response.url
                record['participants']  = { home_sk: ('1',''), away_sk: ('0','')}
                record['rich_data']     = {'location': {'city': city, 'country': country, \
                                       'continent': continent, 'state': state, \
                                       'stadium': stadium}, 'game_note': ''}
                tz_info = get_tzinfo(city = city)
                record['tz_info'] = tz_info
                if not tz_info:
                    if country == "England" or country == "Wales":
                        tz_info = get_tzinfo(country = "United Kingdom")
                        record['tz_info'] = tz_info
                    else:
                        tz_info = get_tzinfo(country = country)
                        record['tz_info'] = tz_info
                if city == "Worcester" or city == "Jersey":
                    tz_info = get_tzinfo(country = "United Kingdom")
                    record['tz_info'] = tz_info

                if status == "scheduled":
                    record['result'] = {}
                    yield record
                elif status == "completed":
                    if int(home_final) > int(away_final):
                        winner = home_sk
                    elif int(home_final) < int(away_final):
                        winner = away_sk
                    else:
                        winner = ''

                    totla_score = home_final + "-" + away_final
                    record['result'] = {'0': {'score': totla_score, 'winner': winner}, \
                                    home_sk: {
                                    'final': home_final},
                                    away_sk: {
                                   'final': away_final}}

                    yield record
        elif self.spider_type == "schedules":
            sh_nodes = get_nodes(hxs, '//div[@class="tabs-container"]//div[@class="row item"]')
            for node in sh_nodes:
                home_sk = extract_data(node, './/div[contains(@class, "text-right")]//a//@href')
                home_sk = home_sk.split('/')[-1]
                away_sk = extract_data(node, './/div[contains(@class, "text-left")]//a//@href')
                away_sk = away_sk.split('/')[-1]
                game_date = extract_list_data(node, './/preceding-sibling::div[@class="row fixturedate"]//text()')[-1]
                game_date = game_date.replace('th ', ' ').replace('st ', ' ').replace('rd ', ' ').replace('nd ', ' ')
                game_time = extract_data(node, './/div[@class="fr-result small-2 medium-2 large-2"]//span//text()')
                if "-" in game_time:
                    continue
                DATES_ = game_date.split(' ')[-1].strip()
                if DATES_ in DATE_:
                    year = "2015"
                else:
                    year = "2016"
                if "TBC" in game_time:
                    game_datetime = game_date + " " + year
                    pattern = '%A %d %B %Y'
                else:
                    game_datetime = game_date + " " + year + " "+ game_time
                    pattern = '%A %d %B %Y %H:%M'
                status = "scheduled"
                time_unknown =  0
                stadium  = extract_data(node, './/div[@class="fr-result small-2 medium-2 large-2"]//div//text()')
                game_datetime = get_utc_time(game_datetime, pattern, 'UTC')
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
                final_stadium = STADIUM_CITY_DICT.get(home_sk, [])
                if final_stadium:
                    stadium  = final_stadium[0]
                    continent = final_stadium[1]
                    country   = final_stadium[2]
                    state     = final_stadium[3]
                    city      = final_stadium[4]
                else:
                    stadium   = stadium
                    continent = ''
                    country   = ''
                    state     = ''
                record['affiliation'] = 'irb'
                record['game_datetime'] = game_datetime
                record['game'] = 'rugby'
                record['source'] = 'england_rugby'
                record['game_status'] = status
                record['tournament'] = "RFU Championship"
                record['event'] = ''
                record['participant_type'] = "team"
                record['source_key'] = game_sk
                record['time_unknown'] = time_unknown
                record['reference_url'] = response.url
                record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
                record['rich_data'] = {'location': {'city': city, 'country': country, \
                                       'continent': continent, 'state': state, \
                                       'stadium': stadium}, 'game_note': ''}
                tz_info = get_tzinfo(city = city)
                record['tz_info'] = tz_info
                if not tz_info:
                    if country == "England" or country == "Wales":
                        tz_info = get_tzinfo(country = "United Kingdom")
                        record['tz_info'] = tz_info
                    else:
                        tz_info = get_tzinfo(country = country)
                        record['tz_info'] = tz_info
                if city == "Worcester" or city == "Jersey":
                    tz_info = get_tzinfo(country = "United Kingdom")
                    record['tz_info'] = tz_info

                if status == "scheduled":
                    record['result'] = {}
                    import pdb;pdb.set_trace()
                    yield record

