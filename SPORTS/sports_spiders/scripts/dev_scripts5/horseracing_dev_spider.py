import time
import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import pytz
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, log

true    = True
false   = False
null    = ''



class KentuckyDerbySpider(VTVSpider):
    ''' Class main '''
    name            = "horseracing_games"
    #allowed_domains = ["horseracingnation.com"]
    start_urls      = ['http://www.horseracingnation.com/race/2014_Breeders_Cup_Classic']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="adnav"]//a')
        for node in nodes[::1]:
            link = extract_data(node, './/@href')
            print link 
            if "superscreener.com" in link:
                continue
            yield Request(link , callback=self.parse_game_details, meta = {})
    @log
    def parse_game_details(self, response):
        hxs = Selector(response)
        record          = SportsSetupItem()
        tou_name = extract_data(hxs, '//h1[@class="headline"]/text()').strip().split('(')[0].strip().split('2014')[-1].strip()
        tou_date = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Date/Track:")]\
                               /following-sibling::div[@class="value"]/text()').replace(',','')
        tou_time = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Post Time:")]\
                                /following-sibling::div[@class="value"]/text()')
        if "Breeders" in tou_name:
            tou_name = "Breeder's Cup"
        if tou_time:
            _date = tou_date+ " "+tou_time
            _date = _date.replace('ET', '').replace('PT', '').strip()
            pattern = "%m/%d/%Y %I:%M %p"
            _datetime = get_utc_time(_date, pattern, 'US/Eastern')
        else:
            _date = tou_date
            pattern = "%m/%d/%Y"
            _datetime = get_utc_time(_date, pattern, 'US/Eastern')
        today_date = str(datetime.datetime.utcnow())
        if today_date <  _datetime:
            status ="scheduled"
        else:
            status = "completed"
        winner = extract_data(hxs, '//table/tr//td[@class="rank"][1][contains(text(), "st")]/following-sibling::td/span[@class="horse-name-link"]/a/@href')
        winner = winner.split('/')[-1]

        game_sk = response.url.split('/')[-1]
        stad_link = extract_data(hxs, '//div[@class="row"]/div[@class="title"][contains(text(),"Date/Track:")]\
                                 /following-sibling::div[@class="value"]/a/@href')
        record['participants'] = {}
        record['result'] = {}
        if  winner:
            record['result'] = {'0': {'winner': winner}}

        nodes = get_nodes(hxs, '//table[@id="ctl00_MainContent_uxRaceDetail_uxRankings_uxRankMain_uxGridView2"]/tr')
        for node in nodes:
            rank = extract_data(node, './td[@class="rank"][not(contains(text(), "-"))]/text()').strip()
            print "rank" , rank
            if "st" in rank:
                rank = rank.replace('st', '') 
            elif "nd" in rank:
                rank = rank.replace('nd', '') 
            elif "rd" in rank:
                rank = rank.replace('rd', '') 
            elif "th" in rank:
                rank = rank.replace('th', '') 
            sk = extract_data(node, './td/span[@class="horse-name-link"]/a/@href')
            title = extract_data(node, './a/text()')
            rating = extract_data(node, './td[@class="rank hidesmall"]//text()')
            sk = sk.split('/')[-1]
            if sk == '': 
                continue
            record['participants'].update({sk: (0, title)})
 
            if status == "completed" :
                record['result'].update({sk: {'rank': rank, 'rating': rating}})

        ref = response.url
        record['affiliation'] = "ntra"
        record['game'] = "horseracing"
        record['game_datetime'] = _datetime
        record['game_status'] = status
        record['participant_type'] = "player"
        record['tournament'] = tou_name
        record['reference_url'] = ref
        record['source'] = 'horseracing_nation'
        record['source_key'] = game_sk
        location = {'stadium': "Santa Anita Park", 'city': "Arcadia", 'country': "USA", 'state': 'California'}
        game_note = tou_name.split('Cup')[-1].strip()
        record['rich_data'] = {}
        record['time_unknown'] = 0
        record['tz_info'] = get_tzinfo(city = "Arcadia")
        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(country = "USA")
        record['rich_data'].update({'channel':'', 'location': location, 'game_note': game_note})
        import pdb;pdb.set_trace()
        yield record
