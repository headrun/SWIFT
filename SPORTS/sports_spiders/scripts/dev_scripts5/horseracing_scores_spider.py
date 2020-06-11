import time
import datetime
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
import urllib2
from scrapy_spiders_dev.items import SportsSetupItem
import re
import pytz
from vtvspider_dev import VTVSpider, extract_data, get_nodes, get_utc_time

true    = True
false   = False
null    = ''


class KentuckyDerbySpider(VTVSpider):
    ''' Class main '''
    name            = "Floridaderby_games"
    allowed_domains = ["horseracingnation.com"]
    start_urls      = []
    record          = SportsSetupItem()

    def start_requests(self):
        req = []
        if self.spider_type == "scores":
            top_url = 'http://www.horseracingnation.com/'
            top_url = 'http://www.horseracingnation.com/race/2014_Breeders_Cup_Classic'
            yield Request(top_url, callback=self.parse, meta = {'type': self.spider_type})
    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="adnav"]//a')
        for node in nodes[::1]:
            link = extract_data(node, './/@href')
            if "superscreener.com" in link:
                continue
            yield Request(link , callback=self.parse_game_details, meta = {}) 

    '''def parse_scheduled(self, response):
        hxs     = HtmlXPathSelector(response)
        schedule_link = extract_data(hxs, '//ul[@class="score-nav tabs"]/li/a[contains(text(), "Kentucky Derby")]/@href')
        yield Request(schedule_link, callback=self.parse_game_info)

    def parse_game_info(self, response):
        hxs     = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//td[@class="hidesmall"]//tr/td/span/a[contains(text(), "Kentucky Derby")]')
        for node in nodes:
            link = extract_data(node, './@href')
            #link = 'http://www.horseracingnation.com/race/2014_Florida_Derby'
            link = 'http://www.horseracingnation.com/race/2014_Kentucky_Derby'
            if link:
                yield Request(link, callback=self.parse_game_details)'''

    def parse_game_details(self, response):

        hxs = HtmlXPathSelector(response)
        tou_name = extract_data(hxs, '//h1[@class="headline"]/text()').strip().split('(')[0].strip().split('2014')[-1].strip()
        tou_date = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Date/Track:")]/following-sibling::div[@class="value"]/text()').replace(',','')
        tou_time = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Post Time:")]/following-sibling::div[@class="value"]/text()')
        _date = tou_date+ " "+tou_time
        _date = _date.replace('ET', '').strip()
        pattern = "%m/%d/%Y %I:%M %p"
        _datetime = get_utc_time(_date, pattern, 'Us/Eastern')
        game_sk = response.url.split('/')[-1]
        winner = extract_data(hxs, '//table/tr//td[@class="rank"][1][contains(text(), "st")]/following-sibling::td/span[@class="horse-name-link"]/a/@href')
        winner = winner.split('/')[-1]
        print "winner", winner
        location = {}
        stadium = 'Gulfstream Park'
        city = "Hallandale"
        state = "Florida"
        country = "USA"
        #location = {'city': city, 'state': state, 'country': country, 'stadium': stadium}
        location = {'city': '', 'state':  '', 'country':'', 'stadium': ''}
        player_nodes = get_nodes(hxs, '//table/tr//td//span[@class="horse-name-link"]')
        self.record['participants'] = {}
        self.record['result'] = {}
        if  winner:
            self.record['result'] = {'0': {'winner': winner}}
        '''for node in player_nodes:
            player_link = extract_data(node, './a/@href')
            player_sk   = player_link.split('/')[-1]
            player_title  = extract_data(node, './a/text()')
            #self.record['participants'].update({player_sk: (0, player_title)})'''
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
            self.record['participants'].update({sk: (0, title)})
            self.record['result'].update({sk: {'rank': rank, 'rating': rating}})
        ref = response.url
        self.record['affiliation'] = "ntra"
        self.record['game'] = "horseracing"
        self.record['game_datetime'] = '2014-06-07 22:20:00'
        self.record['game_status'] = "completed"
        self.record['participant_type'] = "player"
        self.record['tournament'] = 'Belmont Stakes'
        self.record['reference_url'] = ref
        self.record['source'] = 'horseracing_nation'
        self.record['source_key'] = game_sk
        self.record['rich_data'] = {'channel':'','location': location}
        yield self.record
        print "self.record", self.record
