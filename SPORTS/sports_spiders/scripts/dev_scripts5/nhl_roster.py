from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

class NhlRoster(VTVSpider):
    name = "nhlroster"
    start_urls = ['http://www.nhl.com/ice/playersearch.htm#?navid=nav-ply-search']
    participants = {}

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//div[@id="teamMenu"]//a[not(contains(@title, "NHL.com"))]')
        last_node = nodes[-1]
        for node in nodes:
            team_name = extract_data(node, './/@title').strip()
            callsign  = extract_data(node, './/@class')
            callsign = callsign.upper()
            if callsign == "LOS" or callsign == "CLB":
                continue
            #url = 'http://www.nhl.com/ice/playersearch.htm?team=%s' % (callsign)
            #url = 'http://www.nhl.com/ice/playersearch.htm?team=CBJ'
            url = 'http://www.nhl.com/ice/playersearch.htm?team=LAK'
            yield Request(url, callback = self.parse_roster, \
            meta = {'team_name':team_name, 'call_sign': callsign, \
            'record': record})

    def parse_roster(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//table[@class="data playerSearch"]//tbody//tr//td[contains(@style, "text-align")]')
        last_node = nodes[-1]
        record = response.meta['record']
        #source_key = response.url
        #source_key = source_key.split('=')[-1]
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            player_id = extract_data(node, './/a/@href')
            player_id = re.findall(r"\d+", player_id)[0]
            record['participants'] = {}
            player_link = 'http://www.nhl.com/ice/player.htm?id=%s' % (player_id)
            yield Request(player_link, callback = self.parse_details, \
            meta = {'team_name': response.meta['team_name'], 'call_sign': response.meta['call_sign'],
            'player_id': player_id, 'terminal_crawl': terminal_crawl, 'record': record})
            page_url = extract_data(hxs, '//div[@class="pageNumbers"]//a//@href')
            page_url = 'http://www.nhl.com' + page_url
            if page_url:
                yield Request(page_url, callback = self.parse_roster, meta = {'team_name': response.meta['team_name'], \
                'call_sign': response.meta['call_sign'],'player_id': player_id, 'terminal_crawl': terminal_crawl, 'record': record})

    def parse_details(self, response):
        hxs = HtmlXPathSelector(response)
        record = response.meta['record']
        player_sk = response.meta['player_id']
        team_sk   = response.meta['call_sign']
        #source_key = response.meta['source_key']
        season = datetime.datetime.now()
        season = season.year
        nodes = get_nodes(hxs, '//div[contains(@style, "border-bottom: 1px solid #666;")]')
        for node in nodes:
            player_number = extract_data(node, './/span[@class="sweater"]//text()').replace('#', '')
            player_role  = extract_data(node, './/span[@style="color: #666;"]//text()').replace('\n', '')
            players = { player_sk: {'player_role': player_role, 'player_number': player_number, \
                        'season': season, 'status': "active"}}
            self.participants.setdefault('LAK', {}).update(players)
        record['participants'] = self.participants
        record['season'] = season
        record['result_type'] = "roster"
        record['source'] = "NHL"
        if response.meta['terminal_crawl']:
            yield record


