from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

DOMAIN_LINK = "http://www.baku2015.com"

class EocAthletics(VTVSpider):
    name = "eoc_athletics"
    start_urls = ['http://www.baku2015.com/athletics/']

    def parse(self, response):
        sel = Selector(response)
        mens_urls = extract_list_data(sel, '//nav[@role="navigation"]//div[@id="orp-ev-mn-men"]//ul//li/a/@href')
        for url in mens_urls:
            url = DOMAIN_LINK + url
            yield Request(url, callback=self.parse_next)

        womens_urls = extract_list_data(sel,'//nav[@role="navigation"]//div[@id="orp-ev-mn-women"]//ul//li/a/@href')
        for url in womens_urls:
            url = DOMAIN_LINK + url
            yield Request(url, callback=self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        try:
            url = extract_list_data(sel, '//div[@class="or-ebn"]//ul//li//a//@href')[0]
            url = DOMAIN_LINK + url
        except:
            url = response.url
        sk = ''.join(re.findall('phase=(.*)/index', url))
        if not sk:
            sk = ''
        yield Request(url, callback=self.parse_details, meta = {'game_sk' : sk}, dont_filter = True)


    def parse_details(self, response):
        sel = Selector(response)
        round_info = extract_data(sel, '//div[@class="or-ed"]//h2//text()')
        venue = extract_data(sel, '//div[@class="or-ed"]//span//a[contains(@href, "venues")]//text()')
        status = extract_data(sel, '//span[@class="or-st"]//span[@class="or-status-off"]//text()').lower()
        if "official" in status:
            status = "completed"
        elif "live" in status:
            status = "ongoing"
        else:
            status = "scheduled"

        game_date = extract_list_data(sel, '//div[@class="or-ed"]//time//@datetime')[0]
        game_time = extract_list_data(sel, '//div[@class="or-ed"]//time//span//@data-or-utc')[0]
        tz_info = game_date.split(':')[-2].replace('00', '')
        game_datetime = datetime.datetime.strptime(game_time, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        event_info = extract_data(sel, '//h1[@class="or-eh or-disc-pg"]//text()')
        event_name = "European Games " + event_info
        game_sk = response.meta['game_sk']
        if not game_sk:
            _date = ''.join(re.findall('(.*)T', game_date))
            game_sk = str(event_info.lower().replace('-', '').replace('  ', '').replace(' ', '_').replace("'", '').strip() + _date)

        standings_nodes = get_nodes(sel, '//table[@class="or-tbl"]//tbody//tr')
        for node in standings_nodes:
            rank = extract_data(node, './/td[@class="or-rk"]//text()')
            if rank == '-':continue
            mark = extract_data(node, './/td[@class="or-sco  or-hl"]//text()')
            team_points = extract_data(node, './/td[@class="or-sco or-p1"]//text()')
            player_nodes = extract_list_data(node, './/td[@class="or-ath"]//a//@href')
            for player in player_nodes:
                player_sk = ''.join(re.findall('-(\d+)/index', response.url))
                record = SportsSetupItem()
                record['game'] = "athletics"
                record['source'] = "eoc_baku"
                record['tournament'] = "European Games - Athletics"
                record['source_key'] = game_sk
                record['affiliation'] = "eoc"
                record['game_status'] = status
                record['game_datetime'] = game_datetime
                record['event'] = event_info
                record['reference_url'] = response.url
                record['rich_data'] = {'game_note': round_info, 'stadium': venue, \
                        'location': {'city': 'Baku', 'country': 'Azerbaijan'}}
                record['time_unknown'] = 0
                record['event'] = event_name
                record['tz_info'] = tz_info
                record['result'] = {player_sk: {'rank' : rank ,'score' : mark, 'points' : team_points}}
                if rank == 1:
                    record['result'] = {'winner' : player_sk}
                print record
