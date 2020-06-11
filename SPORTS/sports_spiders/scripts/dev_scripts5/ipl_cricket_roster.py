from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

PL_LINK = "http://www.espncricinfo.com"
class IPLCricketRoster(VTVSpider):
    name = "ipl_cricket_roster"
    start_urls = ['http://www.espncricinfo.com/indian-premier-league-2016/content/squad/index.html?object=968923']
    participants = {}


    def parse(self, response):
        hxs = Selector(response)
        season = extract_data (hxs, '//ul[@class="squads_list"]//h2/text()'). \
            split(',')[-1].strip()
        nodes = get_nodes(hxs, '//ul[@class="squads_list"]//li//span//a')
        for node in nodes:
            urls = extract_data(node, './/@href')
            title = extract_data(node, './/text()').split(' Squad')[0]
            urls = PL_LINK + urls
            yield Request(urls, callback =self.parse_next, \
            meta ={'title': title, 'season': season})
    def parse_next(self, response):
        hxs = Selector(response)
        team_sk = response.url.split('/')[-1].replace('.html', '')
        print team_sk
        season = response.meta['season']
        record = SportsSetupItem()
        player_nodes = get_nodes(hxs, '//div[@class="large-20 medium-20 small-20 columns"]//ul//li')
        for node in player_nodes:
            pl_link = extract_data(node, './/h3//a//@href')
            if not pl_link:
                continue
            player_id = re.findall(r'player.(\d+).', pl_link)[0]
            player_role = extract_data(node, './/span[b[contains(text(), "Playing role")]]/text()').strip()
            players = {player_id: {"player_role": player_role,
                                  "player_number": 0,
                                  "season": season, "status": 'active', \
                                  "entity_type": "participant", \
                                  "field_type": "description", "language": "ENG"}}
            self.participants.setdefault\
                (team_sk, {}).update(players)
        record['source'] = 'espn_cricket'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record

