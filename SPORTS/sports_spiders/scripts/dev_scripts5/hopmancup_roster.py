from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import time

class HopmancupRoster(VTVSpider):
    name = "hopmancup_roster"
    start_urls = ['http://hopmancup.com/teams']
    participants = {}

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//div[@class="team-column"]')
        last_node = nodes[-1]
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            team_sk = extract_data(node, './/span[@class="country"]//text()')
            player_link = extract_data(node, './/div[@class="players-team"]//span//a//@href')
            yield Request(player_link, callback =self.parse_roster, meta = {'team_sk': team_sk, 'terminal_crawl': terminal_crawl})

    def parse_roster(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        season = extract_data(hxs, '//div[@class="cup-date mob-hide"]/h4/text()').split(' ')[-1]
        nodes  = get_nodes(hxs, '//div[@class="profile"]')
        for node in nodes:
            player_sk = extract_data(node, './/h3//text()')
            player_role = extract_data(node, './/td[contains(text(), "Plays")]//following-sibling::td//text()')
            team_sk = extract_data(node, './/td[contains(text(), "Country")]//following-sibling::td//text()')
            player_number = ''
            players = {player_sk: {"player_role": player_role,
                    "player_number": 0,
                                  "season": season, "status": 'active', \
                                  "entity_type": "participant", \
                                  "field_type": "description", "language": "ENG"}}
            self.participants.setdefault\
                (response.meta['team_sk'], {}).update(players)
            if response.meta['terminal_crawl']:
                record['source'] = 'hopmancup_tennis'
                record['season'] = season
                record['result_type'] = 'roster'
                record['participants'] = self.participants
                yield record

