from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re

DOMAIN_LINK = "http://www.espncricinfo.com"

class CountyCricketRoster(VTVSpider):
    name = "county_cricket_roster"
    start_urls = ['http://www.espncricinfo.com/county-cricket-2015/content/squad?object=786877']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//div//ul[@class="squads_list"]//li//a')
        season = extract_data (sel, '//ul[@class="squads_list"]//h2/text()'). \
            split(',')[-1].strip()

        for node in tm_nodes:
            team_url = extract_data(node, './/@href')
            team_url = DOMAIN_LINK + team_url
            title = extract_data(node, './/text()').split(' Squad')[0]
            yield Request(team_url, callback = self.parse_next, \
            meta = {'title': title, 'season': season})

    def parse_next(self, response):
        sel = Selector(response)
        team_sk = response.meta['title']
        record = SportsSetupItem()
        season = response.meta['season']
        player_nodes = get_nodes(sel, '//div//ul//li')
        for node in player_nodes:
            pl_link = extract_data(node, './/h3//a//@href')

            if not pl_link:
                continue

            player_id = re.findall(r'player.(\d+).', pl_link)[0]
            player_role = extract_data(node, './/span[b[contains(text(), "Playing role")]]/text()').strip()

            if "Wicketkeeper" in player_role:
                player_role = "Wicket-keeper"
            if "Allrounder" in player_role:
                player_role = "All-rounderi"

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

