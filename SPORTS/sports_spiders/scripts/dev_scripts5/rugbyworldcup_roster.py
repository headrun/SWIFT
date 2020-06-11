from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

DOMAIN_LINK = "http://www.rugbyworldcup.com"

class RugbyWorldCupRoster(VTVSpider):
    name = "worldrugby_roster"
    start_urls = ['http://www.rugbyworldcup.com/teams']
    participants = {}


    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        team_nodes = get_nodes(sel, '//div[@class="teamsListIndex"]//ul//li//a')

        for node in team_nodes:
            team_link = extract_data(node, './/@href')

            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link
            team_link = team_link + "/squad"

            yield Request(team_link, callback = self.parse_next, \
                        meta = {'record': record})

    def parse_next(self, response):
        sel = Selector(response)
        record = response.meta['record']
        pl_nodes = get_nodes(sel, '//section[@class="team-squad"]//ul//li')
        season = extract_data(sel, '//div[@class="date"]//text()').split(' ')[-1].strip()
        team_sk = response.url.split('/')[-2]
        season = '2015'

        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/a//@href')
            head_coach = extract_data(pl_node, './/div[@class="player coach"]//span[contains(text(), "Head Coach")]//following-sibling::h3//text()')
            pl_sk = pl_link.split('/')[-1].strip()
            pl_pos = extract_data(pl_node, './/span[@class="tag"]//text()')

            if head_coach:
                pl_sk = head_coach.replace(' ', '_').lower()
                pl_pos = "Head coach"

            if not pl_sk:
                continue

            pl_name = extract_data(pl_node, './/h3[@class="name"]//text()')
            pl_info = {pl_sk: {'player_role': pl_pos, 'player_number': '', \
        'season': season, 'status': 'active', 'entity_type': 'participant', \
        'field_type': "description", 'language': 'ENG'}}

            self.participants.setdefault(team_sk, {}).update(pl_info)

        record['source'] = 'rugby_worldcup'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record

