from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime


DOMAIN_LINK = 'http://www.espncricinfo.com'

class CPLCricketRoster(VTVSpider):
    name ="cplcricket_roster"
    allowed_domains = []
    start_urls = ['http://www.espncricinfo.com/caribbean-premier-league-2015/content/squad/index.html?object=846823']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        tou_info = extract_data(sel, '//div[@class="icc-home"]//text()')
        tou_name = tou_info.split(',')[0].strip()
        season = tou_info.split(',')[-1].strip()
        rs_nodes =get_nodes(sel, '//div//ul[@class="squads_list"]//li//span//a')

        for node in rs_nodes:
            team_link = extract_data(node, './/@href')
            title = extract_data(node, './/text()').split(' Squad')[0]

            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link

            yield Request(team_link, callback = self.parse_next, \
                        meta = {'title': title, 'tou_name': tou_name, 'season': season})

    def parse_next(self, response):
        sel = Selector(response)

        team_sk = "".join(re.findall('squad/(.*).html', response.url))
        record = SportsSetupItem()

        pl_nodes = get_nodes(sel, '//div[@class="large-13 medium-13 small-13 columns"]')

        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/h3//a//@href')

            if not pl_link:
                continue

            player_id = re.findall(r'player.(\d+).', pl_link)[0]
            player_role = extract_data(pl_node, './/span[b[contains(text(), "Playing role:")]]//text()')\
                        .strip().replace(': ', '').replace('Playing role', '')

            if "Wicketkeeper" in player_role:
                player_role = "Wicket-keeper"

            if "Allrounder" in player_role:
                player_role = "All-rounder"

            players = {player_id: {"player_role": player_role,
                                  "player_number": 0,
                                  "season": response.meta['season'], "status": 'active', \
                                  "entity_type": "participant", \
                                  "field_type": "description", "language": "ENG"}}

            self.participants.setdefault\
                (team_sk, {}).update(players)

        record['source'] = 'espn_cricket'
        record['season'] = response.meta['season']
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record

