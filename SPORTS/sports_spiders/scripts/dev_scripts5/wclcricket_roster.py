from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime


DOMAIN_LINK = 'http://www.espncricinfo.com'

class WCLCricketRosters(VTVSpider):
    name = "wclcricket_rosters"
    allowed_domains = []
    start_urls = ['http://www.espncricinfo.com/wcl-championship-2015-17/content/squad?object=870869']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        rs_nodes =get_nodes(sel, '//div[@role="main"]//ul[@class="squads_list"]//li')
        for node in rs_nodes:
            team_link = extract_data(node, './/@href')
            title = extract_data(node, './/text()').split(' Squad')[0]

            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link

            yield Request(team_link, callback = self.parse_next, \
                        meta = {'title': title})


    def parse_next(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        team_sk = extract_data(sel, '//div[@role="main"]//h1//text()').replace('Squad / Players', '').strip()
        pl_nodes = get_nodes(sel, '//div[@class="content main-section"]//ul//li')

        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/a[contains(@href, "player")]//@href')

            if not pl_link:
                continue

            player_id = re.findall(r'player.(\d+).', pl_link)[0]
            player_role = extract_data(pl_node, './/span[b[contains(text(), "Playing role")]]//following-sibling::text()')\
                        .strip()
            if ":" in player_role:
                player_role = ''

            if "Wicketkeeper" in player_role:
                player_role = "Wicket-keeper"
            if "Allrounder" in player_role:
                player_role = "All-rounder"
            print player_role
            players = {player_id: {"player_role": player_role,
                                  "player_number": 0,
                                  "season": '2015-17', "status": 'active', \
                                  "entity_type": "participant", \
                                  "field_type": "description", "language": "ENG"}}

            self.participants.setdefault(team_sk, {}).update(players)

        record['source'] = 'espn_cricket'
        record['season'] = '2015-17'
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record

