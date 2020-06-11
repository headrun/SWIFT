from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime


class NHLCoachSpider(VTVSpider):
    name = "nhl_coach_info"
    allowed_domains = []
    start_urls = ['http://www.nhl.com/ice/page.htm?id=26095']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//table[@width="320"]//tr')
        record = SportsSetupItem()
        for node in nodes:
            team_img = extract_data(node, './/td//img[contains(@src, "/logos/")]//@src')
            if not team_img:
                continue
            coach_name = extract_data(node, './/td[@valign="middle"]//a//text()').strip()
            if not coach_name:
                continue
            season = "2015-16"
            team_sk = team_img.split('/')[-1].split('_logo')[0].strip()
            pl_sk = coach_name.lower().replace(' ', '_')
            players = { pl_sk: {'player_role': "Head coach", 'player_number': '', \
                        'season': season, 'status': "active" ,'entity_type': "participant", \
                        'field_type': 'description', 'language': "ENG"}}
            self.participants.setdefault(team_sk, {}).update(players)

            record['participants'] = self.participants
            record['season'] = season
            record['result_type'] = "roster"
            record['source'] = "NHL"
            yield record

