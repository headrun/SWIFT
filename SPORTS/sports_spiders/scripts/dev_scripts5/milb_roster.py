from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem

DOMAIN_URL = "http://www.baseballamerica.com"

PL_POS = {'Pitchers': 'Pitcher', 'Catchers': 'Catcher', \
            'Infielders': 'Infielder', 'Outfielders': 'Outfielder', \
            'Injuries': '', 'Designated Hitters': 'Designated Hitter'}

class MexBaseballRoster(VTVSpider):
    name = "mex_roster"
    allowed_domains = []
    start_urls = ['http://www.baseballamerica.com/statistics/leagues/?lg_id=17']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        tm_nodes  = get_nodes(sel, '//table//tr//td//a[contains(@href, "roster")]')

        for tm_node in tm_nodes:
            tm_link = extract_data(tm_node, './/@href')

            if "http" not in tm_link:
                tm_link = DOMAIN_URL + tm_link

            yield Request(tm_link, callback=self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        record = SportsSetupItem()
        season = response.url.split('=')[-1]
        team_sk = response.url.split('=')[-2].replace('&y', '').strip()
        pl_nodes = get_nodes(sel, '//table//tr')

        for node in pl_nodes:
            pl_link = extract_data(node, './/td//a[contains(@href, "player")]//@href')

            if not pl_link or 'tm_id' in pl_link:
                continue

            pl_pos = extract_list_data(node, './preceding-sibling::tr[@class="headerRow"]//td//text()')[-1].title()
            pl_pos = PL_POS.get(pl_pos, '')
            if pl_pos == '':
                continue
            pl_sk = pl_link.split('/')[-1]
            print pl_sk
            pl_no   = extract_data(node, './/td[@align="right"][1]//text()')
            if pl_sk == "43021":
                pl_no = 0
            print pl_no
            pl_info = {pl_sk: {'player_role': pl_pos, 'player_number': pl_no, \
        'season': season, 'status': 'active', 'entity_type': 'participant', \
        'field_type': "description", 'language': 'ENG'}}

            self.participants.setdefault(team_sk, {}).update(pl_info)

        record['source'] = 'baseball_america'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record


