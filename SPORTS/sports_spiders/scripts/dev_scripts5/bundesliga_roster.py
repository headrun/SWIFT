from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes,  \
extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem

PL_POS = {'Guards': 'Guard', 'Forwards': 'Forward', 'Centers': 'Center', 'Coach': 'Head coach'}

DOMAIN_LINK = 'http://www.scoresway.com/'

class LegaBasketballRoster(VTVSpider):
    name = "legabasketball_roster"
    allowed_domains = []
    start_urls = [  'http://www.scoresway.com/?sport=basketball&page=competition&id=2', \
                    'http://www.scoresway.com/?sport=basketball&page=competition&id=1',
                    'http://www.scoresway.com/?sport=basketball&page=competition&id=8']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//table[@class="leaguetable sortable table "]//tr//td')

        for node in tm_nodes:
            team_link = extract_data(node, './/a//@href')
            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link

                yield Request(team_link, callback = self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        pl_nodes = get_nodes(sel, '//div[@class="content"]//div[@class="squad-player"]//span[contains(@class, "name")]')
        team_sk = response.url.split('=')[-1].strip()

        if team_sk == DOMAIN_LINK:
            return

        season = extract_list_data(sel, '//select[@name="season_id"]//optgroup//option//text()')
        if season:
            season = season[0].replace('/20', '-').strip()
        else:
            return
        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/a//@href')
            pl_pos  = extract_list_data(pl_node, '../preceding-sibling::div[@class="squad-position-title group-head"]//text()')[-1]
            pl_pos  = PL_POS.get(pl_pos, '')
            pl_sk = pl_link.split('=')[-1].strip()

            if not pl_sk:
                continue
            pl_sk = "PL" + pl_sk

            players = {pl_sk: {"player_role": pl_pos,
                          "player_number": 0,
                          "season": season, "status": 'active', \
                          "entity_type": "participant", \
                "field_type": "description", "language": "ENG"}}

            self.participants.setdefault(team_sk, {}).update(players)
        record['source'] = 'euro_basketball'
        record['season'] = season
        record['affiliation'] = "euro-nba"
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record



