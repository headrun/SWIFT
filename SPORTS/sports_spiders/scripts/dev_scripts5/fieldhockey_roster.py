import re
import datetime
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, get_nodes


class FieldHockeyRoster(VTVSpider):
    name = 'fieldhockey_roster'
    #allowed_domains = ['www.rabobankhockeyworldcup2014.com']
    start_urls = []

    def start_requests(self):
        top_urls = ['http://www.rabobankhockeyworldcup2014.com/countries']
        for url in top_urls:
            yield Request(url, callback=self.parse, meta = {})

    def parse(self, response):
        import pdb;pdb.set_trace()
        sel = Selector(response)
        nodes = get_nodes(sel, \
                '//div[@class="qualified-block"]//tr[@class="table__row"]')
        for node in nodes:
            team_link = extract_data(node, './/a/@href')
            team = extract_data(node, './/p[@class="country__name"]/text()')
            if "women" in team_link:
                team_sk = team.replace(' ', '_') + '_women'
                team = team + " women's national field hockey team"
            elif "men" in team_link:
                team_sk = team.replace(' ', '_')
                team = team + " national field hockey team"
            yield Request(team_link, callback=self.parse_players, \
                        meta = {'team_sk' : team_sk}, dont_filter=True)

    def parse_players(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        participants = {}
        season = datetime.datetime.now().year
        if "women" in response.meta['team_sk']:
            nodes = get_nodes(sel, \
                    '//div[@id="team-women"]//a[contains(@href, "player")]')
        else:
            nodes = get_nodes(sel, \
                    '//div[@id="team-men"]//a[contains(@href, "player")]')
        for node in nodes:
            player_link = extract_data(node, './@href')
            psmry = extract_data(node, \
                    './/p[@class="player-summary__name"]/text()')
            pnum = psmry.split(' ')[0].strip()
            pid = "".join(re.findall(r'player/(\d+)', player_link))
            players =  {pid: {'player_role': '',
                                   'player_number': pnum,
                                   'season': season,
                                   'status': 'active', 'entity_type': "participant", \
                                   "field_type": "description", "language": "ENG"}}
            participants.setdefault(response.meta['team_sk'], {}).\
                                     update(players)

        record['source'] = 'field_hockey'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = participants
        yield record
