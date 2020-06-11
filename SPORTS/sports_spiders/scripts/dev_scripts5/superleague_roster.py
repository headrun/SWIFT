from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

ROLES_DICT = {'Back Row': 'Back row', 'Half Back': 'Half-back', 'Outside Back': 'Outside back',
              'Fly Half': 'Fly-half', 'Winger': 'Wing'}

DOMAIN_LINK = "http://www.rugby-league.com"
class SuperLeagueRrugbyRoster(VTVSpider):
    name = "superleag_roster"
    start_urls = ['http://www.rugby-league.com/superleague/stats/club_stats']
    participants = {}


    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        team_nodes = get_nodes(sel, '//table[contains(@class, "fullstattable")]//tr//td//a[contains(@href, "/club")]')
        season = extract_list_data(sel, '//select[@name="seasonSelector"]//option//text()')[0]
        for team_node in team_nodes:
            team_url = extract_data(team_node, './/@href')
            if "http" not in team_url:
                team_url = DOMAIN_LINK + team_url
            yield Request(team_url, callback=self.parse_next, meta = {'season': season, 'record': record})

    def parse_next(self, response):
        sel = Selector(response)
        record = response.meta['record']
        team_name = extract_data(sel, '//div[@class="container"]//h2//span[contains(text(), "Club")]//following-sibling::text()').strip()
        pl_nodes = get_nodes(sel, '//div[@class="container"]//div[@class="card player"]//a')
        last_node = pl_nodes[-1]
        for pl_node in pl_nodes:
            terminal_crawl = False
            if pl_node == last_node:
                terminal_crawl = True

            pl_link = extract_data(pl_node, './/@href')
            if "http" not in pl_link:
                pl_link = DOMAIN_LINK +pl_link
            yield Request(pl_link, callback = self.parse_players, meta = {'team_name': team_name, 'season': response.meta['season'], 'terminal_crawl': terminal_crawl, 'record': record})


    def parse_players(self, response):
        sel = Selector(response)
        season =response.meta['season']
        record = response.meta['record']
        team_sk = response.meta['team_name'].lower().replace(' ', '_')
        player_sk = "".join(re.findall(r'\d+', response.url))
        pl_number = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "Shirt:")]//following-sibling::text()')
        pl_role   = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "Position:")]//following-sibling::text()').title()

        pl_role = pl_role.strip().title()

        if ROLES_DICT.get(pl_role, ''):
            pl_role = ROLES_DICT.get(pl_role, '')

        players = {player_sk: {"player_role": pl_role.strip(),
                                  "player_number": pl_number.strip(),
                                  "season": season, "status": 'active', \
                                  "entity_type": "participant", \
                                  "field_type": "description", "language": "ENG"}}
        self.participants.setdefault\
                (team_sk, {}).update(players)
        record['source'] = 'superleague_rugby'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        if response.meta['terminal_crawl']:
            yield record


