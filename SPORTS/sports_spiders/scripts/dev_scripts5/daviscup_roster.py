from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem
import datetime

DOMAIN = "http://www.daviscup.com"

class DaviscupRoster(VTVSpider):
    name = "davis_roster"
    start_urls = ['http://www.daviscup.com/en/teams/teams-a-to-z.aspx']
    participants = {}

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes =  get_nodes(hxs, '//div[@class="az"]/ul//li//ul[@class="azlist clfx"]/li/a')
        for node in nodes:
            team_link = extract_data(node, './@href')
            team_link = DOMAIN + team_link
            yield Request(team_link, callback=self.parse_team_info, meta = {'record': record})

    def parse_team_info(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        team_link  = response.url
        team_sk    = team_link.rsplit('?id=')[-1]
        season = extract_data(hxs, '//span[@class="ajax__tab_inner"]/span[@id="__tab_TabRecentResultsThisYear"]/text()').strip()
        year = datetime.date.today().year
        player_nodes = get_nodes(hxs, '//div[@id="TabRecentResultsThisYear"]//table//tr//td/a[contains(@href, "/players/player")]')
        last_node = player_nodes[-1]
        for pl_node in player_nodes:

            terminal_crawl = False
            if pl_node == last_node:
                terminal_crawl = True

            pl_link = DOMAIN + extract_data(pl_node, './@href').strip()
            if pl_link and year == int(season):
                yield Request(pl_link, callback=self.parse_player_info, \
                    meta = {'team_sk': team_sk, 'season': season, \
                    'terminal_crawl': terminal_crawl, 'record': record})

    def parse_player_info(self, response):
        hxs = Selector(response)
        season = response.meta['season']
        record = response.meta['record']
        players = {}
        player_link = response.url
        player_sk = player_link.rsplit('playerid=')[-1]
        team_sk = extract_data(hxs, '//div[@class="titleNation"]/a[contains(@href, "/teams/team")]/@href').strip().rsplit('?id=')[-1]
        role = extract_data(hxs, '//div[@class="playerDetails"]/ul/li/span[contains(text(), "Plays:")]/following-sibling::strong/text()').strip()
        if role == "Unknown":
            role = ''
        else:
            role = role.split(' (')[0].strip()
        players = { player_sk: {'player_role': role, \
                        'season': season, 'status': "active", \
                        'entity_type': "participant", 'field_type': "description", \
                        'language': "ENG"}}
        self.participants.setdefault(team_sk, {}).update(players)
        record['participants'] = self.participants
        record['season'] = season
        record['result_type'] = "roster"
        record['source'] = "daviscup_tennis"
        if response.meta['terminal_crawl']:
            yield record

