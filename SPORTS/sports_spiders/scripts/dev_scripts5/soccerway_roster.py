from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
import json
import unicodedata

class SoccerwayRoster(VTVSpider):
    start_urls = ['http://int.soccerway.com/teams/club-teams/']
    name       = 'soccerway_roster'

    domain_url   = 'http://int.soccerway.com'
    participants = {}

    major_leagues  = ['Germany', 'Italy', 'France', 'Spain']
    sa_leagues     = ['Argentina','Bolivia','Brazil','Chile', \
                      'Columbia','Ecuador','Paraguay','Peru', \
                      'Uruguay','Venezuela']
    danish_league  = ['Denmark']
    english_league = ['England']
    scandavians    = ['Norway', 'Sweden', 'Finland', 'Iceland']
    uk_leagues     = ['Wales', 'Scotland']

    data = open("soccerway_countries.txt","r").read()
    country_dict = eval(data)

    def parse(self, response):
        hxs = Selector(response)
        leagues = get_nodes(hxs, '//ul[@class="areas"]/li[@class="expandable "]/div[@class="row"]')

        for league in leagues:
            league_name = extract_data(league, './a/text()').strip()
            league_link = extract_data(league, './a/@href').strip()
            league_link = self.domain_url + league_link
            if not league_link:
                continue
            if league_name in self.major_leagues and self.spider_type == 'uefa_leagues':
                yield Request(league_link, self.parse_league, meta={'league': league_name})
            elif league_name in self.scandavians and self.spider_type == 'scandavian_leagues':
                yield Request(league_link, self.parse_league, meta={'league': league_name})
            elif league_name in self.danish_league and self.spider_type == 'danish_league':
                yield Request(league_link, self.parse_league, meta={'league': league_name})
            elif league_name in self.english_league and self.spider_type == 'english_league':
                yield Request(league_link, self.parse_league, meta={'league': league_name})
            elif league_name in self.uk_leagues and self.spider_type == 'uk_leagues':
                yield Request(league_link, self.parse_league, meta={'league': league_name})
            elif league_name in self.sa_leagues and self.spider_type == 'sa_leagues':
                yield Request(league_link, self.parse_league, meta={'league': league_name})

    def parse_league(self, response):
        hxs = Selector(response)
        league = response.meta['league']
        country = extract_data(hxs, '//div[@class="block  clearfix block_competition_left_tree-wrapper"]/h2/text()')
        nodes = get_nodes(hxs, '//ul[@class="left-tree"]/li/a')
        for node in nodes:
            league_link = extract_data(node, './@href')
            for key, value in self.country_dict.iteritems():
                if key in league and value in league_link:
                    league_link = self.domain_url + league_link
                    yield Request(league_link, self.parse_details, dont_filter=True)

    def parse_details(self, response):
        hxs = Selector(response)
        season = response.url.split('/regular-season/')[0].split('/')[-1]
        nodes = get_nodes(hxs, '//table[@class="leaguetable sortable table detailed-table"]//tr[contains(@class,"team_rank")]')
        for node in nodes:
            team_sk = extract_data(node, './/@data-team_id')
            link = self.domain_url + extract_data(node, './/a[contains(@href, "/teams/")]/@href')
            if "-under-19" in link or "u19/" in link:
                continue
            yield Request(link, self.parse_players, meta={'team_sk': team_sk, 'season': season})

    def parse_players(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="table squad sortable"]//tr/td/a[contains(@href, "/players/")] | //a[contains(@href, "/coaches/")]')
        last_node = nodes[-1]
        self.season = extract_data(hxs, '//select[@name="season_id"]//option[@selected]/text()').replace('/2015', '-15').replace('2015/2016', '2015-16').strip()
        if not self.season:
            self.season = extract_list_data(hxs, '//select[@name="season_id"]//option[1]/text()')[0]

        if "norway" in response.url or "sweden" in response.url:
            self.season = response.meta['season']
        print self.season
        record = SportsSetupItem()
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            pl_link     = self.domain_url + extract_data(node, './/@href')
            pl_num = extract_data(node, './/@alt')
            yield Request(pl_link, self.parse_player_details,
                    meta = {'team_sk': response.meta['team_sk'],
                    'terminal_crawl': terminal_crawl, 'record': record, 'pl_num': pl_num},
                    dont_filter=True)

    def parse_player_details(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        position = extract_data(hxs, '//div[@class="yui-u first"]/div[@class="clearfix"]/dl\
                    /dt[contains(text(), "Position")]/following-sibling::dd[1]/text()').replace('Attacker', 'Forward').strip()
        pl_sk       = 'PL' + response.url.split('/')[-2]

        if "/coaches" in response.url:
            position = "Head coach"
        pl_num = response.meta['pl_num']
        if pl_sk not in ['PL178']:
            players = {pl_sk: {'player_role': position, 'status': "active",
                               'entity_type': "participant", 'player_number': pl_num}}

            self.participants.setdefault(response.meta['team_sk'], {}).update(players)
        record['result_type'] = "roster"
        record['source'] = "soccerway_soccer"
        record['season'] = self.season
        record['participants'] = self.participants
        if response.meta['terminal_crawl']:
            yield record
