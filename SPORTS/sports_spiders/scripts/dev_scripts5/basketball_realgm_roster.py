from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
import re

class BasketballRealgmRoster(VTVSpider):
    start_urls = ['http://basketball.realgm.com/international/leagues']
    name = 'basketball_realgm_roster'
    participants = {}

    league_list = ['Danish-Basketligaen', 'Norwegian-BLNO', \
                  'Swedish-Basketligan', 'Icelandic-Dominos-League']
    domain_url = 'http://basketball.realgm.com'

    positions_dict = {'C'  : 'Center', 'F' : 'Forward', 'G' : 'Guard', \
                      'PG' : 'Point guard', 'SG' : 'Shooting guard' , 'SF' : 'Small forward', \
                      'PF' : 'Power forward' , 'FC' : 'Forward-Center' , 'CF' : 'Forward-Center', \
                      'FG' : 'Forward-Guard' , 'GF' : 'Forward-Guard'  , 'HC' : 'Head coach'
                        }

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        league_links = extract_list_data(hxs,'//div[@class="portal widget fullpage"]/div[@class="content linklist"]/a/@href')
        for league in self.league_list:
            for league_link in league_links:
                if league in league_link:
                    league_name = league
                    req_league = self.domain_url + league_link
                    league_id  = league_link.split('/')[-2]
                    yield Request(req_league, callback = self.parse_team, \
                    meta = {'league_name': league_name, 'league_id' : league_id, 'record': record})

    def parse_team(self , response):
        hxs = Selector(response)
        league_name = response.meta['league_name']
        league_id = response.meta['league_id']
        record = response.meta['record']
        team_links = extract_data(hxs,'//a[span[contains(text(),"Teams")]]/@href')
        team_link = self.domain_url + team_links
        yield Request(team_link, callback = self.parse_teams, \
        meta = {'league_name': league_name , 'league_id' : league_id, 'record': record })

    def parse_teams(self , response):
        hxs = Selector(response)
        league_name = response.meta['league_name']
        record = response.meta['record']
        league_id = response.meta['league_id']
        ssn = extract_data(hxs,'//h2[@class="page_title"]/text()')
        years = re.findall('(\d+)', ssn)
        seasons = years[0] +'-'+ years[1]
        season = seasons.replace('2015-2016','2015-16')
        roster_links = extract_list_data(hxs,'//td[@data-th="Rosters"]/a/@href')

        for roster_link in roster_links:
            team_id = roster_link.split('/')[-3]
            rosters_link = self.domain_url + roster_link
            yield Request(rosters_link, callback = self.parse_players, \
            meta = {'team_id' : team_id , 'league_name': league_name , \
            'league_id' : league_id,'season' : season, 'record': record})

    def parse_players(self, response):
        hxs = Selector(response)
        season = response.meta['season']
        record = response.meta['record']
        league_name = response.meta['league_name']
        league_id = response.meta['league_id']
        team_id = response.meta['team_id']
        player_nodes = get_nodes(hxs,'//table[@class="tablesaw"][1]//tbody//tr')
        participants = {}
        last_node = player_nodes[-1]
        last_nodes = extract_list_data(hxs, '//td[@class="nowrap"]//a[contains(@href,"/player/")]/@href')[-1]
        for player_node in player_nodes:
            terminal_crawl = False
            player_link = extract_data(player_node,'.//td[@class="nowrap"]//a[contains(@href,"/player/")]/@href')
            if player_link == last_nodes:
                terminal_crawl = True
            player_id = 'PL' +  player_link.split('/')[-1]
            if player_link:
                player_link = self.domain_url + player_link
            player_num = extract_data(player_node,'./td[@class="nowrap"][1]/text()').strip('-')
            pos = extract_data(player_node,'./td[@class="nowrap"][3]/text()').strip('-')
            role = self.positions_dict.get(pos,"")
            if player_link:
                yield Request(player_link, callback = self.parse_details, \
                            meta = {'team_sk': team_id,
                                    'player_id': player_id, 'terminal_crawl': terminal_crawl, \
                                    'record': record, "player_number": player_num, "status": "active", \
                                    "player_role": role, 'record': record, 'season': season})
    def parse_details(self, response):
        sel = Selector(response)
        record = response.meta['record']
        pl_sk = response.meta['player_id']
        pos   = response.meta['player_role']
        player_number = response.meta['player_number']
        status = response.meta['status']
        season = response.meta['season']
        team_sk = extract_data(sel, '//p//strong[contains(text(), "Current Team:")]//following-sibling::a//@href')
        if team_sk:
            team_sk = team_sk.split('/')[-2]
        if team_sk != response.meta['team_sk']:
            status  = 'inactive'
        else:
            status  = "active"
        players = {pl_sk: {"player_role": pos,
                        "player_number": player_number,
                        "season": season, "status": status, 'language': "ENG", \
                        "entity_type": "participant", "field_type": "description"}}
        self.participants.setdefault(response.meta['team_sk'], {}).update(players)
        record['result_type'] = "roster"
        record['source'] = "basketball_realgm"
        record['season'] = season
        record['participants'] = self.participants
        if response.meta['terminal_crawl']:
            yield record
