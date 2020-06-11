import re
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.http import Request
from scrapy.selector import Selector

PLAYER_POSITION_DICT = {'RB': 'Running back', 'S': 'Safety',
                        'WR': 'Wide receiver', 'DE': 'Defensive end',
                        'IL': 'Inside Linebacker', 'OT': 'Offensive Tackle',
                        'OL': 'Outside linebacker', 'LB': 'Linebacker',
                        'QB': 'Quarterback', 'DL': 'Defensive linemen',
                        'DB': 'Defensive back', 'LS': 'Long Snapper',
                        'FB': 'Fullback', 'CB': 'Cornerback',
                        'TE': 'Tight end', 'PK': 'Placekicker',
                        'DT': 'Defensive tackle', 'P': 'Probable',
                        'NT': "Nose Tackle", 'G': "Guard",
                        'C': "Center", '?': ''}


class NCFRoster(VTVSpider):
    name = "ncf_roster"
    start_urls = ['http://espn.go.com/college-football/teams']
    participants = {}
    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul/li')
        record = SportsSetupItem()
        for node in nodes:
            team_link = extract_data(node, './/a[contains(@href, "college-football")]/@href')
            if team_link:
                team_link = team_link.replace('_', 'roster/_')
                team_link = "http://espn.go.com/college-football/team/roster/_/id/282/indiana-state-sycamores"
                if 'roster' in team_link:
                    yield Request(team_link, callback=self.parse_teams, meta = {'record': record})

    def parse_teams(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        season = re.findall(r'\d+', extract_data(hxs, '//div[@class="mod-content"]/table/tr[@class="stathead"]/td/text()'))
        if season:
            season = str(season[0])
        team_sk = "".join(re.findall('\d+', response.url))
        pl_nodes = get_nodes(hxs, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]')
        last_node = pl_nodes[-1]
        for pl_node in pl_nodes[::1]:
            terminal_crawl = False
            if pl_node == last_node:
                terminal_crawl = True
            pl_data = extract_list_data(pl_node, './td/text()')
            if pl_data[1] == "NA":
                continue
            try:
                if pl_data[1]:
                    pos = PLAYER_POSITION_DICT[pl_data[1]]
                else:
                    pos = ''
            except:
                pos = ''
                print response.url
            status = "active"
            if not pos:
                status = "inactive"
            pl_url = extract_data(pl_node, './td/a/@href')
            if pl_url:
                pl_sk = pl_url.split('/')[-2]
                yield Request(pl_url, callback = self.parse_details, \
                            meta = {'team_sk': team_sk,
                                    'player_id': pl_sk, 'terminal_crawl': terminal_crawl, \
                                    'record': record, "player_number": pl_data[0], "status": status, \
                                    "player_role": pos, 'record': record, 'season': season})

    def parse_details(self, response):
        sel = Selector(response)
        record = response.meta['record']
        pl_sk = response.meta['player_id'] 
        pos   = response.meta['player_role']
        player_number = response.meta['player_number']
        if player_number == "--":
            player_number = ''
        status = response.meta['status']
        season = response.meta['season']
        team_sk = extract_data(sel, '//ul[@class="general-info"]//li[@class="last"]//a//@href')
        if team_sk:
            team_sk = "".join(re.findall('\d+', team_sk))
        if team_sk != response.meta['team_sk']:
            status  = 'inactive'
        else:
            status  = "active"
        players = {pl_sk: {"player_role": pos,
                        "player_number": player_number,
                        "season": season, "status": status, 'language': "ENG", \
                        "entity_type": "participant", "field_type": "description"}}
        self.participants.setdefault(response.meta['team_sk'], {}).update(players)

        record['source'] = 'espn_ncaa-ncf'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        if response.meta['terminal_crawl']:
            yield record
