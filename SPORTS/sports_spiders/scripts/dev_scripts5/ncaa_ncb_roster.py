import re
from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


PLAYER_POSITION_DICT = {'RB': 'Running back', 'S': 'Safety',
                        'WR': 'Wide receiver', 'DE': 'Defensive end',
                        'IL': 'Inside linebacker', 'OT': 'Offensive tackle',
                        'OL': 'Outside linebacker', 'LB': 'Linebacker',
                        'QB': 'Quarterback', 'DL': 'Defensive linemen',
                        'DB': 'Defensive back', 'LS': 'Long Snapper',
                        'FB': 'Fullback', 'CB': 'Cornerback',
                        'TE': 'Tight end', 'PK': 'Placekicker',
                        'DT': 'Defensive tackle', 'P': 'Probable',
                        'NT': "Nose Tackle", 'G': "Guard", "F" : "Forward",
                        'C': "Center", 'G-F': "Forward-Guard",
                        'F-C': "Forward-Center"}


class NCBRoster(VTVSpider):
    name = "ncb_roster"
    start_urls = ['http://espn.go.com/mens-college-basketball/teams']
    participants = {}

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul/li')

        for node in nodes:
            team_link = extract_data(node, './/a[contains(@href, "college-basketball")]/@href')
            if team_link:
                team_link = team_link.replace('_', 'roster/_')

            if "roster" in team_link:
                yield Request(team_link, callback=self.parse_teamdetails, meta= {'record': record})

    def parse_teamdetails(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        participants = {}
        #team_sk = response.url.split('/')[-2]
        team_sk = re.findall('\d+', response.url)
        if len(team_sk) != 1:
            print response.url
            team_sk = response.url.split('/')[-2]
        else:
            team_sk = "".join(re.findall('\d+', response.url))
        pl_nodes = get_nodes(hxs, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]')
        last_node = pl_nodes[-1]
        season = "2015-16"

        for pl_node in pl_nodes:
            terminal_crawl = False
            if pl_node == last_node:
                terminal_crawl = True
            pl_data = extract_list_data(pl_node, './td/text()')
            if pl_data[0] == "NA":
                continue
            if "--" in pl_data[0]:
                player_number = ''
            else:
                player_number =  pl_data[0]
            try:
                if pl_data[1]:
                    pos = PLAYER_POSITION_DICT[pl_data[1]]
                else:
                    pos = ''
            except:
                pos = ''
            pl_link = extract_data(pl_node, './td/a/@href')
            pl_name = extract_data(pl_node, './td/a/text()')

            if pl_link:
                pl_sk = pl_link.split('/')[-2]
                yield Request(pl_link, callback = self.parse_details, \
                            meta = {'team_sk': team_sk,
                                    'player_id': pl_sk, 'terminal_crawl': terminal_crawl, \
                                    'record': record, "player_number": player_number, "status": "active", \
                                    "player_role": pos, 'record': record, 'season': season})

    def parse_details(self, response):
        sel = Selector(response)
        record = response.meta['record']
        pl_sk = response.meta['player_id']
        pos   = response.meta['player_role']
        player_number = response.meta['player_number']
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

        record['source'] = 'espn_ncaa-ncb'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        if response.meta['terminal_crawl']:
            import pdb;pdb.set_trace()
            yield record

