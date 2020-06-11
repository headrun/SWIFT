from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import urllib2

PLAYER_POSITION_DICT = {'RB': 'Running back', 'S': 'Safety',
                        'WR': 'Wide receiver', 'DE': 'Defensive end',
                        'IL': 'Inside Linebacker', 'OT': 'Offensive Tackle',
                        'OL': 'Outside linebacker', 'LB': 'Linebacker',
                        'QB': 'Quarterback', 'DL': 'Defensive linemen',
                        'DB': 'Defensive back', 'LS': 'Long Snapper',
                        'FB': 'Fullback', 'CB': 'Cornerback',
                        'TE': 'Tight end', 'PK': 'Penalty kick',
                        'DT': 'Defensive tackle', 'P': 'Probable',
                        'NT': "Nose Tackle", 'G': "Guard", "F" : "Forward",
                        'C': "Center", 'G-F': "Guard/Forward",
                        'F-C': "Forward/Center"}

def get_position(position):
    pos = ''
    for key, value in PLAYER_POSITION_DICT.iteritems():
        if position == key:
            pos = value
    if pos == "":
        print position
    return pos

class NCBRosterdev(VTVSpider):
    name = "ncb_rosterdev"
    start_urls = ['http://espn.go.com/mens-college-basketball/teams']
    participants = {}
    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul/li')

        for node in nodes[0:1]:
            team_link = extract_data(node, './/a[contains(@href, "college-basketball")]/@href')
            if team_link:
                team_link = team_link.replace('_', 'roster/_')
                yield Request(team_link, callback=self.parse_teamdetails)

    def parse_teamdetails(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()

        team_sk = response.url.split('/')[-2]
        pl_nodes = get_nodes(hxs, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]')
        last_node = pl_nodes[-1]
        for pl_node in pl_nodes:
            terminal_crawl = False
            if pl_node == last_node:
                terminal_crawl = True
            pl_data = extract_list_data(pl_node, './td/text()')
            if "--" in pl_data[0]:
                player_number = ''
            else:
                player_number =  pl_data[0]
            pos = get_position(pl_data[1])
            pl_link = extract_data(pl_node, './td/a/@href')
            record['participants'] = {}
            if pl_link:
                pl_sk = pl_link.split('/')[-2]
                yield Request(pl_link, callback =self.parse_details, meta = {'team_sk': team_sk, 'pl_sk': pl_sk, \
                "player_number": player_number, "player_role": pos, 'terminal_crawl': terminal_crawl, 'record': record})
    def parse_details(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        pl_sk = response.meta['pl_sk']
        team_sk = response.meta['team_sk']
        player_number = response.meta['player_number']
        player_role = response.meta['player_role']
        season_date = extract_data(hxs, '//div[@class="player-stats"]//p//text()')
        if "2013" in season_date:
            status = "inactive"
            season = "2013-14"
        else:
            status = "active"
            season = "2014-15"
        players = {pl_sk: {"player_role": player_role,
                            "player_number": player_number,
                            "season": season, "status": status}}
        self.participants.setdefault(team_sk, {}).update(players)

        record['source'] = 'espn_ncaa-ncb'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        if response.meta['terminal_crawl']:
            import pdb;pdb.set_trace()
            yield record
