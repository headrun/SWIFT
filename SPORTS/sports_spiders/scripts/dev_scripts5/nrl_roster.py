import re
from vtvspider import VTVSpider, \
get_nodes, extract_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem

DOMAIN = "http://www.nrl.com"

class NRLRoster(VTVSpider):
    name ="nrl_roster"
    allowed_doamins = []
    start_urls = ['http://www.nrl.com/DrawResults/Statistics/PlayerStatistics/tabid/10877/Default.aspx']
    participants = {}
    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//div[@class="nwTeam"]//a')
        for team_node in team_nodes:
            team_url = extract_data(team_node, './/@href')
            if team_url:
                team_url = DOMAIN + team_url
                yield Request(team_url, callback = self.parse_players)

    def parse_players(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        pl_nodes = get_nodes(sel, '//table[@class="clubPlayerStats__statsTable"]//tr')
        team_sk = "".join(re.findall('playerprofiles/(.*)/tabid', response.url))
        team_sk = team_sk.replace('playerlist', '').title()
        if "Seaeagles" in team_sk:
            team_sk = "Sea Eagles"
        if "Weststigers" in team_sk:
            team_sk = "Wests Tigers"
        season = extract_data(sel, '//div[@class="clubPlayerStats__club__title"]//p//text()')
        season = "".join(re.findall(r'\d+', season))
        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/th//a//@href')
            if pl_link:
                pl_link = DOMAIN + pl_link
                player_sk = "".join(re.findall('playerid/(.*)/seasonid', pl_link))
                pl_pos = extract_data(pl_node, './/td[1]//text()')
                pl_pos = pl_pos.strip().replace(' 2', '').replace(' 1', '')

                if pl_pos == '-':
                    pl_pos = ''
                if pl_pos == "Interchange":
                    pl_pos = "Prop"
                if "2nd Row" in pl_pos:
                    pl_pos = "Second-row"
                if "Halfback" in pl_pos:
                    pl_pos = "Half back"
                if "Wing" in pl_pos:
                    pl_pos = "Winger"
                players = {player_sk: {"player_role": pl_pos,
                                      "player_number": '',
                                      "season": season, "status": 'active', \
                                      "entity_type": "participant", \
                                      "field_type": "description", "language": "ENG"}}
                self.participants.setdefault\
                        (team_sk, {}).update(players)
        record['source'] = 'nrl_rugby'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = self.participants
        yield record

