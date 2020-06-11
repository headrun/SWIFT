from scrapy.http import Request
import json
from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

team_list = ['4444','532','579','496','442','520','523','528','536','502','562','569','5010','447','560', '434']

class MexLeagueRosters(VTVSpider):
    name            = "mex_league_rosters"
    start_urls      = []
    participants = {}

    start_url = 'http://www.milb.com/lookup/json/named.roster_all.bam?team_id=%s'
    for team_lis in team_list:
        top_ul = start_url % (team_lis)
        start_urls.append(top_ul)

    print start_urls
    def parse(self, response):
        data = eval(response.body)
        roster_data = data['roster_all']['queryResults']['row']
        record = SportsSetupItem()
        for rs_data in roster_data:
            player_sk = rs_data['player_id']
            status    = rs_data['status_short']
            pos       = rs_data['position_desig']
            team_sk   = rs_data['team_abbrev']
            pl_number = rs_data['jersey_number']
            players = {player_sk: { "player_role": pos, "player_number": pl_number, \
                         "season": "2015", "status": status}} 
            self.participants.setdefault(team_sk, {}).update(players)
        record['source'] = "MLB"
        record['season'] = "2015"
        record['result_type'] = "roster"
        record['game'] = 'baseball'
        record['reference_url'] = response.url
        record['participants'] = self.participants
        import pdb;pdb.set_trace()
        yield record




