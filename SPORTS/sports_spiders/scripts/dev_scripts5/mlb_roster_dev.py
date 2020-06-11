from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime



ROLE_MAP = {"P" : "Pitcher", "C" : "Catcher",  \
            "1B": "First baseman", \
            "2B": "Second baseman","3B": "Third baseman", \
            "SS": "Shortstop", \
            "LF": "Left fielder", "CF": "Center fielder", "RF": "Right fielder", \
            "DH": "Designated hitter", "OF": "Outfielder"}

class MlbRosterspider(VTVSpider):
    name            = "mlb_rosterspider"
    start_urls      = ['http://mlb.mlb.com/mlb/players/index.jsp']
    participants = {}


    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//select[@id="ps_team"]/option')
        record = SportsSetupItem()
        for node in nodes:
            link = extract_data(node, './@value').strip()
            team_name = extract_data(node, './text()').strip()
            if "Team Rosters" in team_name:
                continue
            if "http:" not in link:
                continue

            yield Request(link, callback = self.parse_listing, \
                    meta = {'team_name':team_name, 'record': record})

    def parse_listing(self, response):
        hxs = HtmlXPathSelector(response)
        record = response.meta['record']
        nodes = get_nodes(hxs, '//table[@class="team_table_results"]/tbody/tr//a')
        record = SportsSetupItem()
        last_node = nodes[-1]
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            player_link = extract_data(node, './@href')
            if not player_link:
                continue
            player_id = re.findall('player_id=(\d*)', player_link)[0]

            player_link = "http://mlb.mlb.com/lookup/json/named.player_info.bam?sport_code='mlb'&player_id='%s'" %(player_id)
            yield Request(player_link, callback = self.parse_details, \
                meta = { 'terminal_crawl': terminal_crawl, \
                        'record': record, 'player_id': player_id})

    def parse_details(self, response):
        record = response.meta['record']
        import pdb;pdb.set_trace()
        season = datetime.datetime.now()
        season = season.year
        raw_data = response.body
        data     = eval(raw_data)
        p_info = data['player_info']['queryResults']['row']
        player_number = p_info['jersey_number']
        role = p_info['primary_position_txt']
        if ROLE_MAP.has_key(role):
            role = ROLE_MAP[role]
        sourcekey = p_info['player_id']
        status  = p_info['status']
        team_callsign =  p_info['team_abbrev']
        record['source'] = "MLB"
        record['season'] = season
        record['result_type'] = "roster"
        record['game'] = 'baseball'
        record['reference_url'] = response.url
        players = { sourcekey: { "player_role": role, "player_number": player_number, \
                    "season": season, "status": status}}
        self.participants.setdefault(team_callsign, {}).update(players)
        record['participants'] = self.participants
        if response.meta['terminal_crawl'] :
            import pdb;pdb.set_trace()
            yield record
