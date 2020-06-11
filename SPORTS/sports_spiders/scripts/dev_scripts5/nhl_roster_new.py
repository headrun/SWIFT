from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import MySQLdb

class NhlRosterNEW(VTVSpider):
    name = "nhlroster_new"
    start_urls = ['http://www.nhl.com/ice/teams.htm?navid=nav-tms-main']
    participants = {}

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//div[@id="realignmentMap"]//ul[@class="teamData"]')
        for node in nodes:
            callsign  = extract_data(node, './/a/@rel')
            callsign = callsign.upper()
            team_url = extract_data(node, './/a[contains(@href, "roster")]/@href').strip()
            if "index" in team_url:
                continue
            yield Request(team_url, callback = self.parse_roster, \
            meta = {'call_sign': callsign, \
            'record': record})


    def parse_roster(self, response):
        hxs = Selector(response)

        nodes = get_nodes(hxs, '//div/table[@class="data"]//tr[not(contains(@class, "hdr"))]')
        last_node = nodes[-2]
        record = response.meta['record']
        for node in nodes[::1]:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            player_id = extract_data(node, './/a/@href')
            player_name = extract_data(node, './/a/text()')
            player_id = re.findall(r"\d+", player_id)
            if not player_id:
                continue
            if player_id:
                player_id = player_id[0]
            player_link = 'http://www.nhl.com/ice/player.htm?id=%s' % (player_id)
            yield Request(player_link, callback = self.parse_details, \
            meta = {'call_sign': response.meta['call_sign'],
            'player_id': player_id, 'terminal_crawl': terminal_crawl, 'record': record})

        coach_nodes = get_nodes(hxs, '//div[@id="siteMenu"]//ul//li//a')
        for coach_node in coach_nodes:
            link = extract_data(coach_node, './/@href')
            title = extract_data(coach_node, './/text()')
            if "Coaches/Training Staff" in title:
                yield Request(link, callback=self.parse_coach, meta ={'call_sign': response.meta['call_sign'], 'title': title, 'record': record})

    def parse_coach(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        team_sk = response.meta['call_sign']
        title = response.meta['title']
        nodes = get_nodes(hxs, '//table//tr')
        for node in nodes:
            pl_link = extract_list_data(node, './/td[@width="25%"]//a//@href')[0]
            player_id  = re.findall(r"\d+", pl_link)
            if player_id:
                player_id = player_id[0]
            season = "2014-15"
            pl_role = extract_data(node, './/td[@width="75%"]/text()')

            if "Head Coach" not in pl_role:
                continue
            players = { player_id: {'player_role': "Head Coach", 'player_number': '', \
                        'season': season, 'status': "active" ,'entity_type': "participant", \
                        'field_type': 'description', 'language': "ENG"}}
            self.participants.setdefault(team_sk, {}).update(players)
        
            record['participants'] = self.participants
            record['season'] = season
            record['result_type'] = "roster"
            record['source'] = "NHL"
            import pdb;pdb.set_trace()
            yield record



    def parse_details(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        player_sk = response.meta['player_id']

        team_sk   = response.meta['call_sign']
        season = "2014-15"
        nodes = get_nodes(hxs, '//div[contains(@style, "border-bottom: 1px solid #666;")]')
        for node in nodes:
            name = extract_data(node, './/h1/div/text()')
            player_number = extract_data(node, './/span[@class="sweater"]//text()').replace('#', '')
            player_role  = extract_data(node, './/span[@style="color: #666;"]//text()').replace('\n', '')
            players = { player_sk: {'player_role': player_role, 'player_number': player_number, \
                        'season': season, 'status': "active" ,'entity_type': "participant", \
                        'field_type': 'description', 'language': "ENG"}}
            self.participants.setdefault(team_sk, {}).update(players)
        record['participants'] = self.participants
        record['season'] = season
        record['result_type'] = "roster"
        record['source'] = "NHL"
        if response.meta['terminal_crawl']:
            yield record
