# -*- coding: utf-8 -*-
from scrapy.http import Request
from scrapy.selector import Selector
from sports_spiders.vtvspider import VTVSpider, extract_data, \
    get_nodes, extract_list_data, url_status
from sports_spiders.items import SportsSetupItem
import re
import datetime
import urllib.request, urllib.error, urllib.parse

ROLE_MAP = {"P": "Pitcher", "C": "Catcher",
            "1B": "Infielder",
            "2B": "Infielder", "3B": "Infielder",
            "SS": "Infielder", 'Bullpen Catcher': 'Bullpen Catcher',
            "LF": "Outfielder", "CF": "Outfielder", "RF": "Outfielder",
            "DH": "Designated Hitter", "OF": "Outfielder",
            'infielder': 'Infielder'}


SK_MAP = {'Athletics': 'OAK',
          'Marlins': 'MIA',
          'Blue Jays': 'TOR',
          'Red Sox': 'BOS',
          'Braves': 'ATL',
          'Reds': 'CIN',
          'Yankees': 'NYY',
          'Twins': 'MIN',
          'Brewers': 'MIL',
          'Nationals': 'WSH',
          'Padres': 'SD',
          'Cubs': 'CHC',
          'Rockies': 'COL',
          'Mets': 'NYM',
          'Rangers': 'TEX',
          'Orioles': 'BAL',
          'Pirates': 'PIT',
          'Tigers': 'DET',
          'Cardinals': 'STL',
          'Mariners': 'SEA',
          'Diamondbacks': 'ARI',
          'Giants': 'SF',
          'Astros': 'HOU',
          'Royals': 'KC',
          'Indians': 'CLE',
          'White Sox': 'CWS',
          'Rays': 'TB',
          'Angels': 'LAA',
          'Phillies': 'PHI',
          'Dodgers': 'LAD'}


class MlbRoster(VTVSpider):
    name = "mlb_roster"
    start_urls = ['http://mlb.mlb.com/mlb/players/index.jsp']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//select[@id="ps_team"]/option')
        record = SportsSetupItem()
        if not nodes:
            message = "Xpath changed"
            url_status(response.url, self.name, message)

        for node in nodes[1:]:
            link = extract_data(node, './@value').strip()
            team_name = extract_data(node, './text()').strip()
            if "Team Rosters" in team_name:
                continue
            if "http:" not in link:
                continue
            #team_sk = link.split('=')[-1].upper()
            team_sk = SK_MAP.get(team_name, '')
            yield Request(link, callback=self.parse_listing,
                          meta={'team_name': team_name, 'record': record, 'team_sk': team_sk})

    def parse_listing(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        nodes = get_nodes(
            hxs, '//table[@class="team_table_results"]/tbody/tr//a')

        if not nodes:
            nodes = get_nodes(
                hxs, '//table[@class="data roster_table"]/tbody/tr/td/a')
        record = SportsSetupItem()
        last_node = nodes[-1]
        participants = {}
        team_name = extract_data(hxs, '//div[@id="header-logo"]//@alt')
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            player_link = extract_data(node, './@href')
            if not player_link:
                continue
            pl_link = response.url.replace('/roster/40-man', '') + player_link
            player_id = pl_link.split('/')[-2].strip()
            player_name = extract_data(node, './/text()')

            player_link = "http://mlb.mlb.com/lookup/json/named.player_info.bam?sport_code='mlb'&player_id=%s" % (
                player_id)
            player_data = urllib.request.urlopen(player_link).read()
            data = eval(player_data)

            season = datetime.datetime.now()
            season = season.year

            p_info = data['player_info']['queryResults']['row']
            player_number = p_info['jersey_number'].strip('\n ')
            role = p_info['primary_position_txt']
            if role in ROLE_MAP:
                role = ROLE_MAP[role]
            if team_name and pl_link and player_name:
                pl_desc = self.get_player_description(
                    player_name, team_name, pl_link, role)
            else:
                pl_desc = ""

            sourcekey = p_info['player_id']
            status = p_info['status']
            status = status.lower()
            #status = status.replace('nri','active')
            if status == 'active':
                status = 'active'
            else:
                status = 'inactive'
            team_callsign = p_info['team_abbrev']
            record['source'] = "MLB"
            record['season'] = season
            record['result_type'] = "roster"
            players = {sourcekey: {"player_role": role,
                                   "player_number": player_number,
                                   "season": season, "status": status,
                                   "field_text": pl_desc,
                                   'field_type': "description", "language": "ENG"}}
            #participants.setdefault(team_callsign, {}).update(players)
            team_sk = response.meta['team_sk']
            participants.setdefault(team_sk, {}).update(players)
            record['participants'] = participants
            if terminal_crawl:
                coaches = extract_data(
                    hxs, '//a[contains(text(), "Coaching Staff")]/@href')
                if not coaches:
                    coaches = extract_data(
                        hxs, '//a[contains(text(), "Coaches")]/@href')
                if coaches:
                    if 'http' not in coaches:
                        coaches = response.url.split('/roster')[0] + coaches
                    yield Request(coaches, self.parse_coaches, meta={'record': record,
                                                                     'participants': participants, 'season': season,
                                                                     'team_callsign': team_callsign})

    def parse_coaches(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        team_callsign = response.meta['team_callsign']
        participants = response.meta['participants']
        season = response.meta['season']
        nodes = get_nodes(hxs, '//tbody[@class="coaches"]/tr')
        if not nodes:
            nodes = get_nodes(hxs, '//table[@class="data roster_table"]//tr')
        for node in nodes:
            link = extract_data(node, './/td//a//@href')
            data = extract_list_data(node, './/td//text()')
            if not data:
                continue
            type_ = data[-1]
            pl_number = data[0].replace('\\u2014', '').strip()
            if link:  # and 'coach' in type_.lower():
                pl_sk = re.findall('\d+', link)[0]
                players = {pl_sk: {'player_role': type_, "season": season,
                                   "status": 'active', 'player_number': pl_number,
                                   'field_type': "description", "language": "ENG"}}
                participants.setdefault(team_callsign, {}).update(players)
        record['participants'] = participants
        yield record

    def get_player_description(self, pl_name, team_name, pl_link, role):
        if role:
            pl_desc = pl_name + " is a baseball " + role + \
                " player playing for MLB Baseball team " + team_name + ". " + pl_link
        else:
            pl_desc = pl_name + " is a baseball player playing for MLB Baseball team " + \
                team_name + ". " + pl_link
        return pl_desc
