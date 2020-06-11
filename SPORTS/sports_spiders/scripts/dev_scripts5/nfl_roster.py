from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

TEAM_LIST = ('NE', 'BAL', 'CIN', 'CLE', 'CHI', 'DET',
             'GB', 'MIN', 'HOU', 'IND', 'JAC', 'TEN',
             'ATL', 'CAR', 'NO', 'TB', 'BUF', 'MIA',
             'DAL', 'NYG', 'PHI', 'NYJ', 'WAS', 'DEN',
             'KC', 'OAK', 'SD', 'ARI', 'SF', 'SEA', 'STL', 'PIT')

ROLES_DICT = {'C': 'Center', 'G': 'Guard', 'T': 'Tackle',
              'QB': 'Quarterback', 'RB': 'Running back',
              'WR': 'Wide receiver', 'TE': 'Tight end',
              'DT': 'Defensive tackle', 'DE': 'Defensive end',
              'MLB': 'Middle linebacker', 'OLB': 'Outside linebacker',
              'CB': 'Cornerback', 'S': 'Safety', 'K': 'Kicker',
              'H': 'Holder', 'LS': 'Long snapper',
              'P': 'Punter', 'PR': 'Punt returner', 'KR': 'Kick returner',
              'FB': 'Fullback', 'HB': 'Halfback', 'ILB': 'Inside Linebacker',
              'TB': 'Tailback', 'RG': 'Right Guard', 'LG': 'Left Guard',
              'RT': 'Right Tackle', 'LT': 'Left Tackle', 'NG': 'Nose Guard',
              'DL': 'Defensive line', 'FS': 'Free safety', 'LB': 'Linebacker',
              'NT': 'Nose Tackle', 'DB': 'Defensive back', 'PK': 'Placekicker',
              'SS': 'Strong safety', 'OG': 'Offensive guard',
              'OT': 'Offensive Tackle', 'OL': 'Offensive Line',
              'SAF': 'Safety'}

STATUS_DICT = {'RES': "Injured reserve", "ACT": "active",
               "NON": "Non football related injured reserve",
               "SUS": "Suspended", "PUP": "Physically unable to perform",
               "UDF": "Unsigned draft pick", "EXE": "Exempt"}

def get_status(status):
    for key, value in STATUS_DICT.iteritems():
        if status == key:
            pl_status = value
            return pl_status

def get_player_role(position):
    for key, value in ROLES_DICT.iteritems():
        if position == key:
            position = value
            return position

class NflRoster(VTVSpider):
    name = "Nfl_Roster"
    start_urls = []

    def start_requests(self):
        st_url = ['http://www.nfl.com/teams/roster?team=%s']
        for url in st_url:
            for team in TEAM_LIST:
                t_url = url % (team)
                yield Request(t_url, callback = self.parse, \
                              meta = {'team_sk': team})

    def parse(self, response):
        hxs = Selector(response)
        season = "2015-16"
        participants = {}
        coach_link = extract_data(hxs, '//div[@id="team-main-nav"]//ul//li//a[contains(@href, "coach")]//@href')
        if "http" not in coach_link:
            coach_link = "http://www.nfl.com/teams/" + coach_link
        team_name = extract_list_data(hxs, '//div[@id="team-stats-wrapper"]//tr//td//text()')[0].strip()
        nodes = get_nodes(hxs, \
                '//div[@id="team-stats-wrapper"]//table//tbody//tr')
        record = SportsSetupItem()
        for node in nodes:
            status = extract_data(node, './td[4]/text()')
            pl_status = get_status(status)
            pl_name = extract_data(node, './td/a/text()').strip().split(',')
            pl_name = pl_name[-1] + " " + pl_name[0]
            link = extract_data(node, './td/a/@href').strip()
            pl_sk = (re.findall(r'/(\d+)/profile', link))
            pl_link = "http://www.nfl.com" + link
            position = extract_data(node, './td[3]/text()')
            pl_num = extract_data(node, './td[1]/text()')
            position = get_player_role(position)

            players =  {pl_sk[0]: {'player_role': position,
                           'player_number': pl_num,
                           'season': season,
                           'status': pl_status, 'entity_type': 'participant', \
                           'field_type': "description", \
                            'language': 'ENG'}}

            participants.setdefault(response.meta['team_sk'], {}).\
                                    update(players)

        yield Request(coach_link, callback = self.parse_coach, \
                        meta = {'participants': participants, \
                        'players': players, 'team_sk': response.meta['team_sk'], \
                        'season': season})

    def parse_coach(sel, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        participants = response.meta['participants']
        players =response.meta['players']
        team_sk = response.meta['team_sk']
        season = response.meta['season']
        coach_name = extract_data(hxs, '//div[@class="coachprofiletext"]//p//span[contains(text(), "Head Coach")]//following-sibling::text()').strip()
        pl_sk = coach_name.lower().replace(' ', '_')

        pl_info = {pl_sk: {'player_role': "Head coach", 'player_number': '', \
        'season': season, 'status': 'active', 'entity_type': 'participant', \
        'field_type': "description", 'language': 'ENG'}}

        participants.setdefault(team_sk, {}).update(pl_info)

        record['source'] = 'NFL'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = participants
        yield record



