from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, url_status
from sports_spiders.items import SportsSetupItem
import re

TEAM_LIST = ('NE', 'BAL', 'CIN', 'CLE', 'CHI', 'DET',
             'GB', 'MIN', 'HOU', 'IND', 'JAC', 'TEN',
             'ATL', 'CAR', 'NO', 'TB', 'BUF', 'MIA',
             'DAL', 'NYG', 'PHI', 'NYJ', 'WAS', 'DEN',
             'KC', 'OAK', 'SD', 'ARI', 'SF', 'SEA', 'STL', 'PIT')

ROLES_DICT = {'C': 'Center', 'G': 'Guard', 'T': 'Tackle',
              'QB': 'Quarterback', 'RB': 'Running Back',
              'WR': 'Wide Receiver', 'TE': 'Tight End',
              'DT': 'Defensive Tackle', 'DE': 'Defensive End',
              'MLB': 'Middle Linebacker', 'OLB': 'Outside Linebacker',
              'CB': 'Cornerback', 'S': 'Safety', 'K': 'Kicker',
              'H': 'Holder', 'LS': 'Long Snapper',
              'P': 'Punter', 'PR': 'Punt Returner', 'KR': 'Kick Returner',
              'FB': 'Fullback', 'HB': 'Halfback', 'ILB': 'Inside Linebacker',
              'TB': 'Tailback', 'RG': 'Right Guard', 'LG': 'Left Guard',
              'RT': 'Right Tackle', 'LT': 'Left Tackle', 'NG': 'Nose Guard',
              'DL': 'Defensive Line', 'FS': 'Free Safety', 'LB': 'Linebacker',
              'NT': 'Defensive Tackle', 'DB': 'Defensive Back', 'PK': 'Placekicker',
              'SS': 'Strong Safety', 'OG': 'Offensive Guard',
              'OT': 'Offensive Tackle', 'OL': 'Offensive Line',
              'SAF': 'Safety'}

STATUS_DICT = {'RES': "Injured reserve", "ACT": "active",
               "NON": "Non football related injured reserve",
               "SUS": "Suspended", "PUP": "Physically unable to perform",
               "UDF": "Unsigned draft pick", "EXE": "Exempt"}

def get_status(status):
    for key, value in list(STATUS_DICT.items()):
        if status == key:
            pl_status = value
            return pl_status

def get_player_role(position):
    for key, value in list(ROLES_DICT.items()):
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
                url_status(t_url, self.name, '')

                yield Request(t_url, callback = self.parse, \
                              meta = {'team_sk': team})

    def parse(self, response):
        hxs = Selector(response)
        season = "2020"
        participants = {}
        coach_link = extract_data(hxs, '//div[@id="team-main-nav"]//ul//li//a[contains(@href, "coach")]//@href')
        if "http" not in coach_link:
            coach_link = "http://www.nfl.com/teams/" + coach_link
        team_name = extract_list_data(hxs, '//div[@id="team-stats-wrapper"]//tr//td//text()')[0].strip()
        nodes = get_nodes(hxs, \
                '//div[@id="team-stats-wrapper"]//table//tbody//tr')
        record = SportsSetupItem()
        if not nodes:
            message = "Xpath changed"
            url_status(response.url, self.name, message)
 
        for node in nodes:
            status = extract_data(node, './td[4]/text()')
            pl_status = get_status(status)
            pl_name = extract_data(node, './td/a/text()').strip().split(',')
            pl_name = pl_name[-1] + " " + pl_name[0]
            link = extract_data(node, './td/a/@href').strip()
            pl_sk = (re.findall(r'/(\d+)/profile', link))
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

    def parse_coach(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        participants = response.meta['participants']
        players = response.meta['players']
        team_sk = response.meta['team_sk']
        season = response.meta['season']
        coach_name = extract_data(hxs, '//div[@class="coachprofiletext"]//p//span[contains(text(), "Head Coach")]//following-sibling::text()').strip()
        pl_sk = coach_name.lower().replace(' ', '_')
        if team_sk == "MIA" and pl_sk =="name_not_available":
            pl_sk = "adam_gase"
        elif team_sk == "IND" and pl_sk == 'name_not_available':
            pl_sk = "frank_reich"
        elif team_sk == "CHI" and pl_sk == 'name_not_available':
            pl_sk = "matt_nagy"
        elif team_sk == "ARI" and pl_sk == 'name_not_available':
            pl_sk = "steve_wilks"
        elif team_sk == "TEN" and pl_sk == 'name_not_available':
            pl_sk = "mike_vrabel"
        elif team_sk == "DET" and pl_sk == 'name_not_available':
            pl_sk = "matt_patricia"
        elif team_sk == "NYG" and pl_sk == 'name_not_available':
            pl_sk = "pat_shurmur"
        elif team_sk == "OAK" and pl_sk == 'name_not_available':
            pl_sk = "jon_gruden"


        pl_info = {pl_sk: {'player_role': "Head Coach", 'player_number': '', \
        'season': season, 'status': 'active', 'entity_type': 'participant', \
        'field_type': "description", 'language': 'ENG'}}

        participants.setdefault(team_sk, {}).update(pl_info)

        record['source'] = 'NFL'
        record['season'] = season
        record['result_type'] = 'roster'
        record['participants'] = participants
        yield record



