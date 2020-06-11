import MySQLdb
from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data

KEYS = {'eu': '72vxpdus7zy22a93kcnhjqct', 'na': 'kpwbhzaxkpxhfgp3zdaqe2xw'}

class RadarSoccer(VTVSpider):
    start_urls = []
    name = 'radar_soccer'

    url = 'https://api.sportradar.us/soccer-p2/%s/teams/hierarchy.xml?api_key=%s'
    team_url = 'https://api.sportradar.us/soccer-p2/%s/teams/%s/profile.xml?api_key=%s'

    def __init__(self):
        self.conn        = MySQLdb.connect(host='10.4.18.183', user='root', db= 'SPORTSDB')
        self.cursor      = self.conn.cursor()
        self.team_apis   = []
        self.teams_file  = open('eu_teams_list', 'w+')
        self.players_file = open('eu_player_details_file', 'w+')

    def populate_team_sk(self, name, radar_sk):
        query = 'select id from sports_participants where game="soccer" and gid like "%TEAM%" and title like %s'
        t_name = '%' + name + '%'
        self.cursor.execute(query, name)

    def start_requests(self):
        for league in ['eu']:#['eu', 'na']:
            api_key = KEYS[league]
            req_url = self.url % (league, api_key)
            yield Request(req_url, self.parse, meta = {'league': league, 'api_key': api_key})

    def parse(self, response):
        league = response.meta['league']
        league_api_key = response.meta['api_key']
        hxs = Selector(response)
        hxs.remove_namespaces()
        countries = get_nodes(hxs, '//category')
        for country in countries:
            tournaments_grps = get_nodes(country, './/tournament_group')
            for grp in tournaments_grps:
                tournaments = get_nodes(grp, './/tournament')
                for tournament in tournaments:
                    teams = get_nodes(tournament, './/team')
                    for team in teams:
                        team_radar_sk = extract_data(team, './@id')
                        name = extract_data(team, './@name')
                        alias = extract_data(team, './@alias')
                        self.team_apis.append(team_radar_sk)
                        record = '<>'.join([name, team_radar_sk])
                        self.teams_file.write('%s\n' % record)
                        #self.populate_team_sk(name, team_radar_sk)

        for api_key in self.team_apis:
            req_url = self.team_url % (league, api_key, league_api_key)
            yield Request(req_url, self.parse_next, meta={'league': league})

    def parse_next(self, response):
        hxs = Selector(response)
        hxs.remove_namespaces()
        team_name = extract_data(hxs, '//team/@name')
        players = get_nodes(hxs, '//roster//player')
        for node in players:
            player_sk = extract_data(node, './@id')
            first_name = extract_data(node, './@first_name')
            last_name  = extract_data(node, './@last_name')
            birthdate  = extract_data(node, './@birthdate')
            name       = first_name + ' ' + last_name
            full_first_name =  extract_data(node, './@full_first_name')
            full_last_name = extract_data(node, './@full_last_name')
            record = [name, birthdate, player_sk, team_name, full_first_name, full_last_name]
            self.players_file.write('%s\n' % record)
            #self.add_player_sk(name, birthdate, player_sk)

    def add_player_sk(self, name, birthdate, radar_sk):
        query = 'select sp.id from sports_participants sp, sports_players pl where game="soccer" and sp.id=pl.participant_id and title =%s and birth_date=%s'
        self.cursor.execute(query, (name, birthdate))
        data = self.cursor.fetchone()
        if data:
            data = str(data[0])
