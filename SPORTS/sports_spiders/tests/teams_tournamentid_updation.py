import re
#from datetime import datetime
#from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
#from vtvspider import extract_data, extract_list_data, get_nodes, log, get_utc_time
import MySQLdb

UPDATE_TEAM_TOU_ID = 'UPDATE sports_teams set tournament_id = %s where participant_id = %s limit 1'

FIELDS = '(participant_id, tournament_id, status, status_remarks, standing, seed, season, created_at, modified_at)'
QUERY = "INSERT INTO sports_tournaments_participants %s" % FIELDS + " VALUES (%s, %s, '', '', '', '', %s, now(), now())"
DUP_QUERY = " ON DUPLICATE KEY UPDATE participant_id = %s, season = %s"

INSERT_TOU_PARTICIPANTS = QUERY + DUP_QUERY

class TeamsTournaments(VTVSpider):
    name = 'teams_tournaments'
    outfile = open('missed_teams', 'w+')
    start_urls = []

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def start_requests(self):
        league_list = ['aut', 'eng', 'den', 'por', 'sco', 'ger', 'gre', 'esp', 'sui',
                       'tur', 'rus', 'fra', 'swe', 'ned', 'ita', 'rou', 'ukr', 'pol',
                       'alb', 'and', 'arm', 'aze', 'blr', 'cyp', 'cze', 'est', 'fro',
                       'fin', 'mkd', 'geo', 'gib', 'hun', 'bih', 'isl', 'isr', 'kaz',
                       'lva', 'ltu', 'lux', 'mlt', 'mda', 'mne', 'nir', 'nor', 'irl',
                       'srb', 'svk', 'svn', 'wal', 'bul', 'cro', 'bel']
        league_list = ['ita']

        top_url = 'http://www.uefa.com/memberassociations/association=%s/domesticleague/standings/index.html'
        for league in league_list:
            url = top_url%league
            yield Request(url, callback=self.parse, meta={})

    def get_tournament_id(self, tournament):
        query = 'select id from sports_tournaments where title = %s'
        values = (tournament)
        self.cursor.execute(query, values)
        tournament_id = self.cursor.fetchone()
        if tournament_id:
            tournament_id = tournament_id[0]

        return tournament_id

    def get_team_id(self, team):
        query = 'select entity_id from sports_source_keys where entity_type = %s and source = %s and source_key = %s'
        values = ('participant', 'uefa_soccer', team)
        self.cursor.execute(query, values)
        team_id = self.cursor.fetchone()
        if team_id:
            team_id = team_id[0]

        return team_id

    def clear_previous_team_tournament_ids(self, tournament_id, tournament):
        query = 'update sports_teams set tournament_id = %s where tournament_id = %s'
        values = (0, tournament_id)
        if tournament_id:
            self.cursor.execute(query, values)
        else:
            print tournament

        query = 'delete from sports_tournaments_participants where tournament_id = %s'
        values = (tournament_id)
        self.cursor.execute(query, values)

    def update_team_tournament_id(self, team_sk, tournament_id, tournament, season):
        team_id = self.get_team_id(team_sk)
        if tournament_id and team_id:
            self.cursor.execute(UPDATE_TEAM_TOU_ID, (tournament_id, team_id))
            self.cursor.execute(INSERT_TOU_PARTICIPANTS, (team_id, tournament_id, season, team_id, season))
        else:
            self.outfile.write("Missed %s, Team_sk %s\n" % (tournament, team_sk))

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//table[@data-plugin="tablesorter"]//tr')
        tournament_list = extract_data(sel, '//div[contains(@class, "t_standing")]//h3[contains(@class, "bigTitle")]/text()').split(' ')
        season = tournament_list[-1].replace('/', '-').strip()
        tournament = " ".join(tournament_list[:-1])
        if "2014-15" in season or "2014" in season:
            if "Scottish" in tournament:
                tournament = "Scottish Premier League"
            tournament_id = self.get_tournament_id(tournament)
            print "%s is in %s season" %(tournament, season)
            self.clear_previous_team_tournament_ids(tournament_id, tournament)
            for node in nodes[2:]:
                team = extract_data(node, './/td[contains(@class, "club")]/a/@href')
                try:
                    team_sk = re.findall(r'club=(\d+)/', team)[0]
                except:
                    continue
                if team_sk and tournament:
                    self.update_team_tournament_id(team_sk, tournament_id, tournament, season)
        else:
            print "Tournament is not in 2014/15 or 2014 season", tournament
