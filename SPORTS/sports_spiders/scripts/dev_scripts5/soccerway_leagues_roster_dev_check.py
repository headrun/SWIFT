import MySQLdb
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="soccerway_soccer" and source_key= "%s"'

TOU_QUERY = 'select category, tournament_id from sports_teams where participant_id=%s'

TEAM_QUERY = 'select title, participant_type from sports_participants where id= %s'

TOU_NAME = 'select title from sports_tournaments where id=%s'

status_dict = {'green' : 'YES', "red" : "NO", "orange": "active"}

def get_html_table(title, headers, table_body):
    table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        table_data += '<th>%s</th>' % header
    table_data += '</tr>'

    for data in table_body:
        table_data += '<tr>'
        for index, row in enumerate(data):
            if index == 4:
                table_data += '<td style="color: %s">%s</td>' % (row, status_dict[row])
            else:
                table_data += '<td>%s</td>' % (str(row))
        table_data += '</tr>'
    table_data += '</table>'

    return table_data

TEAM_ID = 'select entity_id from sports_source_keys where entity_type="participant" and source="%s" and source_key="%s"'

ROSTER_COUNT = 'select count(*) from sports_roster where status="active" and team_id=%s'

class SoccerWAYRostersCheck(VTVSpider):
    name = "rosters_check_soccerway"
    start_urls = ['http://int.soccerway.com/teams/club-teams/']
    domain_url = "http://int.soccerway.com"

    text = ''
    team_dict = {}

    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        #self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'Soccerway League Rosters'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'

        #receivers = ['raman.arunachalam@rovicorp.com', 'vineet.agarwal@rovicorp.com', 'sports@headrun.com']
        receivers = ['jhansi@headrun.com', 'niranjansagar@headrun.com', 'bibeejan@headrun.com']
        #receivers = ['bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')


    def parse(self, response):
        sel = Selector(response)
        leagues = get_nodes(sel, '//ul[@class="areas"]/li[@class="expandable "]/div[@class="row"]')
        for league in leagues:
            league_name = extract_data(league, './a/text()').strip()
            if league_name in ['Iceland']:
                league_link = extract_data(league, './a/@href').strip()
                if league_link:
                    league_link = self.domain_url + league_link
                    yield Request(league_link, self.parse_league)

    def parse_league(self, response):
        sel = Selector(response)
        country = extract_data(sel, '//div[@class="block  clearfix block_competition_left_tree-wrapper"]/h2/text()')
        nodes = get_nodes(sel, '//ul[@class="left-tree"]/li/a')
        for node in nodes:
            league_link = extract_data(node, './@href')
            if 'iceland/urvalsdeild' in league_link:
                league_link = self.domain_url + league_link
                yield Request(league_link, self.parse_details, dont_filter=True)

    def parse_details(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//table[@class="leaguetable sortable table detailed-table"]//tr[contains(@class,"team_rank")]')
        last_node = nodes[-1]
        league_name = extract_data(sel, '//div[@id="subheading"]//h1//text()')
        league_name = "Icelandic Premier League"
        self.boolean.append(league_name)
        for node in nodes:
            team_sk = extract_data(node, './/@data-team_id')
            team_name = extract_data(node, './/td[@class="text team large-link"]//a//text()')

            if type(team_name) == unicode:
                team_name = unicodedata.normalize('NFKD', team_name).encode('ascii','ignore')

            if len(self.boolean) == 1 and node == last_node:
                crawl = True
            else:
                crawl = False


            link = self.domain_url + extract_data(node, './/a[contains(@href, "/teams/")]/@href')
            yield Request(link, self.parse_teams, meta={'team_sk': team_sk, 'team_name': team_name, 'crawl': crawl})

    def parse_teams(self, response):
        sel = Selector(response)
        team_name = response.meta['team_name']
        crawl = response.meta['crawl']
        league_name = "Icelandic Premier League"
        season = extract_data(sel, '//select[@name="season_id"]//option[@selected]/text()').replace('/2015', '-15').strip()
        pl_links = extract_list_data(sel, '//table[@class="table squad sortable"]//tr/td/a[contains(@href, "/players/")]')
        player_count = len(pl_links)
        team_sk = response.meta['team_sk']
        db_count = self.get_db_count('soccerway_soccer', team_sk)
        if not db_count:
            db_count = '0'
            color = 'red'
        else:
            color = self.get_color(db_count, player_count)

        if '14' not in season:
            self.team_dict.setdefault(league_name, []).append([team_name, season, player_count, db_count[0], color])
        else:
            self.team_dict.setdefault(league_name, []).append([])
        headers = ('Team Name', 'Season', 'Web Count', 'DB Count', 'Matched')

        if len(self.team_dict) == 1 and crawl:
            for key, value in self.team_dict.iteritems():
                if value[0]:
                    self.text += get_html_table(key, headers, value)

            self.send_mail(self.text)

    def get_color(self, db_count, web_count):
        if isinstance(db_count, list):
            db_count = db_count[0]
        if int(db_count) == int(web_count):
            color = "green"
        elif int(web_count) > int(db_count):
            color = "red"
        elif int(web_count) < int(db_count):
           color = "orange"
        return color

    def get_db_count(self, source, team_sk):
        self.cursor.execute(TEAM_ID % (source, team_sk))
        team_id = self.cursor.fetchone()
        db_count = 0
        if team_id:
            query = ROSTER_COUNT % (str(team_id[0])) + ' and player_role not like "%coach%"'
            self.cursor.execute(query)
            pt_ids = self.cursor.fetchall()
            db_count = [str(i[0]) for i in pt_ids]
            return db_count

