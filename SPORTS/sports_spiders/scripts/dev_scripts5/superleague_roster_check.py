import MySQLdb
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="superleague_rugby" and source_key= "%s"'

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

class SuperleagueRostersCheck(VTVSpider):
    name = "rosters_check_superleague"
    start_urls = ['http://www.superleague.co.uk/stats/club_stats']
    domain_url = "http://www.superleague.co.uk"

    text = ''
    team_dict = {}

    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'Super League Rugby Rosters'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'

        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')


    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//table[@class="fullstattable"]//tr//td//a[contains(@href, "/club")]')
        last_nodes = team_nodes[-1]
        season = extract_list_data(sel, '//select[@name="seasonSelector"]//option//text()')[0]
        for team_node in team_nodes:
            team_url = extract_data(team_node, './/@href')
            team_name = extract_data(team_node, './/text()')
            if team_node == last_nodes:
                crawl = True
            else:
                crawl = False
            if "http" not in team_url:
                team_url = self.domain_url+ team_url

            yield Request(team_url, callback=self.parse_next, meta = {'season': season, 'team_name':team_name, 'crawl':crawl})

    def parse_next(self, response):
        sel = Selector(response)
        team_name = extract_data(sel, '//table//tr//td[contains(text(), "Club")]//following-sibling::td//text()').strip()
        team_sk = team_name.lower().replace(' ', '_')
        pl_nodes = extract_list_data(sel, '//div[@class="squad"]//div[@class="player"]//a/@href')
        player_count = len(pl_nodes)
        league_name = "Super League Rugby"
        season = response.meta['season']
        crawl = response.meta['crawl']
        db_count = self.get_db_count('superleague_rugby', team_sk)
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



