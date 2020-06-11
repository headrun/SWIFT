import MySQLdb
import re
import datetime
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="espn_ncaa-ncb" and source_key= "%s"'

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

class NCBRostersCheck(VTVSpider):
    name = "rosters_check_espnncb"
    start_urls = ['http://espn.go.com/mens-college-basketball/teams']
    domain_url = "http://espn.go.com"

    text = ''
    team_dict = {}

    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.cursor = self.conn.cursor()

    def send_mail(self, text):
        subject    = "NCAA College Men's Basketball"
        server     = 'localhost'
        sender     = 'headrun@veveo.net'

        receivers = ['bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul/li')
        last_node = nodes[350]

        for node in nodes[:351]:
            team_link = extract_data(node, './/a[contains(@href, "mens-college-basketball")]/@href')

            if team_link:
                team_link = team_link.replace('_', 'roster/_')

            if node == last_node:
                crawl = True
            else:
                crawl = False
            if "roster" in team_link:
                yield Request(team_link, callback=self.parse_teams, meta= {'crawl': crawl})

    def parse_teams(self, response):
        sel = Selector(response)
        crawl = response.meta['crawl']
        league_name = "Men's College Basketball"
        #league_name = "COllege Football"
        season = "2015"
        pl_links = extract_list_data(sel, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]//td//a//@href')
        pl_names = extract_list_data(sel, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]//td//a//text()')
        pl_pos   = extract_list_data(sel, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]//td[3]//text()')
        player_count = len(pl_links)
        if "Null" in pl_names:
            player_count = len(pl_links) - 1

        team_sk = "".join(re.findall('\d+', response.url))
        db_count = self.get_db_count('espn_ncaa-ncb', team_sk)
        team_name = extract_data(sel, '//div[@id="sub-branding"]//a//b//text()')
        if type(team_name) == unicode:
            team_name = unicodedata.normalize('NFKD', team_name).encode('ascii','ignore')

        if not db_count:
            db_count = '0'
            color = 'red'
        else:
            color = self.get_color(db_count, player_count)
            if color == "red" and "NA" in pl_pos:
                player_count = len(pl_links) - 1
                color = self.get_color(db_count, player_count)
        self.team_dict.setdefault(league_name, []).append([team_name, season, player_count, db_count[0], color])
        headers = ('Team Name', 'Season', 'Web Count', 'DB Count', 'Matched')
        print crawl
        if len(self.team_dict) == 1 and crawl:
            for key, value in self.team_dict.iteritems():
                self.text += get_html_table(key, headers, value)
            #self.html_file.write('%s' % self.text)
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

