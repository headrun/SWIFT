import MySQLdb
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata
import re


SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="nrl_rugby" and source_key= "%s"'

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

class NRLRostersCheck(VTVSpider):
    name = "rosters_check_nrl"
    start_urls = ['http://www.nrl.com/DrawResults/Statistics/PlayerStatistics/tabid/10877/Default.aspx']
    domain_url = "http://www.nrl.com"

    text = ''   
    team_dict = {}

    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        #self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'National Rugby League Rosters'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'

        #receivers = ['raman.arunachalam@rovicorp.com', 'vineet.agarwal@rovicorp.com', 'sports@headrun.com']
        #receivers = ['jhansi@headrun.com', 'niranjansagar@headrun.com', 'bibeejan@headrun.com']
        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//div[@class="nwTeam"]//a')
        last_nodes = team_nodes[-1]
        for team_node in team_nodes:
            team_url = extract_data(team_node, './/@href')
            if team_node == last_nodes:
                crawl = True
            else:
                crawl = False
            print crawl
            if team_url:
                team_url = self.domain_url + team_url
                yield Request(team_url, callback = self.parse_players, meta = {'crawl': crawl})

    def parse_players(self, response):
        sel = Selector(response)
        league_name = "NRL Rugby"
        pl_nodes = extract_list_data(sel, '//table[@class="clubPlayerStats__statsTable"]//tr//th//a//@href')
        team_sk = "".join(re.findall('playerprofiles/(.*)/tabid', response.url))
        team_sk = team_sk.replace('playerlist', '').title()
        team_name = extract_data(sel, '//div[@class="clubPlayerStats__club__title"]//h2//text()')
        season = extract_data(sel, '//div[@class="clubPlayerStats__club__title"]//p//text()')
        season = "".join(re.findall(r'\d+', season))
        crawl = response.meta['crawl']
        player_count = len(pl_nodes)

        if "Seaeagles" in team_sk:
            team_sk = "Sea Eagles"
        if "Weststigers" in team_sk:
            team_sk = "Wests Tigers"
 
        db_count = self.get_db_count('nrl_rugby', team_sk)
        if not db_count:
            db_count = '0'
            color = 'red'
        else:
            color = self.get_color(db_count, player_count)

        self.team_dict.setdefault(league_name, []).append([team_name, season, player_count, db_count[0], color])
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


