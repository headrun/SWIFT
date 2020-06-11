import re
import MySQLdb
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="uefa_soccer" and source_key= "%s"'

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

class UEFARostersCheck(VTVSpider):
    name = "rosters_check_uefa"
    start_urls = ['http://www.uefa.com/memberassociations/leaguesandcups/index.html']
    domain_url = "http://www.uefa.com"

    text = ''
    team_dict = {}

    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        #self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

    def send_mail(self, text):
        subject    = 'UEFA Top 5 League Rosters'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'

        #receivers = ['raman.arunachalam@rovicorp.com', 'vineet.agarwal@rovicorp.com', 'sports@headrun.com']
        receivers = ['jhansi@headrun.com', 'niranjansagar@headrun.com', 'bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@id="ListDomesticLeague"]/ul/li')

        for node in nodes:
            league_link = extract_data(node, './div[1]/a[contains(@href,"memberassociations")]/@href').strip()
            if not "http" in league_link:
                req_link = self.domain_url + league_link
                league = league_link.split('association=')[-1].split('/')[0]
                if league in ['den', 'swe']:#['eng', 'esp', 'ita', 'fra', 'ger']:
                    yield Request(req_link, callback=self.parse_league)

    def parse_league(self, response):
        hxs = Selector(response)
        league_name = extract_data(hxs, '//div/h3[contains(@class, "bigTitle")]/text()').split('20')[0].strip()
        if type(league_name) == unicode:
            league_name = unicodedata.normalize('NFKD', league_name).encode('ascii','ignore')
        team_nodes = get_nodes(hxs, '//tr[td[contains(@class,"win nosort")]]')
        season = extract_data(hxs, '//h3[contains(@class, "bigTitle")]/text()')
        season = "".join(re.findall(r' (\d+/\d+)', season))
        self.boolean.append(league_name)
        last_node = team_nodes[-1]

        for node in team_nodes:
            team_name    = extract_data(node, './/a/text()')
            if type(team_name) == unicode:
                team_name = unicodedata.normalize('NFKD', team_name).encode('ascii','ignore')
            team_link    = extract_data(node, './/a/@href')
            if not 'http' in team_link:
                team_link = self.domain_url + team_link
            if len(self.boolean) == 1 and node == last_node:
                crawl = True
            else:
                crawl = False
            yield Request(team_link, self.parse_teams, meta={'team_name': team_name, 'season': season,
                                                             'crawl': crawl, 'league_name': league_name})

    def parse_teams(self, response):
        hxs = Selector(response)
        team_name = response.meta['team_name']
        crawl = response.meta['crawl']
        league_name = response.meta['league_name']
        pl_links = extract_list_data(hxs, '//div[@id="SquadList"]//tr[@class="player"]')
        player_count = len(pl_links)
        team_sk = re.findall('\d+', response.url)[0]
        db_count = self.get_db_count('uefa_soccer', team_sk)
        if not db_count:
            db_count = '0'
            color = 'red'
        else:
            color = self.get_color(db_count, player_count)
        season = response.meta['season']

        if '14' not in season:
            self.team_dict.setdefault(league_name, []).append([team_name, season, player_count, db_count[0], color])
        else:
            self.team_dict.setdefault(league_name, []).append([])
        headers = ('Team Name', 'Season', 'Web Count', 'DB Count', 'Matched')
        if len(self.team_dict) == 1 and crawl:
            print 'here'
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

