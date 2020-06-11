import MySQLdb
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata
import datetime

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
                table_data += '<td style="color: %s">%s</td>' % (row.encode('utf-8'), status_dict[row])
            else:
                try:
                    table_data += '<td>%s</td>' % (str(row))
                except:
                    table_data += '<td>%s</td>' % (str(row.encode('utf-8')))
        table_data += '</tr>'
    table_data += '</table>'

    return table_data

TEAM_ID = 'select entity_id from sports_source_keys where entity_type="participant" and source="%s" and source_key="%s"'

ROSTER_COUNT = 'select count(*) from sports_roster where status="active" and team_id=%s'


def create_cursor():
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor


class SoccerwayRostersCheck(VTVSpider):
    name = "check_soccerway"
    start_urls = ['http://int.soccerway.com/teams/club-teams/']
    domain_url = "http://int.soccerway.com"

    text = ''
    team_dict = {}
    boolean      = []
    subject = ''

    data = open("soccerway_countries.txt","r").read()
    leagues_dict = eval(data)

    tous = {'Brazil': 'Campeonato Brasileiro Serie A',
            'Chile': 'Chilean Primera Division',
            'Peru': 'Peruvian Primera Division',
            'Uruguay': 'Uruguan Primera Division',
            'Venezuela': 'Venezuelan Primera Division',
            'Argentina': 'Argentine Primera Division',
            'Bolivia': 'Liga de Futbol Profesional Boliviano',
            'Finland': 'Finnish First Division',
            'Faroe Islands': 'Faroese Premier Division',
            'Spain': 'La Liga',
            'Germany': 'German Bundesliga',
            'England': 'English Premier League',
            'Italy': 'Serie A',
            'France': 'Ligue 1',
            'Norway': 'Norwegian Premier Division',
            'Sweden': 'Swedish First Division',
            'Finland': 'Finnish First Division',
            'Iceland': 'Icelandic Premier League',
            'Denmark': 'Danish Super League',
            'Colombia': 'Categoria Primera A',
            'Paraguay' : 'Paraguayan Primera Division',
            'Ecuador': 'Ecuadorian Serie A',
            'Wales': 'Wales Premier League',
            'Scotland': 'Scotland Premier League'}

    major_leagues  = ['Germany', 'Italy', 'France', 'Spain']
    sa_leagues     = ['Argentina','Bolivia','Brazil','Chile', \
                      'Columbia','Ecuador','Paraguay','Peru', \
                      'Uruguay','Venezuela']
    danish_league  = ['Denmark']
    english_league = ['England']
    scandavians    = ['Norway', 'Sweden', 'Finland', 'Iceland']
    uk_leagues     = ['Wales', 'Scotland']


    def send_mail(self, text):
        server     = 'localhost'
        sender     = 'headrun@veveo.net'
        receivers  = ['sports@headrun.com', 'bandirmnd@gmail.com']
        vtv_send_html_mail_2('', server, sender, receivers, self.subject, '', text, '')

    def parse(self, response):
        sel = Selector(response)
        leagues = get_nodes(sel, '//ul[@class="areas"]/li[@class="expandable "]/div[@class="row"]')
        for league in leagues:
            league_name = extract_data(league, './a/text()').strip()
            league_link = extract_data(league, './a/@href').strip()
            league_link = self.domain_url + league_link
            if not league_link:
                continue
            if league_name in self.major_leagues and self.spider_type == 'uefa_leagues':
                self.subject = 'UEFA Major Leagues'
                yield Request(league_link, self.parse_league, meta={'league': league_name, 'country_list': 4})
            elif league_name in self.scandavians and self.spider_type == 'scandavian_leagues':
                self.subject = 'Scandavian Leagues'
                yield Request(league_link, self.parse_league, meta={'league': league_name, 'country_list': 4})
            elif league_name in self.danish_league and self.spider_type == 'danish_league':
                self.subject = 'Danish Super League'
                yield Request(league_link, self.parse_league, meta={'league': league_name, 'country_list': 1})
            elif league_name in self.english_league and self.spider_type == 'english_league':
                self.subject = 'English Premier League'
                yield Request(league_link, self.parse_league, meta={'league': league_name, 'country_list': 1})
            elif league_name in self.uk_leagues and self.spider_type == 'uk_leagues':
                self.subject = 'UK Leagues'
                yield Request(league_link, self.parse_league, meta={'league': league_name, 'country_list': 2})
            elif league_name in self.sa_leagues and self.spider_type == 'sa_leagues':
                self.subject = 'South American Leagues'
                yield Request(league_link, self.parse_league, meta={'league': league_name, 'country_list': 10})

    def parse_league(self, response):
        sel = Selector(response)
        league = response.meta['league']
        country_list = response.meta['country_list']
        country = extract_data(sel, '//div[@class="block  clearfix block_competition_left_tree-wrapper"]/h2/text()')
        nodes = get_nodes(sel, '//ul[@class="left-tree"]/li/a')
        for node in nodes:
            league_link = extract_data(node, './@href')
            if self.leagues_dict[league] in league_link:
                league_link = self.domain_url + league_link
                yield Request(league_link, self.parse_details, dont_filter=True, \
                    meta={'league': response.meta['league'], 'country_list': country_list})

    def parse_details(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//table[@class="leaguetable sortable table detailed-table"]//tr[contains(@class,"team_rank")]')
        last_node = nodes[-1]
        country_list = response.meta['country_list']
        league_name = self.tous[response.meta['league']]
        self.boolean.append(league_name)
        for node in nodes:
            team_sk = extract_data(node, './/@data-team_id')
            team_name = extract_data(node, './/td[@class="text team large-link"]//a//text()')

            if type(team_name) == unicode:
                team_name = unicodedata.normalize('NFKD', team_name).encode('ascii','ignore')

            if node == last_node:
                crawl = True
            else:
                crawl = False
            link = self.domain_url + extract_data(node, './/a[contains(@href, "/teams/")]/@href')
            yield Request(link, self.parse_teams, meta={'team_sk': team_sk, \
                'team_name': team_name, 'crawl': crawl, 'league_name': league_name, 'country_list': country_list})

    def parse_teams(self, response):
        sel = Selector(response)
        league_name = response.meta['league_name']
        team_name = response.meta['team_name']
        country_list = response.meta['country_list']
        crawl = response.meta['crawl']
        season = extract_data(sel, '//select[@name="season_id"]//option[@selected]/text()').replace('/2015', '-15').strip().replace('2015/2016', '2015-16')
        if not season:
            season = extract_list_data(sel, '//select[@name="season_id"]//option[1]/text()')[0]
        print season
        pl_links = extract_list_data(sel, '//table[@class="table squad sortable"]//tr/td/a[contains(@href, "/players/")]')
        player_count = len(pl_links)
        team_sk = response.meta['team_sk']
        if team_sk == "1241":
            player_count = len(pl_links) - 1
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
        if len(self.team_dict) == country_list and crawl:
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
        con, cursor = create_cursor()
        cursor.execute(TEAM_ID % (source, team_sk))
        team_id = cursor.fetchone()
        db_count = 0
        if team_id:
            query = ROSTER_COUNT % (str(team_id[0])) + ' and player_role not like "%coach%"'
            cursor.execute(query)
            pt_ids = cursor.fetchall()
            db_count = [str(i[0]) for i in pt_ids]
            return db_count
        else:
            print team_sk
