import MySQLdb
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata
import datetime

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="euro_basketball" and source_key= "%s"'

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

DOMAIN_LINK = 'http://www.scoresway.com/'
class LegaBasketballRosterCheck(VTVSpider):
    name = "legabasketball_check"
    allowed_domains = []
    start_urls = [  'http://www.scoresway.com/?sport=basketball&page=competition&id=2', \
                    'http://www.scoresway.com/?sport=basketball&page=competition&id=1', \
                    'http://www.scoresway.com/?sport=basketball&page=competition&id=8', \
                    'http://www.scoresway.com/?sport=basketball&page=competition&id=24']
    participants = {}
    text = ''
    team_dict = {}
    country_list = ['BUNDESLIGA','LEGA BASKET SERIE A', 'LNB PRO A', 'KORISLIIGA']


    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()
        self.today = str(datetime.datetime.today().date())
        self.html_file = open('/home/veveo/reports/LegaBasketball_Rosters_Stats_%s.html' % self.today, 'w')

    def send_mail(self, text):
        subject    = 'Lega Basketball Rosters'
        server     = '10.4.1.112'
        server     = 'localhost'
        sender     = 'headrun@veveo.net'

        receivers = ['bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text.encode('utf-8'), '')


    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//table[@class="leaguetable sortable table "]//tr//td')
        last_node = tm_nodes[-1]
        league_name = extract_data(sel, '//div[@id="subheading"]//h1//text()').strip()
        self.boolean.append(league_name)
        print self.boolean
        for node in tm_nodes:
            team_link = extract_data(node, './/a//@href')
            team_name = extract_data(node, './/a//text()')
            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link
            if type(team_name) == unicode:
                team_name = unicodedata.normalize('NFKD', team_name).encode('ascii','ignore')


            if  len(self.boolean) == len(self.country_list) and node == last_node:
                crawl = True
            else:
                crawl = False


            yield Request(team_link, callback = self.parse_next, meta = {'crawl': crawl, 'team_name': team_name, 'league_name': league_name})


    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = extract_list_data(sel, '//div[@class="content"]//div[@class="squad-player"]//span[contains(@class, "name")]')
        team_sk = response.url.split('=')[-1].strip()
        team_name = response.meta['team_name']
        league_name = response.meta['league_name']
        crawl  = response.meta['crawl']
        season = extract_list_data(sel, '//select[@name="season_id"]//optgroup//option//text()')
        if season:
            season = season[0].replace('/20', '-').strip()
            print season
        player_count = len(pl_nodes)
        db_count = self.get_db_count('euro_basketball', team_sk)
        if not db_count:
            db_count = '0'
            color = 'red'
        else:
            color = self.get_color(db_count, player_count)
        if season:
            self.team_dict.setdefault(league_name, []).append([team_name, season, player_count, db_count[0], color])
        else:
            self.team_dict.setdefault(league_name, []).append([])
        headers = ('Team Name', 'Season', 'Web Count', 'DB Count', 'Matched')
        print crawl
        if len(self.team_dict) == len(self.country_list) and crawl:
            for key, value in self.team_dict.iteritems():
                print "ok"
                self.text += get_html_table(key, headers, value)

            #self.send_mail(self.text)
            self.html_file.write('%s' % self.text)

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
            query = ROSTER_COUNT % (str(team_id[0]))
            self.cursor.execute(query)
            pt_ids = self.cursor.fetchall()
            db_count = [str(i[0]) for i in pt_ids]
            return db_count
        else:
            print team_sk
 
