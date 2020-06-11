import MySQLdb
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
#from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import unicodedata
import datetime
import re

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="basketball_realgm" and source_key= "%s"'

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

class BasketballRealgmCheck(VTVSpider):
    start_urls = ['http://basketball.realgm.com/international/leagues']
    name = 'basketball_realgm_check'
    league_list = ['Danish-Basketligaen' , 'Norwegian-BLNO' , 'Swedish-Basketligan' , 'Icelandic-Dominos-League']
    domain_url = 'http://basketball.realgm.com'
    text = ''

    def __init__(self):
        self.boolean      = []
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()
        self.today = str(datetime.datetime.today().date())
        self.team_dict = {}
        #self.html_file = open('/home/veveo/reports/Scandinavia_Basketball_Leagues_Rosters_Stats_%s.html' % self.today, 'w')

    def send_mail(self, text):
        subject    = 'Scandinavia Basketball League Rosters'
        server     = '10.4.1.112'
        server     = 'localhost'
        sender     = 'headrun@veveo.net'

        #receivers = ['raman.arunachalam@rovicorp.com', 'vineet.agarwal@rovicorp.com', 'sports@headrun.com']
        receivers = ['jhansi@headrun.com', 'niranjansagar@headrun.com', 'bibeejan@headrun.com']
        #receivers = ['bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text.encode('utf-8'), '')


    def parse(self, response):
        hxs = Selector(response)
        league_links = extract_list_data(hxs,'//div[@class="portal widget fullpage"]/div[@class="content linklist"]/a/@href')
        for league in self.league_list:
            for league_link in league_links:
                if league in league_link:
                    league_name = league
                    req_league = self.domain_url + league_link
                    league_id  = league_link.split('/')[-2]
                    yield Request(req_league, callback = self.parse_team, \
                    meta = {'league_name': league_name, 'league_id' : league_id })

    def parse_team(self , response):
        hxs = Selector(response)
        league_name = response.meta['league_name']
        league_id = response.meta['league_id']
        team_links = extract_data(hxs,'//a[span[contains(text(),"Teams")]]/@href')
        team_link = self.domain_url + team_links
        yield Request(team_link, callback = self.parse_teams, \
        meta = {'league_name': league_name, 'league_id' : league_id })

    def parse_teams(self , response):
        hxs = Selector(response)
        league_name = response.meta['league_name']
        league_id = response.meta['league_id']
        ssn = extract_data(hxs,'//h2[@class="page_title"]/text()')
        years = re.findall('(\d+)',ssn)
        seasons = years[0]+'-'+years[1]
        season = seasons.replace('2015-2016','2015-16')
        roster_links = get_nodes(hxs, '//td[@data-th="Rosters"]//a')
        last_node = roster_links[-1]
        self.boolean.append(league_name)
        for roster_link in roster_links:
            team_link = extract_data(roster_link, './/@href')
            team_name = extract_data(roster_link, './/text()')
            team_id = team_link.split('/')[-3]
            if len(self.boolean) == len(self.league_list) and roster_link == last_node:
                crawl = True
            else:
                crawl = False


            rosters_link = self.domain_url + team_link

            yield Request(rosters_link, callback = self.parse_players, \
            meta = {'team_id' : team_id , \
            'league_name': league_name, 'league_id' : league_id, \
            'season' : season, \
            'crawl': crawl, 'team_name': team_name})

    def parse_players(self,response):
        hxs = Selector(response)
        season = response.meta['season']
        league_name = response.meta['league_name']
        league_id = response.meta['league_id']
        team_id = response.meta['team_id']
        team_name = extract_list_data(hxs, '//div[@class="main-container"]//h2//text()')[2]. \
        replace('2015-2016 ', '').replace(' Roster', '').replace('Depth Chart', '').strip()
        crawl = response.meta['crawl']
        player_links = extract_list_data(hxs, '//table//tr//td[@class="nowrap"]//a[contains(@href,"/player/")]/@href')
        player_count = len(player_links)
        db_count = self.get_db_count('basketball_realgm', team_id)
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
        if len(self.team_dict) == len(self.league_list) and crawl:
            for key, value in self.team_dict.iteritems():
                if value[0]:
                    self.text += get_html_table(key, headers, value)

            self.send_mail(self.text)
            #self.html_file.write('%s' % self.text)

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

    def get_db_count(self, source, team_id):
        self.cursor.execute(TEAM_ID % (source, team_id))
        team_id = self.cursor.fetchone()
        db_count = 0
        if team_id:
            query = ROSTER_COUNT % (str(team_id[0]))
            self.cursor.execute(query)
            pt_ids = self.cursor.fetchall()
            db_count = [str(i[0]) for i in pt_ids]
            return db_count
        else:
            print team_id


