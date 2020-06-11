from vtvspider import VTVSpider, get_nodes, extract_data, log, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import MySQLdb
from lxml import etree
import urllib
import datetime

status_dict = {'green' : 'YES', "red" : "NO", "orange": "Extra Players"}

def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor

def get_html_table(title, headers, table_body):
    table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        table_data += '<th>%s</th>' % header
    table_data += '</tr>'

    for data in table_body:
        table_data += '<tr>'
        for index, row in enumerate(data):
            if index == 3:
                table_data += '<td style="color: %s">%s</td>' % (row, status_dict[row])
            else:
                table_data += '<td>%s</td>' % (str(row))

        table_data += '</tr>'
    table_data += '</table>'

    return table_data

def clear_text(data):
    if data:
        data = data.replace('\n', '')
        final_data = data.encode('ascii', errors = 'ignore')
        return final_data
    else:
        final_data = ''
        return final_data
TEAM_LIST = ('NE', 'BAL', 'CIN', 'CLE', 'CHI', 'DET',
            'GB', 'MIN', 'HOU', 'IND', 'JAC', 'TEN',
            'ATL', 'CAR', 'NO', 'TB', 'BUF', 'MIA',
            'DAL', 'NYG', 'PHI', 'NYJ', 'WAS', 'DEN',
            'KC', 'OAK', 'SD', 'ARI', 'SF', 'SEA', 'STL', 'PIT')
CALLSIGN_MAP = {"Boston Celtics": "BOS", "Chicago Bulls": "CHI", \
                "Atlanta Hawks" : "ATL", "New Jersey Nets" : "BKN", \
                "Cleveland Cavaliers" : "CLE", "Charlotte Bobcats" : "CHA", \
                "New York Knicks" : "NYK", "Detroit Pistons" : "DET", \
                "Miami Heat" : "MIA", "Philadelphia 76ers" : "PHI", \
                "Indiana Pacers": "IND", "Orlando Magic" : "ORL", \
                "Toronto Raptors" : "TOR", "Milwaukee Bucks" : "MIL", \
                "Washington Wizards" : "WAS", "Dallas Mavericks" : "DAL", \
                "Denver Nuggets" : "DEN", "Golden State Warriors" : "GSW", \
                "Houston Rockets" : "HOU", "Minnesota Timberwolves" : "MIN", \
                "Los Angeles Lakers": "LAL", "Memphis Grizzlies" : "MEM", \
                "Portland Trail Blazers" : "POR", \
                "Los Angeles Clippers" : "LAC", "New Orleans Hornets" : "NOH", \
                "Oklahoma City Thunder" : "OKC", "Phoenix Suns" : "PHX", \
                "San Antonio Spurs" : "SAS", "Utah Jazz" : "UTA", \
                "Sacramento Kings" : "SAC", "FBU": "FBU", "RMD": "RMD", \
                "MPS": "MPS", "EAM": "EAM", "FCB": "FCB", "MAC": "MAC", \
                "ALB": "ALB", "New Orleans Pelicans": "NOP", \
                "Eastern Conference" : "ECF", 'Western Conference' : "WCF", \
                "Brooklyn Nets": "BKN", "Charlotte Hornets": "CHA",
                "Phoenix Suns": "PHO"}

ROSTER_COUNT = 'select count(*) from sports_roster where status="active" and team_id=%s'

TEAM_ID = 'select entity_id from sports_source_keys where entity_type="participant" and source="%s" and source_key="%s"'

def get_mlb_teams(url):
    res = urllib.urlopen(url)
    data = res.read()
    html_data = etree.HTML(data)
    team_urls = html_data.xpath('//select[@id="ps_team"]/option/@value')
    return team_urls

class RosterCheck(VTVSpider):
    name = "roster_check"
    start_urls = []
    dates_dict = {}
    text = ''
    nba_mail = False
    nhl_mail = False

    def start_requests(self):
        top_urls = ['http://www.nfl.com/teams/roster?team=%s',
                    'http://mlb.mlb.com/mlb/players/index.jsp',
                    'http://www.nba.com/teams/',
                    'http://www.nhl.com/ice/teams.htm?navid=nav-tms-main']

        for url in top_urls:
            if "mlb" in url:
                team_urls = get_mlb_teams(url)
                for team_url in team_urls:
                    if team_url:
                        yield Request(team_url, self.parse)
            elif "nfl" in url:
                for team in TEAM_LIST:
                    t_url = url % (team)
                    yield Request(t_url, self.parse, meta = {'team_sk': team})
            else:
                yield Request(url, self.parse)

    def __init__(self):
        #self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.open_file = open('Major_Leagues_Roster_Check', 'w')

    def send_mail(self, text):
        subject    = 'Major Leagues Roster Check'
        server     = 'localhost'
        sender     = 'headrun@veveo.net'
        #receivers = ['raman.arunachalam@rovicorp.com', 'vineet.agarwal@rovicorp.com', 'sports@headrun.com']
        receivers = ['niranjansagar@headrun.com', 'bibeejan@headrun.com', 'jhansi@headrun.com']
        self.today = str(datetime.datetime.today().date())
        self.html_file = open('/home/veveo/reports/Major_Leagues_Rosters_Stats_%s.html' % self.today, 'w')
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

    def get_db_count(self, source, team_sk):
        conn, cursor = mysql_connection()
        cursor.execute(TEAM_ID % (source, team_sk))
        team_id = cursor.fetchone()
        db_count = 0
        if team_id:
            query = ROSTER_COUNT % (str(team_id[0])) + ' and player_role not like "%coach%"'
            cursor.execute(query)
            pt_ids = cursor.fetchall()
            db_count = [str(i[0]) for i in pt_ids]
            conn.close()
            return db_count

    def get_color(self, db_count, web_count):
        color = ''
        if int(db_count) == int(web_count):
            color = "green"
        elif int(db_count) > int(web_count):
            color = "orange"
        elif int(db_count) < int(web_count):
            color = "red"
        return color

    def parse(self, response):
        true = "true"
        false = "false"
        null = "null"
        hxs = Selector(response)
        mlb_header = "MLB Roster Count"
        nfl_header = "NFL Roster Count"
        self.text = ''
        source = ''
        if "mlb" in response.url:
            source = "MLB"
            header = "MLB Roster Count"
            team_name = extract_data(hxs, '//meta[@property="og:site_name"]/@content')
            if team_name == "Los Angeles Angels":
                team_sk = "laa"
            elif team_name == "Los Angeles Dodgers":
                team_sk = "LAD"
            elif team_name == "Washington Nationals":
                team_sk = "wsh"
            else:
                team_sk = response.url.split('=')[-1]
            nodes = get_nodes(hxs, '//table[@class="team_table_results"]/tbody/tr')
            count = 0
            for node in nodes:
                inactive = extract_data(node, './/td[2]/text()') or \
                            extract_data(node, './/td/span[@class="status_note"]/text()')
                if not inactive:
                    count += 1
            web_count = count
        elif "nfl" in response.url:
            source = "NFL"
            header = "NFL Roster Count"
            team_sk = response.meta['team_sk']
            team_name = extract_data(hxs, '//div[@class="article-decorator"]//a[contains(@href, "team")]/text()')
            nodes = get_nodes(hxs, '//div[@id="team-stats-wrapper"]//table//tbody//tr[td[contains(text(), "ACT")]]')
            web_count = len(nodes)
        if source:
            db_count = self.get_db_count(source, team_sk)
            color = self.get_color(db_count[0], web_count)
            if web_count and db_count:
                self.dates_dict.setdefault(header, []).append([team_name, web_count, db_count[0], color])
                self.open_file.write('%s\n' % [header, team_name, web_count, db_count[0], color])
        if self.dates_dict:
            for key, value in self.dates_dict.iteritems():
                count_ = len(value)
                headers = ('Team Name', 'Web Count', 'DB Count', 'Matched')
                if key == mlb_header and count_ == 30:
                    self.text += get_html_table(key, headers, value)
                elif key == nfl_header and count_ == len(TEAM_LIST):
                    self.text += get_html_table(key, headers, value)
        if "nba" in response.url:
            nodes = get_nodes(hxs, '//td[@class="nbateamname"]/a')
            for node in nodes:
                team_name = extract_data(node, './text()')
                url = extract_data(node, './@href')
                callsign = CALLSIGN_MAP.get(team_name, '')
                if callsign == "DAL":
                    url = "http://www.mavs.com/team/player-roster/"
                else:
                    url = 'http://www.nba.com'+url + 'roster/'
                yield Request(url, callback =self.parse_nba,
                        meta = {'team_name': team_name, 'callsign': callsign})
        elif "nhl" in response.url:
            nodes = get_nodes(hxs, '//div[@id="realignmentMap"]//ul[@class="teamData"]')
            for node in nodes:
                team_name = ' '.join(extract_list_data(node, './/preceding-sibling::div/span/text()'))
                if "Canadiens" in team_name:
                    team_name = "Montreal Canadiens"
                callsign  = extract_data(node, './/a/@rel')
                callsign = callsign.upper()
                team_url = extract_data(node, './/a[contains(@href, "roster")]/@href').strip()
                if "index" in team_url:
                    continue
                yield Request(team_url, callback = self.parse_nba,
                            meta = {'callsign': callsign, 'team_name': team_name})

    def parse_nba(self, response):
        hxs = Selector(response)
        nba_header = "NBA Roster Count"
        nhl_header = "NHL Roster Count"
        team_name = response.meta['team_name']
        if "nhl" in response.url:
            header = "NHL Roster Count"
            nodes = get_nodes(hxs, '//div[@class="tieUpWrap"]//table[@class="data"]//tr[not(contains(@class, "hdr")) and td[not(contains(text(), "*"))]]')
            source = "NHL"
        else:
            header = "NBA Roster Count"
            nodes  = get_nodes(hxs, "//div[contains(@class, 'roster__player')] \
                        /header[@class='roster__player__header']")
            if not nodes:
                nodes = get_nodes(hxs, '//div[@class="entry-content"]//table//tbody//tr')
            source = "NBA"
        web_count = len(nodes)
        db_count = self.get_db_count(source, response.meta['callsign'])
        color = self.get_color(db_count[0], web_count)
        if web_count and db_count:
            self.dates_dict.setdefault(header, []).append([team_name, web_count, db_count[0], color])
            self.open_file.write('%s\n' % [header, team_name, web_count, db_count[0], color])
        if self.dates_dict:
            for key, value in self.dates_dict.iteritems():
                count = len(value)
                headers = ('Team Name', 'Web Count', 'DB Count', 'Matched')
                if key == nba_header and count == 30:
                    if self.nba_mail == False:
                        self.text += get_html_table(key, headers, value)
                    self.nba_mail = True
                if key == nhl_header and count == 30:
                    if self.nhl_mail == False:
                        self.text += get_html_table(key, headers, value)
                    self.nhl_mail = True
            if self.nba_mail and self.nhl_mail:
                self.html_file.write('%s' % self.text)
                #self.send_mail(self.text)

