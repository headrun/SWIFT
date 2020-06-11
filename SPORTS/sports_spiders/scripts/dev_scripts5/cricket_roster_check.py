from vtvspider import VTVSpider, get_nodes, extract_data, log, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import MySQLdb
from lxml import etree
import urllib

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

class CricketRosterCheck(VTVSpider):
    name = "roster_check_cricket"
    #start_urls = ['http://www.espncricinfo.com/icc-cricket-world-cup-2015/content/squad?object=509587']
    start_urls = ['http://www.espncricinfo.com/indian-premier-league-2015/content/squad/index.html?object=791129']
    roster_dict = {}
    text = ''

    def __init__(self):
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')

    def send_mail(self, text):
        #subject    = 'ICC Cricket World Cup Roster Check'
        subject    = "Indian Premier League Roster Check"
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'

        #receivers = ['sports@headrun.com']
        receivers = ['bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')

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
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//ul[@class="squads_list"]/li')

        for node in nodes:
            country = extract_data(node, './/a/text()')
            link = extract_data(node, './/a/@href')
            link = "http://www.espncricinfo.com" + link
            yield Request(link, self.parse_next, meta={'country': country})

    def parse_next(self, response):
        hxs = Selector(response)
        country = response.meta['country']
        nodes = get_nodes(hxs, '//div[@class="large-20 medium-20 small-20 columns"]/ul/li')

        for node in nodes:
            name = extract_data(node, './/h3/a/text()')
            pl_link = extract_data(node, './/h3/a/@href')
            if not pl_link:
                continue
            pl_sk = pl_link.split('/')[-1].split('.')[0]
            status = self.get_player_status(pl_sk)
            if status == "active":
                color = "green"
            else:
                color = "red"
            self.roster_dict.setdefault(country, []).append([name, 'active', status, color])
        if len(self.roster_dict) == 14:
            for key, value in self.roster_dict.iteritems():
                count = len(value)
                headers = ('Player Name', 'Web Status', 'DB Status', 'Matched')
                self.text += get_html_table(key, headers, value)
            self.send_mail(self.text)

    def get_player_status(self, pl_sk):
        conn, cursor = mysql_connection()
        pl_id = self.get_player_id(pl_sk)
        query = 'select status from sports_roster where status="active" and player_id=%s'
        cursor.execute(query % (pl_id))
        status = cursor.fetchone()
        if status:
            status = status[0]
        conn.close()
        return status

    def get_player_id(self, pl_sk):
        query = 'select entity_id from sports_source_keys where source="espn_cricket" and entity_type="participant" and source_key="%s"'
        conn, cursor = mysql_connection()
        cursor.execute(query % (pl_sk))
        pl_id = cursor.fetchone()
        conn.close()
        pl_id = pl_id[0]
        return pl_id
