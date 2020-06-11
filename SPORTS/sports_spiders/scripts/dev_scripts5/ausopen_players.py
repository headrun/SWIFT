from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, \
extract_list_data, get_nodes
from scrapy.http import Request
import MySQLdb

true = True
false = False
null = ''

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = con.cursor()
    return con, cursor


def check_player(player_id):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % player_id)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    con.close()
    return pl_exists, pl_id

def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'ausopen_tennis', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, 'tennis'))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id

PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="ausopen_tennis" and source_key= "%s"'


class AusopenPlayers(VTVSpider):
    name = "ausopen_players"
    start_urls = []

    def start_requests(self):
        req = []
        top_url = 'http://www.ausopen.com/en_AU/players/profiles.html'
        yield Request(top_url, callback=self.parse, meta = {})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        domain  = "http://www.ausopen.com/en_AU/players/"
        if self.spider_type == "men" :
            nodes =  get_nodes(hxs, '//div[@class="section"]/div[@class="men"]/a')
            for node in nodes:
                link = domain+extract_data(node, './@href')
                yield Request(link, callback=self.parse_team_men, meta = {})
        else:
            nodes =  get_nodes(hxs, '//div[@class="section"]/div[@class="women"]/a')
            for node in nodes:
                link = domain+extract_data(node, './@href')
                yield Request(link, callback=self.parse_team_women, meta = {})

    def parse_team_men(self, response):
        hxs = HtmlXPathSelector(response)
        players_men = {}
        pl_link  = response.url
        player_name  = extract_data(hxs, '//h1[@class="profile"]/text()').strip()
        player_id = response.url.split('/')[-1].replace('.html', '').replace('atp', '')
        pl_exists, pl_id = check_player(player_id)
        if pl_exists == False:
            pts_id = check_title(player_name)
            if pts_id:
                add_source_key(str(pts_id[0]), player_id)
                print "add source key", player_name


    def parse_team_women(self, response):
        hxs = HtmlXPathSelector(response)

        players_women = {}
        pl_link  = response.url
        player_name = extract_data(hxs, '//h1[@class="profile"]/text()').strip()
        player_id = response.url.split('/')[-1].replace('.html', '').replace('wta', '')
        pl_exists, pl_id = check_player(player_id)
        if pl_exists == False:
            pts_id = check_title(player_name)
            if pts_id:
                add_source_key(str(pts_id[0]), player_id)
                print "add source key", player_name
 
