from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import MySQLdb

def create_cursor():
    con = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor
def X(data):
   try:
       return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
   except:
       return data.encode('utf8')

PARTICIPANT_DESC = 'insert ignore into sports_description(entity_id, entity_type, language, description) values(%s, "participant", "", "%s")'

class NCBasketball(VTVSpider):
    name = "ncb_description"
    start_urls = ['http://espn.go.com/mens-college-basketball/teams']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul/li')

        for node in nodes:
            team_link = extract_data(node, './/a[contains(@href, "college-basketball")]/@href')
            if team_link:
                team_link = team_link.replace('_', 'roster/_')
                team_link =  "http://espn.go.com/mens-college-basketball/team/roster/_/id/2029/arkansas-pine-bluff-golden-lions"
                yield Request(team_link, callback=self.parse_teamdetails)

    def parse_players(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        participants = {}
        team_sk = response.url.split('/')[-2]
        team_name = extract_data(hxs, '//div[@id="sub-branding"]//h2/a/b/text()').strip()
        team_name = team_name + " men's basketball."
        pl_nodes = get_nodes(hxs, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]')
        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './td/a/@href')
            pl_name = extract_data(pl_node, './td/a/text()')
            pl_name = X(pl_name)
            con, cursor = create_cursor()
            if pl_name:
                query = 'select id, title, reference_url from sports_participants where game = "basketball" and participant_type = "player" and title= "%s"' %(pl_name)
                cursor.execute(query)
                data = cursor.fetchall()
                if not data:
                    continue
                id_ = data[0][0]
                title = data[0][1]
                ref_url = data[0][2]
                desc = title + " is a basketball player playing for College Basketball team "+  team_name + " " + ref_url
                values = (str(id_), desc)
                cursor.execute(PARTICIPANT_DESC %(values))
                con.close()
