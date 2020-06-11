from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import MySQLdb


def create_cursor():
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor

class Football(VTVSpider):
    name = "football_spi"
    allowed_domains = []
    start_urls = []
    data = {}
    def start_requests(self):
        top_url = 'http://www.cbssports.com/collegefootball/playersearch?last_name_begins=%s&print_rows=9999'
        next_order = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
        for next_ in next_order:
            top_urls = top_url % (next_)
            yield Request(top_urls, callback = self.parse, meta = {})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//tr[contains(@class, "row")]//td//a[contains(@href, "player")]')
        for node in nodes:
            player_url   = extract_data(node, './/@href')
            player_link = "http://www.cbssports.com/" + player_url

            yield Request(player_link, callback = self.parse_images, meta = {})
    def parse_images(self, response):
        hxs = HtmlXPathSelector(response)
        player_name = response.url.split('/')[-1].replace('-', ' ')
        player_image = extract_data(hxs, '//div[@class="photo"]/img/@src')
        con, cursor = create_cursor()
        if player_name:
            query = 'select id from sports_participants where game = "football" and participant_type = "player" and image_link = "" and title= "%s"' %(player_name)
            cursor.execute(query)

            p_id = cursor.fetchone()
            if not p_id :
                return
            update_query = 'update sports_participants set image_link = "%s" \
                                where id = "%s" and title= "%s" and game = "football"' %(player_image, p_id[0], player_name)
            cursor.execute(update_query)
            con.close()
