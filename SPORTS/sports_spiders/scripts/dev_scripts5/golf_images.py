from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import MySQLdb

#CURSOR = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB").cursor()
CURSOR = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP").cursor()
class GolfSpider(VTVSpider):
    name = "golf_spider"
    allowed_domains = []
    start_urls = ['http://www.pgatour.com/players.html', \
                  'http://www.pgatour.com/champions/players.html', \
                  'http://www.pgatour.com/webcom/players.html']
    file_name = open('images.txt', "w+")
    data = {}
    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//div[@class="item-data"]//ul[@class="ul-inline items"]//li//a')
        for node in nodes:
            player_url   = extract_data(node, './@href')
            player_id    = "". join (re.findall(r'\d+' , player_url))
            player_link = "http://www.pgatour.com" + player_url

            yield Request(player_link, callback = self.parse_images, meta = {'player_id': player_id})

    def parse_images(self, response):
        hxs = HtmlXPathSelector(response)
        player_id = response.meta['player_id']
        nodes = get_nodes(hxs, '//div[@class="player-bio-frame1 hidden-small"]//ul[@class="player-bio-cols"]')
        for node in nodes:
            player_name = extract_data(node, './/li[@class="col2"]//h3/text()')
            print player_name
            player_image = extract_data(node, './/li[@class="col1"]/img//@src')

            data = {'image_url': player_image, 'title': player_name, 'player_id': player_id}
            self.file_name.write('%s\n'%repr(data))
            if player_name:
                #query = 'select entity_id from sports_source_keys where entity_type = "participant" and source = "pga_golf" and source_key ="%s"' %(player_id)
                query = 'select id from sports_participants where game = "golf" and participant_type = "player" and title= "%s"' %(player_name)
                CURSOR.execute(query)

                p_id = CURSOR.fetchone()
                if not p_id :
                    continue
                update_query = 'update sports_participants set image_link = "%s" \
                                where id = "%s" and title= "%s" and game = "golf"' %(player_image, p_id[0], player_name)
                CURSOR.execute(update_query)
