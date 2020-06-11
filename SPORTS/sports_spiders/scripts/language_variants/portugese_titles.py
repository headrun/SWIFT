from sports_spiders.items import SportsSetupItem
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
import MySQLdb


AUTO_TOU_QUERY = 'select id, type from sports_tournaments where title like %s and game="auto racing" and affiliation =%s and type = "tournament"'

MOTO_TOU_QUERY = 'select id, type from sports_tournaments where title like %s and game="motorcycle racing" and affiliation =%s and type = "tournament"'

class Portugesetitles(VTVSpider):
    name = 'portugese_titles'
    start_urls = ['http://grandepremio.uol.com.br/f1/calendarios/calendario-2016', 'http://grandepremio.uol.com.br/indy/calendarios/calendario-2016', 'http://grandepremio.uol.com.br/motogp/calendarios/calendario-2016']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        sel = Selector(response)
        nodes = sel.xpath('//div[@class="mhv-calendario-gp mhv-float-left"]')
        count = 0
        for node in nodes:
            title = extract_data(node,'./text()').split('.')[-1].split('|')[0].strip()
            short_titles = short_title = title.split(' da ')[-1].split(' de ')[-1].split(' do ')[-1].split(' dos ')[-1]
            short_title = '%' + short_titles[0:3] + '%'

            if response.url == 'http://grandepremio.uol.com.br/f1/calendarios/calendario-2016':
                affiliation = "f1"
                game = 'auto racing'

            elif response.url == 'http://grandepremio.uol.com.br/indy/calendarios/calendario-2016':
                affiliation = "indycar"
                game = 'auto racing'

            elif response.url == 'http://grandepremio.uol.com.br/motogp/calendarios/calendario-2016':
                affiliation = "motogp"
                game = 'motorcycle racing'

            if u'Austr\xe1lia' in title:
                short_title = 'Austral%'
            elif u'\xc1ustria' in title:
                short_title = 'Austri%'
            elif 'Petersburgo' in title:
                short_title = '%Pete%'
            elif u'GP de Indian\xe1polis' in title:
                short_title = 'Grand Prix of Indianapo%'
            elif '500 Milhas de Indian' in title:
                short_title = 'Indianapolis 50%'
            elif 'GP de Detroit' in title:
                short_title = 'Detroit Belle Isle Grand%'
            elif 'Cingapura' in title:
                short_title = 'Singapore%'
            elif 'EUA' in title:
                short_title = 'United States Grand%'
            elif 'Catar' in title:
                short_title = 'Qatar%'
            elif 'Espanha' in title:
                short_title = 'Spanish%'
            elif 'Inglaterra' in title:
                short_title = 'Britis%'
            elif 'Alemanha' in title:
                short_title = 'German%'
            elif 'Fran' in title:
                short_title = 'Frenc%'
            elif 'Holanda' in title:
                short_title = 'Dutch%'
            elif u'Am\xe9ricas'in title:
                short_title = '%Ameri%'
            count = count+1
            values =(short_title, affiliation)
            if game == "auto racing":
                self.cursor.execute(AUTO_TOU_QUERY, values)
                new_values = self.cursor.fetchone()

            elif game == "motorcycle racing":
                self.cursor.execute(MOTO_TOU_QUERY, values)
                new_values = self.cursor.fetchone()

            if new_values:
                tou_id, tou_type  = new_values
                iso = 'PRT'
                titles_query = "insert into sports_titles_regions (entity_id,entity_type,title,aka,short_title,iso,description,modified_at,created_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                values = (str(tou_id), tou_type, title, '', '', iso, '')
                self.cursor.execute(titles_query, values)

            else:
                file("failed_cases", "ab+").write("%s %s %s \n"%(title.encode('utf-8'), game, affiliation))
