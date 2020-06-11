from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes

class RugbyProDStandings(VTVSpider):
    name = "rugbyprod_standings"
    start_urls = ['http://www.lnr.fr/classement-pro-d2.html']


    def parse(self, response):
        hxs = Selector(response)
        season = extract_data(hxs, '//div[@id="ctarticle-header"]//h1/text()')
        season = season.split(' ')[-1].replace('-20', '-').strip()
        st_nodes = get_nodes(hxs, '//table[@id="classement-general"]//tr')
        for node in st_nodes:
            position  = extract_data(node, './/td[@class="top"]//text()')
            team      = extract_data(node, './/td[@class="club"]//span[@class="sort"]//text()'). \
                encode('utf-8').replace('\xc3\xa9', '')
            if not team:
                continue
            points = extract_data(node, './/td[@class="pts"]//span[@class="sort"]//text()')
            bonus  = extract_data(node, './/td[@class="reste bonus"]//span[@class="sort"]//text()')
            jouer  = extract_data(node, './/td[@class="reste jouer"]//span[@class="sort"]//text()')
            gagner = extract_data(node, './/td[@class="reste gagner"]//span[@class="sort"]//text()')
            null   = extract_data(node, './/td[@class="reste nul"]//span[@class="sort"]//text()')
            perdu  = extract_data(node, './/td[@class="reste perdu"]//span[@class="sort"]//text()')
            pts_mark = extract_data(node, './/td[@class="reste pts-mark"]//span[@class="sort"]//text()')
            pts_enc  = extract_data(node, './/td[@class="reste pts-enc"]//span[@class="sort"]//text()')
            pts_diff = extract_data(node, './/td[@class="reste pts-diff"]//span[@class="sort"]//text()')

            record = SportsSetupItem()
            record['result_type'] = "tournament_standings"
            record['season'] = season
            record['tournament'] = "Rugby Pro D2"
            record['participant_type'] = "team"
            record['source'] = 'lnr_rugby'
            record['result'] = {team: {'rank': position,  'points': points, \
                   'bonus': bonus, 'jouer': jouer, 'gagner': gagner, \
                   'null': null, 'perdu': perdu, \
                   'pts_mark': pts_mark, 'pts_enc': pts_enc, \
                   'pts_diff': pts_diff}}
            import pdb;pdb.set_trace()
            yield record

