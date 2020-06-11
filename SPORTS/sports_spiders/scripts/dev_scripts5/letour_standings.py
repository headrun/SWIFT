import re
from vtvspider_dev import VTVSpider
from scrapy.http import Request
#from sports_spiders.items import SportsSetupItem
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, \
                              get_nodes, get_utc_time

domain = 'http://www.letour.fr/us'

def get_sk(sk):
    sk = sk.split('/')[-1].split('.htm')[0].strip()
    sk = sk.strip()
    return sk

class LetourStandings(VTVSpider):
    name            = 'letour_standings'
    #allowed_domains = ['letour.fr']
    start_urls      = []

    def start_requests(self):
        top_url = 'http://www.letour.fr/us/'
        yield Request(top_url, callback=self.parse, meta= {})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        top_url = response.url
        link = extract_data(hxs, '//div[@class="header-wrapper"]\
                            //p[@class="logo-tour"]/a/@href')
        season = ''.join(re.findall('le-tour\/(\d+)\/.*\/', link))
        if not link:
            link = extract_data(hxs, '//div[contains(@class, "logo")]/a/@href')
            season = link.split('/')[-3].strip()
        tou_name = extract_data(hxs, '//div[@class="header-wrapper"]\
                               //p[@class="logo-tour"]/a/text()')
        url = domain.replace('/us', link)+'classifications.html'
        yield Request(url, callback=self.parse_details, meta= {'season': season, \
                                                        'tou_name': tou_name})

    def parse_details(self, response):
        hxs = HtmlXPathSelector(response)
        season = response.meta['season']
        tou_name = response.meta['tou_name']
        stage = extract_data(hxs, '//div[@class="classements"]/@stage')
        import pdb;pdb.set_trace()
        #stage = '1100'
        if stage:
            stand_link = 'http://www.letour.fr/le-tour/%s/us/%s/classement/bloc-classement-page/ITG.html' % (season, stage)
            yield Request(stand_link, callback=self.parse_standings, meta= {'season': season, \
                                                                    'tou_name': tou_name})

    def parse_standings(self, response):
        import pdb;pdb.set_trace()
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//table//tbody//tr')
        for node in nodes:
            rank    = extract_data(node, './/td[1]/text()').replace('.', '').strip()
            pl_link = extract_data(node, './/td//a[contains(@href, "riders")]/@href')
            pid     = get_sk(pl_link)
            time    = extract_data(node, './/td[5]/text()').strip().replace(' pts', '')
            gap     = extract_data(node, './/td[6]/text()').replace('+ ', '')
            record['tournament']    = response.meta['tou_name']
            record['source']        = 'tourdefrance_cycling'
            record['season']        = response.meta['season']
            record['participant_type'] = 'participant'
            record['affiliation'] = 'uci'
            record['result_type']   = 'tournament_standings'
            record['result']        = {pid : {'rank' : rank, 'time' : time, 'gap': gap}}
            print record
            yield record
