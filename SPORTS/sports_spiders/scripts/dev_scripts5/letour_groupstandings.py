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
    sk_ = sk.split('/')[-1].split('.htm')[0].strip()
    sk_ = sk_.strip()
    if sk_ == "teams":
        sk_ = sk.split('/')[-1].split('#')[-1].strip()
    return sk_

class LetourGroupStandingss(VTVSpider):
    name            = 'letour_greenstandingss'
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
        tou_name = extract_data(hxs, '//div[@class="header-wrapper"] \
        //p[@class="logo-tour"]/a/text()')
        if not link:
            link = extract_data(hxs, '//div[contains(@class, "logo")]/a/@href')
            season = link.split('/')[-3].strip()
            tou_name = extract_data(hxs, '//div[@class="description"]/h3/text()').replace(season, '').strip()
        url = domain.replace('/us', link)+'classifications.html'
        yield Request(url, callback=self.parse_details, meta= {'season': season, \
                                                        'tou_name': tou_name})

    def parse_details(self, response):
        hxs = HtmlXPathSelector(response)
        season = response.meta['season']
        tou_name = response.meta['tou_name']
        stage = extract_data(hxs, '//div[@class="classements"]/@stage')
        if not stage:
            stage = extract_data(hxs, '//div[@class="stage-nav"]/@stage')
        #stage = '1100'
        tou_lst = ['IJG']
        for tou_list in tou_lst:
            tou_nm = tou_list
            if stage:
                stand_link = 'http://www.letour.fr/le-tour/%s/us/%s/classement/bloc-classement-page/%s.html' % (season, stage, tou_nm)
                yield Request(stand_link, callback = self.parse_standings, \
                meta= {'season': season, 'tou_name': tou_name})

    def parse_standings(self, response):
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//table//tbody//tr')
        tou_name = extract_data(hxs, '//div[@class="banner"]//h3//text()').strip()

        if "young rider" in tou_name:
            tou_name = "Tour De France Youth jersey"
        elif "team" in tou_name:
            tou_name = "Tour De France Team Standings"
        elif "climber" in tou_name:
            tou_name = "Tour De France Mountain Jersey"
        elif "individual time" in tou_name:
            tou_name = "Tour De France Yellow Jersey"
        else:
            tou_name = "Tour De France Green Jersey"

        for node in nodes:
            rank    = extract_data(node, './/td[1]/text()').replace('.', '').strip()
            pl_link = extract_data(node, './/td//a[contains(@href, "riders")]/@href')

            if not pl_link:
                pl_link =   extract_data(node, './/td//a[contains(@href, "teams")]/@href')
            pid     = get_sk(pl_link)

            if "Youth" in tou_name:
                gap     = extract_data(node, './/td[6]/text()').replace('+ ', '')
                pl_num  = extract_data(node, './/td[3]/text()')
                time    = extract_data(node, './/td[5]/text()').strip()
                results = {pid : {'rank' : rank, 'time': time , \
                'player_number': pl_num, 'time_gape': gap}}

            elif "Team" in tou_name:
                gap     = extract_data(node, './/td[4]//text()').replace('+ ', '')
                time    = extract_data(node, './/td[3]//text()').strip()
                results = {pid : {'rank' : rank, 'time': time ,'time_gape': gap}}

            elif "Mountain" in tou_name:
                pl_num  = extract_data(node, './/td[3]/text()')
                points  = extract_data(node, './/td[5]/text()').strip().replace(' pts', '')
                results = {pid : {'rank' : rank, 'player_number': pl_num, 'points' : points}}

            elif "Yellow" in tou_name:
                pl_num  = extract_data(node, './/td[3]/text()')
                gap     = extract_data(node, './/td[6]/text()').replace('+ ', '')
                time    = extract_data(node, './/td[5]/text()').strip()
                results = {pid : {'rank' : rank, 'time': time , 'gap': gap, 'player_number': pl_num}}

            else:
                points  = extract_data(node, './/td[5]/text()').strip().replace(' pts', '')
                pl_num  = extract_data(node, './/td[3]/text()')
                results = {pid : {'rank' : rank, 'points' : points, 'player_number': pl_num}}

            record['tournament']    = tou_name
            record['source']        = 'tourdefrance_cycling'
            record['season']        = response.meta['season']
            record['participant_type'] = 'participant'
            record['affiliation'] = 'uci'
            record['result_type']   = 'group_standings'
            record['result']        = results
            import pdb;pdb.set_trace()
            yield record
