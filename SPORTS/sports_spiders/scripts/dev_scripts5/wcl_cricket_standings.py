from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
get_nodes
from scrapy_spiders_dev.items import SportsSetupItem


class WCLCricketStandings(VTVSpider):
    name = "wcl_standings"
    allowed_domains = []
    start_urls = ['http://www.espncricinfo.com/wcl-championship-2015-17/engine/current/series/870869.html?view=pointstable']

    def parse(self, response):
        sel = Selector(response)
        st_nodes = get_nodes(sel, '//div[@role="main"]//table//tbody//tr')
        count   = 0
        record  = SportsSetupItem()
        for node in st_nodes[:9]:
            team      = extract_data(node, './/td[@class="tdh"]/text()')

            if not team :
                continue

            count    += 1
            matches   = extract_data(node, './/td[2]/text()') \
                            .replace('*', '')
            won       = extract_data(node, './/td[3]/text()')
            lost      = extract_data(node, './/td[4]/text()')
            tied      = extract_data(node, './/td[5]/text()')
            nr_pts    = extract_data(node, './/td[6]/text()')
            points    = extract_data(node, './/td[7]/text()')
            net_rr    = extract_data(node, './/td[8]/text()')
            pts_for   = extract_data(node, './/td[9]/text()')
            pts_again = extract_data(node, './/td[10]/text()')

            record['source'] = 'espn_cricket'
            record['tournament'] = "ICC World Cricket League Championship"
            record['participant_type']  =  'team'
            record['season'] = "2015-17"
            record['affiliation'] = 'icc'
            record['game'] = 'cricket'
            record['result_type'] = 'tournament_standings'
            record['result'] = {team: {'matches': matches, 'won': won, \
                                        'lost': lost, 'tied': tied,\
                                        'nr_pts': nr_pts, 'pts': points, \
                                        'for': pts_for, 'against': pts_again, \
                                        'rank' : count, 'net_rr': net_rr}}
            import pdb;pdb.set_trace()
            yield record

