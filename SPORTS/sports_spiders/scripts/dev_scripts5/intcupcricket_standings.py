from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


class ICCIntCupStandings(VTVSpider):
    name = "iccintcup_standings"
    allowed_domains = []
    start_urls = ['http://www.espncricinfo.com/icc-intercontinental-cup-2015-17/engine/current/series/870857.html?view=pointstable']

    def parse(self, response):
        sel = Selector(response)
        st_nodes = get_nodes(sel, '//div[@role="main"]//table//tr')
        count   = 0
        record  = SportsSetupItem()
        for node in st_nodes[:9]:
            team      = extract_data(node, './/td[@class="tdh"]/text()')
            if not team or team == "Team":
                continue
            count    += 1
            matches   = extract_data(node, './/td[2]/text()')
            matches   = matches.replace('*', '')
            won       = extract_data(node, './/td[3]/text()')
            lost      = extract_data(node, './/td[4]/text()')
            tied      = extract_data(node, './/td[5]/text()')
            draw      = extract_data(node, './/td[6]/text()')
            aban      = extract_data(node, './/td[7]/text()')
            points    = extract_data(node, './/td[8]/text()')
            quotient  = extract_data(node, './/td[9]/text()')
            pts_for   = extract_data(node, './/td[10]/text()')
            pts_again = extract_data(node, './/td[11]/text()')

            record['source'] = 'espn_cricket'
            record['tournament'] = "ICC Intercontinental Cup"
            record['participant_type']  =  'team'
            record['season'] = "2015-17"
            record['affiliation'] = 'icc'
            record['game'] = 'cricket'
            record['result_type'] = 'tournament_standings'
            record['result'] = {team: {'matches': matches, 'won': won, \
                                        'lost': lost, 'tied': tied,\
                                        'draw': draw, 'aban': aban, 'pts': points, \
                                        'for': pts_for, 'against': pts_again, \
                                        'rank' : count, 'quotient': quotient}}
            yield record

