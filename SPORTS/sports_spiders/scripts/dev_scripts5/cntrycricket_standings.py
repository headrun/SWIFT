from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem


class CountyCricketStandings(VTVSpider):
    name = 'coucricket_standings'
    allowed_domains = ["www.espncricinfo.com"]
    start_urls =  ["http://www.espncricinfo.com/county-championship-div1-2015/engine/series/803387.html?view=pointstable", "http://www.espncricinfo.com/county-championship-div2-2015/engine/series/803389.html?view=pointstable"]


    def parse(self, response):
        record  = SportsSetupItem()
        sel     = Selector(response)

        if "county-championship-div1" in response.url:
            divison_name = "County Championship Division One"
        elif "county-championship-div2" in response.url:
            divison_name = "County Championship Division Two"

        #season = extract_list_data(sel, '//h1[@class="SubnavSitesection"]/text()')[0]
        #season = season.split(',')[-1].strip()
        season = response.url.split('/')[3].split('-')[-1].strip()
        print season
        st_nodes = get_nodes(sel, '//div[@role="main"]//table//tr')
        count = 0

        for node in st_nodes:
            team    = extract_data(node, './/td[@class="tdh"]//text()')

            if not team:
                continue

            count  += 1
            matches = extract_data(node, './/td[2]//text()').replace('*', '').strip()
            wons    = extract_data(node, './/td[3]//text()')
            losts   = extract_data(node, './/td[4]//text()')
            tied    = extract_data(node, './/td[5]//text()')
            draw    = extract_data(node, './/td[6]//text()')
            aban    = extract_data(node, './/td[7]//text()')
            total_pts = extract_data(node, './/td[8]//text()')

            record['source'] = 'espn_cricket'
            record['tournament'] = divison_name
            record['participant_type']  =  'team'
            record['season'] = season
            record['affiliation'] = 'icc'
            record['game'] = 'cricket'
            record['result_type'] = 'group_standings'
            record['result'] = {team: {'matches': matches, 'won': wons, \
                                'lost': losts, 'tied': tied, \
                                'draw': draw, \
                                'aban': aban, 'total_pts': total_pts, \
                                'rank' : count}}
            yield record

