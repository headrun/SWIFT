from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem


class RoyalOneDayCupStandings(VTVSpider):
    name = 'royaloneday_standings'
    allowed_domains = []
    start_urls =  ["http://www.espncricinfo.com/royal-london-one-day-cup-2015/engine/series/803391.html?view=pointstable"]


    def parse(self, response):
        record  = SportsSetupItem()
        sel     = Selector(response)
        season = response.url.split('/')[3].split('-')[-1].strip()
        group_nodes = get_nodes(sel, '//table')

        for group_node in group_nodes:
            st_nodes = get_nodes(group_node, './/tr')
            count = 0

            for node in st_nodes[:10]:
                team    = extract_data(node, './/td[@class="tdh"]//text()')

                if not team:
                    continue
                count = count + 1
                group_name = extract_data(node, '../../preceding-sibling::h4//text()').strip()
                group_name = "Royal London One-Day Cup " + group_name
                matches = extract_data(node, './/td[2]//text()')
                wons    = extract_data(node, './/td[3]//text()')
                losts   = extract_data(node, './/td[4]//text()')
                tied    = extract_data(node, './/td[5]//text()')
                nrr    = extract_data(node, './/td[6]//text()')
                total_pts = extract_data(node, './/td[7]//text()')
                net_rr    =  extract_data(node, './/td[8]//text()')
                pts_for   =  extract_data(node, './/td[9]//text()')
                pts_again =  extract_data(node, './/td[10]//text()')

                record['source'] = 'espn_cricket'
                record['tournament'] = group_name
                record['participant_type']  =  'team'
                record['season'] = season
                record['affiliation'] = 'icc'
                record['game'] = 'cricket'
                record['result_type'] = 'group_standings'
                record['result'] = {team: {'matches': matches, 'won': wons, \
                                    'lost': losts, 'tied': tied, \
                                    'nr': nrr, \
                                    'net_rr': net_rr, 'total_pts': total_pts, \
                                    'pts_for': pts_for, 'pts_again': pts_again, \
                                    'rank' : count}}
                yield record

