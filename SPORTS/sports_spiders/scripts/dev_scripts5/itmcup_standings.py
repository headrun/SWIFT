from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes, extract_list_data
import re

class ITMCupRugbyStandings(VTVSpider):
    name = "itmcup_standings"
    allowed_domains = []
    start_urls = ['http://www.itmcup.co.nz/Fixtures/Standings/Itm2015']

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//table[@width="504"]//tr')
        count = 0
        record = SportsSetupItem()
        for node in nodes:
            season = "".join(re.findall(r'\d+', response.url))
            group_name = extract_list_data(node, '..//preceding-sibling::h3//text()')[-1]
            tou_name = extract_list_data(node, '..//preceding-sibling::div//text()')
            tou_name = tou_name[-2]
            team = extract_data(node, './/td[1]//text()').lower().replace(' ', '_')
            if "Finals" in tou_name or not team:
                continue
            if "ITM Cup Premiership" in group_name:
                count = count + 1
            if "ITM Cup Championship" in group_name:
                count = count - 6
            played = extract_data(node, './/td[2]//text()')
            wins   = extract_data(node, './/td[3]//text()')
            draws  = extract_data(node, './/td[4]//text()')
            losses = extract_data(node, './/td[5]//text()')
            pts_for = extract_data(node, './/td[6]//text()')
            pts_again = extract_data(node, './/td[7]//text()')
            pts_diff = extract_data(node, './/td[8]//text()')
            bp1      = extract_data(node, './/td[9]//text()')
            bp2      = extract_data(node, './/td[10]//text()')
            pts      = extract_data(node, './/td[11]//text()')

            record['result_type'] = "group_standings"
            record['season'] = season
            record['tournament'] = group_name
            record['participant_type'] = 'team'
            record['source']           = 'itmcup_rugby'
            record['affiliation']      = 'irb'

            record['result'] = {team: {'rank': count,  'played': played, \
                   'wins': wins, 'draws': draws, \
                   'losses': losses, \
                   'pts_for': pts_for, 'pts_again': pts_again, \
                   'pts_diff': pts_diff, 'bp1': bp1, \
                   'bp2': bp2, 'pts': pts }}
            import pdb;pdb.set_trace()
            yield record

