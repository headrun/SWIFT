from vtvspider_dev import VTVSpider, extract_data, \
                extract_list_data, get_nodes
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

class ICCWorldCupStandings(VTVSpider):
    name = "iccworldcup_standings"
    start_urls = ['http://www.espncricinfo.com/icc-cricket-world-cup-2015/engine/series/509587.html?view=pointstable']
    allowed_domains = []

    def parse(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        group_nodes = get_nodes(hxs, '//section[@class="main-pointstable"]')
        tou = extract_data(hxs, '//div[@class="icc-home"]//a//text()')
        tour = tou.replace('2015', '')
        season = tou.replace('ICC Cricket World Cup ', '')
        for group_node in group_nodes:
            group_name = extract_data(group_node, './/h4//text()')
            group_name = tour + group_name
            nodes = get_nodes(group_node, './/table//tr[contains(@class, "prow")]')
            rank = 0
            for node in nodes[:7]:
                rank  +=1
                team_sk = extract_data(node, './/td[1]//text()')
                mat = extract_data(node, './/td[2]//text()')
                won = extract_data(node, './/td[3]//text()')
                lost = extract_data(node, './/td[4]//text()')
                tied = extract_data(node, './/td[5]//text()')
                nr  = extract_data(node, './/td[6]//text()')
                pts = extract_data(node, './/td[7]//text()')
                net_rr = extract_data(node, './/td[8]//text()')
                forr = extract_data(node, './/td[9]//text()')
                against = extract_data(node, './/td[10]//text()')
                record['tournament'] = group_name
                record['season'] = season
                record['source'] = 'espn_cricket'
                record['participant_type']  =  'team'
                record['result_type'] = 'group_standings'
                record['affiliation'] = 'icc'
                record['game'] = 'cricket'
                record['result'] = {team_sk: {'rank': rank, 'mat': mat, \
                                              'won': won, 'lost': lost, \
                                              'tied': tied, 'n/r': nr, \
                                              'pts': pts, 'net_rr': net_rr, \
                                              'for': forr, 'against' : against}}
                yield record


