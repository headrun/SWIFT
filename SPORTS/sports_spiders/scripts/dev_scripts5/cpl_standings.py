from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


class CPLStandings(VTVSpider):
    name = 'cpl_standings'
    allowed_domains = ["www.espncricinfo.com"]
    start_urls =  ["http://www.espncricinfo.com/caribbean-premier-league-2015/engine/series/846823.html?view=pointstable"]


    def parse(self, response):
        record  = SportsSetupItem()
        sel     = Selector(response)
        nodes   = get_nodes(sel, '//table//tr')
        count   = 0

        for node in nodes:
            team = extract_data(node, './/td[@class="tdh"]//text()') \
                .strip().replace(' & ', ' ').replace(' and ', ' ')
            team_sk = team.replace(' ', '_').lower()

            if "teams" in team :
                continue

            if team:
                count += 1
                matches = extract_data(node, './/td[2]//text()')
                won = extract_data(node, './/td[3]//text()')
                lost = extract_data(node, './/td[4]//text()')
                tied = extract_data(node, './/td[5]//text()')
                nr = extract_data(node, './/td[6]//text()')
                pts = extract_data(node, './/td[7]/text()')
                netrr = extract_data(node, './/td[8]/text()')
                pts_for = extract_data(node, './/td[9]/text()')
                against = extract_data(node, './/td[10]/text()')

                record['source'] = 'espn_cricket'
                record['tournament'] = "Caribbean Premier League"
                record['participant_type']  =  'team'
                record['season'] = "2015"
                record['affiliation'] = 'cpl'
                record['game'] = 'cricket'
                record['result_type'] = 'tournament_standings'
                record['result'] = {team_sk: {'matches': matches, 'won': won, \
                                            'lost': lost, 'tied': tied,\
                                            'nr': nr, 'pts': pts, 'netrr': netrr, \
                                            'for': pts_for, 'against': against, \
                                            'rank' : count}}
                yield record




