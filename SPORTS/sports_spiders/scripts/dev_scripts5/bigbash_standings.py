from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, get_nodes


class BigbashStandings(VTVSpider):
    name = 'bigbash_standings'
    allowed_domains = ["www.espncricinfo.com"]
    start_urls = ["http://www.espncricinfo.com/big-bash-league-2015-16/engine/series/897689.html?view=pointstable"]


    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//table//tr')
        record = SportsSetupItem()
        count = 0

        for node in tm_nodes[:9]:
            team_sk = extract_data(node, './/td[1]//text()')

            if team_sk == "Teams":
                continue

            count +=1
            match = extract_data(node, './/td[2]//text()')
            won   = extract_data(node, './/td[3]//text()')
            lost  = extract_data(node, './/td[4]//text()')
            tied  = extract_data(node, './/td[5]//text()')
            tm_nn = extract_data(node, './/td[6]//text()')
            pts   = extract_data(node, './/td[7]//text()')
            net_rr = extract_data(node, './/td[8]//text()')
            pts_for = extract_data(node, './/td[9]//text()')
            pts_again = extract_data(node, './/td[10]//text()')


            record['tournament'] = "Big Bash League"
            record['season'] = '2015-16'
            record['source'] = 'espn_cricket'
            record['participant_type']  =  'team'
            record['result_type'] = 'tournament_standings'
            record['affiliation'] = 'icc'
            record['game'] = 'cricket'
            record['result'] = {team_sk: {'rank': count, 'mat': match, \
                                              'won': won, 'lost': lost, \
                                              'tied': tied, 'n/r': tm_nn, \
                                              'pts': pts, 'net_rr': net_rr, \
                                              'for': pts_for, 'against' : pts_again}}
            yield record


