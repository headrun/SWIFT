from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes

class NRLRugbyStandings(VTVSpider):
    name = "nrl_rugbystandings"
    start_urls = ['http://www.nrl.com/DrawResults/TelstraPremiership/Ladder/tabid/10251/Default.aspx']
    allowed_domains = []

    def parse(self, response):
        hxs = Selector(response)
        pl_nodes = get_nodes(hxs, '//table[@id="LadderGrid"]//tbody//tr')
        season = extract_data(hxs, './/h1[@class="pageTitle"]/text()')
        season = season.replace(' Telstra Premiership Ladder', '')
        for node in pl_nodes:
            position = extract_data(node, './/th[@class="ladderFull__th__rank"]/text()').replace('.', '')
            team     = extract_data(node, './/td[1]/text()')
            played   = extract_data(node, './/td[2]/text()')
            wins     =  extract_data(node, './/td[3]/text()')
            draws    =  extract_data(node, './/td[4]/text()')
            losses   =  extract_data(node, './/td[5]/text()')
            bonous   =  extract_data(node, './/td[6]/text()')
            pts_for  =  extract_data(node, './/td[7]/text()')
            pts_again =  extract_data(node, './/td[8]/text()')
            pts_diff  = extract_data(node, './/td[9]/text()')
            home      = extract_data(node, './/td[10]/text()')
            away      = extract_data(node, './/td[11]/text()')
            pts       = extract_data(node, './/td[@class="ladderFull__td__pts"]/text()')
            record = SportsSetupItem()
            record['result_type'] = "tournament_standings"
            record['season'] = season
            record['tournament'] = "National Rugby League"
            record['participant_type'] = "team"
            record['source'] = 'nrl_rugby'
            record['result'] = {team: {'rank': position,  'played': played, \
                   'wins': wins, 'draws': draws, 'losses': losses,  'bonous': bonous, \
                   'pts_for': pts_for, 'pts_again': pts_again, \
                   'pts_diff': pts_diff, 'home': home, \
                   'away': away, 'pts':pts }}
            yield record


