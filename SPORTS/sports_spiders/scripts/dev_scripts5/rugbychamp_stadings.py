from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, extract_data, \
             get_nodes, extract_list_data

class RugbyChampStandings(VTVSpider):
    name = "rugbychamp_standings"
    start_urls = ['http://www.espn.co.uk/rugby/table/_/league/244293']

    def parse(self, response):
        sel = Selector(response)
        st_nodes = get_nodes(sel, '//table//tr')
        tou_det = extract_data(sel, '//header[@class="sub-header-module"]//h1//text()')
        tou_name = tou_det.split('-')[0].strip()
        season = tou_det.split('-')[-1].strip()

        for st_node in st_nodes:
            team_name = extract_data(st_node, './/td//a//span[@class="team-names"]//text()').lower()
            rank = extract_data(st_node, './/td//span[@class="number"]//text()')
            played = extract_data(st_node, './/td[2]//text()')
            won   = extract_data(st_node, './/td[3]//text()')
            draws  = extract_data(st_node, './/td[4]//text()')
            loss   = extract_data(st_node, './/td[5]//text()')
            points = extract_data(st_node, './/td[6]//text()')

            record = SportsSetupItem()
            record['result_type'] = "tournament_standings"
            record['season'] = season
            record['tournament'] = tou_name
            record['participant_type'] = "team"
            record['source'] = 'espn_rugby'
            record['result'] = { team_name: {'rank': rank, 'played': played, \
                                'won': won, 'draws': draws, \
                                'losses': loss, 'points': points}}
            yield record

