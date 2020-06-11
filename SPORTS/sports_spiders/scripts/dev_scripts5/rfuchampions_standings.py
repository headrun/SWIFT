from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes

class RFUChampionsStandings(VTVSpider):
    name = "rfu_standings"
    allowed_domains = ['www.englandrugby.com']
    start_urls = ['http://www.englandrugby.com/fixtures-and-results/competitions/greene-king-ipa-championship/']

    def parse(self, response):
        hxs = Selector(response)
        season = extract_data(hxs, '//div[@class="selected_season"]//text()').replace('-20', '-').strip()
        st_nodes = get_nodes(hxs, '//div[@class="row table_item"]//div[@class="small-12 large-12 medium-12 columns"]//ul')
        for node in st_nodes :
            rank       = extract_data(node, './/li[1]//text()')
            team       = extract_data(node, './/li[2]//text()').replace(' ', '-').lower()
            played     = extract_data(node, './/li[3]//text()')
            wins       = extract_data(node, './/li[4]//text()')
            draws      = extract_data(node, './/li[5]//text()')
            losses     = extract_data(node, './/li[6]//text()')
            pts_for    = extract_data(node, './/li[7]//text()')
            pts_again  = extract_data(node, './/li[8]//text()')
            pts_diff   = extract_data(node, './/li[9]//text()')
            tb         = extract_data(node, './/li[10]//text()')
            lb         = extract_data(node, './/li[11]//text()')
            field_pts  = extract_data(node, './/li[12]//text()')

            record                     = SportsSetupItem()
            record['result_type']      = "tournament_standings"
            record['season']           = season
            record['tournament']       = "RFU Championship"
            record['participant_type'] = "team"
            record['source']           = 'england_rugby'
            record['result']           = {team: {'rank': rank,  'played': played, \
                   'wins': wins, 'draws': draws, 'losses': losses, \
                   'pts_for': pts_for, 'pts_again': pts_again, \
                   'pts_diff': pts_diff, 'tb': tb, \
                   'lb': lb, 'field_pts': field_pts }}
            yield record


