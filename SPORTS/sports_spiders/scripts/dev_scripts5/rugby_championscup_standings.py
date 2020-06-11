from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes

class RugbyChampionsStandings(VTVSpider):
    name = "champions_rugbystandings"
    start_urls = ['http://www.espnscrum.com/european-rugby-champions-cup-2014-15/rugby/series/224327.html?template=pointstable']


    def parse(self, response):
        hxs = Selector(response)
        season = extract_data(hxs, '//div[@class="header_container"]//h1//text()')
        nodes = get_nodes(hxs, '//div[@id="scrumArticlesBoxContent"]//table//tr[@class="pointsTblContWht"]')
        for node in nodes:
            group_name = extract_data(node, '..//preceding-sibling::caption[@class="pointsTblCapt"]//text()')
            group_name = "European Rugby Champions Cup " +group_name
            position  = extract_data(node, './/td[1]//text()')
            team      = extract_data(node, './/td[2]//text()').lower(). \
            encode('utf-8').replace('\xe7', '').replace('\xc3\xa7', '') 
            if not team:
                continue
            played    = extract_data(node, './/td[3]//text()')
            wins      = extract_data(node, './/td[4]//text()')
            draws     = extract_data(node, './/td[5]//text()')
            losses    = extract_data(node, './/td[6]//text()')
            pts_for   = extract_data(node, './/td[7]//text()')
            pts_again = extract_data(node, './/td[8]//text()')
            pts_diff  = extract_data(node, './/td[9]//text()')
            try_for   = extract_data(node, './/td[10]//text()')
            mp        = extract_data(node, './/td[11]//text()')
            bp       = extract_data(node, './/td[12]//text()')
            field_pts = extract_data(node, './/td[13]//text()')

            record = SportsSetupItem()
            record['result_type'] = "group_standings"
            record['season'] = "2014-15"
            record['tournament'] = group_name
            record['participant_type'] = "team"
            record['source'] = 'espn_rugby'
            record['result'] = {team: {'rank': position,  'played': played, \
                   'wins': wins, 'draws': draws, 'losses': losses, \
                   'pts_for': pts_for, 'pts_again': pts_again, \
                   'pts_diff': pts_diff, 'try_for': try_for, \
                   'mp': mp, \
                   'bp': bp, 'field_pts': field_pts }}
            import pdb;pdb.set_trace()
            yield record

