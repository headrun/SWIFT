from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes

class SuperRugbyStandings(VTVSpider):
    name = "superrugby_standings"
    allowed_domains = []
    start_urls = []

    def start_requests(self):
        top_urls = {'conference': 'http://en.espn.co.uk/super-rugby-2015/rugby/series/242041.html?noredir=1;template=pointstable',
                    'league'    : 'http://en.espn.co.uk/super-rugby-2015/rugby/page/256657.html'}
        for standings_type, url in top_urls.iteritems():
            yield Request(url, callback = self.parse, \
                    meta = {'standings_type': standings_type})

    def parse(self, response):
        sel = Selector(response)
        record =  SportsSetupItem()
        record['participant_type'] = 'team'
        record['source']           = 'espn_rugby'
        record['affiliation']      = 'irb'

        if response.meta['standings_type'] == 'conference':
            nodes = get_nodes(sel, '//div[@id="scrumArticlesBoxContent"]//table//tr[@class="pointsTblContWht"]')
            for node in nodes:
                season = response.url.split('/')[3].replace('super-rugby-', '').strip()
                group_name = extract_data(node, '..//preceding-sibling::caption[@class="pointsTblCapt"]//text()')
                group_name = group_name + " Conference"
                position  = extract_data(node, './/td[1]//text()')
                team      = extract_data(node, './/td[2]//text()').lower()
                team      = team.replace(' ', '_')
                if not team:
                    continue
                played    = extract_data(node, './/td[3]//text()')
                wins      = extract_data(node, './/td[4]//text()')
                draws     = extract_data(node, './/td[5]//text()')
                losses    = extract_data(node, './/td[6]//text()')
                bye       = extract_data(node, './/td[7]//text()')
                pts_for   = extract_data(node, './/td[8]//text()')
                pts_again = extract_data(node, './/td[9]//text()')
                pts_diff  = extract_data(node, './/td[10]//text()')
                try_for   = extract_data(node, './/td[11]//text()')
                try_again = extract_data(node, './/td[12]//text()')
                tbp        = extract_data(node, './/td[13]//text()')
                lbp       = extract_data(node, './/td[14]//text()')
                total_pts = extract_data(node, './/td[15]//text()')

                record['result_type'] = "group_standings"
                record['season'] = season
                record['tournament'] = group_name
                record['result'] = {team: {'rank': position,  'played': played, \
                       'wins': wins, 'draws': draws, \
                       'losses': losses, 'bye' : bye, \
                       'pts_for': pts_for, 'pts_again': pts_again, \
                       'pts_diff': pts_diff, 'try_for': try_for, \
                       'try_again': try_again, 'tbp': tbp, \
                       'lbp': lbp, 'total_pts': total_pts }}
                yield record

        if response.meta['standings_type'] == 'league':
            nodes = get_nodes(sel, '//table//tr[@class="pointsTblContWht"]')
            for node in nodes:
                position = extract_data(node, './/td[1]//text()').replace('.', '')
                team     = extract_data(node, './/td[2]//text()').lower().replace(' ', '_')
                points   = extract_data(node, './/td[4]//text()')
                season   = response.url.split('/')[3].replace('super-rugby-', '').strip()

                record['result_type'] = "tournament_standings"
                record['season']      = season
                record['tournament']  = "Super Rugby"
                record['result']      = {team: {'rank': position,  'points': points}}
                yield record
