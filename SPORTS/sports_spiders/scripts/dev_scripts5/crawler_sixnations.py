import re
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider import VTVSpider, extract_data, get_nodes

true = True
false = False
null = ''

class SixNationsCrawler(VTVSpider):
    name = "sixnations"
    allowed_domains = ["rbs6nations.com"]
    start_urls = []
    record = SportsSetupItem()

    def start_requests(self):
        req = []
        top_url = 'http://www.rbs6nations.com/en/matchcentre/league_table.php'
        yield Request(top_url, callback=self.parse_standings)

    def parse_standings(self, response):
        hxs = HtmlXPathSelector(response)
        season = extract_data(hxs, '//td[@class="heading"]/text()')
        tou    = 'Six Nations Championship'
        season = season.split(' - ')[0]
        end    = "".join(re.findall(r'20(\d+)', season.split(' - ')[0].split('-')[1]))
        season = season.split('-')[0] +"-"+end
        nodes  = get_nodes(hxs, '//tr[contains(@id, "leaguetab")]')
        for node in nodes:
            position = extract_data(node, './/td[@class="field_Position"]/text()')
            position = "".join(re.findall(r'(\d+)', position))
            team     = extract_data(node, './td[@class="field_TeamDisplay"]/text()')
            played   = extract_data(node, './td[@class="field_Played"]/text()')
            wins     = extract_data(node, './td[@class="field_Win"]/text()')
            draws    = extract_data(node, './td[@class="field_Draw"]/text()')
            losses   = extract_data(node, './td[@class="field_Lose"]/text()')
            pts_for  = extract_data(node, './td[@class="field_PtsFor"]/text()')
            pts_again = extract_data(node, './td[@class="field_PtsAgainst"]/text()')
            pts_diff = extract_data(node, './td[@class="field_PtsDiff"]/text()')
            try_for = extract_data(node, './td[@class="field_TryFor"]/text()')
            try_again = extract_data(node, './td[@class="field_TryAgainst"]/text()')
            field_pts = extract_data(node, './td[@class="field_Points"]/text()')
            self.record['result_type'] = "tournament_standings"
            self.record['season'] = season
            self.record['tournament'] = tou
            self.record['participant_type'] = "team"
            self.record['source'] = 'espn_rugby'
            self.record['result'] = {team:{'rank': position,  'played': played,\
                   'wins': wins, 'draws': draws, 'losses': losses, 'pts_for': pts_for, 'pts_again': pts_again,\
                   'pts_diff': pts_diff, 'try_for': try_for, 'try_again': try_again, 'field_pts': field_pts }}
            yield self.record

