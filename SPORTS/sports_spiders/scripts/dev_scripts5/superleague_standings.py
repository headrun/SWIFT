from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes, extract_list_data


class SuperLeagueStandings(VTVSpider):
    name = "superleague_standings"
    allowed_domains = []
    start_urls = ['http://www.rugby-league.com/superleague/tables']

    def parse(self, response):
        sel = Selector(response)
        st_nodes = get_nodes(sel, '//div[@id="tables"]//table//tbody//tr')
        season = extract_list_data(sel, '//select[@name="table-season-select"]//option//text()')[0]
        for node in st_nodes:
            position = extract_data(node, './/td[1]/text()')
            team_sk  = extract_data(node, './/td[3]//text()').strip()
            if not team_sk:
                continue
            team_sk  = team_sk.lower().replace(' ', '_')
            played   = extract_data(node, './/td[4]//text()')
            wins     = extract_data(node, './/td[5]//text()')
            loss     = extract_data(node, './/td[6]//text()')
            draw     = extract_data(node, './/td[7]//text()')
            pts_for  = extract_data(node, './/td[8]//text()')
            pts_again = extract_data(node, './/td[9]//text()')
            pts_diff  = extract_data(node, './/td[10]//text()')
            pts_bns   = extract_data(node, './/td[11]//text()')
            pts       = extract_data(node, './/td[12]//text()')


            record = SportsSetupItem()
            record['result_type'] = "tournament_standings"
            record['season'] = season
            record['tournament'] = "Super League"
            record['participant_type'] = "team"
            record['source'] = 'superleague_rugby'
            record['result'] = {team_sk: {'rank': position, 'played': played, \
                   'wins': wins, 'draws': draw, 'losses': loss, \
                   'pts_for': pts_for, 'pts_again': pts_again, \
                   'pts_diff': pts_diff, 'total_pts': pts, 'bns': pts_bns }}
            yield record


