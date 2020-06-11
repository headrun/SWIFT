import re
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, get_nodes, extract_data


class HeartlandStandings(VTVSpider):
    name = 'heartland_standings'
    allowed_domains = []
    start_urls = ['http://rugbyheartland.co.nz/wp/2015-heartland-standings/']


    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()

        season = "".join(re.findall('\d+', response.url))
        st_nodes = get_nodes(sel, '//div[@class="entry-content"]//table//tr')
        count = 0

        for st_node in st_nodes[:15]:
            team_name = extract_data(st_node, './/td[1]//text()')

            if not team_name or 'Heartland Championship' in team_name or 'Team' in team_name:
                continue
            count += 1
            team_sk = team_name.split('{')[0].strip().replace(' ', '_').lower()
            team_sk = team_sk.replace('-', '_').replace('.', '').strip()
            print team_sk
            played    = extract_data(st_node, './/td[2]//text()')
            wins      = extract_data(st_node, './/td[3]//text()')
            draws     = extract_data(st_node, './/td[4]//text()')
            losses    = extract_data(st_node, './/td[5]//text()')
            pts_for = extract_data(st_node, './/td[6]//text()')
            pts_again = extract_data(st_node, './/td[7]//text()')
            plt_diff       = extract_data(st_node, './/td[8]//text()')
            bp       = extract_data(st_node, './/td[9]//text()')
            total_pts = extract_data(st_node, './/td[10]//text()')

            record['participant_type']  = 'team'
            record['result_type']      = 'tournament_standings'
            record['source'] =  'heartland_rugby'
            record['affiliation'] = 'irb'
            record['game'] = "rugby union"
            record['season'] = season
            record['tournament'] = 'Heartland Championship'
            record['result'] = {team_sk : {'wins': wins, 'losses': losses, \
                    'draws': draws, 'played': played, 'pts_for': pts_for, \
                                'pts_again': pts_again, 'pts_diff': plt_diff, \
                                'bp': bp, 'rank' : count,
                                'pts': total_pts}}
            yield record

