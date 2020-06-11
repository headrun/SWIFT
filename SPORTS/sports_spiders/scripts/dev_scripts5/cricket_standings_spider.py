from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, get_nodes


class CricketStandings(VTVSpider):
    name = 'cricket_standings'
    allowed_domains = ["www.espncricinfo.com"]
    start_urls = ["http://www.espncricinfo.com/rankings/content/page/211271.html"]

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//table[@class="StoryengineTable"]//tr')
        test_count = 0
        odi_count = 0
        t20_count = 0
        for node in nodes:
            tou = extract_data(node, './../preceding-sibling::h3[1]/text()')
            team_sk = extract_data(node, './td[1]/text()')
            if team_sk:
                matches = extract_data(node, './td[2]/text()')
                points = extract_data(node, './td[3]/text()')
                rating = extract_data(node, './td[4]/text()')
                if "Test" in tou:
                    tou = "Test cricket"
                    test_count += 1
                    rank = test_count
                if "ODI" in tou:
                    tou = "ODI cricket"
                    odi_count += 1
                    rank = odi_count
                if "Twenty20" in tou:
                    tou = "Twenty20 cricket"
                    t20_count += 1
                    rank = t20_count

                record['tournament'] = tou
                record['season'] = '2015'
                record['source'] = 'espn_cricket'
                record['participant_type']  =  'team'
                record['result_type'] = 'tournament_standings'
                record['affiliation'] = 'icc'
                record['game'] = 'cricket'
                record['result'] = {team_sk: {'Matches': matches, \
                                'Points': points, \
                                'Rating': rating, 'rank' : rank}}
                import pdb;pdb.set_trace()
                yield record

