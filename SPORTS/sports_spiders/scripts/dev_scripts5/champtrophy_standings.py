from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem



class ChampTrophyStandings(VTVSpider):
    name  = "champ_standings"
    allowed_domains = []
    start_urls      = []
    record = SportsSetupItem()
    result = {}

    def start_requests(self):
        top_url = 'http://events.fih.ch/new/competition/347/standings'
        yield Request(top_url, callback=self.parse)

    def parse(self, response):
        hxs    = Selector(response)
        group_nodes = get_nodes(hxs, '//div[@class="band_grey"]//div[@class="row"]//ul[@class=" small-block-grid-1 large-block-grid-2"]//li')
        for group_node in group_nodes:
            group_name = extract_data(group_node, './/h4//a//text()')
            nodes =  get_nodes(group_node, './/table/tr')
            count = 0
            for node in nodes[:4]:
                count = count + 1
                team_sk = extract_data(node, './td/a/text()')
                played = extract_data(node, './td[2]/text()')
                wins = extract_data(node, './td[3]/text()')
                draws = extract_data(node, './td[4]/text()')
                losses = extract_data(node, './td[5]/text()')
                gf = extract_data(node, './td[6]/text()')
                ga = extract_data(node, './td[7]/text()')
                gd = extract_data(node, './td[8]/text()')
                points = extract_data(node, './td[9]/text()')
                self.record['tournament'] = "Hockey Champions Trophy" + " " +group_name
                self.record['participant_type'] = 'team'
                self.record['season'] = "2014"
                self.record['source'] = 'field_hockey'
                self.record['affiliation'] = 'fih'
                self.record['result_type']   = "group_standings"
                self.record['result'] = {team_sk : {'Played': played, 'Wins': wins, "Draws": draws, "Losses": losses, \
                                            "GF": gf, "GA": ga, "GD": gd , "Points": points, 'Rank': count}}
                yield self.record

