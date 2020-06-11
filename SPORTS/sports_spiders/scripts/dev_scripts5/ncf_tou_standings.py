from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem



class NCFStandings(VTVSpider):
    name  = "standings"
    #allowed_domains = ["espn.go.com"]
    start_urls      = []
    record = SportsSetupItem()
    result = {}

    def start_requests(self):
        top_url = 'http://espn.go.com/college-football/playoffPicture'
        yield Request(top_url, callback=self.parse_next)

    def parse_next(self, response):
        hxs    = Selector(response)
        nodes  = get_nodes(hxs, '//div[@class="mod-content"]//table//tr[contains(@class, "team")]')
        rank = 0
        for node in nodes:
            rank = rank + 1
            team_name = extract_data(node, './td//a/text()')
            team_link = extract_data(node, './td//a/@href')
            team_sk = team_link.split('/')[-2]
            record = extract_data(node, './td[2]//text()')
            cfp = extract_data(node, './td[3]//text()')
            ap_pole = extract_data(node, './td[4]//text()')
            if "--" in ap_pole:
                ap_pole = ''
            sos = extract_data(node, './td[5]//text()')
            sor = extract_data(node, './td[6]//text()')
            gc = extract_data(node, './td[7]//text()')
            rat = extract_data(node, './td[8]//text()')
            rk  = extract_data(node, './td[9]//text()')
            self.record['tournament'] = "NCAA College Football"
            self.record['participant_type'] = 'team'
            self.record['season'] = "2014-15"
            self.record['source'] = 'espn_ncaa-ncf'
            self.record['affiliation'] = 'ncaa-ncf'
            self.record['result_type']   = "tournament_standings"
            self.record['result'] = {team_sk : {'record':record, 'cfp':cfp, 'ap_pole': ap_pole, \
                                    'sos': sos, 'sor': sor, 'gc': gc, "rat": rat, \
                                        'rk' : rk, 'rank': rank}}
            yield self.record

