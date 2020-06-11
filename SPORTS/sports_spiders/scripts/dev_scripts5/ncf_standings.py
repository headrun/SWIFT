from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem



class NCFStandingsSpider(VTVSpider):
    name            = "ncf_standings"
    allowed_domains = ["espn.go.com"]
    start_urls      = []

    def start_requests(self):
        top_url = 'http://espn.go.com/college-football/standings'
        yield Request(top_url, callback=self.parse_next)

    def parse_next(self, response):
        hxs    = Selector(response)
        tou    = extract_data(hxs,'//div[@class="mod-content"]/h2//text()')
        _tou   = tou.split(' Standings - ')
        season = _tou[-1]
        nodes = get_nodes(hxs, '//tr[contains(@class,"row")]/td/a[contains(@href,"conferences/standings")]')
        for node in nodes:
            st_link = extract_data(node, './@href')
            st_link = "http://espn.go.com" + st_link
            yield Request(st_link, callback = self.parse_standings, \
                            meta = {'season': season})

    def parse_standings(self, response):
        hxs        = Selector(response)
        season     = response.meta['season']
        record     = SportsSetupItem()
        conf_name  = extract_data(hxs,'//div[@id="sub-branding"]/h1[@class="h2 logo"]//text()').split(' Conference')[0]+" Football"
        team_nodes = get_nodes(hxs, '//table[@class="tablehead"]//tr[@class="stathead"]/td[contains(text(),"STANDINGS")]/../../tr[contains(@class, "row")]')
        rank = 0 
        record['result'] = {}
        result = {}
        for _node in team_nodes:
            import pdb;pdb.set_trace()
            rank          += 1
            team_link      = extract_data(_node, './td/preceding-sibling::td/a[contains(@href,"/team/")]/@href')
            source_key     = team_link.split('/id/')[-1].split('/')[0]
            gb             = extract_data(_node, './td[2]//text()')
            ov_w_l         = extract_data(_node, './td[4]//text()')
            overall_wins   = ov_w_l.split('-')[0]
            overall_losses = ov_w_l.split('-')[1]
            pct            = extract_data(_node, './td[5]//text()')
            strk           = extract_data(_node, './td[6]//text()')
            standings = {'gb':gb, 'losses':overall_losses, \
                        'pct':pct,'rank':rank, 'wins':overall_wins, \
                         'strk':strk }
            result.setdefault(source_key, {}).update(standings)
        expand_nodes = get_nodes(hxs, '//table[@class="tablehead"]//tr[@class="stathead"]/td[contains(text(),"EXPANDED")]/../../tr[contains(@class, "row")]')
        for _nodes in expand_nodes:
            team_link   = extract_data(_nodes, './td[@align="center"]/preceding-sibling::td/a[contains(@href,"/team/")]/@href')
            source_key  = team_link.split('/id/')[-1].split('/')[0]
            home        = extract_data(_nodes, './td[contains(@align, "center")][3]//text()')
            road        = extract_data(_nodes, './td[contains(@align, "center")][4]//text()')
            pf          = extract_data(_nodes, './td[contains(@align, "center")][5]//text()')
            pa          = extract_data(_nodes, './td[contains(@align, "center")][6]//text()')
            standings_ = {'home': home, 'road': road, 'pa': pa, 'pf': pf}
            result.setdefault(source_key, {}).update(standings_)
        record['result_type'] = 'group_standings'
        record['participant_type'] = 'team'
        record['season'] = season
        record['source'] = 'espn_ncaa-ncf'
        record['tournament'] = conf_name
        record['result'] = result
        import pdb;pdb.set_trace()
        yield record

