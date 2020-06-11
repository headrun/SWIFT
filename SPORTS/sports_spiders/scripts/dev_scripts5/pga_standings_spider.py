import re
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


class PGAStandings(VTVSpider):
    name = "pga_standings"
    start_urls = ['http://www.pgatour.com/fedexcup.html']

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        standings_url = extract_data(hxs, '//div[@class="fedexcup-top5"]/div[@class="visible-large"]/a/@href')
        yield Request(standings_url, callback = self.parse_standings, meta = {})

    def parse_standings(self, response):
        sportssetupitem = SportsSetupItem()
        player_ids  = []
        items       = []
        players     = {}
        ids_map     = {}
        hxs         = HtmlXPathSelector(response)
        tou_name    = extract_data(hxs, '//div[@class="header"]/p/strong/text()')
        _name       = tou_name.split(',')[0]
        year        = tou_name.split(',')[-1].strip()
        nodes       = get_nodes(hxs, '//div[@class="details-table-wrap"]//tbody/tr')
        for node in nodes:
            rank_this_week  = extract_data(node, './td[1]/text()')
            player_name     = extract_data(node, './td[@class="player-name"]/a/text()')
            player_url      = extract_data(node, './td[@class="player-name"]/a/@href')
            player_id       = "".join(re.findall(r'\d+', player_url))
            events          = extract_data(node, './td[4]/text()')
            points          = extract_data(node, './td[5]/text()')
            wins            = extract_data(node, './td[6]/text()')
            points_behind   = extract_data(node, './td[8]/text()')

            if player_id:
                players[player_id] = {'rank' : rank_this_week, 'points' : points, 'wins' : wins, \
                                        'points_behind_lead' : points_behind, 'events' : events}
                sportssetupitem['result'] = players
                ids_map[player_id] = player_name
                player_ids.append(player_id)

        sportssetupitem['participant_type']     = 'player'
        sportssetupitem['result_type']          = 'tournament_standings'
        sportssetupitem['source']               = 'pga_golf'
        sportssetupitem['season']               = year
        sportssetupitem['game']                 = 'golf'
        sportssetupitem['tournament']           = _name
        items.append(sportssetupitem)
        for item in items:
            import pdb;pdb.set_trace()
            yield item
