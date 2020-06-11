import os
import re
import time
from datetime import datetime
from vtvspider import VTVSpider
from vtvspider import extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
#from sports_spiders.items import SportsSetupItem
from scrapy_spiders_dev.items import SportsSetupItem


class CandidatesChessStandings(VTVSpider):

    name            = 'candidates_standings'
    allowed_domains = ["candidates2014.fide.com"]
    start_urls      = ["http://candidates2014.fide.com/standings/"]
    record          = SportsSetupItem()
    result          = {}

    

    def parse(self, response):
        hxs         = HtmlXPathSelector(response)
        nodes       = get_nodes(hxs, '//table[@class="bordered2"]//tr')
        count       = 0
        season      = "http://candidates2014.fide.com/standings/"
        season      = "".join(re.findall('.*(...\d)',season))
        for node in nodes[1:]:
            count   += 1
            rank    = extract_data(node, './/td[1]/text()')
            sno     = extract_data(node, './/td[2]/text()')
            unk     = extract_data(node, './/td[3]/text()')
            name    = extract_data(node, './/td[4]/text()')
            rtg     = extract_data(node, './/td[5]/text()')
            fed     = extract_data(node, './/td[6]/text()')
            pts     = extract_data(node, './/td[7]/text()').encode('utf-8').split('\xc2')
            if len(pts) == 2:
                try:
                    pts = int(pts[0]) + 0.5
                except:
                    pts = 0.5
            else:
                pts     = pts[0]

            res     = extract_data(node, './/td[8]/text()').encode('utf-8').replace("\xc2\xbd", "0.5")
            vict    = extract_data(node, './/td[9]/text()')
            sb      = extract_data(node, './/td[10]/text()')

            self.record['tournament'] = 'Candidates chess Tournament'
            self.record['result_type'] = 'tournament_standings'
            self.record['participant_type'] = 'team'
            self.record['source'] = 'fide'
            self.record['season'] = season
            self.record['result'] = {name:{'fed':fed, 'pts':pts, 'rank':rank,'res':res, 'rtg':rtg, 'sb':sb, 'sno':sno, 'unk':unk, 'vict':vict }}
            yield self.record



