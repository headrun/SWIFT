import datetime, time
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data, log
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re

class CyclingStandingss(VTVSpider):
    name        = 'cycling_standingss'
    #start_urls  = ['http://www.uci.html.infostradasports.com/cache/2/TheASP.asp@PageID%3D102007&SportID%3D102&CompetitionID%3D24055&SeasonID%3D488&EventID%3D12146&GenderID%3D1&ClassID%3D1&EventPhaseID%3D1186645&Phase1ID%3D1186647&TaalCode%3D2&StyleID%3D0&Cache%3D2.html?539455']
    start_urls  = ['http://www.uci.html.infostradasports.com/asp/index.asp?PageID=19005&SportID=102&ClassID=1&GenderID=1&CompetitionCodeInv=1152&Detail=1&Ranking=1&All=0&TaalCode=2&StyleID=0&Cache=4']
    start_urls  = ['http://www.uci.html.infostradasports.com/cache/2/TheASP.asp@PageID%3D102007&SportID%3D102&CompetitionID%3D24055&SeasonID%3D488&EventID%3D12146&GenderID%3D1&ClassID%3D1&EventPhaseID%3D1186645&Phase1ID%3D1186661&TaalCode%3D2&StyleID%3D0&Cache%3D2.html?91991']
    def parse(self, response):
        sportssetupitem = SportsSetupItem()
        hxs             = HtmlXPathSelector(response)
        riders          = {}
        year            = extract_data(hxs, '//table[@class="datatable"]//tr//td[@class="caption"]//text()')
        tou_year        = "".join(re.findall(r'\d+', year))
        tou_name        = 'UCI World Tour'
        nodes           = get_nodes(hxs, '//table[@class="datatable"]//tr[@valign="top"]')
        for node in nodes[::1]:
            rank        = extract_data(node, './/td[1]//text()')
            rider_name  = extract_data(node, './/td[3]//text()')
            points      = extract_data(node, './/td[@align="right"]//text()')
            rider_sk    = rider_name.lower().replace(' ', '-')

            if rider_name:
                riders[rider_sk] = {'rank' : rank, 'points' : points}

            sportssetupitem['result']           = riders
            sportssetupitem['season']           = tou_year
            sportssetupitem['source']           = 'uciworldtour_cycling'
            sportssetupitem['tournament']       = tou_name
            sportssetupitem['participant_type'] = 'player'
            sportssetupitem['result_type']      = 'tournament_standings'
            import pdb;pdb.set_trace()
            yield sportssetupitem



