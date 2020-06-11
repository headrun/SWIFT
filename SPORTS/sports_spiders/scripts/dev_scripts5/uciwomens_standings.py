from vtvspider_dev import VTVSpider, \
get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


class UCIWomensStandings(VTVSpider):
    name = "uciwomens_standings"
    allowed_domains = []
    start_urls      = ['http://62.50.72.82/uciroot/cdmroa/2015/Standings.aspx?id=48993&language=eng&event=Ronde%20van%20Vlaanderen%20/%20Tour%20des%20Flandres']


    def parse(self, response):
        sel     = Selector(response)
        record  = SportsSetupItem()
        riders  = {}
        st_nodes = get_nodes(sel, '//table[@cellpadding="2"]//tr')
        for node in st_nodes:
            rank    = extract_data(node, './/td[1]//text()')
            if "Rank" in rank:
                continue
            pl_name = extract_data(node, './/td[5]//text()')
            pl_sk   = pl_name.replace(' ', '_').replace("'", '_').replace('-', '_').lower()
            points  = extract_data(node, './/td[8]//text()')

            if pl_name:
                riders[pl_sk] = {'rank' : rank, 'points' : points}

            record['result']           = riders
            record['season']           = "2015"
            record['source']           = 'uciworldtour_cycling'
            record['tournament']       = "UCI Women's Road World Cup"
            record['participant_type'] = 'player'
            record['result_type']      = 'tournament_standings'
            yield record

