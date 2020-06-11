from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher

class Basketabll(VTVSpider):
    name = "basketball_"
    start_urls = ['http://basketball.realgm.com/international/league/67/Danish-Basketligaen/teams']

    def parse(self, resposne):
        sel = Selector(resposne)
        links = extract_list_data(sel, '//table[@class="basketball"]//tr//td[@data-th="Rosters"]//a//@href')
        for link in links:
            url =link
            url ="http://basketball.realgm.com" + url
            yield Request(url, callback=self.parse_next)

    def parse_next(self, resposne):
        import pdb;pdb.set_trace()
