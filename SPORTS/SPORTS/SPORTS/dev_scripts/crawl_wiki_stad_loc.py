from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import Selector
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('final_stad_loc_output.txt', 'w')

class Wiki_loc(BaseSpider):
    name = 'wiki_loc_crawl'

    def start_requests(self):
        input_lines = open('stadiums_with_location.txt').readlines()
        url = 'https://en.wikipedia.org/?curid=%s'
        for line in input_lines:
            line = line.strip('\n ')
            wiki_gid = line.split('<>')[0].replace('<WIKI>', '').strip()
            yield Request(url%wiki_gid, self.get_loc, meta = {'line': line})

    def get_loc(self, response):
        sel = Selector(response)
        loc_data = ','.join(sel.xpath('//table[@class="infobox vcard"]//th[contains(text(),"Location")]/following-sibling::td//text()').extract())
        wiki_til = ''.join(sel.xpath('//h1[@id="firstHeading"]//text()').extract())
        db_data = response.meta['line'].replace('########', ' - ')
        
        f.write('%s :-:-:-: %s\n'%(db_data, wiki_til+'<>'+loc_data))
       
