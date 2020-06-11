from scrapy.spider import BaseSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
import requests
import urllib2

class Sample(BaseSpider):
    name = "sample"
    start_urls = ['http://moonsy.com/page_authority/']
    def parse(self, response):
        hxs = HtmlXPathSelector(response)

        return [FormRequest(url="http://moonsy.com/page_authority/", method="post",
                formdata={'domain': 'bigbasket.com', 'qType': 'pA', 'qSign': '56c05cb6', 'Submit': 'CHECK'}, callback=self.parse_details)]
    
    def parse_details(self, response):
        import pdb;pdb.set_trace()
        hxs = HtmlXPathSelector(response)
        result = "".join(hxs.select('//div[@class="seomozResult"]//strong[@class="dac"]//text()').extract())

