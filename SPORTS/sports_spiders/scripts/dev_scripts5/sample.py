from scrapy.spider import BaseSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector

class Engvidios(BaseSpider):
    name = "videos"
    allowed_domains = ["engineering.com"]
    start_urls = ['http://www.engineering.com/Videos/tabid/4624/Default.aspx']

    def parse(self,response):
        hxs = HtmlXPathSelector(response)
        nodes = hxs.select('//div[@class="SubSubHead"]')
        for node in nodes:
            title = "".join(node.select('./a//text()').extract())
            print title
