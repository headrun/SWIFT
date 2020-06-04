import json
from os import path, getcwd
from urllib.parse import urljoin
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.selector import Selector

class IataCodesSpider(Spider):
    name = 'get_iata_codes'

    def __init__(self, *args, **kwargs):
        super(IataCodesSpider, self).__init__(*args, **kwargs)
        self.dict = {}
        self.headers = {
            'authority': 'www.nationsonline.org',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7'
        }
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self):
        if self.dict:
            out_path = path.abspath(path.join(getcwd(), '../../../flights/flights/liftoff/airport_codes.txt'))
            with open(out_path, 'w+') as _file:
                _file.write(json.dumps(self.dict))

    def start_requests(self):
        url = 'https://www.nationsonline.org/oneworld/airport_code.htm'
        yield Request(url, callback=(self.parse), headers=(self.headers))

    def parse(self, response):
        sel = Selector(response)
        iata_links = sel.xpath('//a[contains(@href, "IATA_Code_")]/@href').extract()
        for iata_link in iata_links:
            if 'http' not in iata_link:
                iata_link = urljoin('https://www.nationsonline.org/oneworld/', iata_link)
            yield Request(iata_link, callback=self.parse_iata, headers=self.headers)

    def parse_iata(self, response):
        sel = Selector(response)
        iata_nodes = sel.xpath('//table[contains(@summary, "IATA Airport-Codes")]//tr')
        for iata_node in iata_nodes:
            code = ''.join(iata_node.xpath('./td[1]//text()').extract()).split('(')[0].strip()
            city = ''.join(iata_node.xpath('./td[2]//text()').extract()).split('(')[0].strip()
            airport = ''.join(iata_node.xpath('./td[3]//text()').extract()).split('(')[0].strip()
            country = ''.join(iata_node.xpath('./td[4]//text()').extract()).split('(')[0].strip()

            self.dict.update({code: {'city': city, 'name': airport, 'country': country}})
