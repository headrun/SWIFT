from json import loads
from csv import writer
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request

class IndiaMartBrowse(Spider):
    name = 'indiamart_browse'
    start_urls = []

    def __init__(self):
        self.values = []
        self.headers = ['Seller Name', 'Location', 'Address', 'Contact No.', 'Website URL', 'Business Type', 'Additional Business Types']
        self.url = 'https://dir.indiamart.com/search.mp/next?akKey=eyJ0eXAiOiJKV1QiLCJhbGciOiJzaGEyNTYifQ.eyJpc3MiOiJVU0VSIiwiYXVkIjoiOSowKjYqNyo3KiIsImV4cCI6MTU4OTUzNzQzNSwiaWF0IjoxNTg5NDUxMDM1LCJzdWIiOiIyODQ0NjE3OCIsImNkdCI6IjE0LTA1LTIwMjAifQ.iLvUJ9ZnI-WZd5YYhyRxBUuBysXZ8GY6bEpFHkv9F9c&ss=laptop&c=IN&scroll=1&language=en&city_only=&pr=0&pg=%s'
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        with open('indiamart_sellers_data.csv', 'w+') as _file:
            csv_writer = writer(_file)
            csv_writer.writerow(self.headers)
            csv_writer.writerows(self.values)

    def start_requests(self):
        url = self.url % 1
        yield Request(url, callback=self.parse, meta={'page': 1})

    def parse(self, response):
        page = response.meta['page']
        _data = loads(response.body)
        status = _data.get('status', '')
        content = _data.get('content', '').replace('\"', '').replace('^', '')
        sel = Selector(text=content)
        nodes = sel.xpath('//li[contains(@id, "LST")]//div[@class="r-cl"]')
        for node in nodes:
            title = ''.join(node.xpath('.//h4[@data-click="CompanyName"]/text()').extract())
            location = ' '.join(node.xpath('./p[@class="sm"]/text()').extract()).strip()
            address = ' '.join(node.xpath('./p[@class="sm"]/span[@class="to-txt"]/text()').extract()).strip()
            contact_number = ', '.join(node.xpath('.//span[contains(@id, "pns")]/text()').extract())
            reference_url = ''.join(node.xpath('./a/@href').extract())

            values = [title, location, address, contact_number, reference_url]
            if reference_url:
                profile_url = '%sprofile.html' % reference_url
                yield Request(profile_url, callback=self.parse_next, meta={'values': values})

        if page < 10:
            page = page + 1
            url = self.url % page
            yield Request(url, callback=self.parse, meta={'page': page})

    def parse_next(self, response):
        sel = Selector(response)
        values = response.meta['values']
        bussiness_type = ', '.join(sel.xpath('//tbody/tr/td[contains(text(), "Nature of Business")]/following-sibling::td/text()').extract())
        additional_business_types = ', '.join(sel.xpath('//tbody/tr/td[contains(text(), "Additional Business")]/following-sibling::td//text()').extract())

        values.extend((bussiness_type, additional_business_types))
        self.values.append(values)
