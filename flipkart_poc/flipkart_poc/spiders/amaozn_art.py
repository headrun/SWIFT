import scrapy
import pandas as pd
import csv
from scrapy.selector import Selector

class Amazon(scrapy.Spider):
    name = "art"
    
    def __init__(self, *args, **kwargs):
        super(Amazon, self).__init__(*args, **kwargs)
        oupf = open('amazon_art.csv', 'w')
        self.csv_file = csv.writer(oupf)
        self.csv_file.writerow(['title', 'price', 'sku', 'mrp', 'ratings' ,'image_list', 'product_features'])


    def start_requests(self):

        self.headers = {
            'authority': 'www.amazon.in',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'rtt': '100',
            'downlink': '1.8',
            'ect': '4g',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

        params = (
            ('srs', '15096034031'),
        )

        urls = ['https://www.amazon.in/s?srs=15096034031',
                'https://www.amazon.in/s?srs=9836898031'
                ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


    def parse(self, response):
        sel = Selector(response)
        urls = sel.xpath('//a[@class="a-link-normal a-text-normal"]/@href').extract()
        for url in urls:
            yield scrapy.Request('https://www.amazon.in%s'%url, callback=self.product_details, headers=self.headers, dont_filter=True)
        next_page = ''.join(sel.xpath('//li[@class="a-last"]/a/@href').extract())
        print('Next PAGE:', next_page)
        yield scrapy.Request('https://www.amazon.in%s'%next_page, headers=self.headers, callback=self.parse)

    def product_details(self, response):
        sel = Selector(response)
        title = ''.join(sel.xpath('//span[@id="productTitle"]//text()').extract()).strip()
        ASIN  = ''.join(sel.xpath('//div[@class="pdTab"]//tbody/tr//td[contains(text(), "ASIN")]//following-sibling::td[@class="value"]//text()').extract())
        mrp = ''.join(sel.xpath('//td[@class="a-span12 a-color-secondary a-size-base"]//span[@class="priceBlockStrikePriceString a-text-strike"]//text()').extract()).replace('\xa0', '').strip()
        price = ''.join(sel.xpath('//span[@id="priceblock_ourprice"]//text()').extract()).replace('\xa0', '').strip()
        ratings = ''.join(sel.xpath('//div[@id="averageCustomerReviews"]//a/i[@class="a-icon a-icon-star a-star-4"]/span/text()').extract())
        image_list = '<>'.join(sel.xpath('//div[@id="altImages"]//li//a[@class="a-button-text"]/img/@src').extract())
        if not image_list:
            image_list  = '<>'.join(sel.xpath('//div[@id="altImages"]//li//span[@class="a-button-text"]/img/@src').extract())
        product_features = '<>'.join(sel.xpath('//div[@id="feature-bullets"]//ul//li//text()').extract()).replace('\n', '').replace('\t', '')
        if title:
            self.csv_file.writerow([repr(title), repr(ASIN), repr(mrp), repr(price), repr(ratings), repr(image_list), repr(product_features)])
            