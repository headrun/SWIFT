import re
import json
from urllib.parse import urljoin
from ecommerce.common_utils import *
from scrapy.selector import Selector
from ecommerce.items import InsightItem, MetaItem

class NextSpider(EcommSpider): 
    name = 'next_browse'
    domain_url = "https://www.next.us/en/shop"
    handle_httpstatus_list = [404] 

    def __init__(self, *args, **kwargs):
        super(NextSpider, self).__init__(*args, **kwargs)
        self.category_array = ['gender-men/feat-newin', 'gender-men-productaffiliation-tops']
        self.headers = {
            'authority': 'www.next.us',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': '*/*',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'}

    def start_requests(self):
        for category in self.category_array:    
            url = 'https://www.next.us/en/shop/%s-minprice-1000-maxprice-58000-srt-0'%category
            meta = {'category':category, 'page':0}
            yield Request(url, callback=self.parse, headers=self.headers, meta=meta, method='GET') 

    def parse(self, response):
        sel = Selector(response)
        page = response.meta['page']
        category = response.meta['category']
        price_ = extract_data(sel, '//div[@class="Options Price"]//script[contains(@type, "text/javascript")]/text()').replace('\r\n', '')
        min_price = ''.join(re.findall('PriceFilterMin = (.*)  PriceFilterMax', price_)).replace(';', '').strip()
        max_price = ''.join(re.findall('PriceFilterMax = (.*)  PriceFilterFrom', price_)).replace(';', '').strip() 
        urls = extract_list_data(sel, '//h2[@class="Title"]//a//@href')
        for url in urls:
            url = 'https:'+url
            meta = {'category':category, 'page':page, 'url':response.url}
            yield Request(url, callback=self.parse_next, headers=self.headers, meta=meta)
        if urls:
            page = page+24
            url = 'https://www.next.us/en/shop/%s-minprice-%s-maxprice-%s-srt-%s'%(category, min_price, max_price, page)
            meta = {'page':page, 'category':category, 'min_price':min_price, 'max_price':max_price}
            yield Request(url, callback=self.parse, meta=meta, headers=self.headers) 
            
    def parse_next(self, response):
        source = 'next'
        sel = Selector(response)
        category = response.meta['category']
        rating = extract_data(sel, '//div[@class="reviewsContainer Content "]//@data-overallrating')
        reviews = extract_data(sel, '//div[@class="reviewsContainer Content "]//@data-reviewscount')
        json_data = json.loads(extract_data(sel, '//script[@type="application/ld+json"]//text()'))
        skuid = json_data.get('sku', '')
        currency = json_data.get('offers', {}).get('priceCurrency', '')
        product_category = category.split('-')[1]
        sub_category = category.split('-')[-1]
        product_name = extract_data(sel, '//div[contains(@class,"Title")]/h1/text()')
        text = extract_data(sel, '//script[contains(text(), "var shotData")]/text()').replace(';', '')
        data_ = ''.join(re.findall('var shotData = (.*)', text)).strip()
        data = json.loads(data_)
        products = data.get('Styles', [])[0]
        fits = products.get('Fits', [])
        for fit in fits:
            brandname = fit.get('Name', '')
            items = fit.get('Items', [])
            for item in items:
                name = item.get('SearchDescription', '')
                imageurl = item.get('ShotImage', '')
                description = item.get('Composition', '')
                product_id = item.get('ItemNumber', '')
                aux_info = {'product_id': product_id, 'json_page': response.url}
                sizes = item.get('Options', '')
                for size in sizes:
                    number = size.get('Number', '')
                    size_ = size.get('Name', '')
                    availability = 0
                    if size.get('StockStatus', ''):
                        availability = 1
                    price = ''.join(size.get('Price', '')).replace('$', '')
                    insight_item = InsightItem()
                    hd_id = encode_md5('%s%s%s' % (source, str(skuid), size))
                    insight_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': skuid, 'size': size_,
                        'category':product_category, 'sub_category': sub_category, 'brand': brandname,
                        'ratings_count': rating, 'reviews_count': reviews, 'mrp': price,
                        'selling_price': price, 'currency': currency,
                        'discount_percentage': 0, 'is_available': availability
                    })
                    yield insight_item
                    meta_item = MetaItem()
                    meta_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': skuid, 'web_id': product_id, 'size': size_,
                        'title': product_name, 'category':product_category, 'sub_category': sub_category,
                        'brand': brandname, 'rating':rating, 'ratings_count': 0, 'reviews_count': reviews,
                        'mrp':price, 'selling_price': price, 'currency': currency,
                        'discount_percentage':0, 'is_available': availability,
                        'descripion': name, 'specs': description, 'image_url': imageurl,
                        'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                    })
                    yield meta_item
