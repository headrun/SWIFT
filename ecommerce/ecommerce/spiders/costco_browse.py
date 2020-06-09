import json
from ecommerce.common_utils import *
from scrapy.selector import Selector
from ecommerce.items import InsightItem, MetaItem

class CostcoSpider(EcommSpider):
    name = 'costco_browse'

    def __init__(self, *args, **kwargs):
        super(CostcoSpider, self).__init__(*args, **kwargs)
        self.category_array = ['mens-outerwear.html', 'mens-activewear.html', 'pants.html', 'mens-shirts.html', 'mens-sweaters.html', 'mens-suits.html', 'mens-sleepwear.html', 'mens-shorts.html', 'mens-swimwear.html']
        self.headers = {
            'authority': 'www.costco.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
        }
    def start_requests(self):
        for category in self.category_array:
            url = 'https://www.costco.com/%s'%category
            yield Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        sel = Selector(response)
        urls = extract_list_data(sel, '//p[@class="description"]//a//@href')
        reviews = extract_list_data(sel, '//meta[@itemprop="reviewCount"]/@content')
        ratings = extract_list_data(sel, '//meta[@itemprop="ratingValue"]//@content')
        availabilities = extract_list_data(sel, '//input[@id="in_Stock"]//@value')
        for url, review, rating, availability in zip(urls, reviews, ratings, availabilities):
            meta = {'category':response.url, 'review':review, 'rating':rating, 'availability':availability}
            yield Request(url, callback=self.parse_data, headers=self.headers, meta=meta)

    def parse_data(self, response):
        source = 'costco'
        sel = Selector(response)
        reviews = response.meta['review']
        rating = response.meta['rating']
        category = response.meta['category']
        availability = 0
        if response.meta['availability']:
            availability = 1
        item = extract_data(sel, '//p[@class="member-only"]//text()')
        brandname = extract_data(sel, '//div[@itemprop="brand"]//text()')
        product_name = extract_data(sel, '//h1[@itemprop="name"]//text()')
        description = extract(sel, '//ul[@class="pdp-features"]//li//text()')
        currency_ = extract_data(sel, '//meta[@property="product:price:currency"]//@content')
        mrp = extract_data(sel, '//meta[@property="product:price:amount"]//@content')
        try:
            category_ = category.split('/')[-1].split('-')[0].replace('s','')
            sub_category = category.split('/')[-1].split('-')[1].replace('.html', '')
        except:
            category_ = 'men'
            sub_category = category.split('/')[-1].split('-')[0].replace('.html', '')
        product_id = response.url.split('.')[-2]
        aux_info = {'product_id': product_id, 'json_page': response.url}
        image_url = extract_data(sel, '//img[@class="img-responsive"]//@src')
        sizes = extract_list_data(sel, '//select[@class="varis form-control"]//option[contains(@data-attr-name, "Size")]//text()')
        skus = extract_list_data(sel, '//select[@class="varis form-control"]//option[contains(@data-attr-name, "Size")]//@value')
        for size, sku in zip(sizes, skus):
            size = size
            sku_id = sku
            hd_id = encode_md5('%s%s%s' % (source, str(sku_id), size))
            insight_item = InsightItem()
            insight_item.update({'hd_id': hd_id, 'source': source, 'sku': sku_id, 'size': size, 'category':category_, 'sub_category': sub_category, 'brand': brandname, 'ratings_count':0, 'reviews_count': reviews, 'mrp':mrp, 'currency':currency_, 'selling_price':0, 'discount_percentage':0, 'is_available': availability})
            yield insight_item
            meta_item = MetaItem()
            meta_item.update({'hd_id': hd_id, 'source': source, 'sku': sku_id, 'web_id':product_id, 'size': size, 'title': product_name, 'category':category_, 'sub_category':sub_category, 'brand':brandname, 'rating':rating, 'ratings_count':0, 'reviews_count':reviews, 'mrp':mrp, 'currency':currency_, 'selling_price':0, 'discount_percentage':0, 'is_available': availability, 'descripion': description, 'specs':'', 'image_url':image_url, 'reference_url': response.url, 'aux_info': json.dumps(aux_info)})            
            yield meta_item
