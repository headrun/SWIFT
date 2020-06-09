import re
import json
import requests
from urllib.parse import urljoin
from ecommerce.common_utils import *
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from ecommerce.items import InsightItem, MetaItem

class WalmartSpider(EcommSpider):
    name = 'walmart_browse'
    domain_url = "https://www.walmart.com"
    handle_httpstatus_list = [400] 

    def __init__(self, *args, **kwargs):
        super(WalmartSpider, self).__init__(*args, **kwargs)
        self.category_array = ['5438_133197_4237948_3187021', '5438_133197_4237948_1085040', '5438_133197_4237948_7617810', '5438_133197_4237948_5178426', '5438_133197_1224676_2301886', '5438_133197_1224676_5337017', '5438_133197_1224676_1224684', '5438_133197_1224676_2906643']
        self.headers = headers = {
           'authority': 'www.walmart.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'origin': 'https://www.walmart.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            }

    def start_requests(self):
        for category_id in self.category_array:
            page_len = 1
            meta = {'category_id': category_id, 'page_len': page_len}
            url = 'https://www.walmart.com/search/api/preso?prg=desktop&page=%s&ps=48&cat_id=%s'%(page_len, category_id)
            yield Request(url, callback=self.parse, headers=self.headers, meta=meta)

    def parse(self, response):
        source = self.name.split('_')[0]
        _data = json.loads(response.text)
        category_id = response.meta['category_id']
        page_len = response.meta['page_len']
        page_meta = _data.get('pageMetadata', '')
        if page_meta:
            canonical_next = page_meta.get('canonicalNext', '')
        else:
            canonical_next = ''
        products = _data.get('items', [])
        for product in products:
            product_id = product.get('productId', '')
            name = product.get('title', '')
            rating = product.get('customerRating', '')
            rating_count = product.get('numReviews', '')
            image_url = product.get('imageUrl', '')
            sub_category = product.get('seeAllName', '')
            price_det = product.get('primaryOffer', '')
            if price_det: 
                price = price_det.get('minPrice', '')
                if not price: price = price_det.get('offerPrice', '')
            try: currency = product['fulfillment']['thresholdCurrencyCode']
            except: currency = 'USD'
            reference_url = urljoin(self.domain_url, product.get('productPageUrl', ''))
            meta = {'image_url': image_url, 'reference_url': reference_url, 'rating': rating, 
            'rating_count':rating_count, 'category_id': category_id, 'page_len':page_len, 
            'currency': currency, 'product_id': product_id,'name':name, 'sub_category':sub_category, 'price':price}
            yield Request(reference_url, callback=self.parse_data, headers=self.headers, meta=meta)
        if canonical_next:
            page_len += 1
            url = 'https://www.walmart.com/search/api/preso?prg=desktop&page=%s&ps=48&cat_id=%s'%(page_len, category_id)
            meta = {'page_len': page_len, 'category_id': category_id}
            yield Request(url, callback=self.parse, headers=self.headers, meta=meta)

    def parse_data(self, response):
            source = self.name.split('_')[0]
            category_id = response.meta['category_id']
            page_len = response.meta['page_len']
            reference_url = response.meta['reference_url']
            image_url = response.meta['image_url']
            rating = response.meta['rating']
            currency = response.meta['currency']
            rating_count = response.meta['rating_count']
            product_id = response.meta['product_id']
            name = response.meta['name']
            sub_category = response.meta['sub_category']
            price = response.meta['price']
            items = response.xpath("//script[contains(@id, 'item')]/text()")
            if isinstance(items, list) and len(items) > 0:
                items = response.xpath("//script[contains(@id, 'item')]/text()")[0]
                txt = items.extract()
                dict1 = json.loads(txt)
            if dict1:
                brand = dict1['item']['product']['midasContext']['brand']
                aux_info = {'product_id': product_id, 'json_page': response.url}
                description = dict1['item']['product']['buyBox']['products'][0]['idmlSections']['idmlShortDescription']
                main_category = dict1['item']['product']['buyBox']['products'][0]['idmlSections']['specifications']
                if main_category:
                    cat_dict = next(i for i in main_category if i['name']=='Gender')
                    category = cat_dict.get('value', '')
                    if category == 'Male': category = 'men'
                try: sku = dict1['item']['product']['buyBox']['products'][0]['skuId']
                except: sku = product_id
                sizes = extract(response, '//*[@id="variants-section"]/div[2]/div/div[2]/label/div/div/div/text()').split()
                availabilitystatus = extract(response, '//*[@id="variants-section"]/div[2]/div/div[2]/label/@availabilitystatus').split()
                size_details = list(zip(sizes, availabilitystatus))
                for size_detail in size_details:
                    size = size_detail[0]
                    availability = size_detail[1]
                    if availability == 'AVAILABLE': availability = 1
                    else: availability = 0
                    hd_id = encode_md5('%s%s%s' % (source, str(sku), size))
                    insight_item = InsightItem()
                    insight_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'category':category,
                        'sub_category': sub_category, 'brand': brand, 'ratings_count': rating_count,
                        'reviews_count': 0, 'mrp': price, 'currency':currency, 'selling_price': 0,
                        'discount_percentage': 0,'is_available': availability
                    })
                    yield insight_item
                    meta_item = MetaItem()
                    meta_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': sku, 'web_id': product_id, 'size': size, 'title': name,
                        'category':category, 'sub_category': sub_category, 'brand':brand, 'rating':rating,'ratings_count': rating_count,
                        'reviews_count': 0, 'mrp':price, 'currency':currency, 'selling_price': 0,'discount_percentage':0,
                        'is_available': availability, 'descripion': description, 'specs': '', 'image_url': image_url, 
                        'reference_url': reference_url, 'aux_info': json.dumps(aux_info)
                    })
                    yield meta_item
