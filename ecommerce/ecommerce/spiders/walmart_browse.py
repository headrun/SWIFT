import re
import json
from urllib.parse import urljoin
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem

class WalmartSpider(EcommSpider):
    name = 'walmart_browse'
    domain_url = "https://www.walmart.com"
    handle_httpstatus_list = [400] 

    def __init__(self, *args, **kwargs):
        super(WalmartSpider, self).__init__(*args, **kwargs)
        self.category_array = ['5438_133197_4237948_3187021']
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
        for category in self.category_array:
            for page_len in range(1,21):
                url = 'https://www.walmart.com/search/api/preso?prg=desktop&page=%s&ps=48&cat_id=%s'%(page_len,category)
                # meta = {'range': 0, 'page': 1, 'category': category}
                # print(url)
                yield Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        # page_range = response.meta['range']
        # page = response.meta['page']
        # request_category = response.meta['category']
        source = self.name.split('_')[0]
        _data = json.loads(response.text)
        # _data = json.loads(response.body.decode('utf-8'))
        products = _data.get('items', [])
        print(len(products))
        for product in products:
            product_id = product.get('productId', '')
            name = product.get('title', '')
            rating = product.get('customerRating', '')
            reviews_count = product.get('numReviews', '')
            brand = product.get('brand','')
            if brand: brand = brand[0]
            category = product.get('seeAllName', '')
            image_url = product.get('imageUrl', '')
            description = product.get('description', '')
            description = re.compile(r'<[^>]+>').sub('', description)
            price_details = product.get('primaryOffer','')
            min_price = price_details.get('minPrice','')
            max_price = price_details.get('maxPrice','')
            price=''
            if 'offerPrice' in price_details:
                price = price_details.get('offerPrice','')
            # currency_code = product.get('fulfillment', '')
            # currency_code = currency_code.get(thresholdCurrencyCode','')
            # reference_url= urljoin(self.domain_url, product.get('productPageUrl', '')).strip(',')
            aux_info = {'product_id': product_id, 'json_page': response.url}
            availability = product.get('quantity', '')
            sku,size,sub_category,rating_count,discount_percentage.reference_url = '','','','','',''
            hd_id = encode_md5('%s%s%s' % (source, str(sku), size))
            insight_item = InsightItem()
            insight_item.update({
                'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'category':category,
                'sub_category': sub_category, 'brand': brand, 'ratings_count': rating_count,
                'reviews_count': reviews_count, 'mrp': min_price, 'selling_price': max_price,
                'discount_percentage': discount_percentage,'is_available': ''
            })
            yield insight_item

            meta_item = MetaItem()
            meta_item.update({
                'hd_id': hd_id, 'source': source, 'sku': sku, 'web_id': product_id, 'size': size, 'title': name,
                'category':category,'sub_category':sub_category,'brand':brand,'rating':rating,'ratings_count':rating_count,
                'reviews_count': reviews_count,'mrp':min_price,'selling_price':max_price,'discount_percentage':discount_percentage,
                'is_available':'','descripion': '', 'specs': '', 'image_url': image_url, 
                'reference_url': reference_url, 'aux_info': json.dumps(aux_info),
                'rating':rating
            })
            yield meta_item
            print(product_id,name,rating,rating_count,brand,sub_category,image_url,min_price,max_price,price,reference_url)

