import json
from re import findall
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem

class MyntraSpider(EcommSpider):
    name = "myntra_browse"

    def __init__(self, *args, **kwargs):
        super(MyntraSpider, self).__init__(*args, **kwargs)
        self.category_array = ['men-tshirts', 'men-casual-shirts', 'men-formal-shirts', 'men-sweatshirts', 'men-sweaters', 'men-jackets', 'men-blazers', 'men-suits', 'rain-jacket']
        self.headers = headers = {
            'authority': 'www.myntra.com',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

    def start_requests(self):
        for category in self.category_array:
            url = 'https://www.myntra.com/web/v2/search/%s?p=1&rows=100&o=0&sort=popularity' % category
            meta = {'range': 0, 'page': 1, 'category': category}
            yield Request(url, headers=self.headers, callback=self.parse, meta=meta)

    def parse(self, response):
        page_range = response.meta['range']
        page = response.meta['page']
        request_category = response.meta['category']
        source = self.name.split('_')[0]
        _data = json.loads(response.body.decode('utf-8'))
        products = _data.get('products', [])
        for product in products:
            product_id = product.get('productId', '')
            name = product.get('productName', '')
            rating = product.get('rating', '')
            rating_count = product.get('ratingCount', '')
            brand = product.get('brand', '')
            mrp = product.get('mrp', '')
            price = product.get('price', '')
            sub_category = product.get('category', '')
            category = product.get('gender', '')
            image_url = product.get('searchImage', '')
            reference_url = product.get('landingPageUrl', '')
            discount_percentage = ''.join(findall('\(([^\)]+)\)', product.get('discountDisplayLabel', '')))
            aux_info = {'product_id': product_id, 'json_page': response.url}

            availabilities_info = product.get('inventoryInfo', [])
            for availability_info in availabilities_info:
                sku = availability_info.get('skuId', '')
                size = availability_info.get('label', '')
                availability = 1 if availability_info.get('available', '') else 0
                hd_id = encode_md5('%s%s%s' % (source, str(sku), size))
                discount_percentage = discount_percentage.lower().replace('off', '').replace('%', '').strip()

                insight_item = InsightItem()
                insight_item.update({
                    'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'category':category,
                    'sub_category': sub_category, 'brand': brand, 'ratings_count': rating_count,
                    'reviews_count': '', 'mrp': mrp, 'selling_price': price,
                    'discount_percentage': discount_percentage,'is_available': availability
                })
                yield insight_item

                meta_item = MetaItem()
                meta_item.update({
                    'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'title': name,
                    'descripion': '', 'specs': '', 'image_url': image_url, 
                    'reference_url': reference_url, 'aux_info': json.dumps(aux_info)
                })
                yield meta_item

        if products:
            page += 1
            page_range = (page-1) * 100 - 1
            url = 'https://www.myntra.com/web/v2/search/%s?p=%s&rows=100&o=%s&sort=popularity' % (request_category, page, page_range)
            meta = {'range': page_range, 'page': page,'category': request_category}
            yield Request(url, callback=self.parse, headers=self.headers, meta=meta)
