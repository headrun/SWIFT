import json
from re import findall
from urllib.parse import urljoin
from scrapy.http import Request
from ecommerce.items import InsightItem, MetaItem
from ecommerce.common_utils import EcommSpider, normalize,\
    encode_md5


class MyntraSpider(EcommSpider):
    name = "myntra_browse"
    domain_url = "https://www.myntra.com"

    def __init__(self, *args, **kwargs):
        super(MyntraSpider, self).__init__(*args, **kwargs)
        self.category_array = ['men-tshirts', 'men-casual-shirts', 'men-formal-shirts', 'men-sweatshirts', 'men-sweaters', 'men-jackets', 'men-blazers', 'men-suits', 'rain-jacket', 'men-kurtas', 'sherwani', 'nehru-jackets', 'dhoti', 'men-jeans', 'men-casual-trousers', 'men-formal-trousers', 'mens-shorts', 'men-trackpants', 'men-briefs-and-trunks', 'men-boxers', 'men-innerwear-vests', 'men-nightwear', 'men-thermals', 'women-kurtas-kurtis-suits', 'ethnic-tops', 'ethnic-wear-dresses-menu', 'skirts-palazzos', 'saree', 'dress-material', 'lehenga-choli', 'dupatta-shawl', 'women-ethnic-wear-jackets', 'women-ethnic-bottomwear', 'western-wear-dresses-menu', 'women-shirts-tops-tees', 'women-jeans-jeggings', 'women-trousers', 'women-shorts-skirts', 'women-shrugs', 'women-sweaters-sweatshirts', 'women-jackets-coats', 'women-blazers-waistcoats', 'boys-tshirts', 'boys-shirts', 'kids?f=Categories%3ALounge%20Shorts%2CShorts%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3ATrousers%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3AClothing%20Set%2CDungarees%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3ABlazers%2CKurta%20Sets%2CKurtas%2CNehru%20Jackets%2CSherwani%2CWaistcoat%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3ALounge%20Pants%2CPyjamas%2CTrack%20Pants%2CTracksuits%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3ALounge%20Pants%2CPyjamas%2CTrack%20Pants%2CTracksuits%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3ACoats%2CJackets%2CSweaters%2CSweatshirts%2CThermal%20Bottoms%2CThermal%20Set%2CThermal%20Tops%3A%3AGender%3Aboys%2Cboys%20girls', 'kids?f=Categories%3ACoats%2CJackets%2CSweaters%2CSweatshirts%2CThermal%20Bottoms%2CThermal%20Set%2CThermal%20Tops%3A%3AGender%3Aboys%2Cboys%20girls',
                               'kids?f=Categories%3ABoxers%2CBriefs%2CInnerwear%20Vests%2CNight%20suits%2CSleepsuit%3A%3AGender%3Aboys%2Cboys%20girls', 'boys-jeans-trousers', 'girls-dresses', 'girls-tops', 'girls-clothing-set', 'kids?f=Categories%3ALounge%20Tshirts%2CTshirts%3A%3AGender%3Aboys%20girls%2Cgirls', 'drpgirl?f=Categories%3AKurta%20Sets%2CKurtas%2CKurtis%2CLehenga%20Choli%2CTunics', 'kids?f=Categories%3ADungarees%2CJumpsuit%3A%3AGender%3Aboys%20girls%2Cgirls', 'kids?f=Categories%3ALounge%20Shorts%2CShorts%2CSkirts%3A%3AGender%3Aboys%20girls%2Cgirls', 'girls-leggings', 'girls-jeans-trousers-capris', 'kids?f=Categories%3ACoats%2CJackets%2CSweaters%2CSweatshirts%2CThermal%20Bottoms%2CThermal%20Set%2CThermal%20Tops%3A%3AGender%3Aboys%20girls%2Cgirls', 'kids?f=Categories%3ABoxers%2CBra%2CBriefs%2CInnerwear%20Vests%2CLounge%20Pants%2CLounge%20Shorts%2CLounge%20Tshirts%2CNight%20suits%2CNightdress%2CSleepsuit%3A%3AGender%3Aboys%20girls%2Cgirls', 'kids?f=Categories%3ABodysuit%2CRompers%2CSleepsuit', 'kids?f=Age%3A0M-6M%2C0m-6m%2C1Y-2Y%2C1y-2y%2C6M-1Y%2C6m-1y%3A%3ACategories%3AClothing%20Set', 'kids?f=Categories%3ALounge%20Tshirts%2CShirts%2CTops%2CTshirts%3A%3AAge%3A0m-6m%2C1y-2y%2C6m-1y', 'girls-dresses', 'kids?f=Categories%3ACapris%2CJeans%2CJeggings%2CLeggings%2CLounge%20Pants%2CLounge%20Shorts%2CPyjamas%2CShorts%2CSkirts%2CTrack%20Pants%2CTrousers%3A%3AAge%3A0m-6m%2C1y-2y%2C6m-1y', 'kids?f=Categories%3ACoats%2CJackets%2CSweaters%2CSweatshirts%2CThermal%20Bottoms%2CThermal%20Set%2CThermal%20Tops%3A%3AAge%3A0m-6m%2C1y-2y%2C6m-1y', 'kids?f=Categories%3ABoxers%2CBriefs%2CInnerwear%20Vests%2CLounge%20Pants%2CLounge%20Shorts%2CLounge%20Tshirts%2CNight%20suits%2CNightdress%2CSleepsuit%3A%3AAge%3A0m-6m%2C1y-2y%2C6m-1y']
        self.headers = {
            'authority': 'www.myntra.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
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
            product_id = normalize(product.get('productId', ''))
            name = normalize(product.get('productName', ''))
            rating = normalize(product.get('rating', ''))
            rating_count = normalize(product.get('ratingCount', ''))
            brand = normalize(product.get('brand', ''))
            mrp = normalize(product.get('mrp', ''))
            price = normalize(product.get('price', ''))
            sub_category = normalize(product.get('category', ''))
            category = normalize(product.get('gender', ''))
            image_url = normalize(product.get('searchImage', ''))
            reference_url = normalize(urljoin(self.domain_url, product.get('landingPageUrl', '')))
            discount_percentage = ''.join(findall(r'\(([^\)]+)\)', product.get('discountDisplayLabel', '')))
            aux_info = {'product_id': product_id, 'json_page': response.url}
            availabilities_info = product.get('inventoryInfo', [])
            for availability_info in availabilities_info:
                sku = normalize(availability_info.get('skuId', ''))
                size = normalize(availability_info.get('label', ''))
                availability = 0
                if availability_info.get('available', False):
                    availability = 1
                hd_id = encode_md5('%s%s%s' % (source, str(sku), size))
                discount_percentage = normalize(discount_percentage.lower().replace('off', '').replace('%', '').strip())

                insight_item = InsightItem()
                insight_item.update({
                    'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'category': category,
                    'sub_category': sub_category, 'brand': brand, 'ratings_count': rating_count,
                    'reviews_count': '', 'mrp': mrp, 'selling_price': price, 'currency': 'INR',
                    'discount_percentage': discount_percentage, 'is_available': availability
                })
                yield insight_item

                meta_item = MetaItem()
                meta_item.update({
                    'hd_id': hd_id, 'source': source, 'sku': sku, 'web_id': product_id, 'size': size,
                    'title': name, 'category': category, 'sub_category': sub_category, 'brand': brand,
                    'rating': rating, 'ratings_count': rating_count, 'reviews_count': '', 'mrp': mrp,
                    'selling_price': price, 'currency': 'INR', 'discount_percentage': discount_percentage,
                    'is_available': availability, 'descripion': '', 'specs': '', 'image_url': image_url,
                    'reference_url': reference_url, 'aux_info': json.dumps(aux_info)
                })
                yield meta_item

        if products:
            page += 1
            page_range = (page-1) * 100 - 1
            url = 'https://www.myntra.com/web/v2/search/%s?p=%s&rows=100&o=%s&sort=popularity' % (
                request_category, page, page_range)
            meta = {'range': page_range, 'page': page,
                    'category': request_category}
            yield Request(url, callback=self.parse, headers=self.headers, meta=meta)
