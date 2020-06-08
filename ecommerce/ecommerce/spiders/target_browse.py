""" Spider to crawl target site data """
import json
from urllib.parse import urlencode
from scrapy.selector import Selector
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem


class Target(EcommSpider):
    name = "target_browse"
    def __init__(self, *args, **kwargs):
        super(Target, self).__init__(*args, **kwargs)
        self.headers = {
            'Accept': 'application/json',
            'Referer': 'https://www.target.com/s?sortBy=relevance&Nao=0&category=18y1l&searchTerm=mens+apparel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
        }

    def start_requests(self):
        params = (
            ('category', '18y1l'),
            ('channel', 'web'),
            ('count', '24'),
            ('default_purchasability_filter', 'true'),
            ('facet_recovery', 'false'),
            ('fulfillment_test_mode', 'grocery_opu_team_member_test'),
            ('isDLP', 'false'),
            ('keyword', 'mens apparel'),
            ('offset', '0'),
            ('pageId', '/s/mens apparel'),
            ('pricing_store_id', '1768'),
            ('sort_by', 'relevance'),
            ('include_sponsored_search_v2', 'true'),
            ('ppatok', 'AOxT33a'),
            ('platform', 'desktop'),
            ('useragent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36(KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36'),
            ('excludes', 'available_to_promise_qualitative,available_to_promise_location_qualitative'),
            ('key', 'eb2551e4accc14f38cc42d32fbc2b2ea'),
        )
        url = 'https://redsky.target.com/v2/plp/search/?' + urlencode(params)
        meta = {'headers':self.headers, 'params':params, 'offset':0}
        yield Request(url, headers=self.headers, meta=meta, callback=self.parse)

    def parse(self, response):
        headers = response.meta.get('headers', '')
        params = response.meta.get('params', '')
        offset = response.meta.get('offset', 0)
        tcins = []
        target_data = response.text
        target_data = json.loads(target_data)
        errormsg = target_data.get('error_response', '')
        if errormsg:
            return
        tcins_data = target_data['search_response']['items']['Item']
        for tcin in tcins_data:
            tcin_val = tcin['tcin']
            tcins += [tcin_val]
        tcins = str(tcins)
        tcins = tcins.strip('[').strip(']').replace("'", '')
        meta = {'headers': headers, 'params': params, 'offset': offset}
        url = "https://redsky.target.com/redsky_aggregations/v1/web/plp_client_v1?key=eb2551e4accc14f38cc42d32fbc2b2ea&tcins=%s&pricing_store_id=1010" % tcins
        yield Request(url, headers=headers, callback=self.parse_next, meta=meta)

    def parse_next(self, response):
        headers = response.meta.get('headers', '')
        params = response.meta.get('params', '')
        offset = response.meta.get('offset', 0)
        # Write extraction part here for tcins
        target_data = response.text
        target_data = json.loads(target_data)
        main_data = target_data['data']
        data = main_data['product_summaries']
        for details in data:
            # get product url, size and price values
            product_url = details['item']['enrichment']['buy_url']
            price = details['price']['formatted_current_price']
            size = details['item']['product_description']['title']
            size = size.split(' ')[-1]
            meta = {'price': price, 'size': size, 'product_url': product_url}
            yield Request(product_url, meta=meta, callback=self.parse_data)
        offset = offset + 24
        listparams = list(params)
        listparams[8] = ('offset', str(offset))
        params = tuple(listparams)
        meta = {'headers': headers, 'params': params, 'offset': offset}
        url = 'https://redsky.target.com/v2/plp/search/?' + urlencode(params)
        yield Request(url, headers=headers, meta=meta, callback=self.parse)

    def parse_data(self, response):
        # getting product details here
        sel = Selector(response)
        price_review_data = ''.join(response.xpath\
        ('//script[@type = "application/ld+json"]//text()').extract())
        price_data = json.loads(price_review_data)
        price_data = price_data.get('@graph', '')
        price_data = price_data[0]
        currency_type = price_data['offers']['priceCurrency']
        try:
            review_count = price_data['aggregateRating']['reviewCount']
        except:
            review_count = ''
        source = 'target'
        category = sel.xpath('//*[@id="specAndDescript"]/div[1]/div[1]/div[1]/div/text()').extract()
        category = category[0].strip()
        size = response.meta['size']
        price = response.meta['price']
        json_data = sel.xpath('//*[@id="viewport"]/div[5]/script//text()').extract()
        json_data = json_data[0]
        json_data = json.loads(json_data)
        prod_data = json_data['@graph']
        prod_data = prod_data[0]
        name = prod_data['name']
        brand = prod_data['brand']
        image_url = prod_data['image']
        sku = prod_data['sku']
        product_id = sku
        description = prod_data['description']
        description = description.replace('<br />', '')
        availability = prod_data['offers']['availability']
        reference_url = response.url
        aux_info = {'product_id': product_id, 'json_page': response.url}
        rating_data = prod_data['review']
        for rating in rating_data:
            rating = rating['reviewRating']['ratingValue']
        if not rating_data:
            rating = ''
        hd_id = encode_md5('%s%s%s' % (source, str(sku), size))
        mrp = price
        insight_item = InsightItem()
        insight_item.update({'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, \
                            'category':category, 'sub_category': '', 'brand': brand,\
                            'ratings_count': '', 'reviews_count': review_count, 'mrp': mrp,\
                            'selling_price': price, 'discount_percentage': '',\
                            'is_available': availability
                            })
        yield insight_item
        meta_item = MetaItem()
        meta_item.update({'hd_id': hd_id, 'source': source, 'sku': sku, \
                         'web_id': product_id, 'size': size, 'title': name,\
                         'category': category, 'sub_category': '', 'brand': brand, \
                         'ratings_count': '', 'reviews_count': review_count, 'mrp': mrp,\
                         'selling_price': price, 'discount_percentage': '',\
            		     'is_available': availability, 'descripion': description,\
                         'specs': '', 'image_url': image_url, 'reference_url': reference_url,\
                         'aux_info': json.dumps(aux_info), 'rating': rating
                         })
        yield meta_item
        print(meta_item)
