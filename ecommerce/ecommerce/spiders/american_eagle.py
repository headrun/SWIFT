""" Spider to crawl american eagle site data. """
import json
from urllib.parse import urlencode
from scrapy.http import FormRequest
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem


class AmericanSpider(EcommSpider):
    name = "american_eagle"
    handle_httpstatus_list = [400]

    def __init__(self, *args, **kwargs):
        super(AmericanSpider, self).__init__(*args, **kwargs)
        self.category_array = ['/c/men/tops/cat10025', '/c/men/bottoms/cat10027', '/c/men/tops/jackets/cat380145', '/c/women/tops/cat10049', '/c/women/bottoms/jeans/cat6430042', '/c/women/bottoms/joggers-sweatpants/cat7010091', '/c/women/bottoms/pants/cat90034', '/c/women/bottoms/skirts/cat5920105', '/c/women/bottoms/soft-pants/cat6710002', '/c/women/bottoms/leggings/cat200043']
        self.headers = {'authority': 'www.ae.com',
                        'aesite': '',
                        'authorization': 'Basic MTA5ODU2NzQzMjplZDc4OWFjNzM0YWExMjBm',
                        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'accept': 'application/vnd.oracle.resource+json',
                        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
                        'aelang': '',
                        'origin': 'https://www.ae.com',
                        'sec-fetch-site': 'same-origin',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-dest': 'empty',
                        'referer': 'https://www.ae.com/us/en/c/men/tops/cat10025?pagetype=plp',
                        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'}

    def start_requests(self):
        # getting the token to include in api url
        data = {'grant_type': 'client_credentials'}
        url = 'https://www.ae.com/ugp-api/auth/oauth/v2/token'
        yield FormRequest(url, headers=self.headers, formdata=data, callback=self.parse)

    def parse(self, response):
        # Adding the token value to main api
        data = response.text
        data = json.loads(data)
        token = data.get('access_token', '')
        headers = {
            'authority': 'www.ae.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'requestedurl': 'plp',
            'aesite': 'AEO_US',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'content-type': 'application/json',
            'accept': 'application/vnd.oracle.resource+json',
            'x-requested-with': 'XMLHttpRequest',
            'x-access-token': token,
            'aelang': 'en_US',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': 'https://www.ae.com/us/en',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'}
        params = (
            ('No', ''),
            ('Nrpp', ''),
            ('Ns', ''),
            ('isFiltered', 'false'),
            ('results', ''),
            ('showHidden', ''),
                )
        paramsb = params
        for category in self.category_array:
            cat_id = category.split('/')[-1]
            url = "https://www.ae.com/ugp-api/catalog/v1/category/%s?" % cat_id
            yield Request(url+urlencode(params), headers=headers, meta={'paramsb': paramsb, 'headers': headers}, callback=self.parse_data)

    def parse_data(self, response):
        # getting the url's of each category
        headers = response.meta.get('headers', '')
        params = response.meta.get('paramsb', '')
        site_url = 'https://www.ae.com/us/en'
        data_ = response.text
        data__ = json.loads(data_)
        prod_data = data__.get('data', {}).get('contents', '')
        for pro_data in prod_data:
            pro_cont = pro_data.get('productContent', '')
            for pro_con in pro_cont:
                pro_urls = pro_con.get('productGroup', '')
                for cat_url in pro_urls:
                    prod_url = cat_url.get('url', '').replace('\n', '').strip()
                    category_url = site_url + prod_url
                    yield Request(category_url+'?'+urlencode(params), headers=headers, callback=self.parse_category, meta={'headers': headers})

    def parse_category(self, response):
        # getting the url of each product
        headers = response.meta.get('headers')
        urls = response.xpath('//a[@class="xm-link-to qa-xm-link-to tile-link"]//@href').extract()
        for url in urls:
            url = 'https://www.ae.com/' + url
            yield Request(url, callback=self.parse_product, headers=headers, meta={'headers': headers})

    def parse_product(self, response):
        # Extracting the product details here
        headers = response.meta.get('headers', '')
        try:
            image = json.loads(''.join(response.xpath('//script[@class="qa-pdp-schema-org"][contains(@type,"application/ld+json")]//text()').extract()).replace('\n','').strip())
            image = image.get('image', '')
            image_url = "https:" + image
        except: image_url = ''
        product_name = ''.join(response.xpath('//h1[@class="product-name"]//text()').extract()).replace('\n', '').strip()
        sell_price = ''.join(response.xpath('//div[@class="product-sale-price ember-view"]//text()').extract()).replace('$', '').strip()
        mrp = ''.join(response.xpath('//div[@class="product-list-price product-list-price-on-sale ember-view"]//text()').extract()).replace('$', '').strip()
        discount = ''.join(response.xpath('//div[@class="product-you-save"]//text()').extract()).replace('SAVE', '').strip()
        gender = response.xpath('//span[@itemprop="name"]/text()').extract()[0]
        sub_category = response.xpath('//span[@itemprop="name"]/text()').extract()[2]
        brand = product_name.split(' ')[0]
        description = ''.join(response.xpath('//div[@class="equity-group-intro-txt equity-group-intro-equit"]//text()').extract()).replace('\n', '').strip()
        reference_url = response.url
        product_id = response.url.split('?')[0].split('/')[-1]
        meta_details = {'title': product_name, 'category': gender, 'sub_category': sub_category, 'brand': brand, 'image_url': image_url,
                'description': description, 'reference_url': reference_url, 'discount': discount, 'sell_price': sell_price, 'mrp': mrp}
        url = 'https://www.ae.com/ugp-api/catalog/v1/product/sizes?productIds=%s' % product_id
        yield Request(url, headers=headers, meta=meta_details, callback=self.parse_size)

    def parse_size(self, response):
        # Extracting the product size details here
        source = "American Eagle"
        currency_type = 'USD'
        title_name = response.meta.get('title', '').replace('\n', '').strip()
        category_name = response.meta.get('category', '')
        sub_category_name = response.meta.get('sub_category', '')
        brand_name = response.meta.get('brand', '')
        prod_description = response.meta.get('description', '').replace('\n', '').strip()
        reference_url = response.meta.get('reference_url', '').replace('\n', '').strip()
        discount = response.meta.get('discount', '')
        image_url = response.meta.get('image_url', '')
        mrp = response.meta.get('mrp', '')
        sell_price = response.meta.get('sell_price', '')
        if mrp == '0' or mrp == '':
            return
        else:
            size_data = response.text
            size_data = json.loads(size_data)
            sku_size_data = size_data.get('data', {}).get('records', '')
            for sku_size in sku_size_data:
                sku_size = sku_size.get('sizes', {}).get('skus')
                for size_data in sku_size:
                    size = size_data.get('size', '')
                    sku_id = size_data.get('skuId', '')
                    is_avail = size_data.get('onlineOnly', '')
                    if is_avail == True:
                        is_avail = 1
                    elif is_avail == False:
                        is_avail = 0
                    hd_id_ = encode_md5('%s%s%s' % (source, str(sku_id), size))
                    aux_info = {'product_id': sku_id, 'json_page': reference_url}
           
                    insight_item = InsightItem()
                    insight_item.update({
                        'hd_id': hd_id_, 'source': source, 'sku': sku_id, 'size': size,
                        'category': category_name, 'sub_category': sub_category_name, 'brand': brand_name,
                        'ratings_count': '', 'reviews_count': '', 'mrp': mrp,
                        'selling_price': sell_price, 'currency': currency_type,
                        'discount_percentage': discount, 'is_available': is_avail
                                    })
                    yield insight_item
                    meta_item = MetaItem()
                    meta_item.update({
                        'hd_id': hd_id_, 'source': source, 'sku': sku_id, 'web_id': sku_id, 'size': size,
                        'title': title_name, 'category':category_name, 'sub_category': sub_category_name,
                        'brand': brand_name, 'rating': '', 'ratings_count': '', 'reviews_count': '',
                        'mrp': mrp, 'selling_price': sell_price, 'currency': currency_type,
                        'discount_percentage': discount, 'is_available': is_avail,
                        'descripion': prod_description, 'specs': '', 'image_url': image_url,
                        'reference_url': reference_url, 'aux_info': json.dumps(aux_info)
                                })
                    yield meta_item
