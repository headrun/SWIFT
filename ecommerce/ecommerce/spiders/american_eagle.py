""" Spider to crawl american eagle site data. """
import json
from scrapy.http import FormRequest
from urllib.parse import urlencode
from scrapy.selector import Selector
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem


class AmericanSpider(EcommSpider):
    name = "american_eagle"
    handle_httpstatus_list = [400]

    def __init__(self, *args, **kwargs):
        super(AmericanSpider, self).__init__(*args, **kwargs)
        self.category_array = ['/c/men/tops/cat10025', '/c/women/tops/cat10049']
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
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
            }

    def start_requests(self):
        #import pdb; pdb.set_trace()
        meta = {'headers': self.headers}
        data = {'grant_type': 'client_credentials'}
        url = 'https://www.ae.com/ugp-api/auth/oauth/v2/token'
        yield FormRequest(url, headers=self.headers, formdata=data, callback=self.parse)

    def parse(self, response):
        #import pdb; pdb.set_trace()
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
        # 'x-access-token': '84bc010d-13e7-4cfd-95a7-a4d5a3d1cf32',
        'x-access-token': token,
        'aelang': 'en_US',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'referer': 'https://www.ae.com/us/en',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                  }
        params = (
            ('No', ''),
            ('Nrpp', ''),
            ('Ns', ''),
            ('isFiltered', 'false'),
            ('results', ''),
            ('showHidden', ''),
                )
        paramsb = params
        yield Request('https://www.ae.com/ugp-api/catalog/v1/category/cat10025/?'+urlencode(params), headers=headers, meta={'paramsb':paramsb, 'headers':headers}, callback=self.parse_data)

    def parse_data(self, response):
        #import pdb; pdb.set_trace()
        headers = response.meta.get('headers','')
        params = response.meta.get('paramsb','')
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
                    yield Request(category_url+'?'+urlencode(params), headers=headers, callback=self.parse_category,meta={'headers':headers})
                    print(category_url)

    def parse_category(self, response):
        headers = response.meta.get('headers')
        urls = response.xpath('//a[@class="xm-link-to qa-xm-link-to tile-link"]//@href').extract()
        for url in urls:
            url = 'https://www.ae.com/'+url
            yield Request(url,callback=self.parse_product,headers=headers, meta={'headers':headers})

    def parse_product(self, response):
        #import pdb;pdb.set_trace()
        headers = response.meta.get('headers', '')
        source = "American Eagle"
        product_name = ''.join(response.xpath('//h1[@class="product-name"]//text()').extract()).replace('\n','').strip()
        sell_price = ''.join(response.xpath('//div[@class="product-sale-price ember-view"]//text()').extract()).replace('$','').strip()
        mrp = ''.join(response.xpath('//div[@class="product-list-price product-list-price-on-sale ember-view"]//text()').extract()).replace('$','').strip()
        currency_type = 'USD'
        discount = ''.join(response.xpath('//div[@class="product-you-save"]//text()').extract()).replace('SAVE','').strip()
        gender = response.xpath('//span[@itemprop="name"]/text()').extract()[0]
        sub_category = response.xpath('//span[@itemprop="name"]/text()').extract()[2]
        brand = product_name.split(' ')[0]
        description = ''.join(response.xpath('//div[@class="equity-group-intro-txt equity-group-intro-equit"]//text()').extract()).replace('\n', '').strip()
        reference_url = response.url
        data = {'source':source, 'title':product_name, 'selling_pice':sell_price, 'mrp':mrp, 'currency_type': currency_type, 'discount':discount, 'category':gender, 'sub_category':sub_category, 'brand':brand, 'description':description, 'reference_url':reference_url}
        print(data)
        product_id = response.url.split('?')[0].split('/')[-1]
        url = 'https://www.ae.com/ugp-api/catalog/v1/product/sizes?productIds=%s' % product_id
        yield Request(url, headers=headers, callback=self.parse_size)

    def parse_size(self, response):
        #import pdb; pdb.set_trace()
        size_data = response.text
