import re
import json
from urllib.parse import urljoin
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem

class AjioSpider(EcommSpider):
    name = 'ajio_siri'
    domain_url = "https://www.ajio.com"
    handle_httpstatus_list = [400] 
    def __init__(self, *args, **kwargs):
        super(AjioSpider, self).__init__(*args, **kwargs)
        self.headers = headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Referer': 'https://www.ajio.com/women-western-wear/c/830316',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

    def start_requests(self):
        url = 'https://www.ajio.com/shop/men'
        yield Request(url, callback=self.parse)

    def parse(self, response):
        nodes = response.xpath('//ul[@class="level-first false"]/li')
        for node in nodes:
            url = ''.join(node.xpath('./a/@href').extract())
            link = self.domain_url+url 
            yield Request(link, callback=self.parse_next, meta={"node":node, "handle_httpstatus_list":[400]})

    def parse_next(self, response):
        urls = response.xpath('//li//a[contains(@href, "/men") or contains(@href, "/women") or contains(@href, "/kids") or contains(@href, "/indie") or contains(@href, "/all") or contains(@href, "/sale") or contains(@href, "/update-your-wardrobe-3951-66521")]//..//div[@class="menu-flyout close-seo-dropdown-menu"]//a/@href').extract()
        for url in urls:
            sub_category_url = self.domain_url+url
            yield Request(sub_category_url, callback=self.parse_meta) 

    def parse_meta(self, response):
        text = ''.join(response.xpath('//script[contains(text(), "window.__PRELOADED_STATE__")]/text()').extract()).replace('\r\n', '').replace(';', '')
        data = ''.join(re.findall('window.__PRELOADED_STATE__ = (.*)', text)).strip()
        datas = ''
        try:
            datas = json.loads(data)
        except: pass
        if datas:   
            curatedId = datas.get('request', {}).get('query', {}).get('curatedid', '')
            curted = datas.get('request', {}).get('query', {}).get('curated', '')
            if curatedId:
                num = response.xpath('//script[@type="application/ld+json"][2]//text()').extract()
                num = ''.join(''.join(re.findall('@id(.*)', ''.join(re.findall('item(.*)', ''.join(re.findall('@id(.*)', ''.join(re.findall('item(.*)', ''.join(num))))))))).split(',')[0].split('/')[-1]).split('"')[0]
                url = 'https://www.ajio.com/api/category/%s?fields=SITE&currentPage=1&pageSize=45&format=json&query=relevance&sortBy=relevance&curated=true&curatedid=%s&gridColumns=3&facets=&advfilter=true' % (num, curatedId)
            else:
                num = response.url.split('/')[-1]
                url = 'https://www.ajio.com/api/category/%s?fields=SITE&currentPage=1&pageSize=45&format=json&query=relevance&sortBy=relevance&gridColumns=3&facets=&advfilter=true'% num
            meta = {'range': 0, 'page': 1, 'curatedId':curatedId, 'num':num, "handle_httpstatus_list":[400]}
            yield Request(url, callback=self.parse_data, meta=meta)

    def parse_data(self, response):
        curatedId = response.meta['curatedId']
        data = ''
        try:
            data = json.loads(response.text)
        except: pass
        number = response.meta['num']
        code_ = data.get('categoryCode', '')
        category = data.get('categoryforheader', '')
        sub_category = data.get('categoryName', '')
        page = response.meta['page']
        source = self.name.split('_')[0]
        meta_data = {'category':category, 'sub_category':sub_category}
        products = data.get('products', [])
        for product in products:
            code = product.get('code', '')
            brandname = product.get('fnlColorVariantData', {}).get('brandName', '')
            outfiturl = product.get('fnlColorVariantData', {}).get('outfitPictureURL', '')
            discount = product.get('discountPercent', '')
            mrp = product.get('price', {}).get('displayformattedValue', '')
            selling_price = product.get('wasPriceData', {}).get('displayformattedValue', '')
            product_name = product.get('name', '')
            product_url = urljoin(self.domain_url, product.get('url', ''))
            sizes = product.get('productSizeData', {}).get('sizeVariants', '')
            hd_id = encode_md5('%s%s%s' % (source, str(code), sizes))
            aux_info = {'product_id': code_, 'json_page': response.url}
            insight_item = InsightItem() 
            insight_item.update({'hd_id': hd_id, 'source': source, 'sku': code, 'size': sizes, 'category':category, 'sub_category': sub_category, 'brand': brandname, 'ratings_count': '', 'reviews_count': '', 'mrp':mrp, 'selling_price': selling_price, 'discount_percentage': discount, 'is_available': ''})
            yield insight_item
            meta_item = MetaItem()
            meta_item.update({'hd_id': hd_id, 'source': source, 'sku': code, 'size': sizes, 'title': product_name, 'descripion': '', 'specs':'', 'image_url':outfiturl, 'reference_url': response.url, 'aux_info': json.dumps(aux_info)})
            yield meta_item

        if products:
            page += 1
            if curatedId:
                url = 'https://www.ajio.com/api/category/%s?fields=SITE&currentPage=%s&pageSize=45&format=json&query=relevance&sortBy=relevance&curated=true&curatedid=%s&gridColumns=3&facets=&advfilter=true' % (number, page, curatedId)
            else:
                url = 'https://www.ajio.com/api/category/%s?fields=SITE&currentPage=%s&pageSize=45&format=json&query=relevance&sortBy=relevance&curated=true&curatedid=scotch-and-soda-3483-6296&gridColumns=3&facets=&advfilter=true'% (number, page)
    
            meta = {'page': page, 'num':number, 'curatedId':curatedId}
            yield Request(url, headers=self.headers, callback=self.parse_data, meta=meta)
