import re
import json
from urllib.parse import urljoin
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem

class AjioSpider(EcommSpider):
    name = 'ajio_browse'
    domain_url = "https://www.ajio.com"
    handle_httpstatus_list = [400] 

    def __init__(self, *args, **kwargs):
        super(AjioSpider, self).__init__(*args, **kwargs)
        self.category_array = ['s/men-newin-clothing', 'men-jackets-coats/c/830216010', 'men-jeans/c/830216001', 'men-shirts/c/830216013', 'men-shorts-3-4ths/c/830216002', 'men-sweatshirt-hoodies/c/830216011', 'men-track-pants/c/830216003', 'men-trousers-pants/c/830216004', 'men-tshirts/c/830216014', 's/kurtas-for-men', 's/men-jackets-collection', 's/men-shirts-collection', 's/fresh-arrivals-women-clothing', 'women-tops/c/830316017', 'women-tshirts/c/830316018', 'women-jeans-jeggings/c/830316001', 'women-dresses/c/830316007', 'women-trousers-pants/c/830316006', 'women-shirts/c/830316016', 'women-track-pants/c/830316005', 's/women-skirts-and-shorts-3621-51391', 'women-jackets-coats/c/830316012', 'women-jumpsuits-playsuits/c/830316008', 'women-shrugs-boleros/c/830316011', 'women-sweatshirts-hoodies/c/830316013', 'women-sweaters-cardigans/c/830316019', 'women-kurtas/c/830303011', 'women-salwars-churidars/c/830303002', 'women-kurtis-tunics/c/830303012', 'women-sarees/c/830303008', 's/dupattas-women', 'women-dress-material/c/830303004', 'women-kurta-suit-sets/c/830303009' ,'women-blouses/c/830303007', 'women-leggings/c/830316002', 's/women-jackets-and-shrugs-3621-51391', 'women-skirts-ghagras/c/830303003', 's/shawls-and-wraps', 's/women-palazzos-and-culottes-3621-51391', 'women-dresses-gowns/c/830309004', 's/fusion-kurtas-3650-1875', 's/fusion-kurtis-and-tunics-3650-1875', 'women-pants-shorts/c/830309002', 'women-jackets-shrugs/c/830309006', 'women-shirts-tops-tunic/c/830309010', 's/boys-denims-trousers', 's/boys-joggers-track-pants', 's/boys-outerwear', 's/boys-shirts', 's/boys-shorts', 's/boys-tshirts', 's/girls-dresses-frocks', 's/girls-jeans-jeggings', 's/girls-leggings', 's/girls-outerwear', 's/girls-skirts-shorts', 's/girls-tops-tshirts']
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
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8'}

    def start_requests(self):
        for category in self.category_array:
            url = 'https://www.ajio.com/%s'%category
            yield Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response):
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
            yield Request(url, callback=self.parse_data, meta=meta, headers=self.headers)

    def parse_data(self, response):
        curatedId = response.meta['curatedId']
        data = ''
        try:
            data = json.loads(response.text)
        except: pass
        number = response.meta['num']
        code_ = data.get('categoryCode', '')
        category = data.get('rilfnlBreadCrumbList', {}).get('rilfnlBreadCrumb', [])[0].get('name','')
        sub_category = data.get('freeTextSearch', '')
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
            aux_info = {'product_id': code_, 'json_page': product_url}
            insight_item = InsightItem() 
            insight_item.update({'hd_id': hd_id, 'source': source, 'sku': code, 'size': sizes, 'category':category, 'sub_category': sub_category, 'brand': brandname, 'ratings_count': '', 'reviews_count': '', 'mrp':mrp, 'selling_price': selling_price, 'discount_percentage': discount, 'is_available': ''})
            yield insight_item
            meta_item = MetaItem()
            meta_item.update({'hd_id': hd_id, 'source': source, 'sku': code, 'web_id':code_, 'size': sizes, 'title': product_name, 'descripion': '', 'specs':'', 'image_url':outfiturl, 'reference_url': product_url, 'aux_info': json.dumps(aux_info)})
            yield meta_item

        if products:
            page += 1
            if curatedId:
                url = 'https://www.ajio.com/api/category/%s?fields=SITE&currentPage=%s&pageSize=45&format=json&query=relevance&sortBy=relevance&curated=true&curatedid=%s&gridColumns=3&facets=&advfilter=true' % (number, page, curatedId)
            else:
                url = 'https://www.ajio.com/api/category/%s?fields=SITE&currentPage=%s&pageSize=45&format=json&query=relevance&sortBy=relevance&gridColumns=3&facets=&advfilter=true'% (number, page)                
    
            meta = {'page': page, 'num':number, 'curatedId':curatedId}
            yield Request(url, headers=self.headers, callback=self.parse_data, meta=meta)
