import re
import json
from urllib.parse import urljoin
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem


class ajiospider(EcommSpider):
    name = 'ajio_browse'
    domain_url = "https://www.ajio.com"
    start_urls = []
    handle_httpstatus_list = [400]

    def __init__(self, *args, **kwargs):
        super(ajiospider, self).__init__(*args, **kwargs)
        self.headers = {
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
                'Sec-Fetch-User': '?1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-Mode': 'navigate',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

    def start_requests(self):
        url = 'https://www.ajio.com/shop/men'
        yield Request(url, callback=self.parse, headers=self.headers)

    def parse(self,response):
        nodes = response.xpath('//ul[@class="level-first false"]/li')
        for node in nodes:
            url = ''.join(node.xpath('./a/@href').extract())
            link =  urljoin(self.domain_url, url) 
            yield Request(link,callback=self.parse_next,meta = {"node" : node},headers=self.headers)

    def parse_next(self,response):
        urls = response.xpath('//li//a[contains(@href, "/men") or contains(@href, "/women") or contains(@href, "/kids") or contains(@href, "/indie") or contains(@href, "/all") or contains(@href, "/sale") or contains(@href, "/update-your-wardrobe-3951-66521")]//..//div[@class="menu-flyout close-seo-dropdown-menu"]//a/@href').extract()
        for url in urls:
            sub_category_url = urljoin(self.domain_url, url)
            yield Request(sub_category_url, callback=self.parse_meta, headers=self.headers,meta = {'url':url}) 

    def parse_meta(self,response):
        text = ''.join(response.xpath('//script[contains(text(),"window.__PRELOADED_STATE__")]/text()').extract()).replace('\r\n','').replace(';','')
        data = ''.join(re.findall('window.__PRELOADED_STATE__ = (.*)',text)).strip() 
        datas = ''
        try:
            datas = json.loads(data)
        except:pass
        if datas:
            categories = datas.get('navigation',{})
            source = 'ajio'
            for category in categories:
                category = category.get('name','')
            products  = datas.get('grid',{}).get('results',[])
            for product in products:
                product = product
                data = datas.get('grid','').get('entities','').get(product,'').get('fnlColorVariantData',{})
                details = datas.get('grid',{}).get('entities',{}).get(product,{})
                price = details.get('price',{}).get('displayformattedValue','')
                web_price = details.get('wasPriceData','').get('displayformattedValue','')
                sizes = details.get('productSizeData',{}).get('sizeVariants',[])
                discountpercentage = details.get('discountPercent','')
                productname = details.get('name','')
                producturl = details.get('url','')
                sku = details.get('code','')
                brandname = data.get('brandName','')
                color = data.get('colorGroup','')
                Imagepath = data.get('outfitPictureURL','')
                insight_item = InsightItem()
                hd_id = encode_md5('%s%s%s' % (source, str(sku), sizes))
                aux_info = {'product_id': product, 'json_page': response.url}
                insight_item = InsightItem()
                insight_item.update({
                            'hd_id': hd_id, 'source': source, 'sku': sku, 'size': sizes, 'category':category,
                            'sub_category': '', 'brand': brandname, 'ratings_count': '',
                            'reviews_count': '', 'mrp':price, 'selling_price': web_price,
                            'discount_percentage': discountpercentage,'is_available': ''
                        })
                yield insight_item

                meta_item = MetaItem()
                meta_item.update({
                            'hd_id': hd_id, 'source': source, 'sku': sku, 'size': sizes, 'title': productname,
                            'descripion': '', 'specs':'', 'image_url': Imagepath,
                            'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                })
                yield meta_item


