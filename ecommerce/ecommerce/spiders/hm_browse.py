import json
from ecommerce.common_utils import *
from urllib.parse import urlencode
from scrapy.selector import Selector
from ecommerce.items import InsightItem, MetaItem

class HmSpider(EcommSpider):
    name = 'hm_browse'

    def __init__(self, *args, **kwargs):
        super(HmSpider, self).__init__(*args, **kwargs)
        self.category_array = ['t-shirts-tank-tops']
        self.product_type = ['men_tshirtstanks']
        self.headers = {
            'authority': 'www2.hm.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'content-type': 'application/json; charset=utf-8',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
    def start_requests(self):
        for category, product_type in zip(self.category_array, self.product_type):
            url = 'https://www2.hm.com/en_us/men/products/%s/_jcr_content/main/productlisting.display.json?product-type=%s&sort=stock&image-size=small&image=model&offset=0&page-size=36'%(category, product_type)
            meta = {'page':0,'category':category, 'product_type':product_type}
            yield Request(url, callback=self.parse, headers=self.headers, meta=meta)

    def parse(self, response):
        data = json.loads(response.text)
        page = response.meta['page']
        category = response.meta['category']
        product_type = response.meta['product_type']
        sizes = data.get('filters',[])[0]
        products = data.get('products',[])
        for product in products:
            product_link = 'https://www2.hm.com/'+product.get('link','')
            product_id = product.get('articleCode','') 
            meta = {'sizes':sizes,'product_id':product_id, 'page':page}
            yield Request(product_link, headers=self.headers, callback=self.parse_meta,meta=meta)
        if products:
            page = page+36
            url = 'https://www2.hm.com/en_us/men/products/%s/_jcr_content/main/productlisting.display.json?product-type=%s&sort=stock&image-size=small&image=model&offset=%s&page-size=36'%(category, product_type, page)
            meta = {'page':page,'category':category, 'product_type':product_type}
            yield Request(url, headers=self.headers, meta=meta, callback=self.parse)


    def parse_meta(self, response):
        sizes_ = response.meta['sizes']
        product_id = response.meta['product_id']
        sizes = sizes_.get('filtervalues',[])
        data = json.loads(''.join(response.xpath('//script[@type="application/ld+json"]//text()').extract()).replace('\r\n',''))
        product_name = data.get('name','')
        product_category = response.xpath('//span[@itemprop="name"]//text()')[1].extract()
        image_url = 'https:'+data.get('image','')
        description = data.get('description','')
        skuid = data.get('sku','')
        sku_id = data.get('sku','') + '001'
        brandname = data.get('brand',{}).get('name','').replace('amp;','')
        sub_category = data.get('category',{}).get('name','')
        currency = data.get('offers',[])[0].get('priceCurrency','')
        price =  data.get('offers',[])[0].get('price','')
        meta = {'product_name':product_name , 'product_category':product_category, 
                'image_url':image_url, 'description':description, 'skuid':skuid, 
                'brandname': brandname, 'currency':currency, 'price':price,
                'product_id':product_id, 'sizes':sizes, 'sub_category':sub_category,
                'url':response.url, 'sku_id':sku_id}
        url = 'https://www2.hm.com/en_us/reviews/rrs/ugcsummary?sku=%s'%(sku_id)
        yield Request(url, headers=self.headers,callback=self.parse_next,meta=meta)

    def parse_next(self, response):
        source = 'hm'
        data = json.loads(response.text)[0]
        reviews = data.get('reviews','')
        rating_count = data.get('ratings','')
        rating = data.get('averageRating','')
        sub_category = response.meta['sub_category']
        product_name = response.meta['product_name']
        product_category = response.meta['product_category']    
        image_url = response.meta['image_url']
        description = response.meta['description']
        skuid = response.meta['skuid']
        sku_id = response.meta['sku_id']
        url = response.meta['url']
        product_id = response.meta['product_id']
        brandname = response.meta['brandname']
        currency = response.meta['currency']
        price = response.meta['price']
        sizes = response.meta['sizes']
        aux_info = {'product_id':product_id, 'json_page':url}
        for size in sizes:
            size = size.get('name','')
            if size == '2xl':
                size = 'xxl'
            insight_item = InsightItem()
            hd_id = encode_md5('%s%s%s' % (source, str(skuid), size))
            insight_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': sku_id, 'size': size,
                        'category':product_category, 'sub_category': sub_category, 'brand': brandname,
                        'ratings_count': rating_count, 'reviews_count':reviews, 'mrp': price,
                        'selling_price': price, 'currency': currency,
                        'discount_percentage': 0, 'is_available': 0
            })
            yield insight_item
            meta_item = MetaItem()
            meta_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': sku_id, 'web_id': product_id, 'size': size,
                        'title': product_name, 'category':product_category, 'sub_category': sub_category,
                        'brand': brandname, 'rating':rating, 'ratings_count':rating_count, 'reviews_count':reviews,
                        'mrp':price, 'selling_price': price, 'currency': currency,
                        'discount_percentage':0, 'is_available':0,
                        'descripion': description, 'specs': '', 'image_url': image_url,
                        'reference_url': url, 'aux_info': json.dumps(aux_info)
            })
            yield meta_item
 
            
        
        
        
            


