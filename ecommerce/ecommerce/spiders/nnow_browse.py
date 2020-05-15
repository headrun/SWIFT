import json
from ecommerce.common_utils import *
import re
from ecommerce.items import InsightItem, MetaItem


class NowSpider(EcommSpider):
    name = "nnow_browse"

    def __init__(self, *args, **kwargs):
        super(NowSpider, self).__init__(*args, **kwargs)
        self.category_array = ['men_t_shirts_polos', 'men-casual-shirts']
        self.cids = ['men_t_shirts_polos','men_casual_shirts']
        self.headers = headers = {
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/json',
                'accept': 'application/json',
                'module': 'odin',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
                'bbversion': 'v2',
                'Origin': 'https://www.nnnow.com',
                'Sec-Fetch-Site': 'same-site',
                'Sec-Fetch-Mode': 'cors',
                'Referer': 'https://www.nnnow.com/men-t-shirts-polos',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',

        }

    def start_requests(self):
        for category in self.category_array:
            for cid in self.cids:
                cid_nu = 'tn_'+cid
                data = {"deeplinkurl":"/%s?p=1&cid=%s"%(category,cid_nu)}
                url = 'https://api.nnnow.com/d/apiV2/listing/products' 
                meta = {'range': 0, 'page': 1, 'category': category,'cid':cid_nu}
                yield Request(url, headers=self.headers, callback=self.parse, meta=meta,body=json.dumps(data),method = 'POST')

    def parse(self, response):
        headers = self.headers
        page = response.meta['page']
        request_category = response.meta['category']
        cid2 = response.meta['cid']
        source = self.name.split('_')[0]
        _data = json.loads(response.body.decode('utf-8'))
        products = _data.get('data',{}).get('styles',{}).get('styleList',[])
        for product in products:
            product_url = 'https://www.nnnow.com' + product.get('url','')
            yield Request(product_url,callback=self.parse_next)
        if products:
            page += 1
            data = {"deeplinkurl":"/%s?p=%s&cid=%s"%(request_category,page,cid2)}
            url = 'https://api.nnnow.com/d/apiV2/listing/products'
            meta = {'page': page,'category': request_category,'cid':cid2}
            yield Request(url, headers=self.headers, callback=self.parse, meta=meta,body=json.dumps(data),method = 'POST')

        
    def parse_next(self,response):
        datas = response.xpath('//script[contains(text(),"window.DATA")]/text()').extract()
        for data in datas:
            total_data = re.findall('window.DATA= (.*)',data)
            for total in total_data:
                source = 'nnow'
                data1 = json.loads(total)
                style = data1.get('ProductStore',{}).get('PdpData',{}).get('mainStyle','')
                product_id = style.get('styleId','')
                brandname = style.get('brandName','')
                name = style.get('name','')
                status = style.get('status','')
                url = style.get('url','')
                category = style.get('gender','')
                availability = style.get('inStock','')
                title = style.get('finerDetails',{}).get('compositionAndCare',{}).get('title','')
                details = ','.join(style.get('finerDetails',{}).get('compositionAndCare',{}).get('list',''))
                description = title+', '+details
                specs_title = style.get('finerDetails',{}).get('specs',{}).get('title','')
                specs_details = ','.join(style.get('finerDetails',{}).get('specs',{}).get('list',''))
                specs = specs_title+', '+specs_details
                aux_info = {'product_id': product_id, 'json_page': response.url}
                tags = style.get('productTags',[])
                for tag in tags:
                    tagtext = tag.get('tagText','')
                images = style.get('images',[])
                for image in images:
                    large_image = image.get('large','')
                skus = style.get('skus',[])
                for sku in skus:
                    size = sku.get('size','')
                    skuid = sku.get('skuId','')
                    quantity = sku.get('maxQuantityDisplayed','')
                    mrp = sku.get('mrp','')
                    price = sku.get('price','')
                    discount = sku.get('discountInPercentage','')
                    sellable = sku.get('sellableQuantity','')
                    insight_item = InsightItem()
                    hd_id = encode_md5('%s%s%s' % (source, str(skuid), size))
                    insight_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'category':category,
                        'sub_category': '', 'brand': brandname, 'ratings_count': '',
                        'reviews_count': '', 'mrp': mrp, 'selling_price': price,
                        'discount_percentage': discount,'is_available': availability
                    })
                    yield insight_item    
                    
                    meta_item = MetaItem()
                    meta_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': sku, 'size': size, 'title': name,
                        'descripion': description, 'specs':specs, 'image_url': large_image, 
                        'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                    })
                    yield meta_item

