import json
import re
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem

class NowSpider(EcommSpider):
    name = "nnnow_fashion_terminal"

    def parse(self,response):
        product_category = response.meta['data']['category']
        product_sub_category = response.meta['data']['sub_category']
        datas = response.xpath('//script[contains(text(),"window.DATA")]/text()').extract()
        for data in datas:
            total_data = re.findall('window.DATA= (.*)',data)
            for total in total_data:
                source = 'nnnow'
                data1 = json.loads(total)
                style = data1.get('ProductStore',{}).get('PdpData',{}).get('mainStyle','')
                product_id = style.get('styleId','')
                brandname = style.get('brandName','')
                name = style.get('name','')
                status = style.get('status','')
                url = style.get('url','')
                category = style.get('gender','')
                availability = 0
                if style.get('inStock',False):
                    availability = 1
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
                        'hd_id': hd_id, 'source': source, 'sku': skuid, 'size': size, 'category':product_category,
                        'sub_category': product_sub_category, 'brand': brandname, 'ratings_count': '',
                        'reviews_count': '', 'mrp': mrp, 'selling_price': price,
                        'discount_percentage': discount,'is_available': availability
                    })
                    yield insight_item    
                    
                    meta_item = MetaItem()
                    meta_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': skuid, 'web_id':product_id, 'size': size, 'title': name,
                        'category':product_category, 'sub_category':product_sub_category,'brand':brandname,'rating':'',
                        'ratings_count':'','reviews_count':'','mrp':mrp,'selling_price':price,
                        'discount_percentage':discount,'is_available':availability,
                        'descripion': description, 'specs':specs, 'image_url': large_image, 
                        'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                    })
                    yield meta_item    
