import json
import re
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem
from ecommerce.settings import USER_AGENT


class AjioSpider(EcommSpider):
    name = 'ajio_fashion_terminal'
    handle_httpstatus_list = [404,402,400,500]
    

    def parse(self,response):
        text = ''.join(response.xpath('//script[contains(text(),"window.__PRELOADED_STATE__")]/text()').extract()).replace('\r\n','').replace(';','')
        data = ''.join(re.findall('window.__PRELOADED_STATE__ = (.*)',text)).strip()
        datas = json.loads(data)
        categories = datas.get('navigation',{})
        source = 'ajio'
        for category in categories:
            category = category.get('name','')
        products  = datas.get('grid',{}).get('results',[])
        for product in products:
            product = product
            details = datas.get('grid',{}).get('entities',{}).get(product,{})
            sku = details.get('code','')
            discountpercentage = details.get('discountPercent','')
            price = details.get('price',{}).get('displayformattedValue','')
            web_price = details.get('wasPriceData','').get('displayformattedValue','')
            productname = details.get('name','')
            producturl = details.get('url','')
            sizes = details.get('productSizeData',{}).get('sizeVariants',[])
            data = datas.get('grid','').get('entities','').get(product,'').get('fnlColorVariantData',{})
            brandname = data.get('brandName','')
            color = data.get('colorGroup','')
            Imagepath = data.get('outfitPictureURL','')
            product_id = ''
            insight_item = InsightItem()
            hd_id = encode_md5('%s%s%s' % (source, str(sku), sizes))
            aux_info = {'product_id': product, 'json_page': response.url}
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
