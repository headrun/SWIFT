import re
import json
from ecommerce.items import InsightItem, MetaItem
from ecommerce.common_utils import normalize, EcommSpider,\
    encode_md5


class NowSpider(EcommSpider):
    name = "nnnow_fashion_terminal"

    def parse(self, response):
        product_category = response.meta['data']['category']
        product_sub_category = response.meta['data']['sub_category']
        datas = response.xpath('//script[contains(text(),"window.DATA")]/text()').extract()
        for data in datas:
            total_data = re.findall('window.DATA= (.*)', data)
            for total in total_data:
                source = 'nnnow'
                data1 = json.loads(total)
                style = data1.get('ProductStore', {}).get(
                    'PdpData', {}).get('mainStyle', '')
                product_id = style.get('styleId', '')
                brandname = style.get('brandName', '')
                name = style.get('name', '')
                availability = 0
                if style.get('inStock', False):
                    availability = 1
                title = normalize(style.get('finerDetails', {}).get(
                    'compositionAndCare', {}).get('title', ''))
                details = normalize(','.join(style.get('finerDetails', {}).get(
                    'compositionAndCare', {}).get('list', '')))
                description = normalize(', '.join([title, details]))
                specs_title = normalize(
                    style.get('finerDetails', {}).get('specs', {}).get('title', ''))
                specs_details = normalize(
                    ','.join(style.get('finerDetails', {}).get('specs', {}).get('list', '')))
                specs = normalize(', '.join([specs_title, specs_details]))
                aux_info = {'product_id': product_id,
                            'json_page': response.url}
                images = style.get('images', [])
                for image in images:
                    large_image = normalize(image.get('large', ''))
                skus = style.get('skus', [])
                for sku in skus:
                    size = normalize(sku.get('size', ''))
                    skuid = normalize(sku.get('skuId', ''))
                    mrp = normalize(sku.get('mrp', ''))
                    price = normalize(sku.get('price', ''))
                    discount = normalize(sku.get('discountInPercentage', ''))
                    insight_item = InsightItem()
                    hd_id = encode_md5('%s%s%s' % (source, str(skuid), size))
                    insight_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': skuid, 'size': size, 'category': product_category,
                        'sub_category': product_sub_category, 'brand': brandname, 'ratings_count': '',
                        'reviews_count': '', 'mrp': mrp, 'selling_price': price, 'currency': 'INR',
                        'discount_percentage': discount, 'is_available': availability
                    })
                    yield insight_item

                    meta_item = MetaItem()
                    meta_item.update({
                        'hd_id': hd_id, 'source': source, 'sku': skuid, 'web_id': product_id, 'size': size, 'title': name,
                        'category': product_category, 'sub_category': product_sub_category, 'brand': brandname, 'rating': '',
                        'ratings_count': '', 'reviews_count': '', 'mrp': mrp, 'selling_price': price, 'currency': 'INR',
                        'discount_percentage': discount, 'is_available': availability,
                        'descripion': description, 'specs': specs, 'image_url': large_image,
                        'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                    })
                    yield meta_item
