""" Spider to crawl target site data. """
import json
from scrapy.selector import Selector
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem


class TargetSpider(EcommSpider):
    name = "target_browse"
    handle_httpstatus_list = [400]

    def __init__(self, *args, **kwargs):
        super(TargetSpider, self).__init__(*args, **kwargs)
        self.category_array = ['/c/workout-shirts-activewear-men-s-clothing/-/N-550z2', '/c/workout-shorts-activewear-men-s-clothing/-/N-550z1', '/c/workout-jackets-vests-activewear-men-s-clothing/-/N-550yz', '/c/workout-pants-activewear-men-s-clothing/-/N-550z0', '/c/compression-base-layers-activewear-men-s-clothing/-/N-4sppi', '/c/travelwear-commute-activewear-men-s-clothing/-/N-hyb5i', '/c/graphic-tees-t-shirts-men-s-clothing/-/N-55cxi', '/c/hoodies-sweatshirts-men-s-clothing/-/N-551v0', '/c/jackets-coats-men-s-clothing/-/N-5xu2a', '/c/jeans-men-s-clothing/-/N-5xu2b', '/c/pajama-bottoms-pajamas-robes-men-s-clothing/-/N-5xu25', '/c/pajama-sets-pajamas-robes-men-s-clothing/-/N-5xu23', '/c/robes-pajamas-men-s-clothing/-/N-5xu24', '/c/jogger-lounge-pants-men-s-clothing/-/N-4y5j8', '/c/cargo-pants-men-s-clothing/-/N-4y5j9', '/c/chino-pants-men-s-clothing/-/N-4y5jb', '/c/dress-pants-men-s-clothing/-/N-4y5ja', '/c/casual-button-downs-shirts-men-s-clothing/-/N-55cxe', '/c/polo-shirts-men-s-clothing/-/N-55cxg', '/c/basic-tees-t-shirts-men-s-clothing/-/N-4ujf8', '/c/graphic-tees-t-shirts-men-s-clothing/-/N-55cxi', '/c/henley-tees-t-shirts-men-s-clothing/-/N-xt0j8', '/c/tanks-shirts-men-s-clothing/-/N-4y5j6', '/c/shorts-men-s-clothing/-/N-5xu27', '/c/socks-men-s-clothing/-/N-5xu21', '/c/suits-men-s-clothing/-/N-5xu20', '/c/swimsuits-men-s-clothing/-/N-5xu1y', '/c/travelwear-commute-activewear-men-s-clothing/-/N-hyb5i', '/c/undershirts-men-s-clothing/-/N-4vr7u', '/c/underwear-men-s-clothing/-/N-4vr7v', '/c/big-tall-shirts-clothing-men/-/N-4yda2', '/c/big-tall-jeans-clothing-men/-/N-4yd9w', '/c/big-tall-hoodies-sweatshirts-clothing-men/-/N-4xwcg', '/c/big-tall-outerwear-men-s-clothing/-/N-4vpsy', '/c/big-tall-pajamas-robes-clothing-men/-/N-xpgqv', '/c/big-tall-pants-clothing-men/-/N-4yd9v', '/c/big-tall-shorts-clothing-men/-/N-4yd9u', '/c/big-tall-swimsuits-clothing-men/-/N-5tais', '/c/big-tall-suit-separates-clothing-men/-/N-4yd9x', '/c/big-tall-workwear-clothing-men/-/N-4xwcf', '/c/hoodies-sweatshirts-men-s-clothing/-/N-551v0', '/c/jackets-coats-men-s-clothing/-/N-5xu2a']
        self.headers = {
            'authority': 'redsky.target.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
            'origin': 'https://www.target.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.target.com/c/casual-button-downs-shirts-men-s-clothing/-/N-55cxe',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,te;q=0.7',
        }
    def start_requests(self):
        for category in self.category_array:
            sub_category = category.split('/')[2]
            category_id = category.split('-')[-1]
            meta = {'headers': self.headers, 'offset': 0, 'sub_category': sub_category, 'category_id': category_id}
            url = 'https://redsky.target.com/v2/plp/search/?category=%s&channel=web&count=24&offset=0&default_purchasability_filter=true&facet_recovery=false&pricing_store_id=1010&key=eb2551e4accc14f38cc42d32fbc2b2ea' % category_id
            yield Request(url, headers=self.headers, meta=meta, callback=self.parse)

    def parse(self, response):
        domain_url = 'https://www.target.com'
        offset = response.meta.get('offset', 0)
        category_id = response.meta.get('category_id', 0)
        sub_category = response.meta.get('sub_category', 0)
        headers = response.meta.get('headers', 0)
        data = response.text
        data = json.loads(data)
        errormsg = data.get('error_response', '')
        if errormsg:
            return
        url_data = data.get('search_response', {}).get('items', {}).get('Item', '')
        for url in url_data:
            prod_url = url.get('url', '')
            tcin = url.get('tcin', '')
            product_url = domain_url + prod_url
            meta_1 = {'product_url': product_url, 'tcin': tcin, 'sub_category': sub_category}
            yield Request(product_url, headers=self.headers, meta=meta_1, callback=self.parse_data)
        if url_data:
            offset = offset + 24
            url = 'https://redsky.target.com/v2/plp/search/?category=%s&channel=web&count=24&offset=%s&default_purchasability_filter=true&facet_recovery=false&pricing_store_id=1010&key=eb2551e4accc14f38cc42d32fbc2b2ea' % (category_id, offset)
            product_meta = {'headers': headers, 'offset':offset, 'sub_category':sub_category, 'category_id':category_id}
            yield Request(url, headers=headers, meta=product_meta, callback=self.parse)

    def parse_data(self, response):
        headers = response.meta.get('headers', 0)
        tcin = response.meta.get('tcin', 0)
        sub_category = response.meta.get('sub_category', 0)
        sel = Selector(response)
        category = sel.xpath('//*[@id="viewport"]/div[5]/div/div[1]/div[1]/span[2]/a/span//text()').extract()
        try:
            category = category[0]
        except:
            category = category
        json_data = sel.xpath('//*[@id="viewport"]/div[5]/script//text()').extract()
        try:
            json_data = json_data[0]
            json_data = json_data
            json_data = json.loads(json_data)
            prod_data = json_data['@graph']
            prod_data = prod_data[0]
            name = prod_data.get('name', '')
            brand = prod_data.get('brand', '')
            image_url = prod_data.get('image', '')
            sku = prod_data.get('sku', '')
            description = prod_data.get('description', '')
            description = description.replace('<br />', '').replace('\n', '')
            reference_url = response.url
            aux_info = {'product_id': sku, 'json_page': response.url}
            rating_data = prod_data.get('review', '')
            for rating in rating_data:
                rating = rating.get('reviewRating', {}).get('ratingValue', '')
            if not rating_data:
                rating = ''
            meta_details = {'sku': sku, 'title': name,\
                'category': category, 'sub_category': sub_category, 'brand': brand, \
                'descripion': description, 'image_url': image_url, 'reference_url': reference_url,\
                'aux_info': json.dumps(aux_info), 'rating': rating, 'tcin': tcin
                       }
            url = "https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1?key=eb2551e4accc14f38cc42d32fbc2b2ea&tcin=%s&store_id=1010&scheduled_delivery_store_id=1010&has_scheduled_delivery_store_id=true" % tcin
            yield Request(url, headers=headers, meta=meta_details, callback=self.parse_size)
        except:
            return

    def parse_size(self, response):
        previous_data = response.meta
        headers = response.headers
        tcin = response.meta.get('tcin', '')
        size_dat = response.text
        size_data = json.loads(size_dat)
        size_data = size_data.get('data', {}).get('product', {}).get('variation_hierarchy', '')
        size_details = []
        avail_details = []
        for size in size_data:
            is_available = size.get('availability', {}).get('is_shipping_available', '')
            if is_available == True:
                is_available = 1
                avail_details.append(is_available)
            elif is_available == False:
                is_available = 0
                avail_details.append(is_available)
            size = size.get('value', '')
            size_details.append(size)
        previous_data.update({'size_details': size_details, 'avail_details': avail_details})
        url = "https://redsky.target.com/web/pdp_location/v1/tcin/%s?pricing_store_id=1010&key=eb2551e4accc14f38cc42d32fbc2b2ea" % tcin
        yield Request(url, headers=headers, meta=previous_data, callback=self.parse_price)

    def parse_price(self, response):
        item_details = response.meta
        price_details_ = response.text
        price_details = json.loads(price_details_)
        mrp_ = price_details.get('price', {}).get('reg_retail_min', '')
        selling_price = price_details.get('price', {}).get('formatted_current_price', '')
        selling_price_ = selling_price.replace('$', '').strip()
        disc_percentage = price_details.get('child_items', {})
        data = 0
        for disc_percent in disc_percentage:
            if data == 0:
                try:
                    discount_percentage_ = disc_percent.get('price', {}).get('save_percent', 0)
                except: discount_percentage_ = 0
            data += 1
        source__ = "Target"
        size_detail_ = item_details.get('size_details', '')
        avial_detail_ = item_details.get('avail_details', '')
        sku_ = item_details.get('sku', '')
        title_ = item_details.get('title', '')
        category_ = item_details.get('category', '')
        sub_category_ = item_details.get('sub_category', '')
        brand_ = item_details.get('brand', '')
        rating_ = item_details.get('rating', '')
        descripion_ = item_details.get('descripion', '')
        image_url_ = item_details.get('image_url', '')
        reference_url_ = item_details.get('reference_url', '')
        aux_info_ = item_details.get('aux_info', '')
        currency_type = "USD"
        if mrp_ == '0' or mrp_ == '':
            return
        else:
            for size_, avail in zip(size_detail_, avial_detail_):
                hd_id_ = encode_md5('%s%s%s' % (source__, str(sku_), size_))
                is_avail = avail
                insight_item = InsightItem()
                insight_item.update({
                    'hd_id': hd_id_, 'source': source__, 'sku': sku_, 'size': size_,
                    'category':category_, 'sub_category': sub_category_, 'brand': brand_,
                    'ratings_count': '', 'reviews_count': '', 'mrp': mrp_,
                    'selling_price': selling_price_, 'currency': currency_type,
                    'discount_percentage': discount_percentage_, 'is_available': is_avail
                                    })
                yield insight_item
                meta_item = MetaItem()
                meta_item.update({
                    'hd_id': hd_id_, 'source': source__, 'sku': sku_, 'web_id': sku_, 'size': size_,
                    'title': title_, 'category':category_, 'sub_category': sub_category_,
                    'brand': brand_, 'rating':rating_, 'ratings_count': '', 'reviews_count': '',
                    'mrp':mrp_, 'selling_price': selling_price_, 'currency': currency_type,
                    'discount_percentage': discount_percentage_, 'is_available': is_avail,
                    'descripion': descripion_, 'specs': '', 'image_url': image_url_,
                    'reference_url': reference_url_, 'aux_info': aux_info_
                                })
                yield meta_item
