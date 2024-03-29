import json
import re
from re import findall
from urllib.parse import urljoin
from scrapy.selector import Selector
from ecommerce.common_utils import EcommSpider,\
    get_nodes, extract_data, extract_list_data,\
    encode_md5, Request
from ecommerce.items import InsightItem, MetaItem


class AmazonUSFashionTerminal(EcommSpider):
    name = 'amazonus_fashions_terminal'
    domain_url = 'https://www.amazon.com'

    def __init__(self, *args, **kwargs):
        super(AmazonUSFashionTerminal, self).__init__(*args, **kwargs)
        self.source = self.name.split('_')[0]
        self.request_headers = {
            'authority': 'www.amazon.in',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7'}

    def parse(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse, headers=self.request_headers, meta=response.meta, dont_filter=True)
        else:
            _id = response.meta['sk']
            category = response.meta.get('data', {}).get('category', '')
            sub_category = response.meta.get('data', {}).get('sub_category', '')
            aux_info = {'product_id': _id, 'json_page': response.url}
            brand = extract_data(sel, '//a[@id="bylineInfo"]/text()').lower().replace('brand:', '').strip()
            title = extract_data(sel, '//span[@id="productTitle"]/text()').strip()
            description = extract_data(sel, '//div[@id="productDescription"]/p/text()').strip()
            description = description.encode('ascii', 'ignore').decode('utf-8') if description else ''
            rating_text = extract_data(sel, '//span[@id="acrPopover"]/@title')
            rating_count_text = extract_data(sel, '//span[@id="acrCustomerReviewText"]/text()')
            images = extract_data(sel, '//div[@id="imgTagWrapperId"]/img/@data-a-dynamic-image')
            specs = extract_list_data(sel, '//div[@id="feature-bullets"]/ul/li//text()')
            discount_per = extract_data(sel, '//td[contains(@class, "priceBlockSavingsString")]/text()')
            rating = re.findall("\d+.\d+", rating_text)[0] if rating_text else 0
            rating_count = re.findall("\d+", rating_count_text)[0] if rating_count_text else 0
            image_url = [item for item in eval(images).keys()][0] if images else ''
            discount = re.search(r'\((.+)\)', discount_per).group(0).strip("()%") if discount_per else 0
            specs = '. '.join([item.strip() for item in specs if item.strip()])
            mrp = extract_data(sel, '//span[@class="priceBlockStrikePriceString a-text-strike"]/text()').split('\xa0')[(-1)].replace(',', '').strip('$')
            price = extract_data(sel, '//tr[@id="priceblock_saleprice_row"]//span[@id="priceblock_saleprice"]/text()') or\
                extract_data(sel, '//tr[@id="priceblock_ourprice_row"]//span[@id="priceblock_ourprice"]/text()')
            price = price.replace(',', '').split('\xa0')[(-1)].strip('$')
            availability = 1 if price else 0
            size_nodes = get_nodes(sel, '//select[@name="dropdown_selected_size_name"]/option[not(contains(@value, "-1"))]') or\
                get_nodes(sel, '//div[@id="variation_size_name"]//span[@class="selection"]')
            for size_node in size_nodes:
                size = extract_data(size_node, './text()')
                sku = extract_data(size_node, './@value').split(',')[(-1)] or _id
                hd_id = encode_md5('%s%s%s' % (self.source, sku, size))
                meta_item = MetaItem()
                meta_item.update({
                    'hd_id': hd_id, 'source': self.source, 'sku': sku, 'web_id': _id, 'size': size, 'title': title,
                    'category': category, 'sub_category': sub_category, 'brand': brand, 'rating': rating,
                    'ratings_count': rating_count, 'reviews_count': 0, 'mrp': mrp, 'selling_price': price, 'currency': 'USD',
                    'discount_percentage': discount, 'is_available': availability, 'descripion': description, 'specs': specs,
                    'image_url': image_url, 'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                })
                yield meta_item

                insights_item = InsightItem()
                insights_item.update({
                    'hd_id': hd_id, 'source': self.source, 'sku': sku, 'size': size, 'category': category,
                    'sub_category': sub_category, 'brand': brand, 'ratings_count': rating_count,
                    'reviews_count': 0, 'mrp': mrp, 'selling_price': price, 'currency': 'USD', 'discount_percentage': discount,
                    'is_available': availability
                })

                yield insights_item

                self.got_page(_id, got_pageval=1)

            if not size_nodes:
                size = ''
                hd_id = encode_md5('%s%s%s' % (self.source, _id, size))
                meta_item = MetaItem()
                meta_item.update({
                    'hd_id': hd_id, 'source': self.source, 'sku': _id, 'web_id': _id, 'size': size, 'title': title,
                    'category': category, 'sub_category': sub_category, 'brand': brand, 'rating': rating,
                    'ratings_count': rating_count, 'reviews_count': 0, 'mrp': mrp, 'selling_price': price, 'currency': 'USD',
                    'discount_percentage': discount, 'is_available': availability, 'descripion': description,
                    'specs': specs, 'image_url': image_url, 'reference_url': response.url, 'aux_info': json.dumps(aux_info)
                })
                yield meta_item

                insights_item = InsightItem()
                insights_item.update({
                    'hd_id': hd_id, 'source': self.source, 'sku': _id, 'size': size, 'category': category,
                    'sub_category': sub_category, 'brand': brand, 'ratings_count': rating_count,
                    'reviews_count': 0, 'mrp': mrp, 'selling_price': price, 'currency': 'USD', 'discount_percentage': discount,
                    'is_available': availability
                })

                yield insights_item

                self.got_page(_id, got_pageval=1)

            reviews_link = extract_data(sel, '//a[@data-hook="see-all-reviews-link-foot"]/@href')
            if reviews_link:
                if 'http' not in reviews_link:
                    reviews_link = urljoin(self.domain_url, reviews_link)
                meta = {'insights_item': insights_item}
                yield Request(reviews_link, callback=self.parse_reviews, headers=self.request_headers, meta=meta)

    def parse_reviews(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse_reviews, headers=self.request_headers, meta=response.meta, dont_filter=True)
        else:
            reviews_count_text = extract_data(sel, '//span[@data-hook="cr-filter-info-review-count"]/text()')
            reviews_count = re.findall("of (\d+)",reviews_count_text)[0] if reviews_count_text else 0
            item = response.meta.get('insights_item', {})
            item.update({'reviews_count': reviews_count})
            yield item
