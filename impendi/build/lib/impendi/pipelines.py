# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from impendi.items import EbayItem

class ImpendiPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, EbayItem):
            ebay_values = '#<>#'.join([
                str(item['source_key']), str(item['search_key']), str(item['item_id']),
                str(item['top_rated']), str(item['title']), str(item['location']),
                str(item['postal_code']), str(item['returns_accepted']), str(item['is_multi']),
                str(item['category_id']), str(item['category']), str(item['expedited_shipping']),
                str(item['ship_to_locations']), str(item['shipping_type']), str(item['shipping_service_cost']),
                str(item['shipping_service_currency']), str(item['current_price']), str(item['current_price_currency']),
                str(item['converted_current_price']), str(item['converted_current_price_currency']),str(item['selling_state']),
                str(item['condition']), str(item['listing_type']), str(item['best_offer_enabled']), str(item['buy_it_now_available']),
                str(item['start_time']), str(item['end_time']), str(item['image_url']), str(item['item_url']), str(item['timestamp'])
            ])

            spider.queries_file.write('%s\n' % ebay_values)
            spider.queries_file.flush()

        return item
