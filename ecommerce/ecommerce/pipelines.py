# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from .items import *
from scrapy import signals
from pydispatch import dispatcher
from scrapy import signals
class EcommercePipeline(object):
    def process_item(self, item, spider):
    	if isinstance(item, urlQueuemeta):
    		tracking_meta_values = '#<>#'.join([str(item.get('sk', '')), str(item.get('url', '')), str(item.get('source', '')), str(item.get('crawl_type', '')), str(item.get('content_type','')), str(item.get('related_type', '')), str(item.get('crawl_status', '')), str(item.get('meta_data', '')), str(item.get('created_at', '')), str(item.get('modified_at', ''))])
    		spider.out_put_file.write('%s\n' % tracking_meta_values)
    		spider.out_put_file.flush()
    	if isinstance(item, productInfo):
    		tracking_meta_values = '#<>#'.join([str(item.get('hd_id', '')), str(item.get('source', '')), str(item.get('sku', '')), str(item.get('size', '')), str(item.get('title','')), str(item.get('descripion', '')), str(item.get('image_url', '')), str(item.get('reference_url', '')), str(item.get('aux_info', '')), str(item.get('created_at', '')), str(item.get('modified_at', ''))])
    		spider.out_put_file.write('%s\n' % tracking_meta_values)
    		spider.out_put_file.flush()
    	if isinstance(item, productInsights):
    		tracking_meta_values = '#<>#'.join([str(item.get('hd_id', '')), str(item.get('source', '')), str(item.get('sku', '')), str(item.get('size', '')), str(item.get('category','')), str(item.get('sub_category', '')), str(item.get('brand', '')), str(item.get('ratings_count', '')), str(item.get('reviews_count', '')), str(item.get('mrp', '')), str(item.get('selling_price', '')), str(item.get('discount_percentage', '')), str(item.get('is_available', '')), str(item.get('modified_at', ''))])
    		spider.out_put_file1.write('%s\n' % tracking_meta_values)
    		spider.out_put_file1.flush()

    	return item
