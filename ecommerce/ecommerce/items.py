# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from scrapy.item import Item, Field
import scrapy

class urlQueuemeta(Item):
	sk = Field()
	url = Field()
	source = Field()
	crawl_type = Field()
	content_type = Field()
	related_type = Field()
	crawl_status = Field()
	meta_data = Field()
	created_at = Field()
	modified_at = Field()

class productInfo(Item):
	hd_id = Field()
	source = Field()
	sku = Field()
	size = Field()
	title = Field()
	descripion = Field()
	image_url = Field()
	reference_url = Field()
	aux_info = Field()
	created_at = Field()
	modified_at = Field()

class productInsights(Item):
	hd_id = Field()
	source = Field()
	sku = Field()
	size = Field()
	category = Field()
	sub_category = Field()
	brand = Field()
	ratings_count = Field()
	reviews_count = Field()
	mrp = Field()
	selling_price = Field()
	discount_percentage = Field()
	is_available = Field()
	created_at = Field()
	modified_at = Field()
