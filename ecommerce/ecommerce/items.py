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
