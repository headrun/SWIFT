# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class InsightItem(Item):
    hd_id                       = Field()
    source                      = Field()
    sku                         = Field()
    size                        = Field()
    category                    = Field()
    sub_category                = Field()
    brand                       = Field()
    ratings_count               = Field()
    reviews_count               = Field()
    mrp                         = Field()
    selling_price               = Field()
    discount_percentage         = Field()
    is_available                = Field()

class MetaItem(Item):
    hd_id                       = Field()
    source                      = Field()
    sku                         = Field()
    web_id                      = Field()
    size                        = Field()
    title                       = Field()
    descripion                  = Field()
    specs                       = Field()
    image_url                   = Field()
    reference_url               = Field()
    aux_info                    = Field()
