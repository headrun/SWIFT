# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class EbayItem(Item):
    source_key = Field()
    search_key = Field()
    item_id = Field()
    top_rated = Field()
    title = Field()
    location = Field()
    postal_code = Field()
    returns_accepted = Field()
    is_multi = Field()
    category_id = Field()
    category = Field()
    expedited_shipping = Field()
    ship_to_locations = Field()
    shipping_type = Field()
    shipping_service_cost = Field()
    shipping_service_currency = Field()
    current_price = Field()
    current_price_currency = Field()
    converted_current_price = Field()
    converted_current_price_currency = Field()
    selling_state = Field()
    listing_type = Field()
    best_offer_enabled = Field()
    buy_it_now_available = Field()
    start_time = Field()
    condition = Field()
    end_time = Field()
    image_url = Field()
    item_url = Field()
    timestamp = Field()

