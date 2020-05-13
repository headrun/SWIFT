import scrapy
from scrapy.http import Request
from urllib.parse import urlencode
import requests
import json
import math
from datetime import datetime
from ecommerce.items import *
from ecommerce.generic_functions import *
from pydispatch import dispatcher
from scrapy import signals
from re import findall
headers = {
    'authority': 'www.myntra.com',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
	}
class MyntraSpider(scrapy.Spider):
	name = "Myntra"
	def __init__(self, *args, **kwargs):
		create_default_dirs()
		self.out_put_file = get_out_file("%s__%s" % ('meta_data', self.name))
		self.out_put_file1 = get_out_file("%s__%s" % ('insights_data', self.name))
		self.category_array = ['men-tshirts','men-casual-shirts','men-formal-shirts','men-sweatshirts','men-sweaters','men-jackets','men-blazers','men-suits','rain-jacket']
		dispatcher.connect(self.spider_closed, signals.spider_closed)
	def spider_closed(self, spider):
		crawlout_processing(self.out_put_file)
		crawlout_processing(self.out_put_file1)
	def start_requests(self):
		for category in self.category_array:
			url = 'https://www.myntra.com/web/v1/search/%s?p=1&rows=100&o=0&sort=popularity' % category
			meta = {'range': 0, 'page': 1, 'category': category}
			yield Request(url, headers=headers, callback=self.parse, meta=meta)	
	def parse(self,response):
		page_range = response.meta['range']
		page = response.meta['page']
		request_category = response.meta['category']
		_data = json.loads(response.body.decode('utf-8'))
		products = _data.get('products', [])
		for product in products:
			availabilities_info = product.get('inventoryInfo', [])
			for availability_info in availabilities_info:
				product_info = productInfo()
				product_info["source"] = source = "Myntra"
				product_info["sku"] = sku = availability_info.get('skuId', '')
				product_info["size"] = size = availability_info.get('label', '')
				hd_id = md5(''.join([source, str(sku), size]))
				product_info["title"] = product.get('productName', '')
				product_info["descripion"] = ""
				product_info["image_url"] = product.get('searchImage', '')
				product_info["reference_url"] = product.get('landingPageUrl', '')
				product_info["aux_info"] = {}
				product_info["created_at"] = str(datetime.now().now()).split('.')[0]
				product_info["modified_at"] = str(datetime.now().now()).split('.')[0]
				product_info["hd_id"] = hd_id
				yield product_info
				product_insights = productInsights()
				product_insights["source"] = "Myntra"
				product_insights["sku"] = availability_info.get('skuId', '')
				product_insights["size"] = availability_info.get('label', '')
				product_insights["category"] = product.get('gender', '')
				product_insights["sub_category"] = product.get('category', '')
				product_insights["brand"] = product.get('brand', '')
				product_insights["ratings_count"] = product.get('ratingCount', '')
				product_insights["reviews_count"] = 0
				product_insights["mrp"] = product.get('mrp', '')
				product_insights["selling_price"] = product.get('price', '')
				product_insights["discount_percentage"] = ''.join(findall('\(([^\)]+)\)', product.get('discountDisplayLabel', '')))
				product_insights["is_available"] = availability_info.get('inventory',0)
				product_insights["created_at"] = str(datetime.now().now()).split('.')[0]
				product_insights["modified_at"] = str(datetime.now().now()).split('.')[0]
				product_insights["hd_id"] = hd_id
				yield product_insights

		if products:
			page += 1
			page_range = (page-1)*100-1
			url = 'https://www.myntra.com/web/v1/search/%s?p=%s&rows=100&o=%s&sort=popularity' % (request_category, page, page_range)
			meta = {'range': page_range, 'page': page, 'category': request_category}
			yield Request(url, callback=self.parse, headers=headers, meta=meta)


