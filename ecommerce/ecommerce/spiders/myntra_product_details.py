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
import copy

class MyntraProductSpider(scrapy.Spider):
	name = "MyntraProduct"
	def __init__(self, *args, **kwargs):
		create_default_dirs()
		self.out_put_file = get_out_file("%s__%s" % ('product_info', self.name))
		self.out_put_file1 = get_out_file("%s__%s" % ('product_insights', self.name))
		dispatcher.connect(self.spider_closed, signals.spider_closed)
	def spider_closed(self, spider):
		crawlout_processing(self.out_put_file)
		crawlout_processing(self.out_put_file1)
	def start_requests(self):
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
		# url = 'https://www.myntra.com/Tshirts/Puma/Puma-Men-Navy-Blue-Wording-Tee-Printed-Round-Neck-T-shirt/7034373/buy'
		url = 'https://www.myntra.com/Tshirts/Moda-Rapido/Moda-Rapido-Men-Maroon-Printed-Round-Neck-T-Shirt/2378362/buy'
		yield Request(url,headers=headers,callback=self.parse)	
	def parse(self,response):
		if response.status == 200:
			response_data = response.xpath('//script')[10].extract().split('window.__myx = ')[1].split('</script>')[0]
			res = json.loads(response_data)
			product_data = res.get('pdpData',{})
			images_array = product_data.get('media',{}).get('albums',[])
			images = []
			for img in images_array:
				image_elements = img.get('images',[])
				for image_element in image_elements:
					imgurl = image_element.get('imageURL','')
					if imgurl:
						images.append(imgurl)
			sizes = product_data.get('sizes',[])
			for size_element in sizes:
				import pdb;pdb.set_trace()
				products_info = productInfo()
				products_info["source"] = "Myntra"
				products_info["sku"] = size_element.get('skuId','') 
				products_info["size"] = size_element.get('label','')
				hd_id = md5(''.join([products_info["source"], str(products_info["sku"]), products_info["size"]]))
				brand = product_data.get('brand',{}).get('name','')
				products_info["title"] = product_data.get('name','').replace(brand,'').strip()
				products_info["descripion"] =  ""
				products_info["image_url"] = str(images)
				products_info["reference_url"]= "https://www.myntra.com/Tshirts/Puma/Puma-Men-Navy-Blue-Wording-Tee-Printed-Round-Neck-T-shirt/7034373/buy"
				products_info["aux_info"] = {"productId":product_data.get('id','')}
				products_info["created_at"] = str(datetime.now().now()).split('.')[0]
				products_info["modified_at"] = str(datetime.now().now()).split('.')[0]
				products_info["hd_id"] = hd_id
				yield products_info
				product_insights = productInsights()
				product_insights["source"] = "Myntra"
				product_insights["sku"]= size_element.get('skuId','')
				product_insights["hd_id"] = hd_id
				product_insights["size"] = size_element.get('label','')
				product_insights["category"] = product_data.get('analytics',{}).get('gender','')
				product_insights["sub_category"] = product_data.get('analytics',{}).get('articleType','')
				product_insights["brand"] = product_data.get('brand',{}).get('name','')
				product_insights["ratings_count"] = product_data.get('ratings',{}).get('totalCount',0)
				product_insights["reviews_count"] = 0
				product_insights["mrp"] = product_data.get('price',{}).get('mrp','')
				discount = product_data.get('price',{}).get('discounted','')
				if discount:
					product_insights["selling_price"] = discount
				else:
					product_insights["selling_price"] = product_insights["mrp"]
				product_insights["discount_percentage"] = product_data.get('price',{}).get('discount',{}).get('discountPercent',0)
				product_insights["is_available"] = size_element.get('inventory',0)
				product_insights["created_at"] = str(datetime.now().now()).split('.')[0]
				product_insights["modified_at"] = str(datetime.now().now()).split('.')[0]
				yield product_insights


