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
category_array = ['men-tshirts']

class MyntraSpider(scrapy.Spider):
	name = "MyntraUrls"
	def __init__(self, *args, **kwargs):
		create_default_dirs()
		self.out_put_file = get_out_file("%s__%s" % ('url_queue_meta', self.name))
		dispatcher.connect(self.spider_closed, signals.spider_closed)
	def spider_closed(self, spider):
		crawlout_processing(self.out_put_file)
	def start_requests(self):
		yield Request('https://www.myntra.com/web/v1/search/men-tshirts?p=3&rows=50&o=99',headers=headers,callback=self.parse)	
	def parse(self,response):
		final_dict = []
		for ele in category_array:
			params = (
			    ('p', '1'),
			    ('rows', '50'),
			    ('sort', 'popularity'),
			    ('o', '0'),
			)
			url = 'https://www.myntra.com/web/v1/search/'+ele+'?'+urlencode(params)
			response = requests.get(url,headers=headers)
			res_data = json.loads(response.text)
			totalNumberOfRecords = res_data.get('totalCount',0)
			total_page = math.ceil(totalNumberOfRecords/100)
			url = 'https://www.myntra.com/web/v1/search/'+ele+'?'
			for i in range(1,total_page+1,1):
				if i==1:
					starting_index = 0
				else:
					starting_index = (i-1)*100-1
				params = (
				    ('p', str(i)),
				    ('rows', '100'),
				    ('sort', 'popularity'),
				    ('o', str(starting_index)),
				)
				request_url = url+urlencode(params)
				res = requests.get(request_url,headers=headers)
				res_json_data = json.loads(res.text)
				products = res_json_data.get('products',[])
				for product in products:
					final_sub_dict = urlQueuemeta()
					final_sub_dict["sk"] = product.get('productId','')
					final_sub_dict["url"] = "https://www.myntra.com/" + product.get('landingPageUrl','')
					final_sub_dict["source"] = "Myntra"
					final_sub_dict["crawl_type"] = "Keepup"
					final_sub_dict["content_type"] = "Fashions"
					final_sub_dict["related_type"] = ""
					final_sub_dict["crawl_status"] = 0
					final_sub_dict["meta_data"] = ""
					final_sub_dict["created_at"] = str(datetime.now().now()).split('.')[0]
					final_sub_dict["modified_at"] = str(datetime.now().now()).split('.')[0]
					yield final_sub_dict


