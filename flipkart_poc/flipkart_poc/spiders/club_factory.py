import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import re
import MySQLdb
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pandas as pd

import requests

class Newshow(scrapy.Spider):
	name = 'newshow'
    
	def start_requests(self):

		headers = {
			'authority': 'www.clubfactory.com',
			'pragma': 'no-cache',
			'cache-control': 'no-cache',
			'upgrade-insecure-requests': '1',
			'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'sec-fetch-site': 'none',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-user': '?1',
			'sec-fetch-dest': 'document',
			'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
		}

		params = (
			('pageSize', '40'),
			('offset', '80'),
			('storeId', '59361'),
		)

		data = '{"headers":{"Content-Type":"application/json"}}'
		url = 'https://www.clubfactory.com/gw/cf-search/api/v1/seller/products?'+urlencode(params) 
		yield Request(url, callback = self.parse, headers=headers)


	def parse(self, response):
		req = [
            'name',
            'cPlatformPriceLocal', 
            'listPriceLocal',
            'imageUrl', 
            'averageRating', 
            'id', 
            ]
		res = json.loads(response.text)
		data=pd.DataFrame(res['body']['products'])[req]
		data.rename(columns = {'name':'Product Name','cPlatformPriceLocal':'MRP','listPriceLocal':'Selling Price', 'imageUrl':'Image URL', 'averageRating':'Rating', 'product_id':'Product Rating'}, inplace=True)
		node = res.get('body',{}).get('products',{})
		data1 =pd.DataFrame()
		for i in node:
			product_id = i.get('id')
			prod_url = 'http://www.clubfactory.com/gw/cf-detail/api/v1/product/info?productId='+str(product_id)
			resp = requests.get(prod_url)
			res1 = json.loads(resp.text)
			img_list = res1['body']['productImageList']
			size = res1['body']['attributeDTOList'][0]['values']
			specs = res1['body']['specificsDTOList']
			temp_dict = {'id':product_id,'img_list': res1['body']['productImageList'],
			 'size':[x['value'] for x in res1['body']['attributeDTOList'][0]['values'] ] }
			for val in specs:
				temp_dict[val['key']] = val['value']
			data1 = data1.append(temp_dict, ignore_index=True)
		data1 = data1.reset_index(drop=True)
		fin_data = pd.merge(data, data1, on='id')
		fin_data['Rating'] = data['Rating'].apply(lambda x: None if x == -1.0 else x)
		fin_data.to_csv('newshow_2.csv', index = False)
