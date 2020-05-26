import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import re
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pandas as pd

import requests

class Alishop(scrapy.Spider):
    name = 'alishop'

    def start_requests(self):
        headers = {
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
            'accept': '*/*',
            'referer': 'https://www.aliexpress.com/store/3878130',
            'authority': 'shoprenderview.aliexpress.com',
        }
        params = (
            ('componentKey', 'productList'),
            ('page', '1'),
            ('country', 'IN'),
            ('site', 'glo'),
            ('sellerId', '232768451'),
            ('groupId', '512980659'),
            ('clientIp', '157.46.5.254'),
            ('pageSize', '12'),
            ('selectType', 'auto'),
            ('currency', 'USD'),
            ('locale', 'en_US'),
            ('buyerId', '0'),
            ('productCount', '100'),
            ('order', 'orders_desc'),
        )
       
        url = 'https://shoprenderview.aliexpress.com/async/execute?'+urlencode(params) 
        yield Request(url, callback = self.parse, headers=headers)


    def parse(self, response):
        req = [
            'subject',
            'formatedPiecePriceStr', 
            'formatedPromotionPiecePriceStr',
            'pcDetailUrl',
            'image640Url', 
            'averageStarRate', 
            'averageStar',
            'id' 
            ]
        res = json.loads(response.text)
        data=pd.DataFrame(res['result']['products']['data'])[req]
        data.rename(columns = {'subject':'Product Name','id':'SKU ID','formatedPiecePriceStr':'MRP','formatedPromotionPiecePriceStr':'Selling Price', 'pcDetailUrl':'Product URL', 'image640Url':'Image URL', 'averageStarRate':'Average Rating', 'averageStar':'Rating'}, inplace=True)
        data['Product URL'] = data['Product URL'].apply(lambda x : 'https:'+ x) 
        data['Image URL'] = data['Image URL'].apply(lambda x : 'https:'+ x) 
        node = res.get('result',{}).get('products',{}).get('data', [])
        data1 =pd.DataFrame()
        for i in node:
            prod_url = 'https:'+i.get('pcDetailUrl', '')
            resp = requests.get(prod_url)
            res = Selector(resp)
            product_data = ''.join(res.xpath('//script[11]//text()').extract()).split('data: ')[1].split('csrfToken:')[0].strip()[:-1]
            temp_data = json.loads(product_data)
            req_fields = ['Cake Tools Type', 'Certification', 'Feature', 'Material', 'Model Number','Size','type1', 'type2', 'type3','type4','type5', 'type6']
            fields ={'Product URL':prod_url}
            for val in temp_data['specsModule']['props']:
                if val['attrName'] in req_fields:
                    fields[val['attrName']] = val['attrValue']
            temp = pd.DataFrame([fields])
            data1 = data1.append(temp)
        data1 = data1.reset_index(drop=True)
        fin_data = pd.merge(data, data1, on='Product URL')
        fin_data.to_csv('11.11store.csv', index=False)
        