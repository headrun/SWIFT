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

class Bse(scrapy.Spider):
    name = 'bse_qrt'

    def start_requests(self):

        headers = {
            'authority': 'api.bseindia.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'origin': 'https://www.bseindia.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/financials-results/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

        params = (
            ('scripcode', '500325'),
        )

        url = 'https://api.bseindia.com/BseIndiaAPI/api/GetReportNewFor_Result/w?'+urlencode(params)
        #response = requests.get('https://api.bseindia.com/BseIndiaAPI/api/GetReportNewFor_Result/w', headers=headers, params=params)
        yield Request(url, callback = self.parse, headers=headers)

    def parse(self, response):
        # res = json.loads(response.text)
        body = json.loads(response.body)
        sel = Selector(text=body.get('QtlyinCr', ''))
        sub_heading = sel.xpath('//tbody//tr//td[@align="left"]//text()').extract()
        main_heading = sel.xpath('//body//thead//tr//td[@class="tableheading"]//text()').extract()
        col_values = sel.xpath('//tbody//tr//td[@align="right"]//text()').extract()
        data = pd.DataFrame()
        # temp = [col_values[:i] for i in range(len(main_heading), len(col_values), len(main_heading)-1)]
        temp = []
        start = 0
        for i in range(len(main_heading)-1, len(col_values)+1, len(main_heading)-1):
            temp.append(col_values[start:i])
            start = i
        data = pd.DataFrame([temp[i] for i in range(len(temp))], columns=main_heading[1:]) 
        data[sub_heading[0]] = sub_heading[1:]            
        data.to_json('qtr.json')
        data.to_csv('quarter.csv', index=False)

        body = json.loads(response.body)
        sel = Selector(text=body.get('AnninCr', ''))
        sub_heading = sel.xpath('//tbody//tr//td[@align="left"]//text()').extract()
        main_heading = sel.xpath('//body//thead//tr//td[@class="tableheading"]//text()').extract()
        col_values = sel.xpath('//tbody//tr//td[@align="right"]//text()').extract()
        data = pd.DataFrame()
        temp = []
        start = 0
        for i in range(len(main_heading)-1, len(col_values)+1, len(main_heading)-1):
            temp.append(col_values[start:i])
            start = i
        data = pd.DataFrame([temp[i] for i in range(len(temp))], columns=main_heading[1:]) 
        data[sub_heading[0]] = sub_heading[1:]            
        data.to_json('ann.json')        
        data.to_csv('ann.csv', index=False)