import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import re
import urllib.parse as urlparse
from urllib.parse import parse_qs
from urllib.parse import parse_qs, urlparse
import pandas as pd
import requests
from sqlalchemy import create_engine
import MySQLdb

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
        scrip_code = ['500325']
        for code in scrip_code:
            params = (
                ('scripcode', code),
            )

            url = 'https://api.bseindia.com/BseIndiaAPI/api/GetReportNewFor_Result/w?'+urlencode(params)
            yield Request(url, callback = self.parse, headers=headers)

    def parse(self, response):
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        conn = MySQLdb.connect(db ='bse', host='localhost', user='mca', passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        cur = conn.cursor()
        body = json.loads(response.body)
        sel = Selector(text=body.get('QtlyinCr', ''))
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
        data['scrip_code'] = scrip_code
        column_names = data.rename(columns={'Mar-20':'`Mar-20`', 'Dec-19':'`Dec-19`', 'Sep-19':'`Sep-19`','Jun-19':'`Jun-19`', 'Mar-19':'`Mar-19`', 'FY 19-20': '`FY 19-20`', 'Income Statement': '`Income Statement`'},inplace=True )
        column_names = data.columns.to_list()
        cur.execute("truncate results_qrt")
        for i in range(len(data)):
            column_values = tuple(data.iloc[i].values)
            values_ = ['%s']* len(column_names)
            query  = "insert ignore into results_qrt  ({0}) values ({1})".format(','.join(column_names), (('%s,')*len(column_names)).strip(','))
            cur.execute(query, column_values)
            conn.commit()

        body = json.loads(response.body)
        conn = MySQLdb.connect(db ='bse', host='localhost', user='mca', passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        cur = conn.cursor()
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
        data['scrip_code'] = scrip_code    
        column_names = data.rename(columns={'2020':'`2020`', '2019':'`2019`', '2018':'`2018`','2017':'`2017`', '2016':'`2016`', 'Income Statement': '`Income Statement`'},inplace=True )
        column_names = data.columns.to_list()
        cur.execute("truncate results_annual")
        for i in range(len(data)):
            column_values = tuple(data.iloc[i].values)
            values_ = ['%s']* len(column_names)
            query  = "insert ignore into  results_annual ({0}) values ({1})".format(','.join(column_names), (('%s,')*len(column_names)).strip(','))
            cur.execute(query, column_values)
            conn.commit()
        cur.close()
        conn.close()
