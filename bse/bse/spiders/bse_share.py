import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import urllib.parse as urlparse
from urllib.parse import parse_qs, urlparse
import pandas as pd
import requests
from sqlalchemy import create_engine

class Bse(scrapy.Spider):
    name = 'bse_share'

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
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/shareholding-pattern/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        scrip_code = ['500325']
        for code in scrip_code:
            params = (
                ('qtrid', ''),
                ('scripcode', code),
            )

            # response = requests.get('https://api.bseindia.com/BseIndiaAPI/api/shpSecSummery_New/w', headers=headers, params=params)
            url = 'https://api.bseindia.com/BseIndiaAPI/api/shpSecSummery_New/w?'+urlencode(params)
            yield Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        fin_values = []
        body = json.loads(response.body)
        sel = Selector(text=body.get('Data', ''))
        node = sel.xpath('//tr/td/b[contains(text(), "Summary statement holding")]/../../following-sibling::tr')
        table_nodes = node.xpath('.//table/tr')
        data = pd.DataFrame()
        for table_node in table_nodes:
            values = []
            column_count = table_node.xpath('./td')
            for header_index in range(1, len(column_count)+1):
                inner_value = ''.join(table_node.xpath('./td[%s]/text()' % header_index).extract())
                values.append(inner_value)
            if values:
                fin_values.append(values)
        # print(fin_values)
        for val in fin_values:
            data[val[0]] = val[1:]
        data['scrip_code'] = scrip_code
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}?charset=utf8".format(user="root", pw="[newpassword]", db="bse"))
        data.to_sql('shareholding_pattern', con = engine, if_exists = 'replace', chunksize = 1000, index=False)