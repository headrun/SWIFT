import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import urllib.parse as urlparse
from urllib.parse import parse_qs
from urllib.parse import parse_qs, urlparse
import pandas as pd
import requests
from sqlalchemy import create_engine

class Bse(scrapy.Spider):
    name = 'bse_annex1'

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
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/corporate-governance/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        scripcode = ['500325']
        for code in scripcode:
            params = (
                ('Masterid', ''),
                ('scripcode', code),
            )

            url = 'https://api.bseindia.com/BseIndiaAPI/api/Annexure1/w?'+urlencode(params)
            yield Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        res = json.loads(response.text)
        resp = pd.DataFrame()
        for i in res:
            x = i.keys()
            r={}
            for key in x:
                r[key] = i[key]
            resp = resp.append(r, ignore_index=True)
        resp['scrip_code'] = scrip_code
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}?charset=utf8".format(user="root", pw="[newpassword]", db="bse"))
        resp.to_sql('corp_annexure_1', con = engine, if_exists = 'replace', chunksize = 1000, index=False)
            