import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pandas as pd
import requests
from sqlalchemy import create_engine

class Bse(scrapy.Spider):
    name = 'bse_peer'

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
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/peer-group-comparison/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        scrip_code = ['500325']
        for code in scrip_code:
            params = (
                ('scripcode', code),
                ('scripcomare', ''),
            )
            url = 'https://api.bseindia.com/BseIndiaAPI/api/PeerGpCom/w?'+urlencode(params)
            yield Request(url, callback=self.parse, headers=headers)


    def parse(self, response):
        res = json.loads(response.text)
        resp = pd.DataFrame()
        for i in range(len(res['Table'])):
            x = res['Table'][i].keys()
            z={}
            for key in x:
                z[key] = res['Table'][i][key]
            resp = resp.append(z, ignore_index=True)
        resp.rename(columns = {'Change':'Change_', 'Foreign':'Foreign_'}, inplace=True)
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}?charset=utf8".format(user="root", pw="[newpassword]", db="bse"))
        resp.to_sql('peer', con = engine, if_exists = 'replace', chunksize = 1000, index=False)