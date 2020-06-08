import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pandas as pd
import requests
import MySQLdb

class Bse(scrapy.Spider):
    name = 'bse_annex2'

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
        scrip_code = ['500325']
        for code in scrip_code:
            params = (
                ('Masterid', ''),
                ('scripcode', code),
            )

            url = 'https://api.bseindia.com/BseIndiaAPI/api/Annexure2/w?'+urlencode(params)
            yield Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        res = json.loads(response.text)
        resp = pd.DataFrame()
        conn = MySQLdb.connect(db ='bse', host='localhost', user='mca', passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        cur = conn.cursor()
        for row in res:
            column_names = [i for i in row.keys()]
            column_values = tuple([row[i] for i in column_names])
            values_ = ['%s']* len(column_names)
            query  = "insert ignore into  corp_annexure_2 ({0}) values {1}".format(','.join(column_names), tuple(values_))
            cur.execute(query % column_values)
            conn.commit()
        cur.close()
        conn.close()
