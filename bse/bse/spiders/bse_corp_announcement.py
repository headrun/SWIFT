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
    name = 'bse_corp_announcement'

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
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/corp-announements/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        scrip_code = ['500325']
        for code in scrip_code:    
            params = (
                ('strCat', '-1'),
                ('strPrevDate', '20100101'),
                ('strScrip', code),
                ('strSearch', 'A'),
                ('strToDate', '20200603'),
                ('strType', 'C'),
            )
            url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?'+urlencode(params)
            yield Request(url, callback = self.parse, headers=headers)

    def parse(self, response):
        res = json.loads(response.text)
        data1 = pd.DataFrame()
        for i in range(len(res['Table'])):
            x = res['Table'][i].keys()
            r={}
            for key in x:
                if key == 'ATTACHMENTNAME':
                    if res['Table'][i][key] != '':
                        r[key] = 'https://www.bseindia.com/xml-data/corpfiling/AttachHis/'+res['Table'][i][key]
                        # self.download_pdf(r[key])
                    else:
                        r[key] = res['Table'][i][key]
                    
                else:
                    r[key] = res['Table'][i][key]
                
            data1 = data1.append(r, ignore_index=True)
        conn = MySQLdb.connect(db ='bse', host='localhost', user='mca', passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        cur = conn.cursor()
        column_names = data1.columns.to_list()
        for i in range(len(data1)):
            column_values = tuple(data1.iloc[i].values)
            values_ = ['%s']* len(column_names)
            query  = "insert ignore into corp_announcement  ({0}) values ({1})".format(','.join(column_names), (('%s,')*len(column_names)).strip(','))
            cur.execute(query, column_values)
            conn.commit()
        cur.close()
        conn.close()

    def download_pdf(self, url):
        get_response = requests.get(url,stream=True)
        file_name  = url.split("/")[-1]
        file_path = '/home/headrun/anandhu/projects/BSE_Corp_Announcement/' + file_name
        with open(file_path, 'wb') as f:
            for chunk in get_response.iter_content(chunk_size=1024):
                if chunk: 
                    f.write(chunk)
