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
    name = 'bse'

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

        params = (
            ('strCat', '-1'),
            ('strPrevDate', '20100101'),
            ('strScrip', '500325'),
            ('strSearch', 'A'),
            ('strToDate', '20200525'),
            ('strType', 'C'),
        )
        url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?'+urlencode(params)
        #response = requests.get('https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w', headers=headers, params=params)
        yield Request(url, callback = self.parse, headers=headers)

    def parse(self, response):
        # import pdb;pdb.set_trace()
        res = json.loads(response.text)
        req = ["NEWSID",
        "SCRIP_CD",
        "XML_NAME"
        "NEWSSUB",
        "MORE",
        "AGENDA_ID",       
        "ATTACHMENTNAME",
        "CATEGORYNAME"
        ,"NEWSSUB"
        ,"SLONGNAME"
        ,"SCRIP_CD"
        ,"NEWS_DT"
        ,"TimeDiff"
        ,"DissemDT"]
        announcement = pd.DataFrame()
        for i in range(len(res['Table'])):
            x = res['Table'][i].keys()
            r={}
            for key in x:
                if key in req:
                    if key == 'ATTACHMENTNAME':
                        if res['Table'][i][key] != '':
                            r[key] = 'https://www.bseindia.com/xml-data/corpfiling/AttachHis/'+res['Table'][i][key]
                            self.download_pdf(r[key])
                        else:
                            r[key] = res['Table'][i][key]
                        
                    else:
                        r[key] = res['Table'][i][key]
                
            # announcement[i]=r
            announcement = announcement.append(r, ignore_index=True)
        print(announcement)
        announcement.to_csv('bes.csv')

    def download_pdf(self, url):
        get_response = requests.get(url,stream=True)
        file_name  = url.split("/")[-1]
        file_path = '/home/headrun/anandhu/projects/BSE_Corp_Announcement/' + file_name
        with open(file_path, 'wb') as f:
            for chunk in get_response.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)