import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import urllib.parse as urlparse
from urllib.parse import parse_qs,urlparse
import pandas as pd
import requests
from sqlalchemy import create_engine

class Bse(scrapy.Spider):
    name = 'bse_final'

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
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/disclosures-insider-trading-2015/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        scrip_code = ['500325']
        for code in scrip_code:
            params = (
                ('fromdt', ''),
                ('scripcode', code),
                ('todt', ''),
                ('year', ''),
                ('Masterid', ''),
                ('type', '0'),
                ('scripcomare', ''),
                ('qtrid', '')
            )

        self.urls = ['https://api.bseindia.com/BseIndiaAPI/api/DebActiveDebtSeries/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/SLBData/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/BoardMeeting/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/ShareHolderMeeting/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/VotingResults/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/InsiderTrade15/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/InsiderTrade92/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/SAST/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/SastAnnual/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/ConsolidatePledge/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/CorpInfoNew/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/PeerGpCom/w?',
                'https://api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?',                
                ]
        self.tbl = {self.urls[0]:'debt', self.urls[1]:'slb',self.urls[2]:'board_meeting', self.urls[3]:'share_holder_meeting', self.urls[4]: 'voting', self.urls[5]: 'corp_action', self.urls[6]:'insider_2015',self.urls[7]:'insider_1992',self.urls[8]:'insider_sast',self.urls[9]:'sast_annual',self.urls[10]:'consolidated_pledge',self.urls[11]:'corp_info',self.urls[12]:'peer',self.urls[13]:'annual_report'}
        for ur in self.urls:
            url = ur+urlencode(params)      
            yield Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        sp_url = response.url.split('?')[0] + '?'
        tbl_name = self.tbl[sp_url]
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        res = json.loads(response.text)
        resp = pd.DataFrame()
        for i in range(len(res['Table'])):
            x = res['Table'][i].keys()
            r={}
            for key in x:
                r[key] = res['Table'][i][key]
            resp = resp.append(r, ignore_index=True)
            resp['scrip_code'] = scrip_code
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}?charset=utf8".format(user="root",pw='' ,db="bse"))
        resp.to_sql(tbl_name, con = engine, if_exists = 'replace', chunksize = 1000, index=False)