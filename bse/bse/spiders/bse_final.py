import json
import os
import requests
from ast import literal_eval
from urllib.parse import parse_qs, urlparse, urlencode
from MySQLdb import connect
from pandas import read_sql
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.http import Request

class Bse(Spider):
    name = 'bse_final'

    def __init__(self, *args, **kwargs):
        super(Bse, self).__init__(*args, **kwargs)
        self.conn = connect(db='bse', host='localhost', user='mca',
                            passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.crawl_list = literal_eval(kwargs.get('jsons', '[]'))
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self):
        self.cursor.close()
        self.conn.close()

    def start_requests(self):

        headers = {
            'authority': 'api.bseindia.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                          (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'origin': 'https://www.bseindia.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/\
                        reliance/500325/disclosures-insider-trading-2015/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

        for code in self.crawl_list:
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
            params1 = (
                ('fromdt', ''),
                ('scripcode', code),
                ('todt', ''),
                ('type', '2'),
                )
            params2 = (
                ('fromdt', ''),
                ('scripcode', code),
                ('todt', ''),
                ('type', '1'),
                )
            params3 = (
                ('Masterid', ''),
                ('scripcode', code),
                )
            params4 = (
                ('scripcode', code),
                ('year', '2020'),
            )
            params5 = (
                ('strCat', '-1'),
                ('strPrevDate', ''),
                ('strScrip', code),
                ('strSearch', 'A'),
                ('strToDate', ''),
                ('strType', 'C'),
            )

            self.urls = ['https://api.bseindia.com/BseIndiaAPI/api/DebActiveDebtSeries/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/SLBData/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/BoardMeeting/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/ShareHolderMeeting/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/VotingResults/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/InsiderTrade92/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/InsiderTrade15/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/SAST/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/SastAnnual/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/ConsolidatePledge/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/CorpInfoNew/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/PeerGpCom/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/BulkblockDeal/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/BulkblockDeal/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/Annexure1/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/Annexure2/w?',
                         'https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?'
                        ]

            self.tbl = [{'table_name':'debt', 'params':params},
                        {'table_name':'slb', 'params':params},
                        {'table_name':'board_meeting', 'params':params},
                        {'table_name':'share_holder_meeting', 'params':params},
                        {'table_name':'voting', 'params':params},
                        {'table_name':'corp_action', 'params':params},
                        {'table_name':'insider_1992', 'params':params},
                        {'table_name':'insider_2015', 'params':params},
                        {'table_name':'insider_sast', 'params':params},
                        {'table_name':'sast_annual', 'params':params4},
                        {'table_name':'consolidated_pledge', 'params':params},
                        {'table_name':'corp_info', 'params':params},
                        {'table_name':'peer', 'params':params},
                        {'table_name':'annual_report', 'params':params},
                        {'table_name':'block_deals', 'params':params2},
                        {'table_name':'bulk_deals', 'params':params1},
                        {'table_name':'corp_annexure_1', 'params':params3},
                        {'table_name':'corp_annexure_2', 'params':params3},
                        {'table_name':'corp_announcement', 'params':params5}
                       ]
            for url, tbl_param in zip(self.urls, self.tbl):
                url = url+urlencode(tbl_param['params'])
                yield Request(url, callback=self.parse, headers=headers, meta={'params':tbl_param})
            self.cursor.execute("update bse_crawl set crawl_status = 1 where security_code = '{0}'".format(code))

    def parse(self, response):
        sp_url = response.url.split('?')[0] + '?'
        tbl_name = response.meta['params'].get('table_name', '')
        res_url = str(response.url)
        try:
            scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        except:
            pass
        try:
            scrip_code = parse_qs(urlparse(res_url).query)['strScrip'][0]
        except:
            pass
        res = json.loads(response.text)
        if type(res) != list:
            if 'Table' in res:
                for row in res['Table']:
                    column_names = [i for i in row.keys()]
                    column_values = ()
                    for i in column_names:
                        if i == 'ATTACHMENTNAME':
                            if row[i]:
                                row[i] = 'https://www.bseindia.com/xml-data/corpfiling/AttachHis/'+row[i]
                                self.download_pdf(row[i], 1, scrip_code)
                        elif i == 'file_name':
                            if row[i]:
                                row[i] = 'https://www.bseindia.com/bseplus/AnnualReport/' +scrip_code+ '/' +row[i]
                                self.download_pdf(row[i], 2, scrip_code)
                        column_values = column_values + (row[i],)
                    try:
                        col_change = column_names.index('Change')
                        column_names[col_change] = 'change_'
                    except:
                        pass
                    try:
                        col_change = column_names.index('Foreign')
                        column_names[col_change] = 'foreign_'
                    except:
                        pass
                    if 'scrip_code' not in column_names and 'SCRIP_CODE' not in column_names and 'Fld_ScripCode' not in column_names and 'SCRIP_CD' not in column_names and 'scripcode' not in column_names and 'Fld_Scripcode' not in column_names:
                        column_names = column_names + ['scrip_code']
                        column_values = tuple(list(column_values) + [scrip_code])
                    column_names = column_names + [ 'created_at', 'modified_at']
                    column_names = [item.lower() for item in column_names if item]
                    query = "insert ignore into  {0} ({1}) values ({2}, now(), now()) on duplicate key update modified_at = now()".format(tbl_name, ','.join(column_names), (('%s,')*(len(column_names)-2)).strip(','))
                    self.cursor.execute(query, column_values)
                    self.conn.commit()
            else :
                print('No data')
        else:
            for row in res:
                column_names = [i for i in row.keys()]
                if 'scrip_code' not in column_names and 'SCRIP_CODE' not in column_names and 'Fld_ScripCode' not in column_names and 'SCRIP_CD' not in column_names and 'scripcode' not in column_names and 'Fld_Scripcode' not in column_names:
                    column_values = tuple([row[i] for i in column_names] + [scrip_code])
                    column_names = column_names + [ 'scrip_code']
                column_values = tuple([row[i] for i in column_names])
                column_names = column_names + [ 'created_at', 'modified_at']
                column_names = [item.lower() for item in column_names if item]
                query = "insert ignore  into {0} ({1}) values ({2}, now(), now()) on duplicate key update modified_at = now()".format(tbl_name, ','.join(column_names), (('%s,')*(len(column_names)-2)).strip(','))
                self.cursor.execute(query, column_values)
                self.conn.commit()

    def download_pdf(self, url, flag, scrip_code):
        home_dir = os.getenv('HOME')
        root_folder = home_dir + '/BSE_PDF/'
        annual = root_folder + 'Annual_Reports/' 
        corp = root_folder + 'Corp_Announcement/'
        if not os.path.exists(annual):
            os.makedirs(annual)
        if not os.path.exists(corp):
            os.makedirs(corp)
        headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',}
        get_response = requests.get(url, stream=True, headers=headers)
        file_name = url.split("/")[-1]
        if flag == 1:
            file_path = corp + scrip_code + '/' 
        elif flag == 2:
            file_path = annual + scrip_code + '/'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        os.chdir(file_path)
        if os.path.isfile(file_name):
            print('File Exists')
        else:
            with open(file_name, 'wb') as f:
                for chunk in get_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

