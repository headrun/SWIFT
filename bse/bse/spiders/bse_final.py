import json
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
        table_name = ['block_deals', 'bulk_deals', 'insider_sast', 'board_meeting',
                      'share_holder_meeting', 'voting', 'corp_action', 'insider_1992',
                      'insider_2015', 'sast_annual', 'corp_info']
        for tbl in table_name:
            self.cursor.execute("truncate {0}".format(tbl))
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
        df = read_sql('select * from bse_crawl', self.conn)
        scrip_code = df['security_code'].to_list()
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

            self.tbl = {self.urls[0]:{'table_name':'debt', 'params':params},
                        self.urls[1]:{'table_name':'slb', 'params':params},
                        self.urls[2]:{'table_name':'board_meeting', 'params':params},
                        self.urls[3]:{'table_name':'share_holder_meeting', 'params':params},
                        self.urls[4]: {'table_name':'voting', 'params':params},
                        self.urls[5]:{'table_name':'corp_action', 'params':params},
                        self.urls[6]:{'table_name':'insider_1992', 'params':params},
                        self.urls[7]:{'table_name':'insider_2015', 'params':params},
                        self.urls[8]:{'table_name':'insider_sast', 'params':params},
                        self.urls[9]:{'table_name':'sast_annual', 'params':params4},
                        self.urls[10]:{'table_name':'consolidated_pledge', 'params':params},
                        self.urls[11]:{'table_name':'corp_info', 'params':params},
                        self.urls[12]:{'table_name':'peer', 'params':params},
                        self.urls[13]:{'table_name':'annual_report', 'params':params},
                        self.urls[14]:{'table_name':'block_deals', 'params':params1},
                        self.urls[15]:{'table_name':'bulk_deals', 'params':params2},
                        self.urls[16]:{'table_name':'corp_annexure_1', 'params':params3},
                        self.urls[17]:{'table_name':'corp_annexure_2', 'params':params3},
                        self.urls[18]:{'table_name':'corp_announcement', 'params':params5}
                       }
            for ur in self.urls:
                url = ur+urlencode(self.tbl[ur]['params'])
                yield Request(url, callback=self.parse, headers=headers)
            self.cursor.execute("update bse_crawl set crawl_status = 1 where security_code = '{0}'".format(code))

    def parse(self, response):
        sp_url = response.url.split('?')[0] + '?'
        tbl_name = self.tbl[sp_url]['table_name']
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
            for row in res['Table']:
                column_names = [i for i in row.keys()]
                column_values = ()
                for i in column_names:
                    if i == 'ATTACHMENTNAME':
                        if row[i]:
                            row[i] = 'https://www.bseindia.com/xml-data/corpfiling/AttachHis/'+row[i]
                            self.download_pdf(row[i], 1)
                    elif i == 'file_name':
                        if row[i]:
                            row[i] = 'https://www.bseindia.com/bseplus/AnnualReport/' +scrip_code+ '/' +row[i]
                            self.download_pdf(row[i], 2)
                    column_values = column_values + (row[i],)
                try:
                    col_change = column_names.index('Change')
                    column_names[col_change] = 'Change_'
                except:
                    pass
                try:
                    col_change = column_names.index('Foreign')
                    column_names[col_change] = 'Foreign_'
                except:
                    pass
                if 'scrip_code' not in column_names and 'SCRIP_CODE' not in column_names and 'Fld_ScripCode' not in column_names and 'SCRIP_CD' not in column_names:
                    column_names = column_names + ['scrip_code']
                    column_values = tuple(list(column_values) + [scrip_code])
                query = "insert ignore into  {0} ({1}) values ({2})".format(tbl_name, ','.join(column_names), (('%s,')*len(column_names)).strip(','))
                self.cursor.execute(query, column_values)
                self.conn.commit()
        else:
            for row in res:
                column_names = [i for i in row.keys()]
                column_values = tuple([row[i] for i in column_names])
                query = "insert ignore  into {0} ({1}) values ({2})".format(tbl_name, ','.join(column_names), (('%s,')*len(column_names)).strip(','))
                self.cursor.execute(query, column_values)
                self.conn.commit()

    def download_pdf(self, url, flag):
        get_response = requests.get(url, stream=True)
        file_name = url.split("/")[-1]
        if flag == 1:
            file_path = '/home/mca/Corp_Announcement' + file_name
        elif flag == 2:
            file_path = '/home/mca/Annual_Reports/' + file_name
        with open(file_path, 'wb') as f:
            for chunk in get_response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
