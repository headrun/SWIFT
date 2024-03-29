import json
import datetime
from ast import literal_eval
from urllib.parse import parse_qs, urlparse, urlencode
from collections import OrderedDict
from MySQLdb import connect
from pandas import DataFrame
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
from scrapy.selector import Selector


class Bse(Spider):
    name = 'bse_xpath'

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
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'origin': 'https://www.bseindia.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.bseindia.com/stock-share-price/reliance-industries-ltd/reliance/500325/financials-results/',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        for code in self.crawl_list:
            params = [('scripcode', code)]

            financials_url = 'https://api.bseindia.com/BseIndiaAPI/api/GetReportNewFor_Result/w?%s' % urlencode(
                params)
            yield Request(financials_url, callback=self.parse_financials, headers=headers)

            params.append(('qtrid', ''))
            shares_url = 'https://api.bseindia.com/BseIndiaAPI/api/shpSecSummery_New/w?%s' % urlencode(
                params)
            yield Request(shares_url, callback=self.parse_shares, headers=headers)

            notice_url = 'https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?txtscripcd=%s' % code
            yield Request(notice_url, callback=self.parse_notices, headers=headers)

    def parse_financials(self, response):
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        body = json.loads(response.body)
        terms = ['QtlyinCr', 'AnninCr']
        for term in terms:
            sel = Selector(text=body.get(term, ''))
            if term in 'QtlyinCr':
                category = 'results_qrt'
            else:
                category = 'results_annual'
            sub_heading = sel.xpath(
                '//tbody//tr//td[@align="left"]//text()').extract()
            main_heading = sel.xpath(
                '//body//thead//tr//td[@class="tableheading"]//text()').extract()
            col_values = sel.xpath(
                '//tbody//tr//td[@align="right"]//text()').extract()
            standalone = sel.xpath(
                '//tbody/tr/td/a[contains(text(),"Standalone")]/@href').extract()
            consolidated = sel.xpath(
                '//tbody/tr/td/a[contains(text(),"Consolidated")]/@href').extract()
            segment = sel.xpath(
                '//tbody/tr/td/a[contains(text(),"Segment")]/@href').extract()
            if term in 'QtlyinCr':
                table = 'result_qrt_trends'
            else:
                table = 'result_annual_trends'
            for urls in standalone:
                standalone_url = 'https://www.bseindia.com' + urls
                yield Request(standalone_url, callback=self.segment_parse, meta={'tbl_name': table, 'type': 'standalone'})
            for urls in consolidated:
                consolidated_url = 'https://www.bseindia.com' + urls
                yield Request(consolidated_url, callback=self.segment_parse, meta={'tbl_name': table, 'type': 'consolidated'})
            for urls in segment:
                segment_url = 'https://www.bseindia.com' + urls
                yield Request(segment_url, callback=self.segment_parse, meta={'tbl_name': table, 'type': 'segment'})

            data = DataFrame()
            temp = []
            start = 0
            for i in range(len(main_heading)-1, len(col_values)+1, len(main_heading)-1):
                temp.append(col_values[start:i])
                start = i
            data = DataFrame([temp[i] for i in range(len(temp))],
                             columns=main_heading[1:])
            data[sub_heading[0]] = sub_heading[1:]
            data = [data.columns.tolist()] + data.values.tolist()
            lis_ = []
            for row in data[1:]:
                ds = OrderedDict()
                for idx, val in enumerate(row):
                    ds[data[0][idx]] = val
                lis_.append(ds)
            output_dict = {category: lis_}
            query = "insert ignore into %s (scrip_code, json_value, created_at, updated_at) values('%s', '%s', now(), now())"
            self.cursor.execute(
                query % (category, scrip_code, json.dumps(output_dict)))
            self.conn.commit()

    def segment_parse(self, response):
        res_url = str(response.url)
        tbl_name = response.meta['tbl_name']
        res_type = response.meta['type']
        scrip_code = parse_qs(urlparse(res_url).query)['Code'][0]
        res = Selector(response)
        trs = res.xpath('//table[@id="ContentPlaceHolder1_tbl_typeID"]/tr')
        fin_out = {}
        for tr in trs:
            tds = tr.xpath('td/text()').extract()
            if len(tds) == 2:
                key, value = tds
                fin_out[key] = value
            elif len(tds) == 1:
                fin_out[tds[0]] = ''
        dt = fin_out['Date End']
        try:
            end_date = datetime.datetime.strptime(
                dt, '%d-%b-%y').strftime('%Y-%m-%d')
        except:
            pass
        try:
            end_date = datetime.datetime.strptime(
                dt, '%d-%b-%Y').strftime('%Y-%m-%d')
        except:
            pass
        query = "insert into %s (scrip_code, res_type, end_date, json_value, created_at, updated_at) values('%s', '%s', '%s', '%s', now(), now()) on duplicate key update json_value = '%s'"
        self.cursor.execute(query % (tbl_name, scrip_code, res_type,
                                     end_date, json.dumps(fin_out), json.dumps(fin_out)))
        self.conn.commit()

    def parse_shares(self, response):
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['scripcode'][0]
        category = 'shareholding_pattern'
        fin_values = []
        body = json.loads(response.body)
        sel = Selector(text=body.get('Data', ''))
        node = sel.xpath(
            '//tr/td/b[contains(text(), "Summary statement holding")]/../../following-sibling::tr')
        table_nodes = node.xpath('.//table/tr')
        data = DataFrame()
        for table_node in table_nodes:
            values = []
            column_count = table_node.xpath('./td')
            for header_index in range(1, len(column_count)+1):
                inner_value = ''.join(table_node.xpath(
                    './td[%s]/text()' % header_index).extract())
                values.append(inner_value)

            if values:
                fin_values.append(values)
        lis_ = []
        for row in fin_values[1:]:
            ds = OrderedDict()
            for idx, val in enumerate(row):
                ds[fin_values[0][idx]] = val
            lis_.append(ds)
        output_dict = {category: lis_}
        query = "insert ignore into shareholding_pattern (scrip_code, json_value, created_at, updated_at) values('%s', '%s', now(), now())"
        self.cursor.execute(query % (scrip_code, json.dumps(output_dict)))
        self.conn.commit()

    def parse_notices(self, response):
        res_url = str(response.url)
        scrip_code = parse_qs(urlparse(res_url).query)['txtscripcd'][0]
        sel = Selector(response)
        whole_table = sel.xpath(
            '//table[contains(@class, "mGrid")]//tr[not(contains(@class,"pgr"))]')[1:-1]
        data1 = DataFrame()
        for node in whole_table:
            notice_no = ''.join(node.xpath(
                './td[@class="TTRow"][1]//text()').extract()).strip()
            subject = ''.join(node.xpath(
                './td[@class="TTRow_left"]//a//text()').extract()).strip()
            subject_link = ''.join(node.xpath(
                './td[2]//a[@class="tablebluelink"]/@href').extract()).strip()
            segmentname = ''.join(node.xpath(
                './td[@class="TTRow"][2]//text()').extract())
            categoryname = ''.join(node.xpath(
                './td[@class="TTRow"][3]//text()').extract()).strip()
            departmentname = ''.join(node.xpath(
                './td[@class="TTRow"][4]//text()').extract()).strip()
            sub_link = 'https://www.bseindia.com/' + subject_link
            fields = {'notice_no': notice_no, 'subject': subject, 'subject_link': sub_link,
                      'segmentname': segmentname, 'categoryname': categoryname, 'departmentname': departmentname}
            temp = DataFrame([fields])
            data1 = data1.append(temp)

        page_no = response.meta.get('page_no', '2')
        data = {
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$GridView2',
            '__EVENTARGUMENT': 'Page$%s' % page_no,
            '__VIEWSTATE': ''.join(sel.xpath('//input[@id="__VIEWSTATE"]/@value').extract()),
            '__VIEWSTATEGENERATOR': ''.join(sel.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').extract()),
            '__VIEWSTATEENCRYPTED': '',
            '__EVENTVALIDATION': ''.join(sel.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract()),
            'ctl00$ContentPlaceHolder1$txtDate': '',
            'ctl00$ContentPlaceHolder1$hidCurrentDate': '',
            'ctl00$ContentPlaceHolder1$txtTodate': '',
            'ctl00$ContentPlaceHolder1$txtNoticeNo': '',
            'ctl00$ContentPlaceHolder1$ddlSegment': 'All',
            'ctl00$ContentPlaceHolder1$ddlCategory': 'All',
            'ctl00$ContentPlaceHolder1$ddlDep': 'All',
            'ctl00$ContentPlaceHolder1$GetQuote1_hdnCode': '',
            'ctl00$ContentPlaceHolder1$SmartSearch$hdnCode': '',
            'ctl00$ContentPlaceHolder1$SmartSearch$smartSearch': '',
            'ctl00$ContentPlaceHolder1$hf_scripcode': '',
            'ctl00$ContentPlaceHolder1$txtSub': '',
            'ctl00$ContentPlaceHolder1$txtfreeText': '',
            'ctl00$ContentPlaceHolder1$hdnPrevious': '',
            'ctl00$ContentPlaceHolder1$hdnNext': '',
            'ctl00$ContentPlaceHolder1$hdnPreDay': '',
            'ctl00$ContentPlaceHolder1$hdnNextDay': ''
        }
        meta = response.meta
        meta['page_no'] = int(page_no) + 1
        yield FormRequest(response.url, formdata=data, callback=self.parse_notices, meta=meta)
        data1['scrip_code'] = scrip_code
        column_names = data1.columns.to_list()
        column_names = column_names + ['created_at', 'modified_at']
        column_names = [item.lower() for item in column_names if item]
        for i in range(len(data1)):
            column_values = tuple(data1.iloc[i].values)
            query = "insert ignore into  notice ({0}) values ({1}, now(), now()) on duplicate key update modified_at = now()".format(
                ','.join(column_names), (('%s,')*(len(column_names)-2)).strip(','))
            self.cursor.execute(query, column_values)
            self.conn.commit()
