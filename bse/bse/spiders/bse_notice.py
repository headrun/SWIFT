import scrapy
import csv
from scrapy.selector import Selector
import pandas as pd
from scrapy.http import FormRequest
from sqlalchemy import create_engine
import MySQLdb

try:
	from urllib.parse import urlparse, urlencod, eparse_qs
except ImportError:
	from urllib.parse import parse_qs,urlparse
	from urllib.parse import urlencode

class Bse(scrapy.Spider):
	name = "bse_notice"

	def __init__(self, *args, **kwargs):
		super(Bse, self).__init__(*args, **kwargs)

	def start_requests(self):
		scrip_code = ['500325']
		conn = MySQLdb.connect(db ='bse', host='localhost', user='mca', passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
		cur = conn.cursor()
		cur.execute("truncate notice")
		for code in scrip_code:
			url = 'https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?txtscripcd='+code
			yield scrapy.Request(url, callback=self.parse)

	def parse(self, response):
		res_url = str(response.url)
		scrip_code = parse_qs(urlparse(res_url).query)['txtscripcd'][0]
		conn = MySQLdb.connect(db ='bse', host='localhost', user='mca', passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
		cur = conn.cursor()
		sel = Selector(response)
		whole_table = sel.xpath('//table[contains(@class, "mGrid")]//tr[not(contains(@class,"pgr"))]')[1:-1]
		data1 = pd.DataFrame()
		for node in whole_table:
			notice_no = ''.join(node.xpath('./td[@class="TTRow"][1]//text()').extract()).strip()
			subject = ''.join(node.xpath('./td[@class="TTRow_left"]//a//text()').extract()).strip()
			subject_link  = ''.join(node.xpath('./td[2]//a[@class="tablebluelink"]/@href').extract()).strip()
			segmentname = ''.join(node.xpath('./td[@class="TTRow"][2]//text()').extract())
			categoryname = ''.join(node.xpath('./td[@class="TTRow"][3]//text()').extract()).strip()
			departmentname = ''.join(node.xpath('./td[@class="TTRow"][4]//text()').extract()).strip()
			sub_link = 'https://www.bseindia.com/'+ subject_link
			fields={'notice_no':notice_no,'subject':subject, 'subject_link':sub_link, 'segmentname':segmentname,'categoryname':categoryname, 'departmentname':departmentname }
			temp = pd.DataFrame([fields])
			data1 = data1.append(temp)
		page_no = response.meta.get('page_no', '2')
		data = {
			'__EVENTTARGET': 'ctl00$ContentPlaceHolder1$GridView2',
			'__EVENTARGUMENT': 'Page$%s'%page_no,
			'__VIEWSTATE': ''.join(sel.xpath('//input[@id="__VIEWSTATE"]/@value').extract()),
			'__VIEWSTATEGENERATOR': ''.join(sel.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').extract()),
			'__VIEWSTATEENCRYPTED': '',
			'__EVENTVALIDATION': ''.join(sel.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract()),
			'ctl00$ContentPlaceHolder1$txtDate': '01/06/2020',
			'ctl00$ContentPlaceHolder1$hidCurrentDate': '',
			'ctl00$ContentPlaceHolder1$txtTodate': '08/06/2020',
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
		yield FormRequest(response.url, formdata=data, callback=self.parse, meta=meta)
		data1['scrip_code'] = scrip_code
		column_names = data1.columns.to_list()
		for i in range(len(data1)):
			column_values = tuple(data1.iloc[i].values)
			values_ = ['%s']* len(column_names)
			query  = "insert ignore into  notice ({0}) values ({1})".format(','.join(column_names), (('%s,')*len(column_names)).strip(','))
			cur.execute(query, column_values)
			conn.commit()
		cur.close()
		conn.close()
