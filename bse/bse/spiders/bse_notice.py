import scrapy
import csv
from scrapy.selector import Selector
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import parse_qs, urlparse

class Bse(scrapy.Spider):
	name = "bse_notice"

	def __init__(self, *args, **kwargs):
		super(Bse, self).__init__(*args, **kwargs)

	def start_requests(self):
		scrip_code = ['500325']
		for code in scrip_code:
			url = 'https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?txtscripcd='+code
			yield scrapy.Request(url, callback=self.parse)

	def parse(self, response):
		res_url = str(response.url)
		scrip_code = parse_qs(urlparse(res_url).query)['txtscripcd'][0]
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
			print('Notice:',notice_no, 'Sub:',subject, 'Sub_Link:',subject_link, 'segmentname:',segmentname, 'Category:',categoryname, 'Dept:',departmentname)
			sub_link = 'https://www.bseindia.com/'+ subject_link
			fields={'notice_no':notice_no,'subject':subject, 'subject_link':sub_link, 'segmentname':segmentname,'categoryname':categoryname, 'departmentname':departmentname }
			temp = pd.DataFrame([fields])
			data1 = data1.append(temp)
		data1['scrip_code'] = scrip_code
		engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}?charset=utf8".format(user="root", pw="[newpassword]", db="bse"))
		data1.to_sql('notice', con = engine, if_exists = 'replace', chunksize = 1000, index=False)