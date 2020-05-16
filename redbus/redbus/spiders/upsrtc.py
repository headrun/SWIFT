#from urllib.parse import urlencode
from scrapy.http import Request, FormRequest
from scrapy.spiders import  Spider
from scrapy.selector import Selector
import json
import re
import datetime
import MySQLdb
import itertools
import pandas as pd
import urllib.parse as urlparse
from urllib.parse import parse_qs
from scrapy.utils.project import get_project_settings

import requests

SETTINGS = get_project_settings()

HOST = SETTINGS['DB_HOST']
USERNAME = SETTINGS['DB_USERNAME']
PASSWORD = SETTINGS['DB_PASSWORD']
DATABASE = SETTINGS['DATABASE']


class Upsrtc(Spider):
    name = 'upsrtconline'

    def __init__(self, start='', stop='', date='', *args, **kwargs):
      super(Upsrtc, self).__init__(*args, **kwargs)
      self.start=start
      self.stop=stop
      self.date=date


    def start_requests(self):
      
      self.headers = {
          'Connection': 'keep-alive',
          'Pragma': 'no-cache',
          'Cache-Control': 'no-cache',
          'Upgrade-Insecure-Requests': '1',
          'Origin': 'https://upsrtconline.co.in',
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
          'Sec-Fetch-Site': 'same-origin',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-User': '?1',
          'Sec-Fetch-Dest': 'document',
          'Referer': 'https://upsrtconline.co.in/',
          'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
      }

      fname = open('upsrtc.json')
      json_data = json.load(fname)
      routes = [json_data['fromstop'][i]+'-'+json_data['tostop'][i] for i in range(len(json_data['tostop']))] 
      dates = [d for d in json_data['journeyDate']]
      tot_route = list(itertools.product(routes, dates))
      for route in tot_route:
        data = (
          ('method', 'busSearch1'),
          ('thisPage', 'TRUE'),
          ('currentdate', '07/05/2020'),
          ('bus_service_no', ''),
          ('bus_type_cd', ''),
          ('deptTm', ''),
          ('deptTm', '00:00'),
          ('route_nm', ''),
          ('adult_fare', '0.0'),
          ('child_fare', '0.0'),
          ('flag_flat_rt', ''),
          ('bus_type_nm', ''),
          ('from_bus_stopn_name', ''),
          ('to_bus_stopn_name', ''),
          ('bus_dept_dt', ''),
          ('route_no', ''),
          ('brd_TM', ''),
          ('fromstop', route[0].split('-')[0]),
          ('tostop', route[0].split('-')[1]),
          ('journeyDate', route[1]),
          ('busservicetype', 'ALL'),
          ('searchCaptcha', ''),
        )
        url = 'https://upsrtconline.co.in/ticket_booking/bussearch1?'
        # response = requests.post(url , headers=headers, cookies=cookies, data=data)
        yield FormRequest(url, headers=self.headers, callback=self.parse, formdata=data, method="GET")

    def parse(self, response):
        # print (response)
        parsed = urlparse.urlparse(response.url)
        src_frm = parse_qs(parsed.query)['fromstop'][0]
        src_to = parse_qs(parsed.query)['tostop'][0]
        price = []
        nodes = response.xpath('//div[@id="divNewTable"]//table[@class="maintable"]//tr')
        for node in nodes[2:]:
            date = node.xpath('.//td[@class="smallsemitext bdrbottomleft"]//text()')[0].extract()
            datetime1 = datetime.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
            arrival_time = datetime.datetime.strftime(datetime1, "%d-%m-%Y %H:%M:%S")
            service_name = node.xpath('.//td[@class="smallsemitext bdrbottomleft"]//text()')[1].extract()
            route_link = ''.join(node.xpath('.//td[@class="smallsemitext bdrbottomleft"]//a//@href').extract()).split('(')[-1].split(',')
            service_id = route_link[1].strip("'")
            csrfPreventionSalt = ''.join(node.xpath('.//input[@id="csrfPreventionSalt"]/@value').extract())
            route_name = node.xpath('.//td[@class="smallsemitext bdrbottomleft"]//text()')[2].extract().title()
            boarding = route_name.split('Via')[0].split('To')[0]
            destination = route_name.split('Via')[0].split('To')[-1].strip(" ")
            seat_availability =  ''.join(node.xpath('.//td[@class="smallsemitext bdrbottomleft"]//input[contains(@onclick, "gotoNext")]').extract()).split('(')[-1].split(',')[-4][1:3]
            jour_date = datetime.datetime.strftime(datetime1, "%d-%m-%Y")
            
            db = MySQLdb.connect(HOST, USERNAME, PASSWORD, DATABASE)
            cursor = db.cursor()
            query = "INSERT INTO upsrtc_dup(service_name, service_id, route_name, boarding, destination, seat_availability, arrival_date, cur_date, src_frm, src_to ) VALUES ('{}', '{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(service_name, service_id, route_name, boarding, destination, seat_availability,arrival_time, jour_date,src_frm,src_to)
            cursor.execute(query)
            db.commit()


            
