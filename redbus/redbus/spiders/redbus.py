import scrapy
from urllib.parse import urlencode
from scrapy.http import Request
from scrapy.selector import Selector
import json
import re
import MySQLdb
import datetime
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



class RedBus(scrapy.Spider):
    name = 'redbusonline'

    def start_requests(self):

        headers = {
            'authority': 'www.redbus.in',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://www.redbus.in',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.redbus.in/search?fromCityName=Lucknow%20%28All%20Locations%29&fromCityId=1439&toCityName=Varanasi&toCityId=70429&onward=19-Jun-2020&opId=0&busType=Any',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            
        }
                
        fname = open('redbus.json')
        data = json.load(fname)
        routes = [data['fromCity'][i]+'-'+data['toCity'][i]+'-'+data['src'][i]+'-'+data['dst'][i] for i in range(len(data['src']))] 
        date = [d for d in data['DOJ']]
        tot_route = list(itertools.product(routes, date))
        for route in tot_route:
            params = (
                ('fromCity', route[0].split('-')[0]),
                ('toCity', route[0].split('-')[1]),
                ('src', route[0].split('-')[2]),
                ('dst', route[0].split('-')[3]),
                ('DOJ', route[1]),
                ('sectionId', '0'),
                ('groupId', '11044'),
                ('limit', '0'),
                ('offset', '0'),
                ('sort', '0'),
                ('sortOrder', '0'),
                ('meta', 'false'),
                ('returnSearch', '0'),
            )
            data = '{"headers":{"Content-Type":"application/json"}}'
            url = 'https://www.redbus.in/search/SearchResults?'+urlencode(params)
            yield Request(url, headers=headers, method='POST', body=json.dumps(data), callback=self.parse)

    def parse(self, response):
        parsed = urlparse.urlparse(response.url)
        src_frm = parse_qs(parsed.query)['src'][0]
        src_to = parse_qs(parsed.query)['dst'][0]
        data = json.loads(response.body)
        fin_data = data.get('inv', [])
        for i in fin_data:
            service = i.get('Tvs', '')
            service_no = i.get('si', '')
            service_type = i.get('bt', '')
            departure = i.get('dt', '')
            arrival = i.get('at', '')
            dur = i.get('Duration', '')
            if dur != '':
                duration = datetime.timedelta(minutes=dur)
            fare = i.get('maxfr', '')
            if i['rt']['ct'] not in 'No Rating':
                rating = i.get('rt', {}).get('totRt', '')
                count = i.get('rt', {}).get('ct', '')
            else:
                rating = ""
                count = ""
            source = i.get('StdBp','')
            destination = i.get('StdDp','')
            date = datetime.datetime.strptime(departure, "%Y-%m-%d %H:%M:%S")
            jour_date = datetime.datetime.strftime(date, "%d-%m-%Y")

            db = MySQLdb.connect(HOST, USERNAME, PASSWORD, DATABASE)
            cursor = db.cursor()
            query = "INSERT INTO red_bus(service, service_no, service_type, departure, arrival, duration, fare, source, destination, cur_date, src_frm, src_to) VALUES ('{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}')".format(service, service_no, service_type, departure, arrival, duration, fare, source, destination, jour_date, src_frm, src_to)         
            cursor.execute(query)
            db.commit()
            