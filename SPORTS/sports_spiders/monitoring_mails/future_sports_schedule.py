#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import time
import sys, traceback
import dateutil.relativedelta
from datetime import datetime
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
from vtvspider import extract_data, extract_list_data, get_nodes, log, get_utc_time

def get_table_header(title, headers):
    table_header = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        table_header += '<th>%s</th>' % header
    table_header += '</tr>'
    return table_header

def get_table_body(removed_list):
    body = '<tr>'
    for data in removed_list:
        #body += '<tr>'
        body += '<td>%s</td>' % str(data.encode('utf-8'))
    body += '</tr>'
    #body += '</table>'
    return body

allowed_sports = ['Golf', 'Formula One', 'Baseball', 'Multi-sport', 'Road cycling']
class FutureSports(VTVSpider):
    name = 'future_sports'
    start_urls = ['http://en.wikipedia.org/wiki/2014_in_sports']

    def __init__(self):
        self.year = datetime.now().year
        self.date = datetime.now()
        self.month_name = self.date.strftime("%B")
        self.month = self.date.strftime("%m")
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')

    def send_mail(self, text):
        subject    = 'Sports Future Schedules'
        server     = '10.4.1.112'
        sender     = 'headrun@veveo.net'
        receivers  = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')


    def construct_date(self, _date, month = ''):
        if isinstance(_date, str) and not month:
            date_format = "%s-%s-%s"
            values = (str(self.year), self.month, _date)
            event_date = date_format% values
        elif isinstance(_date, list) and not month:
            date_format = "%s-%s-%s to %s-%s-%s"
            values = (str(self.year), self.month, _date[0], \
                     str(self.year), self.month, _date[1])
            event_date = date_format% values
        elif isinstance(_date, list) and month:
            date_format = "%s-%s-%s to %s-%s-%s"
            month = str(time.strptime(month, "%B").tm_mon)
            values = (str(self.year), self.month, _date[0], \
                    str(self.year), month, _date[1])
            event_date = date_format% values

        return event_date

    def parse(self, response):
        sel = Selector(response)
        year = datetime.now().year
        text = ''
        body_list = []
        month_dict = {}
        headers = ('start_date - end_date', 'Sport', 'Event', 'Event Type', 'Winners')
        nodes = get_nodes(sel, '//table[@class="wikitable sortable"]//tr')
        for node in nodes:
            event_month = extract_data(node, './../preceding-sibling::h3[1]//text()').replace('[edit]', '')
            prev_month = (self.date + dateutil.relativedelta.relativedelta(months=-1)).strftime('%B')
            _date = extract_data(node, './td[1]/text()').encode('utf-8')
            if (self.month_name == event_month or prev_month == event_month) and _date:
                if "–" in _date and not " " in _date:
                    start_end_date = _date.split('–')
                    event_date = self.construct_date(start_end_date)
                elif " " in _date:
                    data = _date.split(' ')
                    date_list = data[0].split('–')
                    month = data[1]
                    event_date = self.construct_date(date_list, month)
                else:
                    event_date = self.construct_date(_date)

                sport = extract_data(node, './td[2]/a/text()')
                if sport not in allowed_sports:
                    continue

                event = extract_data(node, './td[3]//text()')
                _type = extract_data(node, './td[4]/text()')
                winners = extract_data(node, './td[5]//text()')
                if not month_dict.has_key(event_month): month_dict[event_month] = {}
                month_dict[event_month][event_date] = [event_date, sport, event, _type, winners]

        for key, value in month_dict.iteritems():
            print key
            if self.month_name == key:
                text += get_table_header('Sports %s Schedules' % key, headers)
                for key in sorted(value):
                    text += get_table_body(value[key]).decode('utf-8')
                text += '</table>'
                #import pdb; pdb.set_trace()
                self.send_mail(text.encode('utf-8'))
            else:
                text += get_table_header('Sports %s Results' % key, headers)
                for key in sorted(value):
                    text += get_table_body(value[key]).decode('utf-8')
                text += '</table>'
                #self.send_mail(text.encode('utf-8'))

