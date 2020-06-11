import time
import datetime
import re
from vtvspider_new import VTVSpider, get_nodes,\
extract_data, get_utc_time, extract_list_data, get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

def modify(data):
    try:
        data = ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
        return data
    except ValueError or UnicodeDecodeError or UnicodeEncodeError:
        try:
            return data.encode('utf8')
        except  ValueError or UnicodeEncodeError or UnicodeDecodeError:
            try:
                return data
            except ValueError or UnicodeEncodeError or UnicodeDecodeError:
                try:
                    return data.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    data = "".join('NFKD', data.decode('utf-8')).encode('ascii')
                    return data

def get_time(initial_time, gap):
    if '+' in gap:
        if ':' in gap:
            gap = gap.replace('+', '')
            split_time = gap.split(':')
            if len(split_time) > 2:
                hours = int(split_time[0])
                minutes = int(split_time[1])
                seconds = int(split_time[2])
            elif len(split_time) == 2:
                minutes = int(split_time[0])
                seconds = int(split_time[1])
                hours = 0
            else:
                seconds = gap
                minutes = 0
                hours = 0
        else:
            split_time = gap.split("+")
            if len(split_time) == 2:
                seconds = int(split_time[1])
                minutes = 0
                hours = 0
            else:
                hours = 0
                minutes = 0
                seconds = 0
    if initial_time:
        initial_time_split = initial_time.split(":")
        if len(initial_time_split) > 2:
            initial_hours = initial_time_split[0]
            initial_minutes = initial_time_split[1]
            initial_seconds = initial_time_split[-1]
        elif len(initial_time_split) == 2:
            initial_minutes = initial_time_split[0]
            initial_seconds = initial_time_split[-1]
            initial_hours = 0
        else:
            initial_hours = 0
            initial_minutes = 0
            initial_seconds = initial_time

        total_minutes = int(initial_minutes) + (0, minutes)[minutes > 0]
        total_hours = int(initial_hours) + (0, hours)[hours > 0]
        if int(initial_seconds) >= 0 and int(seconds) >= 0:
            total_seconds = int(initial_seconds) + int(seconds)

            t_seconds = total_seconds

        if t_seconds < 10:
            t_seconds = "0" + str(t_seconds)

        if int(total_seconds) >= 60:
            total_minutes = total_minutes + 1
            t_seconds = t_seconds - 60

        if total_minutes >= 60:
            total_hours =  total_hours + 1
            total_minutes = total_minutes - 60

        if total_minutes < 10:
            total_minutes = "0" + str(total_minutes)

        final_time = ""+str(total_minutes) + "'" + " " + str(t_seconds) + "''"

        final_time = (final_time, str(total_hours)+ "h" + " " + final_time)[total_hours > 0]

        return final_time.strip()


COUNTRY_DICT = {'NED': 'Netherlands', 'USA': 'USA', \
                'ITA': 'Italy', 'BEL': 'Belgium', \
                'CHN': 'China', 'GER': 'Germany', \
                'SWE': 'Sweden', 'FRA': 'France'}

TOU_DICT = {'Boels Rental Ronde van Drenthe': 'Ronde van Drenthe', \
            'Trofeo Alfredo Binda - Comune di Cittiglio': 'Trofeo Alfredo Binda-Comune di Cittiglio', \
            'Ronde van Vlaanderen / Tour des Flandres': 'Tour of Flanders for Women', \
            'La Flche Wallonne Fminine': 'La Fleche Wallonne Feminine', \
            'Tour of Chongming Island World Cup': 'Tour of Chongming Island', \
            'Philadelphia International Cycling Classic': 'The Philadelphia Cycling Classic', \
            'Sparkassen Giro': 'Sparkassen Giro Bochum', \
            'Crescent Women World Cup Vargarda TTT': 'Open de Suede Vargarda TTT', \
            'Crescent Women World Cup Vargarda': 'Open de Suede Vargarda', \
            'GP de Plouay-Bretagne': 'GP de Plouay'}

DOMAIN = "http://62.50.72.82/uciroot/cdmroa/2015/"


class UCIWomensTour(VTVSpider):
    name = "uciwomens_spider"
    allowed_domains = []
    start_urls = ['http://62.50.72.82/uciroot/cdmroa/2015/default.aspx?language=eng']

    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(sel, '//table//tr[contains(@class, "grid")]')
        for node in nodes:
            tou_date = extract_data(node, './/td[1]//text()')
            if "Date" in tou_date:
                continue
            tou_name = extract_data(node, './/td[2]//text()')
            tou_name = modify(tou_name).replace('\xc3\xa8', '').replace('\xc3\xa9', '')
            country  = extract_data(node, './/td[3]//text()')
            country  = COUNTRY_DICT[country]
            tou_name = TOU_DICT[tou_name]
            print tou_name
            if tou_name !="Open de Suede Vargarda TTT":
                continue
            if country == "USA":
                tou_datetime = get_utc_time (tou_date, '%d.%m.%Y', 'US/Eastern')
            elif country == "China":
                tou_datetime = get_utc_time (tou_date, '%d.%m.%Y', 'Asia/Shanghai')
            else:
                tou_datetime = get_utc_time (tou_date, '%d.%m.%Y', 'Europe/Paris')
            tz_info = get_tzinfo(country = country, game_datetime = tou_datetime)
            if country == "USA":
                tz_info = get_tzinfo(city = "New York City", game_datetime = tou_datetime)
            today_date = datetime.datetime.now().date()
            if tou_datetime.split(' ')[0] < str(today_date):
                status = "completed"
            elif tou_datetime.split(' ')[0] == str(today_date):
                status = 'ongoing'
            else:
                status = 'scheduled'
            tou_sk = tou_name.replace(' ', '_') + "_" + tou_datetime.split(' ')[0].replace('-', '')
            record['tournament']    = tou_name
            record['game_datetime'] = tou_datetime
            record['game']          = "cycling"
            record['game_status']   = status
            record['source_key']    = tou_sk
            record['reference_url'] = response.url
            record['source']        = "uciworldtour_cycling"
            record['affiliation']   = "uci"
            rich_data = {'location': {'country': country, 'city': '', 'state': ''}}
            record['rich_data'] = rich_data
            record['time_unknown'] = '1'
            record['tz_info'] = tz_info
            record['result'] = {}
            record['participants'] = {}
            record['participant_type'] = "player"
            if self.spider_type=="schedules" and status== "scheduled":
                yield record
            else:
                ref_url = extract_data(node, './/td//a[contains(@href, "Results")]//@href')
                ref_url = DOMAIN+ref_url
                yield Request(ref_url, callback = self.parse_scores, \
                        meta = {'tou_sk': tou_sk, 'status': status, 'tz_info': tz_info, \
                                'tou_name': tou_name, 'tou_dt': tou_datetime, \
                                'rich_data': rich_data})


    def parse_scores(self, response):
        sel= Selector(response)
        record = SportsSetupItem()
        nodes  = get_nodes(sel, '//table//tr[contains(@class, "grid")]')
        participants = {}
        result       = {}
        for node in nodes:
            rank = extract_data(node, './/td[1]//text()')
            if "Rank" in rank:
                continue
            if not rank:
                continue
            time = ''
            r_name = extract_data(node, './/td[3]//text()')
            r_name = modify(r_name)
            rname_pl = r_name.lower().replace('\xc3\xa9', '').replace('\xc3\x84', '').replace('\xc3\x98', '').replace('\xc3\x96', '')
            rname = r_name.lower().replace(' ', '_').replace('\xc3\x96', '').replace("'", '_').replace('-', '_')
            rname = rname.replace('\xc3\xa9', '').replace('\xc3\x84', '').replace('__', '_').replace('\xc3\x98', '')
            time_ = extract_data(node, './/td[6]//text()')
            if not time_:
                time_ = "+0"
            if not '+' in time_ and time_ != '':
                initial_time = time_
            else:
                time_ = get_time(initial_time, time_)
            if time_:

                if ":" in time_:
                    if len(time_.split(':')) == 3:
                        lap_time_split = time_.split(':')
                        lap_hours = lap_time_split[0]
                        lap_minutes = lap_time_split[1]
                        lap_seconds = lap_time_split[2]
                    elif len(time_.split(':')) == 2:
                        lap_time_split = initial_time.split(':')
                        lap_minutes = lap_time_split[0]
                        lap_seconds = lap_time_split[1]
                        lap_hours = 0
                    else:
                        lap_seconds = time_
                        lap_minutes = 0
                        lap_hours = 0

                    if lap_hours > 0:
                        time_ = str(lap_hours) + "h" + " " + str(lap_minutes) + "'" + " " + str(lap_seconds) + "''"
                    else:
                        time_ = str(lap_minutes) + "'" + " " + str(lap_seconds) + "''"
            winner = ''

            if response.meta['status'] == 'completed':
                if rank == "1" :
                    winner = rname
                    result.setdefault('0', {}).update({'winner' : winner})
            participants[rname] = (0, rname_pl)
            record['participants'] = participants

            if rank:

                riders = {'position': rank, 'final_time' : time_}
                result[rname] = riders
        record['tournament']    = response.meta['tou_name']
        record['game_datetime'] = response.meta['tou_dt']
        record['game']          = "cycling"
        record['game_status']   = response.meta['status']
        record['source_key']    = response.meta['tou_sk']
        record['reference_url'] = response.url
        record['source']        = "uciworldtour_cycling"
        record['affiliation']   = "uci"
        record['rich_data']     = response.meta['rich_data']
        record['result']        = result
        record['tz_info']       = response.meta['tz_info']
        record['participant_type'] = "player"
        import pdb;pdb.set_trace()
        yield record





