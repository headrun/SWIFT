# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, log, \
get_tzinfo, extract_list_data, get_utc_time
import datetime
import time
import re
#from scrapy_spiders_dev.items import SportsSetupItem
from sports_spiders.items import SportsSetupItem

REPLACE_TOU_DICT = {'Qatar': 'Qatar motorcycle Grand Prix',
                    "Commercial Bank Grand Prix of Qatar": 'Qatar motorcycle Grand Prix',
                    'Americas': 'Motorcycle Grand Prix of the Americas',
                    'Argentina': \
                    'Argentine motorcycle Grand Prix',
                    'Gran Premio bwin de': 'Spanish motorcycle Grand Prix',
                    'Gran Premio de España': 'Spanish motorcycle Grand Prix',
                    'Gran Premio Red Bull de España': 'Spanish motorcycle Grand Prix',
                    'France': 'French motorcycle Grand Prix',
                    'Italia TIM': 'Italian motorcycle Grand Prix',
                    "Gran Premio d'Italia": 'Italian motorcycle Grand Prix',
                    'Catalunya': 'Catalan motorcycle Grand Prix',
                    'TT Assen': 'Dutch TT',
                    'Motul TT Assen': 'Dutch TT',
                  'Deutschland': 'German motorcycle Grand Prix',
                   'GoPro Motorrad Grand Prix Deutschland': 'German motorcycle Grand Prix',   
                    'Indianapolis Grand Prix': \
                    'Indianapolis motorcycle Grand Prix',
                    'republiky': 'Czech Republic motorcycle Grand Prix',
                    'Hertz British Grand Prix': \
                    'British motorcycle Grand Prix',
                    'PTT Thailand Grand Prix': \
                    'Thailand motorcycle Grand Prix', \
                    'British Grand Prix' : "British motorcycle Grand Prix",
                    'OCTO British Grand Prix': "British motorcycle Grand Prix",
                    'GP Tim di San Marino e della Riviera di Rimini': \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Gran Premio Tribul Mastercard di San Marino e della Riviera di Rimini': \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "Gran Premio di San Marino e della Riviera di Rimini":
                    "San Marino and Rimini's Coast motorcycle Grand Prix", \
                    "GP di San Marino e della Riviera di Rimini": \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Gran Premio Movistar de': \
                    'Aragon motorcycle Grand Prix',
                    'Grand Prix of Japan': 'Japanese motorcycle Grand Prix',
                    'Australian Grand Prix': \
                    'Australian motorcycle Grand Prix',
                    'Australian Motorcycle Grand Prix':
                    'Australian motorcycle Grand Prix',
                    'Shell Advance Malaysian Motorcycle': \
                    'Malaysian motorcycle Grand Prix',
                    'Shell Malaysia Motorcycle Grand Prix': \
                    'Malaysian motorcycle Grand Prix',
                    'Malaysia Motorcycle Grand Prix':
                    'Malaysian motorcycle Grand Prix', \
                    'Gran Premio Generali de la Comunitat Valenciana': \
                    'Valencian Community motorcycle Grand Prix',
                    'Gran Premio Motul de la Comunitat Valenciana': \
                    'Valencian Community motorcycle Grand Prix', \
                    'Gran Premio de la Comunitat Valenciana': \
                    "Valencian Community motorcycle Grand Prix", \
                    "Malaysian Motorcycle Grand Prix": \
                    "Malaysian motorcycle Grand Prix", \
                    'Gran Premio TIM di San Marino e della Riviera di Rimini': \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Tribul Mastercard GP S.Marino e Riviera di Rimini':\
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "Gran Premio de Aragón": \
                    "Aragon motorcycle Grand Prix", \
                    "Gran Premio Movistar de Aragón": \
                    "Aragon motorcycle Grand Prix", \
                    "Motorrad Grand Prix von Österreich": 
                    "Austrian motorcycle Grand Prix",
                    "GP Octo di San Marino e della Riviera di Rimini":
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "Gran Premio Octo di San Marino e della Riviera di Rimini":
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "Gran Premio Michelin® de Aragon":"Aragon motorcycle Grand Prix"
}

REPLACE_EVENT_DICT = {'Qatar': 'Qatar motorcycle Grand Prix',
                    'Americas': 'Americas motorcycle Grand Prix',
                    'Argentina': 'Argentina motorcycle Grand Prix',
                    'Gran Premio bwin de': 'Spanish motorcycle Grand Prix',
                    'Gran Premio de España': 'Spanish motorcycle Grand Prix',
                    'Gran Premio Red Bull de España': 'Spanish motorcycle Grand Prix',
                    'France': 'French motorcycle Grand Prix',
                    'Italia TIM': 'Italian motorcycle Grand Prix',
                    "Gran Premio d'Italia": 'Italian motorcycle Grand Prix',
                    'Catalunya': 'Catalan motorcycle Grand Prix',
                    'TT Assen': 'Dutch motorcycle Grand Prix',
                    'TT Assen': 'Dutch motorcycle Grand Prix',
                    'Deutschland': 'German motorcycle Grand Prix',
                  'GoPro Motorrad Grand Prix Deutschland': 'German motorcycle Grand Prix',
                    'Indianapolis Grand Prix': \
                    'Indianapolis motorcycle Grand Prix',
                    'republiky': 'Czech Republic motorcycle Grand Prix',
                    'Hertz British Grand Prix': \
                    'British motorcycle Grand Prix',
                    'British Grand Prix' : "British motorcycle Grand Prix",
                    'OCTO British Grand Prix': "British motorcycle Grand Prix",
                    'GP Tim di San Marino e della Riviera di Rimini': \
                    "San Marino motorcycle Grand Prix",
                    'Tribul Mastercard GP S.Marino e Riviera di Rimini':\
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "GP di San Marino e della Riviera di Rimini": \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Gran Premio Tribul Mastercard di San Marino e della Riviera di Rimini': \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Motul TT Assen': 'Dutch motorcycle Grand Prix',
                    "Gran Premio di San Marino e della Riviera di Rimini": \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Gran Premio Movistar de': 'Aragon motorcycle Grand Prix',
                    'Grand Prix of Japan': 'Japanese motorcycle Grand Prix',
                    'Australian Grand Prix': \
                    'Australian motorcycle Grand Prix',
                    'Australian Motorcycle Grand Prix':
                    'Australian motorcycle Grand Prix',
                    'Shell Advance Malaysian Motorcycle': \
                    'Malaysian motorcycle Grand Prix',
                    'Shell Malaysia Motorcycle Grand Prix':
                    'Malaysian motorcycle Grand Prix',
                    'Malaysia Motorcycle Grand Prix':
                    'Malaysian motorcycle Grand Prix', \
                    'Gran Premio TIM di San Marino e della Riviera di Rimini': \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'GP Tim di San Marino e della Riviera di Rimini': \
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    'Gran Premio Generali de la Comunitat Valenciana': \
                    'Valencian motorcycle Grand Prix',
                    'Gran Premio de la Comunitat Valenciana': \
                    'Valencian Community motorcycle Grand Prix', \
                    'Gran Premio Motul de la Comunitat Valenciana': \
                    'Valencian Community motorcycle Grand Prix', \
                    'Malaysian Motorcycle Grand Prix': \
                    "Malaysian motorcycle Grand Prix", \
                    "Gran Premio de Aragón": \
                    "Aragon motorcycle Grand Prix", \
                    "Gran Premio Movistar de Aragón": \
                    "Aragon motorcycle Grand Prix", \
                    "Motorrad Grand Prix von Österreich": \
                    "Austrian motorcycle Grand Prix", 
                    "Commercial Bank Grand Prix of Qatar": \
                    'Qatar motorcycle Grand Prix', \
                    'PTT Thailand Grand Prix': \
                    'Thailand motorcycle Grand Prix',
                    "GP Octo di San Marino e della Riviera di Rimini":
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "Gran Premio Octo di San Marino e della Riviera di Rimini":
                    "San Marino and Rimini's Coast motorcycle Grand Prix",
                    "Gran Premio Michelin® de Aragon":"Aragon motorcycle Grand Prix"
}

MONTH_DICT = {'jan': "January", 'feb': 'February', 'mar': 'March', \
              'apr': "April", 'may': 'May', "jun": 'June', \
              'jul': 'July', 'aug': 'August', 'sep': 'September', \
              'oct': 'October', 'nov': 'November', 'dec': 'December'}

def get_time(initial_time, gap):
    split_time = gap.split("'")
    if len(split_time) > 2:
        hours = int(split_time[0])
        minutes = int(split_time[1])
        seconds = split_time[2]
    elif len(split_time) == 2:
        minutes = int(split_time[0])
        seconds = split_time[1]
        hours = 0
    else:
        seconds = gap
        minutes = 0
        hours = 0
    initial_time_split = initial_time.split("'")

    if len(initial_time_split) > 2:
        initial_hours = initial_time_split[0]
        initial_minutes = initial_time_split[1]
        initial_seconds = initial_time_split[-1]

    if len(initial_time_split) == 2:
        initial_minutes = initial_time_split[0]
        initial_seconds = initial_time_split[-1]
        initial_hours = 0
    else:
        initial_hours = 0
        initial_minutes = 0
        initial_seconds = initial_time

    total_minutes = int(initial_minutes) + (0, minutes)[minutes > 0]
    total_hours = int(initial_hours) + (0, hours)[hours > 0]
    total_seconds = "%.3f" % (float(initial_seconds.replace("+", '').strip()) + float(seconds.replace("+", '').strip()))
    t_seconds = int(float(total_seconds.split(".")[0]))
    tm_seconds = int(float(total_seconds.split(".")[1]))

    if int(float(total_seconds)) >= 60:
        total_minutes = total_minutes + 1
        t_seconds = t_seconds - 60

    if total_minutes >= 60:
        total_hours =  total_hours + 1
        total_minutes = total_minutes - 60
    final_time = "" + str(total_minutes) + "'" + \
                 str(t_seconds) + "." + str(tm_seconds)
    final_time = (final_time, str(total_hours) + "'" + final_time)\
                 [total_hours > 0]
    return final_time.strip()

def X(data):
    try:
        return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
    except ValueError:
        return data.encode('utf8')

def get_tournament_id(tou_name):
    tou_name_db = ''
    for key, value in REPLACE_TOU_DICT.iteritems():
        if key.upper() in X(tou_name).upper():
            tou_name_db = tou_name.replace(tou_name, value).strip()
    return tou_name_db

def get_event_id(tou_name, event_name_rac):
    event_name_db = ''
    for key, value in REPLACE_EVENT_DICT.iteritems():
        if key.upper() in X(tou_name).upper():
            event_name_db = value
            event_name_db = event_name_db.replace('motorcycle', event_name_rac.replace('\u2122', '')).replace(' Community', '').replace("and Rimini's Coast ", '').strip()
    return event_name_db

def get_sk(event_name_rac, event_name_pr, date, t_name):
    t_name = t_name.replace('IVECO DAILY ', '')
    sou_key = event_name_rac.replace(" ",'_') \
              + "_" + event_name_pr + "_" + date.replace(" ",'_')
    return sou_key

RECORD = SportsSetupItem()

class MotogpSpider(VTVSpider):
    name = "mc_racing"
    allowed_domains = ['www.motogp.com']
    start_urls = ['http://www.motogp.com/en/calendar+circuits']

    def parse(self, response):
        hxs = Selector(response)
        events = get_nodes(hxs, '//div[contains(@class, "event shadow_block official ")]//header')
        tou_year = extract_data(hxs, '//p[@class="title"]//text()')
        year = tou_year.split(' ')[0]
        for event in events:
            t_url = extract_data(event, './/a//@href')
            if "2017-provisional-motogp-calendar-announced" in t_url:
                continue
            t_date = extract_data(event, './/div[@class="event_day"]//text()')
            if '2020' in t_date:
                continue
            t_month = extract_data(event, './/div[@class="event_month"]//text()').lower()
            t_month = MONTH_DICT[t_month]
            location =  extract_list_data(event, './/div[@class="location"]//text()')
            tou_date = t_date + " " + t_month + " "+ year
            tou_name = extract_data(event, './/a//text()').split('-')[-1].strip()
    
            if 'Argentina' in tou_name: tou_name = 'Argentina'
            event_record = {'Tou_Url': t_url, 'Year': year, \
                'tou_date': tou_date, 'location': location, \
                'tou_name': tou_name}

            if t_url:
                yield Request(t_url, callback = self.parse_event, \
                              meta = {'Tournament Record': event_record})
    @log
    def parse_event(self, response):
        hxs = Selector(response)
        event_record = response.meta['Tournament Record']
        tou_details  = event_record['location']
        tou_country = tou_details[3].title()
        tou_stadium = tou_details[1].title()
        tou_date = event_record['tou_date']
        stadium = extract_data(hxs, '//div[@class="text"]//p[2]/b/text()')
        tou_name = event_record['tou_name']
        if not stadium:
            stadium = tou_stadium
        elif "From" in stadium:
            stadium = tou_stadium

        RECORD['source'] = "motorcycle racing"
        RECORD['game'] = "motorcycle racing"
        RECORD['affiliation'] = 'motogp'
        RECORD['participant_type'] = 'player'
        RECORD['season'] = event_record['Year']
        RECORD['location_info'] = tou_country
        RECORD['tournament'] = tou_name
        RECORD['time_unknown'] = '0'

        today_date = datetime.datetime.now().date()
        sessions = get_nodes(hxs, '//table[contains(@class, "schedule_table")]//tbody//tr')
        if not sessions:
            sessions = get_nodes(hxs, '//div[contains(@class, "schedule__table")]//div[contains(@class, "schedule__table-row")]')
        for session in sessions:
            event_name_rac = extract_data(session, './/td[2]//text()').strip()
            if not event_name_rac:
                event_name_rac  = extract_data(session, './/div[2]/text()').strip()
            if "Press Conference" in event_name_rac:
                continue
            event_name_pr = extract_data(session, './/td[3]//text()').strip()
            if not event_name_pr:
                event_name_pr = extract_data(session, './/div[3]/span[@class="visible-xs"]/text()').strip()
            if "RAC" in event_name_pr and  "RAC2" not in event_name_pr and 'RookiesCup' not in event_name_rac:
                event_complt_date = extract_data(session, './/td/@data-ini').strip()
                if not event_complt_date:
                    event_complt_date = extract_data(session, './/div/@data-ini').strip()

                if event_complt_date:
                    event_date = event_complt_date
                else:
                    event_date = extract_data(session, './/td//span/@data-ini').strip()
                    if not event_date:
                        event_date = extract_list_data(session, './/div//span/@data-ini-time')[0].strip()
                if not event_date :
                    event_date = tou_date
                if event_date:
                    hour_info = ''
                    hour_info = ("".join((re.findall("\+.*", event_date))))
                    if not hour_info:
                        hour_info = ("".join((re.findall("\:00.*", event_date))))
                        hour_info = ("".join((re.findall("\-.*", hour_info))))
                    if "T" in event_date:
                        event_date = event_date.replace(hour_info, '').replace('T', ' ')
                        #hour_info = ("".join((re.findall("\:00.*", hour_info))))
                        hour_info_int = int(str(hour_info.replace('-', '').\
                                        replace('+', '').replace('00', '').replace(':', '')))
                        tou_name_db = get_tournament_id(tou_name)
                        event_name_db = get_event_id(tou_name, event_name_rac)
                        sou_key = get_sk(event_name_db, event_name_pr, tou_date, tou_name_db)
                        sou_key = sou_key.replace('_0', '_')
                        tournmnt_date = (datetime.datetime(*time.strptime(tou_date.strip(), '%d %B %Y')[0:6])).date()

                        gm_tm = time.strptime(event_date, '%Y-%m-%d %H:%M:%S')
                        if "+" in hour_info:
                            tz_info_r = "+" + str(hour_info_int)
                            event_dt = datetime.datetime(gm_tm.tm_year, \
                                       gm_tm.tm_mon, gm_tm.tm_mday, \
                                       gm_tm.tm_hour - hour_info_int, \
                                       gm_tm.tm_min)
                        else:
                            tz_info_r = "-" + str(hour_info_int)
                            event_dt = datetime.datetime(gm_tm.tm_year, \
                                       gm_tm.tm_mon, gm_tm.tm_mday, \
                                       gm_tm.tm_hour + hour_info_int, \
                                       gm_tm.tm_min)
                        gmt = event_dt.strftime('%Y-%m-%d %H:%M:%S')

                        RECORD['game_datetime'] = gmt
                        tz_info = get_tzinfo(country=tou_country, game_datetime= gmt)
                        if not tz_info:
                            tz_info = tz_info_r
                        RECORD['tz_info'] = tz_info
                    else:
                        RECORD['game_datetime'] = get_utc_time(tou_date, '%d %B %Y', 'US/Eastern')
                        RECORD['tz_info'] = "+3"
                        tournmnt_date = (datetime.datetime(*time.strptime(tou_date.strip(), '%d %B %Y')[0:6])).date()
                        tou_name_db = get_tournament_id(tou_name)
                        event_name_db = get_event_id(tou_name, event_name_rac)
                        sou_key = get_sk(event_name_db, event_name_pr, tou_date, tou_name_db)
                        sou_key = sou_key.replace('_0', '_')
                    if event_date and tournmnt_date <= today_date:
                        if tournmnt_date == today_date:
                            status = "ongoing"
                        elif tournmnt_date < today_date:
                            status = "completed"
                        results_url = extract_data(session, './/td[3]/a[contains(@href, "Statistics")]/@href').strip()
                        if not results_url:
                            results_url = extract_list_data(session, './/div//a[contains(@href, "Statistics")]//@href')[0].strip()
                        details_page = "http://www.motogp.com/en/ajax/results/parse" + re.sub\
                                        (".*Statistics", '', results_url)

                        my_data = {"reference_url": response.url,
                                   "status": status, "sk": sou_key,
                                   'event_name': event_name_rac,
                                   'RAC': event_name_pr, 'date': tou_date,
                                   'stadium': stadium, 'country': tou_country,
                                   't_name': tou_name}

                        yield Request(details_page, callback = self.parse_result, meta = {"my_data": my_data})
                    else:
                        status = "scheduled"
                        if "RAC" in sou_key:
                            tou_name_db = get_tournament_id(tou_name)
                            event_name_db = get_event_id(tou_name, event_name_rac)
                            rich_data = {"channels": '', "game_note": '', \
                                         "location": {'city': '', 'state': '', 'country': tou_country}, \
                                         "stadium": stadium}

                            RECORD['reference_url'] = event_record['Tou_Url']
                            RECORD['participants'] = {}
                            RECORD['game_datetime'] = gmt
                            RECORD['result'] = {}
                            RECORD['game_status'] = status
                            RECORD['source_key'] = sou_key
                            RECORD['event'] = event_name_db
                            RECORD['rich_data'] = rich_data
                            RECORD['tournament'] = tou_name_db
                            yield RECORD

    @log
    def parse_result(self, response):
        result_url = response.url
        riders = {}
        result_data = {}
        if "RAC" in result_url:
            hxs = Selector(response)
            tou_name = extract_data(hxs, '//h1[@class="margintop0 marginbot10 blueh1"]/text()')
            my_data = response.meta['my_data']
            event_name_pr = my_data['RAC']
            tou_date = my_data['date']
            tou_name_db = get_tournament_id(tou_name)
            event_name_db = get_event_id(tou_name, my_data['event_name'])
            date = my_data['date']
            tou_name_db = get_tournament_id(my_data['t_name'])
            event_name_db = get_event_id(my_data['t_name'], my_data['event_name'])
            sou_key = get_sk(event_name_db, event_name_pr, tou_date, tou_name_db)
            sou_key = sou_key.replace('_0', '_')
            rows = get_nodes(hxs, "//table[@class='width100 marginbot10 fonts12']//tbody//td")
            headers_data = get_nodes(hxs, '//table[@class="width100 marginbot10 fonts12"]//thead/tr/th')
            headers = []
            for head in headers_data:
                headers.append(extract_data(head, './text()'))

            headers = [(item) for item in headers]
            last_th = headers[-1]
            initial_time = ""
            data_list = []
            count = 0
            rider_url = ''
            rich_data = {"channels": '', "game_note": '', \
                         "location": {'city': '', 'state': '', 'country': my_data['country']},\
                         "stadium": my_data['stadium']}

            for ind, row in enumerate(rows):
                data = extract_data(row, './text()')
                if not data:
                    data = extract_data(row, './a/text()')
                if not rider_url:
                    rider_url = extract_data(row, "./a/@href")
                if (data != "Not Classified")  and (data != "Not Starting") and (data != "Not Finished 1st Lap") and (data != "Excluded"):
                    if count < 9:
                        data_list.insert(count, data)
                        count += 1
                        if count != 9:
                            continue
                    even_row = [("".join(obj)) for obj in data_list]
                data_list = []
                count = 0
                rider_id = rider_url.split("/")[-1]
                rider_url = ''
                if "Prev." in last_th and len(even_row) > 2:
                    position = even_row[0]
                    avg_speed = even_row[6]
                    time_ = even_row[7]
                    rider = even_row[2]
                elif "Time/Gap" in  last_th and len(even_row) > 2:
                    position = even_row[0]
                    avg_speed = even_row[7]
                    time_ = even_row[8]
                    rider = even_row[3]
                    if ind == 8:
                        initial_time = time_
                        winner = rider_id
                    elif not("Lap" in time_):
                        time_ = get_time(initial_time, time_)

                if position:
                    rider_id = X(rider_id)
                    rider = X(rider)
                    riders.update({rider_id: (0, rider)})

                    result = {'0': {'winner': winner}, \
                              rider_id: {"avg_speed": avg_speed, \
                              "final_position": position, "final_time": time_}}
                    result_data.update(result)
            result_sub_type = self.get_result_sub_type(event_name_db)


            RECORD['reference_url'] = response.url
            RECORD['result_sub_type'] = result_sub_type
            RECORD['participants'] = riders
            RECORD['result'] = result_data
            RECORD['game_status'] = my_data['status']
            RECORD['source_key'] = sou_key
            RECORD['tournament'] = tou_name_db
            RECORD['event'] = event_name_db
            RECORD['rich_data'] = rich_data
            RECORD['game_datetime'] = get_utc_time(date, '%d %B %Y', 'US/Eastern')
            RECORD['tz_info']       = get_tzinfo(country = my_data['country'], game_datetime = RECORD['game_datetime'])
            print RECORD
            yield RECORD

    def get_result_sub_type(self, event_name):
        res_type = ["Moto2", "Moto3", "MotoGP"]
        res_sub_type = ''

        for res in res_type:
            if res in event_name:
                res_sub_type = res
        return res_sub_type
