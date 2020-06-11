import time
import datetime
import re
from vtvspider_new import VTVSpider, \
get_nodes, extract_data, get_utc_time, \
extract_list_data
from scrapy.selector import Selector
from scrapy.selector import XmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


NEEDED_TOURNAMENTS = {'Santos Tour Down Under'              : 'Tour Down Under',     'Paris - Nice'      : 'Paris-Nice', \
                        'Tirreno-Adriatico'               : 'Tirreno - Adriatico',        'Milano - Sanremo'  : 'Milano - Sanremo', \
                        'Volta Ciclista a Catalunya'        : 'Volta Ciclista a Catalunya', 'E3  Harelbeke'      : 'E3 Harelbeke', \
                        'Gent-Wevelgem In Flanders Fields'  : 'Gent-Wevelgem',              'Tour of Beijing'   : 'Tour of Beijing', \
                        'Il Lombardia'                      : 'Il Lombardia',               'Eneco Tour'        : 'Eneco Tour', \
                        'Vuelta a Espana'                   : 'Vuelta a Espana',            'Tour de Suisse'    : 'Tour de Suisse', \
                        'Tour de Pologne'                   : 'Tour de Pologne',            'Paris - Roubaix'   : 'Paris - Roubaix', \
                        'La Fleche Wallonne'                : 'La Fleche Wallonne',         'Amstel Gold Race'  :
                        'Amstel Gold Race', \
                        'Critrium du Dauphin'             : 'Criterium du Dauphine',      "Giro d'Italia"     : "Giro d'Italia", \
                        'GP Ouest France - Plouay'          : 'GP Ouest France Plouay',   'Tour de Romandie'  : 
                        'Tour de Romandie', \
                        'Vuelta Ciclista al Pais Vasco'     : 'Vuelta Ciclista al Pais Vasco',
                        'Bastogne'                          : 'Liege - Bastogne - Liege', \
                        'Liege - Bastogne - Liege'          : 'Liege - Bastogne - Liege', \
                        'Clasica Ciclista San Sebastian'    : 'Clasica Ciclista San Sebastian', \
                        'Vattenfall Cyclassics'             : 'Vattenfall Cyclassics', \
                        'Grand Prix Cycliste de Quebec'     : 'Grand Prix Cycliste de Quebec', \
                        'Grand Prix Cycliste de Montreal'   : 'Grand Prix Cycliste de Montreal', \
                        'Ronde van Vlaanderen/ Tour des Flandres'              : 'Tour des Flandres', \
                        'Championnats du Monde UCI / UCI World Championships TTT'   : 'UCI Road World Championships', \
                        'USA Pro Challenge'                   : 'USA Pro Cycling Challenge'}

REPLACE_MONTH = {'Jan' : '01', 'Feb' : '02', 'Mar' : '03', 'Apr' : '04',
                 'May' : '05', 'Jun' : '06', \
                 'Jul' : '07', 'Aug' : '08', 'Sep' : '09', 'Oct' : '10', \
                 'Nov' : '11', 'Dec' : '12'}

IGNORE_RACES = ['General classification']


def get_tou_dates(date):
    if '-' in date:
        s_date = date.split('-')[0]
        e_date = date.split('-')[1]
        year = e_date.split(' ')[-1]
        s_date = s_date + ' ' + year
        start_date = (datetime.datetime(*time.strptime(s_date.\
                      strip(), '%d %b %Y')[0:6])).date()
        tou_date = start_date.strftime('%b %-d')
    else:
        s_date = date
        year = date.split(' ')[-1]
        start_date = (datetime.datetime(*time.strptime(s_date.\
                      strip(), '%d %b %Y')[0:6])).date()
        tou_date = start_date.strftime('%b %-d')

    return tou_date, year

def get_refined_tou_name(tour_name):
    for i in NEEDED_TOURNAMENTS:
        if i in tour_name:
            tour_name = i

    return tour_name

def get_month(stage_datetime):
    day = stage_datetime.split(' ')[0]
    month = stage_datetime.split(' ')[1]
    year = stage_datetime.split(' ')[-1]
    for key, value in REPLACE_MONTH.iteritems():
        if key in month:
            month = month.replace(key, value).strip()
            stage_datetime = year + month + day
    return stage_datetime

def get_month_new(stage_datetime):
    month = stage_datetime.split(' ')[0]
    day = stage_datetime.split(' ')[1]
    year = stage_datetime.split(' ')[-1]
    for key , value in REPLACE_MONTH.iteritems():
        if key in month:
            month = month.replace(key, value).strip()
            stage_datetime = year + month + day
    return stage_datetime

def get_race_results(races):
    res = 0
    for i in IGNORE_RACES:
        if i in races:
            res = 1
            break
        else:
            res = 0
    return res

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
TODAY_DATE = datetime.datetime.now().date()

SINGLE_TOU = ['Grand Prix Cycliste de Montreal', 'Grand Prix Cycliste de Quebec']


class UCIWorldTour(VTVSpider):
    name = 'uci_spider'
    allowed_domains = []
    start_urls = ['http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19004&SportID=102&CompetitionID=-1&EditionID=-1&SeasonID=490&ClassID=1&GenderID=1&EventID=-1&EventPhaseID=0&Phase1ID=0&Phase2ID=0&Phase3ID=0&CompetitionCodeInv=1&Detail=0&Ranking=0&All=0&TaalCode=2&StyleID=0&Cache=8']
    domain = "http://www.uci.infostradasports.com"

    def parse(self, response):
        hxs = Selector(response)
        events_nodes = get_nodes(hxs, '//table[@class="datatable"]//tr[@valign="top"]')

        for event in events_nodes[:5]:
            tou_date = extract_data(event, './td[1]/nobr/text()').strip().encode('utf8').replace('\xa0', ' ').replace('\xc2', ' ')
            print tou_date
            tou_name = extract_data(event, './td[2]/a/text()').strip()
            tou_name = tou_name.encode('utf-8')
            tou_name = tou_name.replace('\xc3\xa9', '').replace('\xc3\xb1', 'n').strip()
            tou_datetime, year = get_tou_dates(tou_date)
            if int(year) == TODAY_DATE.year:
                for key, value in NEEDED_TOURNAMENTS.iteritems():
                    if tou_name in key:
                        tou_name = tou_name.replace(key, value).strip()
                        results_link = self.domain + extract_data(event, './td[2]/a/@href').strip().replace('amp;', '')
                        country = extract_data(event, './td[3]/text()').strip()
                        winner = extract_data(event, './td[5]/text()').strip()
                        if tou_datetime:
                            tou_datetime = tou_datetime + ' ' + year
                            tou_date = get_utc_time(tou_datetime, '%b %d %Y', 'US/Eastern')

                        if country == "AUS":
                            tou_date = get_utc_time(tou_datetime, '%b %d %Y', 'Australia/Melbourne')

                        details = {'tou_name' : tou_name, 'country' : country , \
                                    'year' : year, 'winner' : winner, 'game_datetime' : tou_date, 'country': country}
                        if "-" in tou_date:
                            yield Request(results_link, callback = self.parse_middle, meta = {'details': details})
                        else:
                            today_date = datetime.datetime.now().date()
                            if tou_datetime.split(' ')[0] < str(today_date):
                                status = "completed"
                            elif tou_datetime.split(' ')[0] == str(today_date):
                                status = 'ongoing'
                            else:
                                status = 'scheduled'
                            tou_sk = tou_name.replace(' ', '_') + "_" + tou_date.split(' ')[0].replace('-', '')
                            if country == "AUS":
                                tou_date = get_utc_time(tou_datetime, '%b %d %Y', 'Australia/Melbourne')

                            yield Request(results_link, \
                            callback = self.parse_details, \
                            meta = {'tou_name': tou_name, 'country': country, \
                            'year': year, 'winner': winner, \
                            'game_dt': tou_date, 'tou_sk': tou_sk, \
                            'status': status, 'stages': ''})


    def parse_middle(self, response):
        hxs = Selector(response)
        details = response.meta['details']
        record = SportsSetupItem()
        stages = get_nodes(hxs, '//table[@class="datatable"]//tr[td[a[contains(text(), "Stage")]]]')
        for stage in stages:
            stage_datetime =  extract_data(stage, './/td[1]/nobr/text()').strip().encode('utf8').replace('\xc2\xa0', ' ')
            game_date = get_utc_time(stage_datetime, '%d %b %Y', 'US/Eastern')
            if details['country'] == "AUS":
                game_date = get_utc_time(stage_datetime, '%d %b %Y', 'Australia/Melbourne')
            if not stage_datetime:
                stage_datetime = details['game_datetime']
                stage_datetime = get_month_new(stage_datetime)
                stage_date = (datetime.datetime(*time.strptime(stage_datetime.strip(), '%Y%m%d')[0:6])).date()
                if stage_date:
                    st_date = int(game_date.split(' ')[0])
                    if st_date < TODAY_DATE:
                        status = "completed"
                    elif st_date == TODAY_DATE:
                        status = 'ongoing'
                    else:
                        status = 'scheduled'
                        record['participants'] = {}
                        record['result'] = {}
                stage_date = (datetime.datetime(*time.strptime(stage_datetime.strip(), '%Y%m%d')[0:6])).date()
                modified_tou_date = stage_datetime.replace('\xa0', '').strip()
                modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
                tou_sk = modified_tou_name + '_' + modified_tou_date
                yield Request(response.url, callback = self.parse_details, meta = {'game_dt' : game_date, 'tou_name' : details['tou_name'], \
                                 'year' : details['year'], 'tou_sk' : tou_sk, 'status' : status, \
                                'game_datetime' : details['game_datetime'], 'stage_date' : stage_datetime, 'stages' : ''})
                break
            stage_datetime = get_month(stage_datetime)
            races = extract_data(stage, './/td[2]/a/text()').replace(':', '')
            races_link = extract_data(stage, './/td[2]/a/@href').strip().replace('amp;', '')
            race_link = self.domain + races_link
            stage_num = "". join (re.findall(r'\d+', races))
            res = get_race_results(races)
            if stage_num and res == 0:
                if races_link:
                    modified_tou_date = "_".join(stage_datetime.split(' ')[:2]).replace('\xa0', '').strip()
                    modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
                    modified_stage    = races.replace(' ', '_').strip()
                    tou_sk = modified_tou_name + '_' + modified_stage + '_' + modified_tou_date
                    if stage_datetime:
                        stage_date = int(game_date.split(' ')[0])
                        if stage_date < TODAY_DATE:
                            status = "completed"
                        elif stage_date == TODAY_DATE:
                            status = "ongoing"
                        else:
                            status = "scheduled"
                            record['participants'] = {}
                            record['result'] = {}
                        yield Request(race_link, callback = self.parse_details, meta = {'game_dt' : game_date, 'tou_name' : details['tou_name'], \
                        'year' : details['year'], 'tou_sk' : tou_sk, 'stages' : races, 'status' : status, \
                        'game_datetime' : details['game_datetime'], 'stage_date' : stage_datetime})

    def parse_details(self, response):
        hxs = Selector(response)
        stages_full_link = extract_data(hxs, '//tr/td[@colspan="6"]/a/img[contains(@src, "AllPages")]/../@href') \
                            .replace('amp;', '').strip()
        if not stages_full_link:
           stages_full_link =  extract_data(hxs, '//tr/td[@colspan="5"]/a/img[contains(@src, "AllPages")]/../@href') \
            .replace('amp;', '').strip()
        if not stages_full_link:
            stages_full_link = extract_data(hxs, '//tr/td[@colspan="8"]/a/img[contains(@src, "AllPages")]/../@href') \
            .replace('amp;', '').strip()
        if not stages_full_link:
            stages_full_link = extract_data(hxs, '//tr/td[@colspan="7"]/a/img[contains(@src, "AllPages")]/../@href') \
            .replace('amp;', '').strip()
        if stages_full_link:
            stages_full_link = "http://www.uci.infostradasports.com/asp/lib/"  + stages_full_link

        if not stages_full_link:
            stages_full_link = response.url

        game_dt = response.meta['game_dt']
        tou_name = response.meta['tou_name']
        year     = response.meta['year']
        tou_sk   = response.meta['tou_sk']
        status   = response.meta['status']
        stage    = response.meta['stages']
        if stages_full_link:
            yield Request(stages_full_link, callback = self.parse_details_next, meta = {'game_dt': game_dt, 'tou_name': tou_name, \
                            'year': year, 'tou_sk': tou_sk, 'status': status, 'stage': stage})

    def parse_details_next(self, response):
        hxs = XmlXPathSelector(response)
        record      = SportsSetupItem()
        game_note = extract_data(hxs, '//div[@class="subtitlered"]//text()').split('2016 -')[-1].strip().replace('Road race:', '').replace(' - ', ' / '). \
                                    replace('\n\t\t', '').strip()
        game_note = game_note.replace(' -', ' to ')
        print game_note
        rider_nodes = get_nodes(hxs, '//table[@class="datatable"]//tr[@valign="top"]')
        participants = {}
        result       = {}
        for r_node in rider_nodes:
            rank = extract_data(r_node, './/td[1]//text()').replace('.', '').strip()
            if not rank:
                continue
            r_name = extract_data(r_node, './/td[2]//text()').strip()
            r_name = modify(r_name)
            rname_pl = r_name.lower().replace('\xc3\xa9', '').replace('\xc3\x84', '').replace('\xc3\xb4', '').replace('\xc3\x96', '').replace('\xc3\xb6', '')
            rname = r_name.lower().replace(' ', '-')
            rname = rname.replace('\xc3\xa9', '').replace('\xc3\x84', '').replace('\xc3\x87', ''). \
            replace('--', '-').replace('\xc3\xb4', ''). \
            replace('\xc3\x96', '').replace('\xc3\xb6', '').replace(',', '').strip()
            rlink = extract_data(r_node, './/td[2]//a/@href').strip()

            if "http" not in rlink:
                rlink = "http://www.uci.html.infostradasports.com" + rlink

            r_team = extract_data(r_node, './/td[3]/text()').strip()
            if '(' in r_team:
                r_extra = "".join(re.findall(r'\(.*\)', r_team))
                r_team = r_team.replace(r_extra, '').strip()
            time_ = (str(extract_data(r_node, './/td[6]//text()').strip()))
            if not time_:
                time_ = (str(extract_data(r_node, './/td[5]//text()').strip()))
            if response.meta['tou_name'] == "UCI Road World Championships":
                time_ = (str(extract_data(r_node, './/td[5]//text()').strip()))
            time_ = time_.replace('.', ':').strip()
            if not '+' in time_:
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
        if response.meta['stage'] and "USA Pro Cycling Challenge" in response.meta['tou_name']:
            record['event'] = response.meta['tou_name'] + " " + response.meta['stage']
        record['tournament']    = response.meta['tou_name']
        record['game_datetime'] = response.meta['game_dt']
        record['game']          = "cycling"
        record['game_status']  = response.meta['status']
        record['source_key']    = response.meta['tou_sk']
        record['reference_url'] = response.url
        record['source']        = "uciworldtour_cycling"
        record['affiliation']   = "uci"
        record['rich_data'] = {'game_note': game_note}
        record['season'] = response.meta['year']
        record['result'] = result
        record['participant_type'] = "player"
        import pdb;pdb.set_trace()
        yield record
