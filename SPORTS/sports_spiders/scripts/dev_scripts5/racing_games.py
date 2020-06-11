import datetime
import time
import urllib2
import re
import unicodedata
from vtvspider_dev import VTVSpider, extract_data, get_nodes, get_utc_time, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from lxml import etree
from datetime import timedelta

RECORD = SportsSetupItem()

TOU_DICT = {
'NASCAR': '',
'Telecast Pres. by Go Daddy': '',
'Honda Grand Prix of Alabama': 'Grand Prix of Alabama',
'Honda Grand Prix of St. Petersburg': 'Grand Prix of St. Petersburg',
'Shell-Pennzoil Grand Prix of Houston 2':\
'Grand Prix of Houston - Race 2',
'Shell-Pennzoil Grand Prix of Houston 1':\
'Grand Prix of Houston -Race 1',
'GoPro Indy Grand Prix of Sonoma': 'GoPro Grand Prix of Sonoma',
'Toyota Grand Prix of Long Beach': 'Grand Prix of Long Beach',
'Honda Indy Grand Prix of Alabama':'Indy Grand Prix of Alabama',
'German Grand Prix': 'Grand Prix of Germany',
'Firestone Grand Prix of St. Petersburg': 'Grand Prix of St. Petersburg',
"Angie's List Grand Prix of Indianapolis": "Grand Prix of Indianapolis",
"Honda Indy 200 at Mid-Ohio": "Honda Indy 200",
"The Brickyard 400": "Brickyard 400",
"Sprint All-Star Race": "Sprint Cup Series All-Star Race",
"Nationwide Series at Ohio": "Nationwide Series at Mid-Ohio",
"Nationwide Series at Miami": "Nationwide Series at Homestead-Miami",
"Phoenix Grand Prix"    : "XM Satellite Radio Indy 200",
"Grand Prix of Louisiana" : "Indy Grand Prix of Louisiana",
"Iowa Corn 300" : "Iowa Corn Indy 300",
"Road America"  : "Grand Prix of Road America",
"Chevrolet Dual in Detroit - Race 1" : "Detroit Belle Isle Grand Prix",
"Chevrolet Dual in Detroit - Race 2" : "Detroit Belle Isle Grand Prix",
"NASCAR XFINITY SERIES AT POCONO" : "Pocono 250"
}

TOU_NAMES_DICT = {'Camping World Truck Series at Kansas': 'SFP 250',
                  'Camping World Truck Series at Charlotte' :\
                  'North Carolina Education Lottery 200',
                  'Camping World Truck Series at Atlanta': 'Atlanta 200',
                  'Camping World Truck Series at Dover' : 'Lucas Oil 200',
                  'Camping World Truck Series at Kentucky' : 'UNOH 225',
                  'Camping World Truck Series at St. Louis': 'CampingWorld.com 200',
                  'Camping World Truck Series at Canada': 'Chevy Silverado 250',
                  'Camping World Truck Series at Iowa' :\
                  'American Ethanol 200',
                  'Camping World Truck Series at Eldora' :\
                  'CarCash Mudsummer Classic',
                  'Camping World Truck Series at Pocono' :\
                  'Pocono Mountains 150',
                  'Camping World Truck Series at Michigan' : 'VFW 200',
                  'Camping World Truck Series at Bristol' : 'UNOH 200',
                  'Camping World Truck Series at Bowmanville':\
                  'Chevy Silverado 250',
                  'Camping World Truck Series at Chicago':\
                  'American Ethanol 225',
                  'Camping World Truck Series at New Hampshire': 'UNOH 175',
                  'Camping World Truck Series at Las Vegas': "Rhino Linings 350",
                  'Camping World Truck Series at Talladega': "Fred's 250",
                  'Camping World Truck Series at Homestead-Miami':\
                  'Ford EcoBoost 200',
                  'Camping World Truck Series at Miami': 'Ford EcoBoost 200',
                  'Camping World Truck Series at Phoenix' : 'Lucas Oil 150',
                  'Camping World Truck Series at Texas': 'WinStar World Casino & Resort 350',
                  'Camping World Truck Series at Martinsville': 'Kroger 250',
                  'Camping World Truck Series at Gateway':\
                  'CampingWorld.com 200',
                  'Michigan National Guard 200': 'VFW 200',
                  'enjoyillinois.com 225': 'American Ethanol 225',
                  'Chevrolet Indy Dual in Detroit - Race 1':\
                  'Indy Dual in Detroit - Race 1',
                  'Chevrolet Indy Dual in Detroit - Race 2':\
                  'Indy Dual in Detroit - Race 2',
                  'Pocono INDYCAR 500': 'Pocono IndyCar 400',
                  'The Shell and Pennzoil Grand Prix of Houston - Race 1':\
                  'Grand Prix of Houston - Race 1',
                  'The Shell and Pennzoil Grand Prix of Houston - Race 2':\
                  'Grand Prix of Houston - Race 2',
                  'WinStar World Casino & Resort 400':\
                  'WinStar World Casino 400',
                  'Gateway 200': 'CampingWorld.com 200',
                  'Grand Prix of Houston (Race 1)':\
                  'Grand Prix of Houston - Race 1',
                  'Grand Prix of Houston (Race 2)':\
                  'Grand Prix of Houston - Race 2',
                  'German Grand Prix': 'Grand Prix of Germany',
                  '1-800-CarCash Mudsummer Classic':\
                  'CarCash Mudsummer Classic',
                  'Kentucky 201': 'UNOH 225',
                  "Camping World Truck Series at Daytona": "Nextera Energy Resources 250",
                  "NASCAR Sprint All-Star Race": "NASCAR Sprint All-Star Race"}

GRAND_PIX_DICT = {'Australian Grand Prix':\
                  'http://en.espnf1.com/australia/motorsport/race/138605.html',
                  'Grand Prix of Malaysia':\
                  'http://en.espnf1.com/malaysia/motorsport/race/138607.html',
                  'Grand Prix of China':\
                  'http://en.espnf1.com/china/motorsport/race/138611.html',
                  'Grand Prix of Bahrain':\
                  'http://en.espnf1.com/bahrain/motorsport/race/138609.html',
                  'Grand Prix Of Spain':\
                  'http://en.espnf1.com/spain/motorsport/race/138613.html',
                  'Grand Prix Of Monaco':\
                  'http://en.espnf1.com/monaco/motorsport/race/138615.html',
                  'Grand Prix Of Canada':\
                  'http://en.espnf1.com/canada/motorsport/race/138617.html',
                  'British Grand Prix':\
                  'http://en.espnf1.com/greatbritain/motorsport/race/138621.html',
                  'Grand Prix of Germany':\
                  'http://en.espnf1.com/germany/motorsport/race/138623.html',
                  'Hungarian Grand Prix':\
                  'http://en.espnf1.com/hungary/motorsport/race/138625.html',
                  'Belgian Grand Prix':\
                  'http://en.espnf1.com/belgium/motorsport/race/138627.html',
                  'Italian Grand Prix':\
                  'http://en.espnf1.com/italy/motorsport/race/138629.html',
                  'Singapore Grand Prix':\
                  'http://en.espnf1.com/singapore/motorsport/race/138631.html',
                  'Korean Grand Prix':\
                  'http://en.espnf1.com/korea/motorsport/race/95823.html',
                  'Japanese Grand Prix':\
                  'http://en.espnf1.com/japan/motorsport/race/138633.html',
                  'Grand Prix of India':\
                  'http://en.espnf1.com/india/motorsport/race/95825.html',
                  'Abu Dhabi Grand Prix':\
                  'http://en.espnf1.com/abudhabi/motorsport/race/138641.html',
                  'United States Grand Prix':\
                  'http://en.espnf1.com/usa/motorsport/race/138637.html',
                  'Grand Prix Of Brazil':\
                  'http://en.espnf1.com/brazil/motorsport/race/138639.html',
                  'Grand Prix of Russia':\
                  'http://en.espnf1.com/russia/motorsport/race/138635.html',
                  'Austrian Grand Prix':\
                  'http://en.espnf1.com/austria/motorsport/race/138619.html',
                  'Russian Grand Prix': 'http://en.espnf1.com/russia/motorsport/race/138635.html',
                  'Mexican Grand Prix': 'http://en.espnf1.com/mexico/motorsport/race/188293.html'}

NASCAR_SPORTS_RESULTS = 'http://espn.go.com/racing/nascar/'

REPLACE_TOU_DICT = {"Children's Hospital 200":\
                    {'tou_name': "Nationwide Series", 'event_name':\
                    "Nationwide Series at Mid-Ohio"},
                    "Honda Indy Toronto (Race 1)":\
                    {'tou_name': "Honda Indy Toronto",\
                    'event_name': "Honda Indy Toronto - Race 1"},
                    "Honda Indy Toronto (Race 2)":\
                    {'tou_name': "Honda Indy Toronto",\
                    'event_name': "Honda Indy Toronto - Race 2"},
                    "Irwin Tools Night Race":\
                    {'tou_name': "Sprint Cup Series",\
                    'event_name': "Sprint Cup Series at Bristol"},
                    "GEICO 400": {'tou_name': "Sprint Cup Series",\
                    'event_name': "Sprint Cup Series at Chicago"},
                    "Ford 400": {'tou_name': "Sprint Cup Series",\
                    'event_name': "Sprint Cup Series at Homestead-Miami"},
                    "Bank of America": {'tou_name': "Sprint Cup Series",\
                    'event_name': "Sprint Cup Series at Charlotte"},
                    "Sprint Cup Series at Darlington":\
                    {'tou_name': "Sprint Cup Series",\
                    'event_name': "Southern 500"},
                    "TreatMyClot.com 300": {'tou_name': "Nationwide Series",\
                    'event_name': "Nationwide Series at California"},
                    "Sprint Unlimited": {'tou_name': "Sprint Cup Series",
                    "event_name": "The Sprint Unlimited"},
                    "Sprint Cup Series at Miami":
                    {'tou_name': "Sprint Cup Series",
                    'event_name': "Sprint Cup Series at Homestead-Miami"}}

def replace_symbols(string):
    REPLACED_SYMBOLS = {'\n' : '', '-' : '', ':' : '', ' ' : '_', "'" : '', '.' : ''}
    for key, val in REPLACED_SYMBOLS.iteritems():
        string = string.replace(key, val)

    return string


REPLACE_WORDS = ['This race has rescheduled to run on']

class RacingGames(VTVSpider):
    name = "auto_racing"
    start_urls = []
    dont_filter = True

    next_week_days = []
    date_time = datetime.datetime.now()
    for i in range(0, 5):
        next_week_days.append((date_time - datetime.timedelta(days=i)).strftime('%Y-%m-%d'))

    def start_requests(self):
        top_urls = {'Sprint Cup Series': 'http://espn.go.com/racing/schedule/_/series/sprintscup'}
                    #'Xfinity Series': 'http://espn.go.com/racing/schedule/_/series/xfinity/year/2015',
                    #'Camping World Truck Series': 'http://espn.go.com/racing/schedule/_/series/camping',
                    #'IndyCar Series': 'http://espn.go.com/racing/schedule/_/series/indycar/year/2015',
                    #'formula one': 'http://espn.go.com/racing/schedule/_/series/f1'}
        for tou, top_url in top_urls.iteritems():
            yield Request(top_url, callback = self.parse, \
                            meta = {'tou_name': tou})

    def get_gamedate(self, game_date, current_year):
        game_datetime = game_date.replace('\n', '').replace(u'\xa0', '').replace('ET', '').\
                        replace('Noon', '12:00 pm').strip()
        if "TBD" in game_datetime:
            game_datetime = game_datetime.replace('TBD', '00:00 am')
            RECORD['time_unknown'] = "1"

        game_datetime = current_year + ' ' + game_datetime
        if "." in game_datetime:
            sk_date = (datetime.datetime(*time.strptime(game_datetime.strip(), '%Y %a,%b.%d  %H:%M %p')[0:6])).date()
            game_dt = get_utc_time(game_datetime, '%Y %a,%b.%d  %H:%M %p', "US/Eastern")
        else:
            sk_date = (datetime.datetime(*time.strptime(game_datetime.strip(), '%Y %a,%b%d  %I:%M %p')[0:6])).date()
            game_dt = get_utc_time(game_datetime, '%Y %a,%b%d  %I:%M %p', "US/Eastern")
        return sk_date, game_dt

    def parse(self, response):
        hxs = Selector(response)
        tou_name = response.meta['tou_name']
        schedule_link = extract_list_data(hxs, '//option[contains(@value, "schedule")]/@value')
        if'indycar' in response.url:
            schedule_link = schedule_link[0]
            yield Request(schedule_link, callback=self.parse_next, meta={'tou_name': tou_name})
        elif "xfinity" in response.url:
            schedule_link = schedule_link[1]
            yield Request(schedule_link, callback=self.parse_next, meta={'tou_name': tou_name})
        else:
            yield Request(response.url, callback=self.parse_next, meta={'tou_name': tou_name}, dont_filter=True)

    def parse_next(self, response):
        hxs = Selector(response)
        if "indycar" in response.url:
            affiliation = "indycar"
        elif "f1" in response.url:
            affiliation = "f1"
        else:
            affiliation = "nascar"

        RECORD['affiliation'] = affiliation
        RECORD['game'] = "auto racing"
        RECORD['source'] = "espn_motorsports"
        RECORD['participant_type'] = 'player'
        RECORD['time_unknown'] = '0'

        nodes = get_nodes(hxs, '//div[@class="mod-content"]/table//tr[contains(@class, "row")]')
        current_year = extract_data(hxs, '//select[@class="tablesm"]//option[@selected]/text()').strip()
        if not current_year:
            current_year = "2016"
        RECORD['season'] = current_year
        for node in nodes:
            game_datetime = extract_data(node, './td[@valign="top"][1]//text()', " ")

            sk_date, game_dt = self.get_gamedate(game_datetime, current_year)

            reschedule_time = ''
            channels = extract_data(node, './/td[3]/text()')
            if not channels:
                channels = extract_data(node, './/td[3]/a/@href')
                if channels:
                    channels = channels.split('/')[-2].replace('sports', '')
            track_name = extract_data(node, './/td[2]/text()').replace('\n*l', '').strip()
            if "*This race has been rescheduled to run on" in track_name:
                track_name = "".join(track_name.split('*')[0]).strip() or \
                            "".join(track_name.split('\n')[0]).strip()
            elif "race has been canceled" in track_name.lower():
                status = "cancelled"
                game_note = "".join(track_name.split('*')[-1]).strip()
                track_name = "".join(track_name.split('*')[0]).strip()
            else:
                status = "scheduled"
                game_note = ''
                track_name = track_name
            rich_data = {'stadium': track_name, 'game_note': game_note, 'location': '', 'channels': str(channels)}

            reschedule_text = extract_data(node, './/td[contains(string(), "*This race has rescheduled")]//text()').strip()
            if not reschedule_text:
                reschedule_text = extract_data(node, './/td[contains(string(), "*This race has been rescheduled")]//text()').strip()

            if reschedule_text:
                reschedule_time = re.findall('This race has rescheduled to run on(.*)', reschedule_text)
                if not reschedule_time:
                    reschedule_time = re.findall('This race has been rescheduled to run on(.*)', reschedule_text)
                reschedule_time = reschedule_time[0].strip()
                reschedule_time = get_utc_time(reschedule_time.replace('ET', '').strip(), '%m/%d/%Y %I:%M %p', "US/Eastern")
                if reschedule_time:
                    game_dt = reschedule_time

            old_tou_name = extract_data(node, './/td[2]/b/text()').replace('NASCAR XFINITY SERIES AT ', 'Nationwide Series at ').strip()
            tournament_name = response.meta['tou_name']

            if old_tou_name=="Nationwide Series at MIAMI":
                old_tou_name ="Nationwide Series at Homestead-Miami" 

            for tou1, tou2 in TOU_DICT.iteritems():
                old_tou_name = old_tou_name.replace(tou1, tou2).strip()

            old_tou_name = old_tou_name.split(' pres. ')[0].strip()
            tou_name = old_tou_name.split(' at ')[0].strip()
            if tou_name == 'Grand Prix of Belgium':
                tou_name= 'Belgian Grand Prix'

            if game_dt:
                game_stamp = str(sk_date).split(' ')
                game_sk = replace_symbols(game_stamp[0])
                if "Can-Am Duel" in old_tou_name:
                    tournament_sign = tournament_name + "_" + old_tou_name.replace('#', '')
                else:
                    tournament_sign = tournament_name
                tou_signature = tournament_sign.replace(' ', '_').replace("'", '').\
                                replace('-', '_').replace('.', '')

                game_sk = '%s_%s' % (tou_signature, game_sk)

            track_name = extract_data(node, './/td[2]/text()').replace('\n*l', '').strip()
            if "*This race has been rescheduled to run on" in track_name:
                track_name = "".join(track_name.split('\n')[0]).strip()
            else:
                track_name = track_name
            game_link = extract_data(node, './td[@valign="top"]//a[contains(text(), "Race Results")]/@href').strip()
            pole_link = extract_data(node, './td[@valign="top"]//a[contains(text(), "Starting Grid")]/@href').strip()
            qual_link = extract_data(node, './td[@valign="top"]//a[contains(text(), "Qualifying Speeds")]/@href').strip()
            tou_details = REPLACE_TOU_DICT.get(old_tou_name, '')
            if tou_details:
                tou_name = tou_details.get('tou_name', '')
                event = tou_details.get('event_name', '')
            else:
                tou_name, event = tournament_name, old_tou_name

            if tou_name == '' and event == '':
                tou_name = tournament_name
                if "/nationwide" in response.url:
                    event = old_tou_name
                elif "/sprintscup" in response.url:
                    event = old_tou_name

            if "Sprint Showdown" in event.title():
                event = 'Sprint Cup Series Showdown'
            if "Sprint All-Star Race" in event.title():
                event = "NASCAR Sprint All-Star Race"
            if "Brickyard" in event.title():
                event = "Brickyard 400"
            if "Sprint Cup Series at Kansas" in event.title():
                event = "Sprint Cup Series at Kansas City"
            if "Sprint Cup Series at Miami" in event.title():
                event = "Sprint Cup Series at Homestead-Miami"

            print event
            if "/camping" in response.url or '/indycar' in response.url:
                if old_tou_name.isupper():
                    old_tou_name = old_tou_name.title().replace(' At ', ' at ')
                tname = TOU_NAMES_DICT.get(old_tou_name, '')
                if tname:
                    tou_name = tname
                    event = ''
            if game_sk == "Nationwide_Series_20150704":
                event = "Subway Firecracker 250"
            if affiliation == "f1":
                tou_name = old_tou_name

            if game_sk == 'Camping_World_Truck_Series_20151031':
                tou_name = 'kroger 200'
                event = ''

            if game_sk == 'Camping_World_Truck_Series_20151106':
                tou_name = 'WinStar World Casino & Resort 350'
                event = ''
            if 'formula_one_20150812' in game_sk:
                continue


            if "NNS" in event:
                event = 'Nationwide Series at Talladega'

            data = {'tou_name': tou_name, 'stats': '',
                    "event": event.replace('#', ''), 'game_sk': game_sk,
                    'rich_data': rich_data, 'affiliation': affiliation}
            if game_dt.split(' ')[0] not in self.next_week_days and game_link:
                continue
            if game_dt.split(' ')[0] not in self.next_week_days and status in ['cancelled']:
                continue
            new_date = str((datetime.datetime(*time.strptime(game_dt, '%Y-%m-%d %H:%M:%S')[0:6])) - timedelta(1))

            if pole_link:
                data.update({'status': "completed", 'game_dt': new_date})
                yield Request(pole_link, callback = self.parse_qualifying, meta={'data': data, 'name_dict': {}})
            if qual_link:
                data.update({'status': "completed", 'game_dt': new_date})
                yield Request(qual_link, callback = self.parse_qualifying, meta={'data': data, 'name_dict': {}})
            if game_link:
                data.update({'status': "completed", 'game_link': game_link, 'game_dt': game_dt})
                if "f1" in response.url:
                    if tou_name == 'Grand Prix of Belgium':
                        tou_name = 'Belgian Grand Prix'
                    race_link = GRAND_PIX_DICT.get(tou_name, '')
                    yield Request(race_link, callback=self.parse_race_time, meta={'data': data})
                else:
                    yield Request(game_link, callback = self.parse_results, \
                              meta = {'data': data, 'name_dict': {}})
            elif pole_link:
                data.update({'status': "ongoing"})
                yield Request(pole_link, callback = self.parse_results, meta={'data': data, 'name_dict': {}})
            elif status not in ['completed', 'ongoing']:
                game_sks = []
                game_sks.append(['Final', game_sk, game_dt])
                if tournament_name.lower() in ['indycar series', 'formula one']:
                    game_sks.append(['Qualifying Speeds', 'qualifying' + "_" + game_sk, new_date])
                game_sks.append(['Grid', 'grid' + "_" + game_sk, new_date])
                for sk in game_sks:
                    game_note, source_key, game_dt = sk
                    rich_data.update({'game_note': game_note})
                    RECORD['rich_data'] = rich_data
                    RECORD['game_status'] = status
                    RECORD['result'] = {}
                    RECORD['source_key'] = source_key
                    RECORD['participants'] = {}
                    RECORD['game_datetime'] = game_dt
                    RECORD['reference_url'] = response.url

                    if 'indycar' in response.url:
                        RECORD['tournament'] = event
                        RECORD['event'] = event
                    else:
                        RECORD['tournament'] = tou_name
                        RECORD['event'] = event
                    yield RECORD

    def parse_qualifying(self, response):
        hxs = Selector(response)
        players = {}
        drivers = {}
        data = response.meta['data']
        RECORD['game_status'] = data['status']
        RECORD['tournament'] = data['tou_name']
        RECORD['game_datetime'] = data['game_dt']
        RECORD['event'] = data['event']
        RECORD['affiliation'] = data['affiliation']

        pole_nodes = get_nodes(hxs, '//div[@class="mod-content"]//tr[contains(@class, "row player")]')
        pole_headers = [(i).strip() for i in extract_list_data(hxs, '//div[@class="mod-content"]//tr[@class="colhead"]//td//text()')]
        is_pole_time = False
        if "TIME BEHIND" in pole_headers:
            is_pole_time = True
        pole_winner = extract_data(hxs, '//div[@class="mod-content"]/table/tr[td[strong[contains(text(), "Driver")]]]/td/a/@href')
        pole_winner_id = ''
        if pole_winner:
            pole_winner_id = ''.join(re.findall('\d+', pole_winner))
        for pole_node in pole_nodes:
            pole_pos = extract_data(pole_node, './/td[1]//text()').strip()
            id_ = extract_data(pole_node, './/td/a[contains(@href, "/driver/")]/@href').strip()
            name = extract_data(pole_node, './/td/a[contains(@href, "/driver/")]//text()').strip()
            if is_pole_time:
                pole_time = extract_data(pole_node, './/td[6]//text()').strip()
            else:
                pole_time = ''
            id_ = id_.split('/_/id/')[-1].split('/')[0].strip()
            players[id_] = (0, name)
            if 'grid' in response.url:
                drivers[id_] = {'pole': pole_pos}
            else:
                drivers[id_] = {'qualifying_position': pole_pos, 'qualifying_time': pole_time}
        if 'grid' in response.url:
            RECORD['source_key'] = 'grid' + "_" + data['game_sk']
            drivers['0'] = {'pole_winner': pole_winner_id}
            data['rich_data'].update({'game_note': 'Grid'})
        elif 'qualifying' in response.url:
            data['rich_data'].update({'game_note': 'Qualifying Speeds'})
            RECORD['source_key'] = 'qualifying' + "_" + data['game_sk']
        RECORD['rich_data'] = data['rich_data']
        RECORD['participants'] = players
        RECORD['result'] = drivers
        RECORD['reference_url'] = response.url
        yield RECORD

    def parse_race_time(self, response):
        name_dict = {}
        hxs =  Selector(response)
        data = response.meta['data']
        race_nodes = get_nodes(hxs, '//div[@id="main"]//tbody//tr[contains(@class, "racedata")]')
        for r_n in race_nodes:
            pname = extract_data(r_n, './/td[@class="left"]/a[contains(@href, "/driver/")]//text()').strip()
            qualifying_time = extract_data(r_n, './/td[contains(@style, "padding-right")][2]/text()').replace('-', ' ').strip() or \
                          extract_data(r_n, './/td[contains(@style, "padding-right")][2]/b/text()').replace('-', ' ').strip()
            final_time = extract_data(r_n, './/td[contains(@style, "padding-right")][1]/preceding-sibling::td[1]//text()').replace('-', ' ').strip() or \
                    extract_data(r_n, './/td[9]//text()')
            laps_completed = extract_data(r_n, './/td[8]//text()').strip()

            pos = extract_data(r_n, './/td[@class="center"]//text()')
            if len(pos) == 3:
                final_pos = extract_data(r_n, './/td[@class="center"][3]//text()').strip()
            elif len(pos) == 2:
                final_pos = extract_data(r_n, './/td[@class="center"][2]//text()').strip()
            else:
                final_pos = extract_data(r_n, './/td[@class="center"][1]//text()').strip()
            if ('-' in final_pos) and (not final_time):
                final_time = " "
            elif not final_time:
                final_time = extract_data(r_n, './/td[contains(@style, "padding-right")][1]/text()').replace('-', ' ').strip()

            status_text = extract_data(r_n, './preceding-sibling::tr[@raceclass]//text()').strip()
            if "retire" in status_text and not final_time:
                final_time = 'Retired'
            if not final_time:
                final_time = 'Retired'
            if type(pname) == unicode:
                pname = unicodedata.normalize('NFKD', pname).encode('ascii','ignore')
                name_dict[pname] = (str(final_time), str(qualifying_time), str(laps_completed))

        yield Request(data['game_link'], callback=self.parse_results, meta={'data': data, 'name_dict': name_dict})


    def parse_results(self, response):
        hxs = Selector(response)
        game_data = {}
        players = {}
        data = response.meta['data']
        RECORD['game_status'] = data['status']
        RECORD['source_key'] = data['game_sk']
        RECORD['tournament'] = data['tou_name']
        RECORD['game_datetime'] = data['game_dt']
        RECORD['event'] = data['event']
        RECORD['rich_data'] = data['rich_data']
        RECORD['affiliation'] = data['affiliation']
        if not data['stats']:
            race_dict = extract_data(hxs, '//script[@type="text/javascript"][contains(text(), "var sbMaster")]')
            if race_dict:
                race_dict = race_dict.replace('var sbMaster = ', '')
                race_dict = eval(race_dict)
                race_dict = race_dict['sports']
                temp_dict = {}
                for race in race_dict:
                    data['stats'] = 1
                    if race['sport'] == 'rpm':
                        entries = race['events'][0]
                        tournament_name = entries['display']
                        date_time = entries['id'][:8]
                        tournament_name = tournament_name.\
                                        split(' at ')[0].strip()
                        for entry in entries['entries']:
                            status = entry['status']
                            id_ = entry['id']
                            temp_dict[id_] = status
                        game_data['drivers'] = temp_dict
                        for tou1, tou2 in TOU_DICT.iteritems():
                            tournament_name = tournament_name.replace(tou1, tou2).strip()
                        tournament_name = tournament_name.replace(' ', '_').replace('-', '_') + '_' + date_time
                        game_data['game_sk'] = tournament_name

        drivers = {}
        game_sk = response.url.split('raceId=')[-1].split('&')[0].strip()
        if "completed" in data['status']:
            nodes = get_nodes(hxs, '//div[@class="mod-content"]//tr[contains(@class, "row player")]')
            final_headers = [(i).strip() for i in extract_list_data(hxs, '//div[@class="mod-content"]//tr[@class="colhead"]//td/text()')]
            is_time = False
            if "TIME BEHIND" in final_headers:
                is_time = True

            race_name_dict = response.meta['name_dict']
            for node in nodes:
                final_pos = extract_data(node, './/td[@align="left"]//text()').strip()
                id_ = extract_data(node, './/td/a[contains(@href, "/driver/")]/@href').strip()
                name = extract_data(node, './/td/a[contains(@href, "/driver/")]//text()').strip()
                id_ = id_.split('/_/id/')[-1].split('/')[0].strip()
                laps_info = extract_list_data(node, './/td[@align="right"]//text()')[0]

                if race_name_dict:
                    res_times = race_name_dict.get(name, '')
                    try:
                        pole_time = res_times[1]
                        final_time = res_times[0]
                        laps_completed = res_times[2]
                    except:
                        pole_time = final_time = laps_completed = ''
                else:
                    if is_time:
                        final_time = extract_data(node, './/td[6]//text()').strip()
                    else:
                        final_time = ''
                    pole_time = ''
                    laps_completed = ''
                if laps_completed:
                    laps_info = laps_completed
                players[id_] = (0, name)
                if final_pos == "1":
                    drivers['0'] = {'winner': id_}
                drivers[id_] = {'final_position': final_pos, 'final_time': final_time, 'qualifying_time': pole_time, 'laps_completed': laps_info}


        RECORD['participants'] = players
        RECORD['result'] = drivers
        RECORD['rich_data']['game_note'] = 'Final'
        RECORD['reference_url'] = response.url

        if RECORD['game_status'] in ["completed", 'ongoing']:
            yield RECORD

    def get_grid_res(self, link):
        res = urllib2.urlopen(link)
        html = res.read()
        data = etree.HTML(html)
        return data
