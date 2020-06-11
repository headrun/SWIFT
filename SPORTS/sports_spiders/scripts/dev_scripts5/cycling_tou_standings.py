import re
from vtvspider_dev import VTVSpider, get_nodes, \
extract_data, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

def get_time(initial_time, gap):
    if '+' in gap:
        if ' ' in gap:
            split_time = gap.split(' ')
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
        initial_time_split = initial_time.split(" ")
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

def get_refined_tou_name(tour_name):
    for i in NEEDED_TOURNAMENTS:
        if i in tour_name:
            tour_name = i

    return tour_name

NEEDED_TOURNAMENTS = {'Tour Down Under' : 'Tour Down Under', \
                        'Paris - Nice'  : 'Paris-Nice', \
                        'Tirreno - Adriatico' : 'Tirreno - Adriatico', \
                        'Volta a Catalunya' : 'Volta Ciclista a Catalunya', \
                        'Vuelta al Pas Vasco' : 'Vuelta Ciclista al Pais Vasco', \
                        'Tour de Romandie' : 'Tour de Romandie',  \
                        'Tour de France': 'Tour de France', \
                        'Eneco Tour'    : 'Eneco Tour', \
                        'Vuelta a Espaa' : 'Vuelta a Espana',  \
                        'Tour de Suisse' : 'Tour de Suisse', \
                        'Tour de Pologne' : 'Tour de Pologne', \
                        'Critrium du Dauphin' : 'Criterium du Dauphine', \
                        "Giro d'Italia" : "Giro d'Italia", \
                        'Vuelta a Espana': 'Vuelta a Espana'
                    }
NEEDED_TOURNAMENTS = {'Vuelta a Espaa' : 'Vuelta a Espana'}

class CyclingTouStandings(VTVSpider):
    name = "cycling_tou_standings"
    allowed_domains = []
    start_urls = ['http://www.eurosport.com/cycling/calendar-result_sea89.shtml']

    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//div[@class="tournament-list"]//ul//li')

        for tm_node in tm_nodes:
            tou_link = extract_data(tm_node, './/a//@href')

            if not tou_link:
                continue
            '''if "criterium-du-dauphine" not in tou_link:
                continue'''
            if "http" not in tou_link:
                tou_link = "http://www.eurosport.com" + tou_link
                yield Request(tou_link, callback = self.parse_standings)

    def parse_standings(self, response):
        riders = {}
        record = SportsSetupItem()
        sel = Selector(response)
        st_nodes = get_nodes(sel, '//div[@class="content standings active"]//tr')
        tou_name = extract_data(sel, '//ul//li[@class="title font-montserrat"]//a//text()')
        tou_name = tou_name.strip().encode('utf8'). \
        replace('\xc3\xa9', '').replace('\xc3\xad', '').replace('\xc3\xb1', '').strip()

        try:
            tou_name = NEEDED_TOURNAMENTS[tou_name]
        except:
            tou_name = ''
            return

        for st_node in st_nodes:
            position = extract_data(st_node, './/td[@class="position"]//text()')
            pl_name  = extract_data(st_node, './/td[@class="player"]//a//text()')

            if not pl_name:
                continue

            pl_url   = extract_data(st_node, './/td[@class="player"]//a//@href')
            time_  = extract_data(st_node, './/td[@class="time"]//text()'). \
            replace('h', '').replace('"', '').replace("'", '')
            pl_sk = pl_url.split('/')[2].split('_')[0]
            if not '+' in time_:
                initial_time = time_
                time_ = extract_data(st_node, './/td[@class="time"]//text()')

            else:
                time_ = get_time(initial_time, time_)
            riders[pl_sk] = {'rank' : position, 'time' : time_}

        record['result']           = riders
        record['season']           = "2015"
        record['source']           = 'eurosport_cycling'
        record['tournament']       = tou_name
        record['participant_type'] = 'player'
        record['result_type']      = 'tournament_standings'
        import pdb;pdb.set_trace()
        yield record

