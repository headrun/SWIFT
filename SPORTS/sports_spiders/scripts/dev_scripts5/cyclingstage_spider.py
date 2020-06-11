import time
import datetime
import re
from vtvspider_dev import VTVSpider, get_nodes, extract_data, \
get_utc_time, extract_list_data
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

class CyclingStage(VTVSpider):
    name = "cycling_stage"
    allowed_domains = []
    start_urls = ['http://www.cyclingstage.com/tour-down-under-2016/', \
                  'http://www.cyclingstage.com/paris-nice-2016/', \
                  'http://www.cyclingstage.com/vuelta-2016-route/', \
                  'http://www.cyclingstage.com/giro-2016-route/', \
                  'http://www.cyclingstage.com/volta-a-catalunya-2016/', \
                  'http://www.cyclingstage.com/tour-of-the-basque-country-2016/', \
                  'http://www.cyclingstage.com/tour-de-romandie-2016/', \
                  'http://www.cyclingstage.com/tour-de-suisse-2016/', \
                  'http://www.cyclingstage.com/eneco-tour-2016/']

    def parse(self, response):
        sel       = Selector(response)
        record    = SportsSetupItem()
        tou_name  = extract_list_data(sel, '//article[@itemprop="articleBody"]//h2/text()')[0]
        tour_name = tou_name.encode('utf-8').replace('\xc3\xa9', '').split(' 2015')[0].strip()

        if "Critrium du Dauphin" in tour_name:
            tour_name = "Criterium du Dauphine"
        if "Giro" in tour_name:
            tour_name = "Giro d'Italia"
        if "Vuelta" in tour_name:
            tour_name = "Vuelta a Espana"
        if "paris-nice" in response.url:
            tour_name = "Paris-Nice"
        if "tour-de-romandie" in response.url:
            tour_name = "Tour de Romandie"

        year      =  tou_name.split(':')[0]
        year      = "".join(re.findall(r'\d+', response.url))
        st_nodes  = get_nodes(sel, '//div//table//tbody//tr')
        for st_node in st_nodes:
            stage_details = extract_list_data(st_node, './/td//text()')

            if not stage_details or "Rest day" in stage_details or "rest day" in stage_details:
                continue

            if len(stage_details) == 10:
                stage, date, tou, start_end, distance, type_, result, winner, leader, leader = stage_details
            elif len(stage_details) == 6:
                stage, date, tou, start_end, distance, type_ = stage_details
            elif len(stage_details) == 4:
                stage, date, start_end, type_ = stage_details
                distance = tou = ''
            elif len(stage_details) == 5:
                stage, date, start_end, distance, type_ = stage_details
                tou = ''
            '''start_end  = modify(start_end).replace('\xc3\xa9', '').\
                        replace('\xe2\x80\x93', '').replace('  ', ' to '). \
                        replace('\xc2\xa0', '').replace(' -', ' to '). \
                        replace('\xc3\xbc', '').replace('\xe2\x80\x88', ' ').\
                        replace('\xc3\xac', '').replace('\xc3\xa8', ''). \
                        replace('\xc3\xba', '').replace('\xc3\xad', ''). \
                        replace('\xc3\xb3', '').replace('\xc2\xb4', ''). \
                        replace('\xc3\x81', '').replace('\xc3\xa1', ''). \
                        replace('\xc3\xb4', '')'''
            if distance:
                game_note  = "Stage " + stage + ":" + " " + start_end + " - "+ distance
            else:
                game_note  = "Stage " + stage + ":" + " " + start_end
            if "Stage P" in game_note:
                continue
            date_      = date.split(' ')[-1].replace('-', '-0').split('-')
            source_key = tour_name.replace(' ', '_').replace('-', '_')+ '_' + "Stage" + "_"+stage + '_'+year +date_[-1]+date_[0]
            game_date  = year +date_[-1]+date_[0]
            pattern = "%Y%m%d"
            tou_datetime = get_utc_time(game_date, pattern, 'US/Eastern')
            record['tournament']    = tour_name
            record['game_datetime'] = tou_datetime
            record['game']          = "cycling"
            record['game_status']   = "scheduled"
            record['source_key']    = source_key
            record['reference_url'] = response.url
            record['source']        = "cycling"
            record['affiliation']   = "uci"
            record['rich_data'] = {'game_note': game_note}
            record['participant_type'] = "player"
            record['time_unknown'] = '1'
            record['result'] = {}
            record['participants'] = {}
            yield record



