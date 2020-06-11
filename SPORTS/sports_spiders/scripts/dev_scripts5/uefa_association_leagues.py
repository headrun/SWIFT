# -*- coding: utf-8 -*-
import re
import time
import datetime
import urllib
from vtvspider_new import VTVSpider
from scrapy.http import Request
from datetime import timedelta
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import log, get_utc_time, get_tzinfo
from vtvspider_new import get_nodes, extract_data, extract_list_data, get_tzinfo

days = []
_now = datetime.datetime.now().date()
yday  = (_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
days.append(yday)
today =  _now.strftime("%Y-%m-%d")
days.append(today)
tomo =  (_now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
days.append(tomo)

LOCATION_DICT = {'Tirane': "Tirana", "Korca": "Korce", "Soligorsk": "Salihorsk", "Borisov": "Barysaw", "Novopolotsk": "Navapolatsk", "Mahilyou": "Mogilev", "Zhodino": "Zhodzina", "Kuopion": "Kuopio"}


class UefaAssociationLeagues(VTVSpider):
    name = 'uefa_association_leagues'
    domain_url = 'http://www.uefa.com/'
    tournaments_dict ={'aze'  : 'Azerbaijan Premier League', 'blr' :'Belarusian Premier League',
             'bih'  : 'Premier League of Bosnia and Herzegovina', 'bul' : 'Bulgarian A Football Group',
             'cyp'  : 'Cypriot First Division', 'cro' : 'Croatian First Football League',
             'cze'  : 'Czech First League', 'est' : 'Meistriliiga',
             'fro'  : 'Faroe Islands Premier League',
             'mkd'  : 'Macedonian First Football League', 'geo'  : 'Umaglesi Liga',
             'gib'  : 'Gibraltar Premier Division', 'isr' : 'Israeli Premier League',
             'hun'  : 'Nemzeti Bajnokság I', 'kaz'   : 'Kazakhstan Premier League',
             'lva'  : 'Latvian First League', 'lux'   : 'Luxembourg National Division',
             'mlt'  : 'Maltese Premier League', 'mda'   : 'Moldovan National Division',
             'mne'  : 'Montenegrin First League', 'irl' : 'Irish Premier Division',
             'nir'  : 'NIFL Premiership',
             'smr'  : 'Campionato Sammarinese di Calcio', 'srb'    : 'Serbian SuperLiga',
             'svk'  : 'Slovak Super Liga', 'svn' : 'Slovenian PrvaLiga',
             'wal'  : 'Welsh Premier League',
             'ltu'  : 'A Lyga', 'arm' : 'Armenian Premier League',
             'and'  : 'Primera Divisió', 'alb' : 'Albanian Superliga'}
    tournaments_dict = {'bul' : 'Bulgarian A Football Group'}

    start_urls = []

    COUNTRY_DICT = {'alb': 'Albania', 'and': 'Andorra', 'arm': 'Armenia', 'aze': 'Azerbaijan', 'blr': "Belarus", 'cyp': "Cyprus", 'cze': "Czech Republic", 'est': "Estonia","fro": "Faroe Islands", 'mkd': "Republic of Macedonia", 'geo': "Georgia", 'hun': "Hungary", 'bih': "Bosnia and Herzegovina", 'isr': "Israel", 'kaz': "Kazakhstan", 'lva': "Latvia", 'ltu': "Lithuania", 'lux': "Luxembourg", 'mlt': "Malta", 'mda': "Moldova", "mne": "Montenegro", "irl": "Ireland", "smr": "San Marino", "srb": "Serbia", "svk": "Slovakia", "svn": "Slovenia", "bul": "Bulgaria", "cro": "Croatia", 'eng': "England"}

    def get_country(self, res_tou):
        country = ''
        for key, value in self.COUNTRY_DICT.iteritems():
            if key == res_tou:
                country = value
        return country

    def start_requests(self):
        required_tournaments = ['alb', 'and', 'arm', 'aze', 'blr', 'cyp', 'cze', 'est', 'fro',
                            'mkd', 'geo', 'gib', 'hun', 'bih', 'isr', 'kaz',
                            'lva', 'ltu', 'lux', 'mlt', 'mda', 'mne' ,'irl',
                            'smr', 'srb', 'svk', 'svn', 'bul', 'cro']
        required_tournaments = ['bul']

        months_list = []
        month = datetime.datetime.now().month
        if self.spider_type == 'scores':
            for i in range(2):
                m = month - i
                months_list.append(m)

        elif self.spider_type == 'schedules':
            current_month = datetime.datetime.now().month
            months_list = range(current_month, 13)
            months_list = [10, 11, 12, 1, 2, 3, 4, 5]

        top_url = 'http://www.uefa.com/memberassociations/association=%s/domesticleague/matches/month=%s/_includematch.html'
        for tou in required_tournaments:
            for month in months_list:
                url = top_url % (tou, month)
                url = "http://www.uefa.com/memberassociations/association=bul/domesticleague/matches/month=3/_includematch.html"
                yield Request(url, callback = self.parse, meta = {'res_tou': tou})

    def parse(self, response):
        sel = Selector(response)
        nodes =  get_nodes(sel, '//tr[contains(@class, "fs15 nbb")]')
        for node in nodes:
            status = ''
            winner = ''
            game_note = ''
            home_id     = extract_data(node, './/td[@class="r"]//a/@href')
            away_id     = extract_data(node, './/td[@class="l"]//a/@href')
            score       = extract_data(node, './/td[@class="fs19 b nw"]//a//text()')
            match_url   = extract_data(node, './/td[@class="fs19 b nw"]//a/@href')

            match_url   = self.domain_url + match_url
            game_time   = extract_data(node, './/preceding::td[@class="headInfo"][1]//p[@class="mlogdate"]//text()').replace("\n", "")

            if "- " in game_time:
                game_note = game_time.split('- ')[1].strip()

            venue       = extract_data(node, './/following::tr[@class="mtc-venue npd-tb"][1]//text()').replace("Venue: ", "")
            game_time   = re.findall("(\d+/\d+/\d+)", game_time)

            match_id    = re.findall('match=(\d+)', match_url)
            match_reson = extract_data(node, './/preceding-sibling::tr//td/a[contains(text(), "Post")]//text()')
            if not match_reson:
                match_reson = extract_data(node, './/preceding-sibling::tr//td//a[contains(text(), "post")]//text()')
            if "postponed" in match_reson:
                game_note = "Match postponed"
                status = "postponed"
            home_id     = re.findall('club=(\d+)', home_id)
            away_id     = re.findall('club=(\d+)', away_id)

            home_scores = extract_data(node, './/following::tr[contains(@class,"mtc-scorers")][1]//td[@class="r"]//text()')
            away_scores = extract_data(node, './/following::tr[contains(@class,"mtc-scorers")][1]//td[@class="l"]//text()')

            hme_scores, awy_scores = [], []
            home_half_time = 0
            for scr in home_scores.split(","):
                scr = re.findall('(\d+)', scr)
                if scr:
                    if int(scr[0]) < 45:
                        home_half_time += 1

            away_half_time = 0
            for scr in away_scores.split(","):
                scr = re.findall('(\d+)', scr)
                if scr:
                     if int(scr[0]) < 45:
                         away_half_time += 1

            if "-" in score and status != "postponed":
                status  = 'completed'
                details = score.split("-")
                if len(details) > 1:

                       home_scores = { 'final' : details[0], 'H1' : home_half_time}
                       away_scores = { 'final' : details[1], 'H1' : away_half_time}

                       if details[0] > details[1]:
                            winner = home_id[0]
                       elif details[0] < details[1]:
                            winner = away_id[0]
                       elif details[0] == details[1]:
                            winner = ''
                else:
                    status = 'postponed'

            elif ":" in score:
                home_scores = ''
                away_scores = ''
                status = "scheduled"

            if status in ["scheduled", "postponed"]:
                if "-" in score: score = "00:00"
                game_time = game_time[0] +" "+score + ":00"
                date_object     = datetime.datetime(*time.strptime(game_time, '%d/%m/%Y %H:%M:%S')[0:6])
                game_date       = date_object.strftime('%Y-%m-%d %H:%M')
                now             = datetime.datetime.utcnow()
                date_now        = now.strftime("%Y-%m-%d %H:%M:%S")

                game_dt_time    = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M')[0:6])
                utc_dt_time     = game_dt_time - datetime.timedelta(hours=1)
                utc_game_date   = time.strftime("%Y-%m-%d %H:%M:%S", utc_dt_time.timetuple())
                score = ''

            elif status  == 'completed':
                game_time       = game_time[0]+ " "+ "12:00:00"
                date_object     = datetime.datetime(*time.strptime(game_time, '%d/%m/%Y %H:%M:%S')[0:6])
                game_date       = date_object.strftime('%Y-%m-%d %H:%M')
                now             = datetime.datetime.utcnow()
                date_now        = now.strftime("%Y-%m-%d %H:%M:%S")

                game_dt_time    = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M')[0:6])
                utc_dt_time     = game_dt_time - datetime.timedelta(hours=1)
                utc_game_date   = time.strftime("%Y-%m-%d %H:%M:%S", utc_dt_time.timetuple())

            tou = re.findall('association=(\w+)', response.url)
            try:
                tou_name = self.tournaments_dict[tou[0]]
            except:
                tou_name = ''
                return
            record   = SportsSetupItem()

            if self.spider_type == 'scores':
                crwl_typ = ['completed', 'postponed']
            else:
                crwl_typ = ['scheduled', 'postponed']

            if LOCATION_DICT.get(venue.encode('utf8'), ''):
                venue_ = LOCATION_DICT.get(venue.encode('utf8'), '')
            else:
                venue_ = venue

            tzinfo = get_tzinfo(city = venue_.encode('utf8'))
            if not tzinfo:
                venue_country = self.get_country(response.meta['res_tou'])
                tzinfo = get_tzinfo(country = venue_country)

            if status in crwl_typ:
                record['source_key']    = match_id[0]
                record['game_status']   = status
                record['game_datetime'] = utc_game_date
                record['participants']  = {str(home_id[0]) : (1, ''), str(away_id[0]) : (0, '')}
                if status != "completed":
                    record['result'] = {}
                else:
                    import pdb;pdb.set_trace()
                    record['result']        = {'0' : {'score' : score, 'winner' : str(winner)},
                                           str(home_id[0]) : home_scores,
                                           str(away_id[0]) : away_scores}
                record['reference_url'] = match_url
                record['tournament']    = tou_name
                record['game']          = "soccer"
                record['rich_data']     = {'location': {'city' : venue_}}
                record['affiliation']   = 'club-football'
                record['event']         = ''
                record['source']        = 'uefa_soccer'
                record['tz_info']       = tzinfo
                date_compare = utc_game_date.split(' ')[0]
                yield record
            #if date_compare in days and status == 'completed':
            #    yield record
            #elif status == 'scheduled':
            #    yield record
