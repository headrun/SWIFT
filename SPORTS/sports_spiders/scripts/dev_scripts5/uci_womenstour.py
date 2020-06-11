import datetime
import re
from vtvspider_dev import VTVSpider, get_nodes,\
extract_data, get_utc_time, extract_list_data, get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

SKIP_LIST = ['26092_Stage_1', '26104_Stage_2a']


def X(data):
   try:
       return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
   except:
       return data.encode('utf8')

def Y(var):
    var = var.replace('&amp;','&').replace('&quot;','"').replace('&gt;','>')
    var = var.replace('\n','').replace('\t', '')
    var = var.replace('\xc2\xab', '').replace('\xc3\xa9', '') \
            .replace('\xc3\x84', '').replace('\xc3\x98', ''). \
            replace('\xc3\x96', '').replace('\xc3\x87', ''). \
            replace('\xc3\x8d', '').replace('\xc3\xad', ''). \
            replace('\xc3\xa1', '').replace('\xc3\xb1', ''). \
            replace('\xc3\xa7', '').replace('\xc3\xbc', '')
    return var.strip()


TOU_DICT = {'Gran Prix San Luis Femenino': 'Gran Prix San Luis Femenino', \
            'Tour Femenino de San Luis': 'Tour Femenino de San Luis', \
            'Ladies Tour of Qatar': 'Ladies Tour of Qatar', \
            'Tour of New Zealand': 'Womens Tour of New Zealand', \
            'Omloop Het Nieuwsblad WE': "Omloop Het Nieuwsblad - Women's race", \
            'Le Samyn des Dames': 'Le Samyn des Dames', \
            'Strade Bianche - WE': 'Strade Bianche Women', \
            'Omloop van het Hageland - Tielt-Winge': 'Omloop van het Hageland', \
            'Drentse 8 van Dwingeloo': 'Acht van Westerveld', \
            'Novilon Eurocup Ronde van Drenthe': 'Novilon EDR Cup', \
            'Cholet Pays de Loire - WE': 'Cholet Pays de Loire Dames', \
            'Gent - Wevelgem WE': 'Gent-Wevelgem (women)', \
            'Grand Prix de Dottignies': 'Grand Prix de Dottignies', \
            "The Maha Chakri Sirindhon's Cup Women's Tour of Thailand": \
            "The Princess Maha Chackri Sirindhon's Cup", \
            'Energiewacht Tour': 'Energiewacht Tour', \
            'Vuelta Ciclista Femenina a Costa Rica': \
            'Vuelta Internacional Femenina a Costa Rica'}

def get_time(initial_time, gap):
    if "+" in gap:
        if ":" in gap:
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


DOMAIN = "http://women.cyclingfever.com/"

class UCIWomensTour(VTVSpider):
    name = "uciwomens_tour"
    allowed_domains = []
    start_urls = ['http://women.cyclingfever.com/calendar.html?_p=women']

    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(sel, '//div[@class="blokje"]//table//tr//td//a[contains(@href, "editie.html")]')
        for node in nodes:
            tou_name = extract_data(node, './/text()')
            tou_name =  Y(X((tou_name)))

            tou_link = extract_data(node, './/@href')
            tou_link = DOMAIN + tou_link
            for key, value in TOU_DICT.iteritems():
                if tou_name in key:
                    tou_name = tou_name.replace(key, value).strip()
                    if "Vuelta Internacional Femenina a Costa Rica" not in tou_name:
                        continue
                else:
                    continue
                yield Request(tou_link, callback=self.parse_next, \
            meta = {'tou_name': tou_name, 'tou_link': tou_link})


    def parse_next(self, response):
        sel = Selector(response)
        tou_name = response.meta['tou_name']
        tou_link =  response.meta['tou_link']
        race_link = extract_list_data(sel, '//div[@class="blokje"]//a//@href')[0]
        race_link = DOMAIN + race_link
        race_name = extract_list_data(sel, '//div[@class="blokje"]//a//text()')[0]
        if "Race information" in race_name:
            yield Request(race_link, callback = self.parse_race, \
            meta = {'tou_name': tou_name, 'tou_link': tou_link})
        if "Route & Stages" in race_name:
            yield Request(race_link, callback = self.parse_stage, \
            meta = {'tou_name': tou_name, 'tou_link': tou_link})

    def parse_race(self, response):
        sel = Selector(response)
        tou_name = response.meta['tou_name']
        tou_link =  response.meta['tou_link']
        score_link = extract_data(sel, '//table[@class="tab100"]//tr//td//a[contains(@href, "uitslag")]/@href')
        score_link = DOMAIN + score_link
        title = extract_data(sel, '//table[@class="tab100"]//tr//td//a[contains(@href, "uitslag")]//text()')
        if "Result" in title:
            yield Request(score_link, callback = self.parse_scores, \
                meta = {'tou_name': tou_name, 'tou_link': tou_link})
    def parse_stage(self, response):
        sel = Selector(response)
        tou_name = response.meta['tou_name']
        tou_link =  response.meta['tou_link']
        nodes = get_nodes(sel, '//div[@class="blokje"]//a[contains(text(), "Stage")]')
        for node in nodes:
            stage_link = extract_data(node, './/@href')
            stage_link = DOMAIN + stage_link
            stage_name = extract_data(node, './/text()')
            if "Route & Stages" in stage_name:
                continue
            if "editie_idd=MjYwOTI=&etappe_idd=MzIyMjE=" in stage_link:
                continue
            if "editie_idd=MjYxMDQ=&etappe_idd=MzI0ODQ=" in stage_link:
                continue
            yield Request(stage_link, callback = self.parse_scores, \
            meta = {'tou_name': tou_name, 'tou_link': tou_link})
    def parse_scores(self, response):
        sel = Selector(response)
        tou_name = response.meta['tou_name']
        tou_link =  response.meta['tou_link']
        record = SportsSetupItem()
        result = {}
        participants = {}
        game_id = "".join(re.findall('\d+', tou_link))
        race_place = extract_list_data(sel, '//div[@class="heading"]//div[@class="kop2a"]//a//text()')
        stage      = extract_data(sel, '//div[@class="heading"]//span//text()')
        game_date  = extract_list_data (sel, '//div[@class="heading"]//b//text()')
        game_date  = game_date[-1]
        game_date  = game_date + " 2015"
        tou_datetime = get_utc_time(game_date, '%d %B %Y', 'US/Eastern')
        game_distance = extract_list_data (sel, '//div[@class="heading"]//text()')[-2].replace(' / ', '')
        today_date = datetime.datetime.now().date()
        if tou_datetime.split(' ')[0] < str(today_date):
            status = "completed"
        elif tou_datetime.split(' ')[0] == str(today_date):
            status = 'ongoing'
        else:
            status = 'scheduled'

        if stage:
            game_note = stage.replace(" >", ":") + race_place[0] + " to " + race_place[-1]
            game_note = game_note.replace('1Fans', '')
            game_id   = game_id + "_" +stage.replace(" >", '').replace(' ', '_')
        else:
            game_note = race_place[0] + " to " + race_place[-1]
            game_note = game_note.replace('1Fans', '')
            game_id   = game_id
        race_nodes = get_nodes(sel, '//div[@class="blokje"]//table[@class="tab100"]//tr')
        for race_node in race_nodes:
            rank  = extract_data(race_node, './/td[1]/text()')
            print rank
            if "." not in rank:
                continue
            r_name  = extract_data(race_node, './/td//a//text()')
            if not r_name:
                r_name = extract_data(race_node, './/td[2]//text()')
            print r_name
            rank  = rank.replace('.', '')
            rname_pl = Y(X(r_name.lower()))
            rname =Y(X(r_name.lower().replace(' ', '_')))
            if game_id in SKIP_LIST:
                continue
            time_ = extract_data(race_node, './/td[5]//text()')
            if "+" not in time_:
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
            if status == 'completed':
                if rank == "1" :
                    winner = rname
                    result.setdefault('0', {}).update({'winner' : winner})
                participants[rname] = (0, rname_pl)
                record['participants'] = participants

                if rank:

                    riders = {'position': rank, 'final_time' : time_}
                    result[rname] = riders
        record['tournament']    = tou_name
        record['game_datetime'] = tou_datetime
        record['game']          = "cycling"
        record['game_status']   = status
        record['source_key']    = game_id
        record['reference_url'] = response.url
        record['source']        = "women_cycling"
        record['affiliation']   = "uci"
        record['rich_data'] = {'game_note': game_note}
        record['result'] = result
        record['time_unknown'] = 1
        record['participant_type'] = "player"
        import pdb;pdb.set_trace()
        yield record

