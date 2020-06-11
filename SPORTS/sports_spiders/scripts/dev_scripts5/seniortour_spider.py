import re
import datetime
import time
from vtvspider import VTVSpider, extract_data, get_nodes
from vtvspider_dev import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position
    return pos

def get_start_end_dates(_date, year):
    if _date and year:
        start_end_date = _date + ' ' + year
        final_date = (datetime.datetime(*time.strptime(start_end_date.\
                      strip(), '%b %d %Y',)[0:6])).date()
    return final_date


class SenooTour(VTVSpider):
    name = "senior_spider"
    allowed_domain = []
    start_urls = ['http://www.europeantour.com/seniortour/season=2014/tournamentid=2014862/library/leaderboard/_leaderboard_v2.html']
    def parse(self,response):
        hxs = Selector(response)
        record = SportsSetupItem()
        result_final = {}
        players = {}
        tou_start = "JUL 24"
        tou_end   = "JUL 27"
        tou_year  = "2014"
        start_date  = get_start_end_dates(tou_start, tou_year)
        end_date = get_start_end_dates(tou_end, tou_year)
        today_date = datetime.datetime.utcnow().date()
        if start_date <= today_date and today_date <= end_date:
            status = "ongoing"
        elif end_date < today_date:
            status = "completed"
        else:
            status = "scheduled"
        record['source_key'] =  "The_Senior_Open_Championship_presented_by_Rolex_Jul_24_Jul_27"
        record['reference_url'] = response.url
        record['game_datetime'] = "2014-07-24 04:00:00"
        record['game_status'] = status

        if status == 'ongoing':
            game_note = extract_data(hxs, '//div[@class="c"]/\
                        div[contains(@style, "width: 25%")]/text()').\
                        replace('Status:', '').replace(',', ' - ').strip()
        else:
            game_note = ''
        stadium = "Royal Porthcawl Golf Club"
        city    = "Bridgend"
        state   = "Wales"
        country = "United Kingdom"
        tz_info = get_tzinfo(city = city)
        record['rich_data'] = {'location': {'city': city, 'state': state, 'country': country },'game_note': game_note, 'stadium': stadium}
        
        nodes = get_nodes(hxs, '//table[@id="lbl"]//tr[not(contains(@id,"boardPromo"))]')
        for node in nodes:
            position = extract_data(node, './/td[@class="b"]/text()')
            pl_pos = get_position(position).replace('\n', '')
            player_name = extract_data(node, './/td[@class="nm"]/div[@class="nm"]/text()')
            player_id = extract_data(node, './@id')
            round1 = extract_data(node, './/td[@class="rnd "][1]/text()') or \
                     extract_data(node, './/td[@class="rnd"][1]/text()')
            round2 = extract_data(node, './/td[@class="rnd "][2]/text()') or \
                     extract_data(node, './/td[@class="rnd"][2]/text()')
            round3 = extract_data(node, './/td[@class="rnd "][3]/text()') or \
                     extract_data(node, './/td[@class="rnd"][3]/text()')
            round4 = extract_data(node, './/td[@class="rnd "][4]/text()') or \
                     extract_data(node, './/td[@class="rnd"][4]/text()')
            hole  = extract_data(node, './/td[9]/text()')
            total = extract_data(node, './/td[15]/text()')
            print total
            to_par = extract_data(node, './/td[10]/text()')
            round1 = round1.replace('-', '')
            round2 = round2.replace('-', '')
            round3 = round3.replace('-', '')
            round4 = round4.replace('-', '')
            total = total.replace('-', '')
            if status == 'completed':
                if position == "1":
                    winner = player_id
                elif position ==  "T1":
                    winner = player_id
            else:
                winner = ''
            result = {'0': {'winner': winner}, \
                          player_id: {'final': total, 'position': pl_pos, \
                          'R1': round1, 'R2': round2, 'R3': round3, 'R4': round4, \
                          'TO PAR': to_par, 'TeeTime': hole}}
            result_final.update(result)
            players.update({player_id: (0, player_name)})

        record['participants'] = players
        record['result'] = result_final
        record['tournament'] =  "Senior Open Championship"
        record['game'] = "golf"
        record['source'] = "champions_golf"
        record['time_unknown'] = 1
        record['tz_info'] = tz_info
        yield record



