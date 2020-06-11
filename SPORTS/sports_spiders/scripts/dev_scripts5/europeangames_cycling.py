from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_new import VTVSpider, get_nodes, extract_data, extract_list_data, get_tzinfo, get_utc_time
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import time
import MySQLdb


class EropeanCyclingGames(VTVSpider):
    name = "european_games_cylcing"
    allowed_domains = []
    start_urls = ['http://www.baku2015.com/cycling-mountain-bike/index.html']

    def parse(self, response):
        sel = Selector(response)
        cl_nodes = get_nodes(sel, '//table//tr//td//span[@class="or-ses-evt-desc"]//a')
        for node in cl_nodes:
            cl_link = extract_data(node, './/@href')
            cl_link =  "http://www.baku2015.com" + cl_link
            yield Request(cl_link, callback = self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        event_name = extract_data(sel, '//div[@id="or-c-page"]//h1//text()')
        game_datetime = extract_list_data(sel, '//div[@class="or-ed"]//time//@datetime')[0]
        venue = extract_data(sel, '//span[@class="or-ev"]//a//text()')
        status = extract_data(sel, '//span[contains(@class, "or-status")]//text()').lower()
        print status
        participants = {}
        result       = {}
        if "official" in status:
            status = "completed"
        elif "live" in status:
            status = "ongoing"
        else:
            status = "scheduled"
        print status
        game_time = extract_list_data(sel, '//div[@class="or-ed"]//time//span//@data-or-utc')[0]
        tz_info = game_datetime.split(':')[-2].replace('00', '')
        game_datetime= datetime.datetime.strptime(game_time, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        race_nodes = get_nodes(sel, '//table[@class="or-tbl"]//tr')
        medals = {}
        medal_nodes = get_nodes(sel, '//ol[@class="or-podium-list"]//li[contains(@class, "or-item-medal")]')
        for node in medal_nodes:
            winner_name = extract_data(node, './/div[@class="or-name"]//a//text()').title()
            winner_name = winner_name.replace('\r\n', ' ')
            winner_sk = extract_data(node, './/div[@class="or-name"]//a//@href')
            winner_sk = "".join(re.findall(r'\d+', winner_sk))
            winner_sk = "PL" +winner_sk
            medal = extract_data(node, './/div[contains(@class, "or-medal")]//span//@title')
            medal = medal.replace('Gold Medal', 'gold').\
                    replace('Silver Medal', 'silver').\
                    replace('Bronze Medal', 'bronze')
            if winner_sk:
                medals.update({winner_sk: medal})


        for race_node in race_nodes:
            pl_pos = extract_data(race_node, './/td[1]//text()').strip()
            if pl_pos == '-':
                pl_pos = ''
            pl_link = extract_data(race_node, './/td[@class="or-ath"]//a//@href')
            if not pl_link:
                continue
            tou_sk = "".join(re.findall('phase=(.*)/index.html', response.url))
            pl_name = extract_data(race_node, './/td[@class="or-ath"]//a//text()').title()
            pl_sk = "".join(re.findall(r'\d+', pl_link))
            pl_sk = "PL" + pl_sk
            nation = extract_data(race_node, './/img//@src')
            nation = nation.split('/')[-1].replace('.png', '').strip()
            for key, value in medals.iteritems():
                if pl_sk == key:
                    medal = value
            results = extract_data(race_node, './/td[3]//text()')
            if results == '-':
                results = ''
            if status == 'completed':
                if pl_pos == "1" :
                    winner = pl_sk
                    result.setdefault('0', {}).update({'winner' : winner})
                if pl_pos == "1" or pl_pos == "2" or pl_pos == "3":
                    riders = {'position': pl_pos, 'final_time' : results, 'nation': nation, 'medal' : medal}
                    result[pl_sk] = riders
                else:
                    riders = {'position': pl_pos, 'final_time' : results, 'nation': nation}
                    result[pl_sk] = riders
            participants[pl_sk] = (0, pl_name)
            record['participants'] = participants

            if pl_sk and status == 'ongoing':

                riders = {'position': pl_pos, 'final_time' : results, 'nation': nation}
                result[pl_sk] = riders
        if status =="scheduled":
            result = {}
        record['tournament']    = "European Games - Cycling"
        record['game_datetime'] = game_datetime
        record['game']          = "cycling"
        record['game_status']  = status
        record['source_key']    = tou_sk
        record['event'] = "European Games " + event_name
        record['reference_url'] = response.url
        record['source']        = "eoc_baku"
        record['affiliation']   = "eoc"
        record['rich_data'] = {'stadium': venue, 'location': {'city': "Baku", "country": "Azerbaijan"}}
        record['season'] = "2015"
        record['result'] = result
        record['participant_type'] = "player"
        record['time_unknown']= 0
        record['tz_info'] = tz_info
        yield record

