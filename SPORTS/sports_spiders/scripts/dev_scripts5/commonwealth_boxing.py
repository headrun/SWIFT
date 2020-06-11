import re
import time
import datetime
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import log, get_utc_time, get_tzinfo
from vtvspider import get_nodes, extract_data, extract_list_data

EVENT_NAMES = {"Boxing Men's Light Fly " : "Boxing Men's Light flyweight", "Boxng Women's Fly " : "Boxing Women's Flyweight", \
                "Boxing Men's Fly " : "Boxing Men's Flyweight", "Boxing Men's Bantam ": "Boxing Men's Bantamweight", \
                "Boxing Men's Light Welter ": "Boxing Men's Light welterweight", "Boxing Men's Light ": "Boxing Men's Lightweight", \
                "Boxing Women's Light ": "Boxing Women's Lightweight", "Boxing Men's Middle ": "Boxing Men's Middleweight", \
                "Boxing Men's Welter " : "Boxing Men's welterweight", "Boxing Men's Light Heavy " : "Boxing Men's Light heavyweight",\
                "Boxing Women's Middle ": "Boxing Women's Middleweight", "Boxing Men's Heavy " : "Boxing Men's heavyweight", \
                "Boxing Men's Super Heavy ": "Boxing Men's Super heavyweight"}

class CommonwealthBoxing(VTVSpider):
    name = "commonwealth_boxing"
    domain_url = "http://results.glasgow2014.com"
    start_urls = ['http://results.glasgow2014.com/sports/bx/boxing.html']
    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//div[contains(@id, "WBREV_BXM")]//tr//td')
        for node in nodes:
            game_link = extract_data(node, './/a[contains(@href, "/event/boxing/")]/@href')
            if "http" not in game_link:
                game_link = self.domain_url + game_link
            status = extract_data(node,'.//span[@class="status_event_bracket"]/text()')
            if status == "Official":
                yield Request(game_link, callback = self.parse_scores, meta = {})

    def parse_scores(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        game = extract_data(sel, '//div[@class="head_text"]/h1/text()')
        event = extract_data(sel, '//div[@class="head_text"]//h2/text()')
        game_datetime = extract_data(sel, '//span[@class="convert_datetime_object"]/@data-time').replace('T', ' ').replace('Z', '')
        stadium = extract_data(sel, '//span[@class="venueevent"]/text()')
        if "SECC" in stadium:
            stadium = "Scottish Exhibition and Conference Centre"
        source_key = re.findall(r'/event/%s/(\w+)/' % game.lower(), response.url)[0]
        event = game + ' ' + event.replace('-', '').strip()
        game_notes = event.split(') ')[1].strip()
        if "(" in event:
            event_name = event.split('(')[0]
            event_name = EVENT_NAMES.get(event_name)
            game_notes =event.split(') ')[1]
        else:
            event_name =  event
            game_notes = game_notes
        results = {}
        participants = {}
        nodes = get_nodes(sel, '//div[contains(@class, "result")]//table//tbody//tr')
        if not nodes:
             nodes = get_nodes(sel, '//table[@class="box_table desp"][1]/tbody//tr')
        for node in nodes:
            nation = extract_data(node, './td/div[contains(@class, "nation")]/a//text()')
            if not nation:
                continue
            player = extract_data(node, './td/div[@class="namecomp"]/a/@href')
            pl_name = extract_data(node, './td/div[@class="namecomp"]/a/text()')
            player_id = "".join(re.findall(r'/(\d+)/', player))
            res = extract_list_data(node, './/div[contains(text(), "Judge")]/span/text()')
            knock_downs = extract_data(node, './/div[contains(text(), "Knock Down")]/span/text()')
            if not knock_downs:
                knock_downs = ''
            results.update({player_id: {'nation': nation}})
            results.update({player_id: {'knockdowns' : knock_downs}})
            if res:
                if len(res) == 3:
                    R1, R2, R3 = res
                    results.update({player_id: {"R1": R1, "R2": R2, "R3": R3}})

            participants.update({player_id: (0, pl_name)})

        winner_nodes = get_nodes(sel, '//table[contains(@class, "ppal_table")]/tbody/tr/td[@scope="row"]')
        for wnode in winner_nodes:
            winner = extract_data(wnode, './/tr//td//span[contains(text(), "WINNER")]/../../preceding-sibling::tr/td/a[contains(@href, "athlete")]/@href')
            if winner:
                winner_id = re.findall(r'/(\d+)/', winner)[0]
                results['0'] = {'winner' : winner_id}

        if 'Final Bout' in game_notes:
            medal_nodes = get_nodes(sel, '//article[@id="medallists"]//section[contains(@id, "medallist")]')
            for mnode in medal_nodes:
                player = extract_data(mnode, './/div[@class="compname"]//a/@href')
                player_id = re.findall(r'/(\d+)/', player)[0]
                medal = extract_data(mnode, './@id').replace('medallist', '')
                data = participants.get(player_id, '')
                if data:
                    results[player_id] = {'medal' : medal}
        elif 'Semifinal' in game_notes:
            medal_nodes = get_nodes(sel, '//article[@id="medallists"]//section[contains(@id, "medallist")]')
            for mnode in medal_nodes:
                player = extract_data(mnode, './/div[@class="compname"]//a/@href')
                player_id = re.findall(r'/(\d+)/', player)[0]
                medal = extract_data(mnode, './@id').replace('medallist', '')
                data = participants.get(player_id, '')
                if data:
                    results[player_id] = {'medal' : medal}
                else:
                    results[player_id] = {'medal' : ''}
        city = "Glasgow"
        record['tz_info']  = get_tzinfo(city = city)
        record['tournament'] = "2014 Commonwealth Games"
        record['event'] = event_name
        record['game'] = game.lower()
        record['participants'] = participants
        record['result'] = results
        record['game_datetime'] = game_datetime
        record['affiliation'] = 'cgf'
        record['participant_type'] = 'player'
        record['source'] = 'cw_glasgow'
        record['source_key'] = source_key
        record['game_status'] = 'completed'
        record['reference_url'] = response.url
        record['rich_data'] = {'game_note' : game_notes, 'stadium' : stadium}
        record['time_unknown'] = 0
        yield record

