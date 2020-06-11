import re
import time
import datetime
import urllib
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import log, get_utc_time, get_tzinfo
from vtvspider_dev import get_nodes, extract_data, extract_list_data


class CommonwealthSquash(VTVSpider):
    name = 'commonwealth_squash'
    domain_url = 'http://results.glasgow2014.com'
    start_urls = ['http://results.glasgow2014.com/sports/sq/squash.html']

    def get_event_name(self, event):
        game_note_list = ['Quarter-final', 'Semi-finals', 'Preliminary Round of 32', 
                       'Preliminary Round of 16', 'Repechage contest', 'Gold Medal Contest',
                       'Bronze Medal Contest A', 'Bronze Medal Contest B', 'Semi-final A',
                       'Semi-final B', 'Round of 64', 'Round of 128', 'Round of 32']
        game_event = game_note = ''
        for game_note in game_note_list:
            if game_note in event:
                game_event = event.replace(game_note, '').strip()
                return game_event, game_note

    def parse(self, response):
        sel = Selector(response)
        nodes = extract_list_data(sel, '//a[contains(@href, "/event/squash/")]/@href')
        nodes = get_nodes(sel, '//div[contains(@class, "WBREV_BX")]//table//tbody//tr')
        for node in nodes:
            game_link = self.domain_url + node
            yield Request(game_link, callback = self.parse_scores, meta = {})


    def parse_scores(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        game = extract_data(sel, '//div[@class="head_text"]/h1/text()')
        event = extract_data(sel, '//div[@class="head_text"]//h2/text()')
        game_datetime = extract_data(sel, '//span[@class="convert_datetime_object"]/@data-time').replace('T', ' ').replace('Z', '')
        stadium = extract_data(sel, '//span[@class="venueevent"]/text()')
        source_key = re.findall(r'/event/%s/(\w+)/' % game.lower(), response.url)[0]
        try:
            event, game_note = self.get_event_name(event)
        except:
            import pdb; pdb.set_trace()
        event = game + ' ' + event.replace('-', '')
        tournament = 'Commonwealth - ' + game

        results = {}
        participants = {}

        nodes = get_nodes(sel, '//table[@class="box_table desp"][2]/tbody//tr')
        if not nodes:
             nodes = get_nodes(sel, '//table[@class="box_table desp"][1]/tbody//tr')
        for node in nodes:
            total_time = extract_data(node, './td/div[@class="title time"]//text()')
            nation = extract_data(node, './td/div[contains(@class, "nation")]/a//text()')
            player = extract_data(node, './td/div[@class="namecomp"]/a/@href')
            player_id = re.findall(r'/(\d+)/', player)[0]
            action = extract_data(node, './td/div[@class="title action"]//text()')

            if not results.has_key(player_id): results[player_id] = {}
            results[player_id] = {'final_time' : total_time, 'nation' : nation, 'action' : action}

            if not participants.has_key(player_id): participants[player_id] = {}
            participants[player_id] = (0, '')

        winner_nodes = get_nodes(sel, '//table[contains(@class, "ppal_table")]/tbody/tr/td[@scope="row"]')
        for wnode in winner_nodes:
            winner = extract_data(wnode, './/tr/th[contains(@class, "nation")]//span[@class="winner"]/../../preceding-sibling::tr//a/@href')
            if winner:
                winner_id = re.findall(r'/(\d+)/', winner)[0]
                results['0'] = {'winner' : winner_id}

        if 'medal' in response.url:
            medal_nodes = get_nodes(sel, '//article[@id="medallists"]//section[contains(@id, "medallist")]')
            for mnode in medal_nodes:
                player = extract_data(mnode, './/div[@class="compname"]//a/@href')
                player_id = re.findall(r'/(\d+)/', player)[0]
                medal = extract_data(mnode, './@id').replace('medallist', '')
                data = participants.get(player_id, '')
                if data:

                    import pdb; pdb.set_trace()
                    results[player_id] = {'medal' : medal}

        record['tournament'] = tournament
        record['event'] = event
        record['participants'] = participants
        record['result'] = results
        record['game_datetime'] = game_datetime
        record['source'] = 'cw_glasgow'
        record['source_key'] = source_key
        record['game_status'] = 'completed'
        record['rich_data'] = {'game_note' : game_note, 'stadium' : stadium}
        yield record
