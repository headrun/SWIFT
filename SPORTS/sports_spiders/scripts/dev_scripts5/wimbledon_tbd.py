from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import datetime,time
import re


class WImbledonSpiderTBD(VTVSpider):
    name = "wimbledontbd_tennis"
    allowed_domains = ['britishtennis.com']
    start_urls = []
    def start_requests(self):
        top_url = 'http://www.britishtennis.com/tickets/wschedule.shtml'
        yield Request(top_url, callback= self.parse)



    def parse(self,response):
        hxs = Selector(response)
        record = SportsSetupItem()
        year = extract_data(hxs, '//p/strong/text()').split(' ')[1].strip()
        nodes = get_nodes(hxs, '//table//tr//td[@class="stdblue"]')
        for node in nodes:
            _date = extract_data(node, './/p[@align="left"]//text()')
            game_datetime = year + " " + _date + " 12.00"
            _date_= datetime.datetime(*time.strptime(game_datetime, '%Y %A %d %B %H.%M%S')[0:6])
            _date = get_utc_time(game_datetime, '%Y %A %d %B %H.%M%S', 'UTC')
            rounds_ = extract_data(node,'.//following-sibling::td[@width="70%"]//p//text()')
            if "Rest Day - No Tennis Played" in  \
            rounds_ or "Champion opens play on Centre Court" in \
            rounds_ or "Championships begin" in rounds_:
                continue
            rounds_ = rounds_.split('\n')[-1]
            if "Quarter-Finals" in rounds_:
                rounds_ = rounds_.replace('Quarter-Finals', "QuarterFinals")
            elif "Semi-Finals" in rounds_:
                rounds_ = rounds_.replace("Semi-Finals", "SemiFinals")
            rounds = rounds_.split('-')[0].replace('matches', '').strip()
            event = rounds_.split('-')[1].replace(" and Ladies'", '').strip()
            event =  "Wimbledon" + " " +  event + " " +rounds
            event = event.replace('* ', '')
            if "Ladies'" in event:
                event = event.replace("Ladies'", "Women's")
            record['event'] = event

            game_sk = _date_.strftime("%Y-%m-%d").replace('-', '_')+"_"+event.replace(' ', '_')
            record['source_key'] = game_sk
            if "Men's" in event:
                record['affiliation'] = 'atp'
            elif "Women's" in event:
                record['affiliation'] = 'wta'
            else:
                record['affiliation'] = 'atp_wta'

            record['game_datetime'] = _date
            pt1 = 'tbd1'
            pt2 = 'tbd2'
            source = "espn_tennis"
            record['source'] = source
            record['participants'] = {pt1: ('0', ''), pt2: ('0', '')}
            record['participant_type'] = 'player'
            record['tournament'] = "Wimbledon"
            record['game_status'] = "scheduled"
            record['result'] = {}
            record['rich_data'] = {'location': {'city': 'Wimbledon','country': 'UK', \
                                    'state': "London", \
                                    'stadium': 'All England Lawn Tennis and Croquet Club'}}
            record['game'] = "tennis"
            record['time_unknown'] = 0
            record['tz_info'] = get_tzinfo(country ="United Kingdom")
            yield record

