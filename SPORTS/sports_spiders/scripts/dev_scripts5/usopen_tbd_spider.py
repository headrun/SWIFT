import datetime
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

YEAR = datetime.datetime.now().year

true = True
false = False
null = ''

accept_titles = ["women's", "men's", "mixed doubles"]

DATE_DICT = {"8/25": "August 25", "8/26" : "August 26", \
             "8/27": "August 27", "8/28": "August 28", "8/29": "August 29", "8/30": "August 30", \
             "8/31": "August 31", "9/1": "September 01", "9/2": "September 02", "9/3": "September 03", \
             "9/4": "September 04", "9/5": "September 05", "9/6": "September 06", "9/7": "September 07", \
             "9/8": "September 08"}


RECORD = SportsSetupItem()
class USOpenTBD(VTVSpider):
    name = "usopen_tbd"
    allowed_domains = []
    start_urls = ['http://www.usopen.org/en_US/scores/schedule/eventschedule.html']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="row even"]')
        stadium = 'USTA Billie Jean King National Tennis Center'
        for node in nodes:
            game_time = extract_data(node, './/div[@class="time"]//text()')
            events = extract_data(node, './/div[@class="singles"]//text()')
            date = extract_data(node, './/div[@class="date"]//text()')
            events = events.split('/ ')[0]
            if "Round" in events:
                continue
            elif "Quarterfinals" in events:
                 continue
            event_name = events.split(' ')[0]
            event_ = events.split(' ')[1]
            events = 'U.S. open '+ event_name  + " "+ "Singles " +  event_
            print events
            game_date = DATE_DICT.get(date)
            game_datetime = game_date + ' ' + str(YEAR) + ' ' + game_time
            pattern = "%B %d %Y %H:%M %p"
            tz_info = 'US/Eastern'
            game_datetime = get_utc_time(game_datetime, pattern, tz_info)
            if "Women's Singles" in events or "Women's Doubles" in events: RECORD['affiliation'] = "wta"
            elif "Men's Singles" in events or "Men's Doubles" in events: RECORD['affiliation'] = "atp"
            elif "Mixed" in events: RECORD['affiliation'] = "atp_wta"

            RECORD['game_datetime'] = game_datetime
            RECORD['rich_data'] =  {'channels': '',
                                        'location': {'city': 'New York',
                                        'country': 'USA',
                                        'continent': 'North America',
                                        'state': 'New York',
                                        'stadium': stadium}}
            RECORD['participants'] = {'tbd1': (0, 'TBD'), 'tbd2': (0, 'TBD')}
            RECORD['rich_data'] ['game_note']= ''
            RECORD['game'] = "tennis"
            RECORD['reference_url'] = response.url
            RECORD['source'] = "usopen_tennis"
            RECORD['tournament'] = "U.S. open (Tennis)"
            RECORD['participant_type'] = "player"
            RECORD['season'] = YEAR
            RECORD['event'] = events
            RECORD['game_status'] = 'scheduled'
            RECORD['source_key'] = str(events.replace(' ', '_'))
            RECORD['result'] = {}
            yield RECORD 




