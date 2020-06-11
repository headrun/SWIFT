import datetime
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
YEAR = datetime.datetime.now().year

true = True
false = False
null = ''

accept_titles = ["Women's", "Men's", "mixed doubles", 'First Round', 'Second Round', 'Third Round', 'Fourth Round', 'Quarterfinals', 'Semifinals', 'Finals']
events_dict = {'1st': 'First Round', '2nd': 'Second Round', \
               '3rd': 'Third Round', 'Round of 16': 'Fourth Round', 'Quarterfinal': 'Quarterfinals',\
               'Semifinals': 'Semifinals', 'Final': 'Final'}

class USOpenTBD(VTVSpider):
    name = "usopen_tennis_tbd"
    allowed_domains = []
    start_urls = ['http://www.usopen.org/schedule/tournament_schedule/']

    def parse(self,response):
        hxs = Selector(response)
        record = SportsSetupItem()
        tou_text = extract_data(hxs, '//div[@class="ModuleContents"]/p/u/strong/text()')
        year = ''.join(re.findall('(\d+).*', tou_text)).strip()
        stadium = ''
        nodes = get_nodes(hxs, '//table//tr')
        for node in nodes[1:25]:

            date = extract_data(node, './/td[1]//text()')
            time  = extract_data(node, './/td[3]//text()')
            events = extract_data(node, './/td[4]//text()')
            if "Round of 16" in events:
                round_ = events.split("Men's/Women's")[1].strip()
            else:
                round_ = events.split(' Round')[0].split(' ')[-1]
            event = events.split(' Round')[0].split(' ')[0].split('/')
            for ev in event:
                record['rich_data'] = {}
                record['result'] = {}
                if "TBA" in time:
                    game_datetime =  year +' '+date
                    pattern = "%Y %A, %B %d"
                    record['time_unknown'] = 1
                else:
                    game_datetime =  year +' '+date+' '+time
                    pattern = "%Y %A, %B %d %H:%M %p"
                    record['time_unknown'] = 0
                game_datetime = get_utc_time(game_datetime, pattern, 'US/Eastern')

                record['game_datetime'] = game_datetime
                record['rich_data'] =  {'channels': '',
                                            'location': {'city': 'New York',
                                            'country': 'USA',
                                            'continent': 'North America',
                                            'state': 'New York'
                                            }}
                record['rich_data'] ['game_note']= ''
                record['game'] = "tennis"
                record['reference_url'] = response.url
                record['source'] = "espn_tennis"
                record['tournament'] = "U.S. Open (Tennis)"
                record['participant_type'] = "player"
                record['tz_info'] = get_tzinfo(city = 'New York')
                record['participants'] = {'tbd1': (0, 'TBA'), 'tbd2': (0, 'TBA')}
                for key, value in events_dict.iteritems():
                    if round_ != key:
                        continue
                    event_name = 'U.S. Open '+ev+' Singles '+round_.replace(key, value)
                    event_name = event_name.replace(key, value).replace('Quarterfinalss', 'Quarterfinals')
                    print event_name
                if "Women's Singles" in event_name or "Women's Doubles" in event_name:
                    record['affiliation'] = "wta"
                elif "Men's Singles" in event_name or "Men's Doubles" in event_name:
                    record['affiliation'] = "atp"
                elif "Mixed" in event_name:
                    record['affiliation'] = "atp_wta"

                record['event'] = event_name
                record['game_status'] = 'scheduled'
                record['source_key'] = str(event_name.replace(' ', '_'))
                yield record

