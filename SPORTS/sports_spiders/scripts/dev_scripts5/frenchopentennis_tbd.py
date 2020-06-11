import datetime
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

RECORD = SportsSetupItem()
class FrenchOpenTours(VTVSpider):
    name = "tours_frenchopen"
    start_urls = ['http://www.tours4tennis.com/roland-garros/schedule.shtml']
    allowd_domains = []

    def parse(self, response):
        sel = Selector(response)
        season = extract_list_data(sel, '//h3[@class="pagetitle"]//text()')
        season = season[-1].split(',')[-1].strip()
        nodes = get_nodes(sel, '//div[@class="rolandgarrospricelist"]//table//tr')
        for node in nodes:
            date_info = extract_list_data(node, './/td[1]//text()')
            if not date_info:
                continue

            date_ = date_info[0].replace('  ', '').strip().replace('Thur', 'Thu').replace('Tues', 'Tue').replace('Thus', 'Thu')
            if "from" in date_:
                continue
            time = date_info[-1].strip().split(' ')[-1].replace('  ', ' ').strip().replace('PhC', '').strip()
            if not time:
                time = "11:00"
            game_info = extract_list_data(node, './/td[2]//text()')
            roun_info = game_info[0].strip().replace('1st', 'First'). \
                replace('2nd', "Second").replace('3rd', "Third").replace('4th', "Fourth"). \
                replace('1/4 Final', 'Quarterfinals').replace('1/2 Final', 'Semifinals').replace('  ', '').strip()
            roun_info = roun_info.encode('utf-8')
            if '/4 Final' in roun_info:
                roun_info = "Quarterfinals"
            if 'Thu Jun 2' in date_ or "Fri Jun 3" in date_:
                roun_info = "Semifinals"
            if date_ and time:
                game_datetime = date_ + " " + season + " " + time
                pattern = '%a %b %d %Y %H:%M'
            elif date_:
                game_datetime = date_ + " " + season
                pattern = '%a %b %d %Y'
            game_datetime = get_utc_time(game_datetime, pattern, 'Europe/Paris')
            event_info = game_info[1:]
            for event in event_info:
                event = event.strip().replace('2', '').replace('1', '').strip()
                event = event + " " + roun_info
                event = "French Open "  + event
                event = event.replace('   ', ' ')
                if "/" in event or "Finale" in event:
                    continue
                source_key = game_datetime.split(' ')[0].replace('-', '_') + "_" + str(event.replace(' ', '_'))
                if "Women's" in event:
                    RECORD['affiliation'] = "wta"
                elif " Men's" in event:
                    RECORD['affiliation'] = "atp"
                elif "Mixed" in event:
                    RECORD['affiliation'] = "atp_wta"
                RECORD['game_datetime'] = game_datetime
                RECORD['rich_data'] =  {'channels': '',
                    'location': {'city': 'Paris',
                    'country': 'France',
                    'state': ''}}
                RECORD['time_unknown'] = 0
                RECORD['game'] = "tennis"
                RECORD['reference_url'] = response.url
                RECORD['source'] = "tennis_tours"
                RECORD['tournament'] = "French Open"
                RECORD['participant_type'] = "player"
                RECORD['tz_info'] = get_tzinfo(city = 'Paris')
                if not RECORD['tz_info']:
                    RECORD['tz_info'] = get_tzinfo(country = 'France')
                RECORD['participants'] = {'tbd1': (0, 'TBA'), 'tbd2': (0, 'TBA')}
                RECORD['event'] = event
                RECORD['game_status'] = 'scheduled'
                RECORD['source_key'] = source_key
                yield RECORD

