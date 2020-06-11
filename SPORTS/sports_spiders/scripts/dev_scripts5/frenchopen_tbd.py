import datetime
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

accept_titles = ["1st round of the Qualifiers", "2nd round of the Qualifiers", \
                "3rd round of the Qualifiers", "4th round of the Qualifiers", \
                "Roland-Garros Kid's Day"]

RECORD = SportsSetupItem()
class FrenchOpenTBD(VTVSpider):
    name = "frenchopen_tbd"
    allowed_domains = []
    start_urls = ['http://rolandgarros.fft-tickets.com/uk/practical-info/provisional-schedule']
    def parse(self, response):
        hxs = Selector(response)
        nodes= get_nodes(hxs, '//div[@id="calendrier"]//table//tr//td//div')
        for node in nodes[1:]:
            game_date = extract_data(node, './/@id')
            game_rounds = extract_data(node, './/div[@class="tooltip"]//div//text()')
            if game_rounds:
                events = game_rounds.replace("Ladies' & ", '').replace("Ladies' and ", '')
                event_name = events
                if event_name in accept_titles:
                    continue
                game_date = game_date.replace('day-', '')
                game_datetime = get_utc_time(game_date, "%Y-%m-%d", "CET")
                event_name = 'French Open '+ events
                event_name = event_name.replace("Gentlemen's", "Women's").replace('1st', 'First'). \
                replace('2nd', "second").replace('3rd', "third").replace('4th', "fourth")
                print event_name
                event_name = event_name.replace('second round Singles', 'Singles second round'). \
                    replace('third round Singles', 'Singles third round'). \
                    replace('fourth round Singles', 'Singles fourth round'). \
                    replace('first round Singles', 'Singles first round'). \
                    replace("1/2 Finals men's Singles", "Men's Singles Semifinals"). \
                    replace("Women's 1/4 Finales Singles", "Women's Singles Quarterfinals"). \
                    replace("Final men's Singles", "Men's Singles Final"). \
                    replace("Final Ladies' Singles", "Women's Singles Final"). \
                    replace("1/2 Finals Ladies' Simples", "Women's Singles Semifinals")
                if "Women's Singles" in event_name or "Women's Doubles" in event_name:
                    RECORD['affiliation'] = "wta"
                elif "Men's Singles" in event_name or "Men's Doubles" in event_name:
                    RECORD['affiliation'] = "atp"
                elif "Mixed" in event_name:
                    RECORD['affiliation'] = "atp_wta"
                RECORD['game_datetime'] = game_datetime
                RECORD['rich_data'] =  {'channels': '',
                    'location': {'city': 'Paris',
                    'country': 'France',
                    'state': ''}}
                RECORD['time_unknown'] = 1
                RECORD['game'] = "tennis"
                RECORD['reference_url'] = response.url
                RECORD['source'] = "espn_tennis"
                RECORD['tournament'] = "French Open"
                RECORD['participant_type'] = "player"
                RECORD['tz_info'] = get_tzinfo(city = 'Paris')
                if not RECORD['tz_info']:
                    RECORD['tz_info'] = get_tzinfo(country = 'France')
                RECORD['participants'] = {'tbd1': (0, 'TBA'), 'tbd2': (0, 'TBA')}
                RECORD['event'] = event_name
                RECORD['game_status'] = 'scheduled'
                RECORD['source_key'] = str(event_name.replace(' ', '_'))
                import pdb;pdb.set_trace()
                yield RECORD

