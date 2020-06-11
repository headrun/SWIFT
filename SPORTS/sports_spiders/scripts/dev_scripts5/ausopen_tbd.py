import datetime
from vtvspider_new import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
YEAR = datetime.datetime.now().year

true = True
false = False
null = ''

accept_titles = ['First Round', 'Second Round', 'Third Round', \
                'Fourth Round', "Men's Doubles Final", \
                "Mixed Doubles Final", "Men's Singles Final",
                "Women's Singles Final", "Women's Doubles Final", \
                "Mixed Doubles Semifinal", "Men's Singles Semifinal", \
                "Women's Singles Semifinal", "Quarterfinals"]


class AUSOpenTBD(VTVSpider):
    name = "ausopen_tennis_tbd"
    allowed_domains = ['ausopen.com']
    start_urls = ['http://www.ausopen.com/event-guide/tournament-schedule']

    def parse(self,response):
        hxs = Selector(response)
        record = SportsSetupItem()
        year = extract_data(hxs, '//div[@class="holder"]/em[@class="date"]//text()').split(' ')[-1].strip()
        nodes = get_nodes(hxs, '//table[@id="provisionalSchedule"]//tr')
        for node in nodes[10:]:
            dates = ''
            date = extract_data(node, './td[@class="date"]/text()').replace('-', '')
            if not date:
                continue
            if date:
                dates = date

            if not date:
                date = dates

            time = extract_data(node, './/td[@class="courts ocha"]/text()')
            rounds = extract_data(node, './/td[@class="rounds"]//text()').strip()
            if not time:
                game_date =  year +' '+date
                pattern = "%Y %a %d"
                game_datetime = get_utc_time(game_date, pattern, 'Australia/Melbourne')
                record['time_unknown'] = 1
            else:
                game_date =  year +' '+date+' '+time
                pattern = "%Y %a %d %H.%M%p"
                record['time_unknown'] = 0
                game_datetime = get_utc_time(game_date, pattern, 'Australia/Melbourne')
            if rounds in accept_titles:
                if "Round" in rounds or "Quarterfinals" in rounds:
                    event_name = "Australian Open" + " " + "Men's Singles" + " " + rounds
                else:
                    event_name = "Australian Open" + " " + rounds
            record['game_datetime'] = game_datetime
            record['rich_data'] =  {  'locations': {'city': 'Melbourne',
                                        'country': 'Australia',
                                        'state': 'Victoria', 
                                        'stadium': "Melbourne Park"
                                        }}
            record['game'] = "tennis"
            record['reference_url'] = response.url
            record['source'] = "espn_tennis"
            record['tournament'] = "Australian Open"
            record['participant_type'] = "player"
            record['tz_info'] = get_tzinfo(city = 'Melbourne')
            record['participants'] = {'tbd1': (0, 'TBA'), 'tbd2': (0, 'TBA')}
            if "Women's Singles" in event_name or "Women's Doubles" in event_name:
                record['affiliation'] = "wta"
            elif "Men's Singles" in event_name or "Men's Doubles" in event_name:
                record['affiliation'] = "atp"
            elif "Mixed" in event_name:
                record['affiliation'] = "atp_wta"

            record['event'] = event_name
            record['game_status'] = 'scheduled'
            record['source_key'] = str(year + "_" +event_name.replace(' ', '_'))
            record['result'] = {}
            yield record

