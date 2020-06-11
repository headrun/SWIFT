from vtvspider_new import VTVSpider, get_nodes, extract_data, extract_list_data, get_utc_time, get_tzinfo
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.http import Request, FormRequest
from scrapy.selector import Selector
import re

def xcode(text, encoding='utf8', mode='ignore'):
    return text.encode(encoding, mode) if isinstance(text, unicode) else text

class MarathonSpider(VTVSpider):
    name = 'marathon_spider'
    start_urls = ['https://en.wikipedia.org/wiki/2016_Tokyo_Marathon']
    allowd_domains = []


    def parse(self, response):
        record = SportsSetupItem()
        sel = Selector(response)
        venue = extract_data(sel, '//table[@class="infobox vevent"]//tr//th[contains(text(), "Venue")]//following-sibling::td//text()')
        tou_details = extract_data(sel, '//h1[@id="firstHeading"]//text()')
        year = tou_details.split(' ')[0].strip()
        tou_name = tou_details.replace(year, '').strip()
        city, country = venue.split(',')
        game_note = extract_data(sel, '//table[@class="infobox vevent"]//tr//th[@class="summary"]//text()').strip()
        date = extract_data(sel, '//table[@class="infobox vevent"]//tr//th[contains(text(), "Dates")]//following-sibling::td//text()')
        root_nodes = get_nodes(sel, '//h2[span[@id="Results"]]//following-sibling::h3')
        for root_node in root_nodes:
            nodes = get_nodes(root_node, './/following-sibling::table[1]//tr[td]')
            event = extract_data(root_node, './span/text()')
            participants = {}
            result = {}

            for node in nodes:
                final_time    = extract_data(node, './/td[5]/text()')
                if not final_time:
                    final_time    = extract_data(node, './td[4]/text()')
                rank          = extract_data(node, './/td[1]//text()').replace('!', '').strip()
                pl_name, nation = extract_list_data(node, './/td//a//text()')[:2]
                pl_sk = pl_name.lower().replace(' ', '-')
                medal = extract_data(node, './/td[1]//img//@src')
                if medal:
                    medal, rank = self.get_medal(medal)
                if rank == '1':
                    result['0'] = {'winner': pl_sk}
                game_date = date + " " +year
                #game_datetime = get_utc_time(game_date, '%d %B %Y', 'Asia/Tokyo')
                game_datetime = get_utc_time(game_date, '%d %B %Y', 'US/Eastern')
                result[xcode(pl_sk)] = {'final_time': final_time, 'position': rank, 'medal': medal}
                participants[xcode(pl_sk)] = (0, '')

                record['rich_data'] = {'game_note': game_note,
                                       'location': {'city': city.strip(), 'country': country.strip()}}
                source_key = tou_name.lower().replace(' ', '_')+ "_" + "elite" + "_"+event.lower()+"_" + game_datetime.split(' ')[0].replace('-', '')
                record['source_key'] = str(source_key)
                record['participants'] = participants
                record['result'] = result
                record['participant_type'] = "player"
                record['game'] = "marathon"
                record['game_datetime']  = game_datetime
                record['game_status'] = "completed"
                record['reference_url'] = response.url
                record['time_unknown'] = '1'
                record['tz_info'] = get_tzinfo(city = city.strip(), country = country.strip(), game_datetime = game_datetime)
                record['sport_name'] = "marathon"
                record['event'] = tou_name + " Elite " + event
                record['affiliation'] = "wmm"
                record['source'] = "wmm_marathon"
            import pdb;pdb.set_trace()
            yield record


    def get_medal(self, medal):
        if 'gold' in medal.lower():
            medal = 'gold'
            rank = '1'
        elif 'silver' in medal.lower():
            medal = 'silver'
            rank = '2'
        elif 'bronze' in medal.lower():
            rank = '3'
            medal = 'bronze'

        return medal, rank

