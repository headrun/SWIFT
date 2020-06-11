from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from vtvspider_dev import extract_list_data, get_utc_time, get_tzinfo
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime


DOMAIN_LINK = "https://en.wikipedia.org"
def xcode(text, encoding='utf8', mode='ignore'):
    return text.encode(encoding, mode) if isinstance(text, unicode) else text

class WCAAthletics(VTVSpider):

    name = "wca_athletics"
    allowed_domains = []
    start_urls = ['https://en.wikipedia.org/wiki/2015_World_Championships_in_Athletics']

    tz_info = get_tzinfo('Beijing')

    def parse(self, response):
        sel = Selector(response)
        events = get_nodes(sel, '//table[@class="multicol"]//tr//td')
        for event in events:
            nodes = get_nodes(event, './/table[@class="wikitable"]//tr[td]')
            for node in nodes:
                link = extract_data(node, './/td//a//@href')
                if "Women%27s_hammer_throw" not in link:
                    continue
                event_url =  DOMAIN_LINK + link
                yield Request(event_url, callback = self.parse_next)


    def parse_next(self, response):
        sel = Selector(response)
        event_name = extract_data(sel, '//h1[@class="firstHeading"]/text()').replace('2015', '').strip()
        check_list = ['preliminary', 'heats', 'semifinals', ' final', 'qualification']
        wiki_gid = ''.join(re.findall('"wgArticleId":\d+', response.body)).strip('"').replace('wgArticleId":', '' ).strip('"').strip()
        dates = extract_list_data(sel, '//th[contains(text(), "Dates")]//following-sibling::td//text()')
        print dates
        dates_dict = {}
        schedules = get_nodes(sel, '//h2[span[@id="Schedule"]]//following-sibling::table[1]/tr[td]')
        if schedules:
            for schedule in schedules:
                details = [text for text in extract_list_data(schedule, './/text()') if text.strip()]
                if len(details) == 3:
                    game_date, game_time, game_note = details
                    game_dt = game_date + ' ' + game_time
                    game_dt = get_utc_time(game_dt, '%d %B %Y %H:%M', 'Asia/Shanghai')
                    dates_dict[game_note.lower()] = game_dt
                else:
                    print details
        else:
            for type_ in check_list:
                for date_ in dates:
                    if type_ in date_:
                        game_dt = date_.split('(')[0].strip()
                        dates_dict[type_] = game_dt
        record = SportsSetupItem()
        game_ = "athletics"
        record['game'] = game_
        record['source'] = "wca_wiki"
        record['tournament'] = "World Championships in Athletics"
        record['affiliation'] = "iaaf"
        record['game_status'] = 'completed'
        record['event'] = event_name
        record['reference_url'] = response.url.encode('unicode_escape')
        record['tz_info'] = self.tz_info

        if 'Women%27s_Masters_400_metres' in response.url:
            game_datetime = dates_dict.get('final')
            if game_datetime.split(' ') == '00:00:00':
                time_unknown = 1
            else:
                time_unknown = 0
            record['game_datetime'] = game_datetime
            record['time_unknown'] = time_unknown
            result, participants = self.marathon_results(sel, response.url)
            source_key = game_datetime.split(' ')[0].strip() + '_' + 'final' + '_' + wiki_gid
            record['source_key'] = source_key
            record['result']     = result
            record['participants'] = participants
            record['game_datetime'] = game_datetime
            record['rich_data']    = {'game_note': 'Final',
                                      'stadium': 'Beijing National Stadium',
                                      'location': {'city': 'Beijing', 'country': 'China'}}
            yield record
        else:
            root_nodes = get_nodes(sel, '//h2[span[@id="Results"]]//following-sibling::h3')
            if not root_nodes:
                root_nodes = get_nodes(sel, '//h2[span[@id="Schedule"]]//following-sibling::h3')
            for root_node in root_nodes:
                nodes = get_nodes(root_node, './/following-sibling::table[1]/tr[td]')
                game_note = extract_data(root_node, './span/text()')
                participants = {}
                result = {}

                for node in nodes:
                    final_time    = extract_data(node, './td[5]/text()')
                    if not final_time and game_note == "Final":
                        final_time    = extract_data(node, './td[4]/text()')
                    if not final_time :
                        final_time    = extract_data(node, './td[6]/text()')
                    if final_time in ['PB', 'SB']:
                        final_time    = extract_data(node, './td[4]/text()')
                    if "," in final_time:
                        final_time = extract_data(node, './td[6]/text()')
                    if "R" in final_time:
                        final_time = 'DQ'
                    final_time = final_time.replace('w', '').strip()
                    rank          = extract_data(node, './/td[1]//text()').strip().split(' ')[0].strip()
                    pl_name, nation = extract_list_data(node, './td//a/text()')[:2]
                    if dates_dict.get(game_note.lower()):
                        game_datetime = dates_dict.get(game_note.lower())
                    else:
                        print 'no time for game note', game_note
                    if game_datetime.split(' ') == '00:00:00':
                        time_unknown = 1
                    record['game_datetime'] = game_datetime
                    pl_sk = pl_name.lower().replace(' ', '_').strip()
                    medal = extract_data(node, './/td[1]//img//@src')
                    if medal:
                        medal, rank = self.get_medal(medal)
                    if rank == '1':
                        result['0'] = {'winner': pl_sk}
                    if 'shot_put' in response.url:
                        final = extract_data(node, './/td[10]//text()')
                        result[xcode(pl_sk)] = {'final': final, 'position': rank, 'medal': medal}
                    elif '_throw' in response.url or '_jump' in response.url:
                        mark = extract_list_data(node, './/td//b//text()')[0]
                        if "AR" in mark:
                            mark = extract_list_data(node, './/td//b//text()')[-2]
                        mark = mark.replace('!', '').strip()
                        result[xcode(pl_sk)] = {'mark': mark, 'position': rank, 'medal': medal}
                    else:

                        result[xcode(pl_sk)] = {'final_time': final_time, 'position': rank, 'medal': medal}
                    participants[xcode(pl_sk)] = (0, '')

                    record['rich_data'] = {'game_note': game_note,
                                           'stadium': 'Beijing National Stadium',
                                           'location': {'city': 'Beijing', 'country': 'China'}}
                    source_key = game_datetime.split(' ')[0].strip() + '_' + game_note.lower().replace('semifinals', 'semi final').replace(' ', '_') + '_' + wiki_gid
                    record['source_key'] = str(source_key)
                    record['participants'] = participants
                    record['result'] = result
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


    def marathon_results(self, sel, ref_url):
        result = {}
        participants = {}
        if '%E2%80%93_Women%27s_Masters_400_metres' in ref_url:
            nodes = get_nodes(sel, '//h2[span[@id="Results"]]//following-sibling::table[1]/tr[td]')
        else:
            nodes = get_nodes(sel, '//h2[span[@id="Results"]]//following-sibling::table[2]/tr[td]')
        for node in nodes:
            final_time    = extract_data(node, './td[4]/text()')
            if 'M' in final_time or not final_time:
                final_time = extract_data(node, './td[6]/text()')
            if final_time.lower() == 'dnf':
                continue
            rank          = extract_data(node, './td[1]/text()').replace('!', '').strip()
            pl_det = extract_list_data(node, './td//a//text()')
            print pl_det
            if pl_det:
                pl_name = pl_det[0]
                nation = pl_det[1]
            pl_sk = pl_name.lower().replace(' ', '_')
            medal = extract_data(node, './/td[1]//img//@src')
            if medal:
                medal, rank = self.get_medal(medal)
            if rank == '1':
                result['0'] = {'winner': pl_sk}
            result[xcode(pl_sk)] = {'final_time': final_time, 'position': rank, 'medal': medal}
            participants[xcode(pl_sk)] = (0, '')

        return result, participants
