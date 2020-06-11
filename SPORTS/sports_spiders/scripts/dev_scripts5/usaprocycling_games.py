import re
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, \
                    extract_list_data, \
                    get_nodes, get_utc_time, get_tzinfo

DOMAIN = 'http://www.usaprocyclingchallenge.com'

def get_sk(sk_):
    sk_ = sk_.split('/')[-1].split('.htm')[0].strip()
    sk_ = sk_.strip()
    return sk_

def modify(data):
    try:
        data = ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
        return data
    except ValueError or UnicodeDecodeError or UnicodeEncodeError:
        try:
            return data.encode('utf8')
        except  ValueError or UnicodeEncodeError or UnicodeDecodeError:
            try:
                return data
            except ValueError or UnicodeEncodeError or UnicodeDecodeError:
                try:
                    return data.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    data = "".join('NFKD', data.decode('utf-8')).encode('ascii')
                    return data


class UsaProChallenge(VTVSpider):
    name            = 'usaprocycling_games'
    start_urls      = []

    def start_requests(self):
        top_url = 'http://www.usaprocyclingchallenge.com/'
        yield Request(top_url, callback=self.parse, meta= {})

    def parse(self, response):
        import pdb;pdb.set_trace()
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//ul/li[contains(@class, "views-row views-row-")]//div[@class="menu-hover"]')
        for node in nodes:
            stage = extract_data(node, './/div[@class="stagenav_title"]//text()')
            _link  = extract_data(node, './/div[@class="stagenav_title"]//a[contains(@href, "/stages/")]//@href')
            link = DOMAIN + _link
            _date = extract_data(node, './div[@class="stagenav_date"]/span/text()')
            city = extract_data(node, './div[@class="stagenav_city stage-start"]/a/text()')
            if "Stage" in city:
                city = ''
            game_note = extract_data(node, './div[@class="stagenav_city stage-end"]/a/text()')
            if game_note:
                game_note = city +" > "+game_note
            else:
                game_note = city
            season = ''.join(re.findall('.*\/(\d+)/\.*', link))

            yield Request(link, callback = self.parse_games, \
                         meta = {'season': season, 'stage': stage, \
                         '_date': _date, 'city': city, \
                    'game_note': game_note, '_link': _link})

    def parse_games(self, response):
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        season    = response.meta['season']
        _date     = response.meta['_date']
        stage     = response.meta['stage']
        _note = response.meta['game_note']
        city      = response.meta['city']
        _link = response.meta['_link']
        time_unknown = ''
        dt_ = extract_data(hxs, '//div[@class="sstat-item"]/div[contains(text(), "Stage Begins")]/following-sibling::div/text()')
        _dt = ''.join(re.findall('.*( \d+:\d+ .*)', dt_)).replace('MT', '').strip()
        distance = extract_data(hxs, '//div[@class="sstat-item"]/div[contains(text(), "Distance")]/following-sibling::div/text()')
        if _dt:
            date_time = season+ " "+_date+" "+ _dt
            pattern = "%Y %B %d %H:%M %p"
            time_unknown = 0
        else:
            date_time = season+ " "+_date
            pattern = "%Y %B %d"
            time_unknown = 1
        game_time = get_utc_time(date_time, pattern, 'MST')
        if distance:
            game_note = _note+ " - "+ distance
        else:
            game_note = _note
        status = extract_data(hxs, '//div[@class="title"]/span/text()')
        if status:
            game_status = "completed"
            url = 'http://www.usaprocyclingchallenge.com/standings/%s' % (stage.replace(' ', '-'))
            yield Request(url, callback = self.parse_scores, \
                meta= {'game_time': game_time, 'game_note': game_note, \
                        'game_status': game_status, '_link': _link, \
                        'stage': stage, 'time_unknown': time_unknown, \
                        'city': city})
        else:
            game_status = "scheduled"
            record['source_key'] = _link
            record['game_status'] =  game_status
            record['reference_url'] = response.url
            record['tournament'] = "USA Pro Cycling Challenge"
            record['event'] = "USA Pro Cycling Challenge "+ stage.title()
            record['game_datetime'] = game_time
            record['source'] = 'cycling'
            record['participants'] = {}
            record['result'] = {}
            record['affiliation'] = 'uci'
            record['game'] = 'cycling'
            record['rich_data'] =  {'game_note': game_note, \
                            'location': {'city': city, 'state': "Colorado", \
                            "country": "USA"} }
            record['time_unknown'] = time_unknown
            record['tz_info'] = get_tzinfo(city = city)
            if not record['tz_info']:
                record['tz_info'] = get_tzinfo(country = "USA")
            yield record
    def parse_scores(self, response):
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        recor = {}
        riders = {}
        winner = extract_data(hxs, '//div[@class="stand-winrider"]//text()').split('|')[0].split(' ')[1].strip()
        nodes = get_nodes(hxs, '//div[@class="view-content"]/table[@class="views-table cols-6"]//tr//td[contains(@class, "views-field views-field-field")]')
        for node in nodes:
            list_data = extract_list_data(node, './/following-sibling::td/text()')
            data = [i.strip().replace('\t', '') for i in list_data if i.strip() != '\n' or  ' ' not in i or i.strip() != '']
            final_data = [rdata.replace('\t', '').strip() for rdata in data if rdata != '']
            rank  =  extract_data(node, './/text()').strip()
            if rank == '':
                continue
            try:
                rider_sk =  modify(final_data[0]).split(',')[0].strip().lower().title()
                final_time  = final_data[1]
                gap  = final_data[2]
            except:
                continue
            recor[rider_sk] =   {'position': rank, 'final_time': final_time, \
                                    'time_gap': gap}
            riders.update({rider_sk:(0, '')})
        record['source_key'] = response.meta['_link']
        record['game_status'] = response.meta['game_status']
        record['reference_url'] = response.url
        record['source'] = 'usapro_cycling'
        record['game'] = 'cycling'
        record['affiliation'] = 'uci'
        record['tournament'] = "USA Pro Cycling Challenge"
        record['game_datetime'] = response.meta['game_time']
        record['rich_data'] =  {'game_note': response.meta['game_note'], \
                                'location': {'city': response.meta['city'], \
                                'state': "Colorado", 'country': "USA"}}
        record['time_unknown'] = response.meta['time_unknown']
        record['tz_info'] = get_tzinfo(country = "USA")
        record['event'] = "USA Pro Cycling Challenge " + response.meta['stage'].title()
        record['result'] = recor
        final_winner = {'0': {'winner' : winner}}
        record['result'].update(final_winner)
        record['participants'] = riders
        yield record
