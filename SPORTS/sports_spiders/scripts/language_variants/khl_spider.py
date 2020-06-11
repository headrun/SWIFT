import re
import datetime
from scrapy.http import Request
from scrapy.selector import Selector
from sports_spiders.items import SportsSetupItem
from vtvspider import VTVSpider, get_nodes, extract_data
from vtvspider import extract_list_data, get_utc_time, get_tzinfo

CITY_DICT = {'saint petersburg': 'Saint-Petersburg',
             'Togliatti'       : 'Tolyatti'}

STADIUM_DICT = {'Slovnaft Arena' : 'Ondrej Nepela Arena',
'Luzhniki Arena' : 'Luzhniki Stadium',
'Arena-2000 Lokomotiv' : 'Arena 2000',
'Uralets Arena' : 'KRK Uralets',
'Tatneft-Arena' : 'TatNeft Arena',
'Neftekhimik Arena' : 'Neftekhimik Ice Palace',
'Traktor Arena' : 'Traktor Ice Arena',
'Omsk Arena' : 'Arena Omsk',
'Fetisov-Arena' : 'Fetisov Arena',
'Platinum-Arena' : 'Platinum Arena',
'Kazakhstan Arena' : 'Astana Arena',
'Metallurgs Sports Palace' : 'Kuznetsk Metallurgists Sports Palace',
'Ufa-Arena' : 'Ufa Arena',
'Sibir Arena' : 'Ice Sports Palace Sibir',
'Trade Union Sport Palace' : 'Nagorny Arena'}

class KHLSpider(VTVSpider):
    name = 'khl_spider'
    start_urls  = ['http://en.khl.ru/calendar/309/00/']

    domian_url = 'http://en.khl.ru'

    def parse(self, response):
        sel = Selector(response)
        links = extract_list_data(sel, '//a[contains(text(), "Summary")]//@href')
        links.extend(extract_list_data(sel, '//a[contains(text(), "Preview")]//@href'))
        for link in links:
            if link == 'http://www.khl.ru/':
                continue
            if link:
                if not 'http' in link:
                    link = self.domian_url + link

                yield Request(link, callback=self.parse_games)

    def parse_games(self, response):
        sel = Selector(response)
        record                     = SportsSetupItem()
        record['game']             = 'hockey'
        record['source']           = "KHL"
        record['tournament']       = "Kontinental Hockey League"
        record['affiliation']      = "Khl"
        record['event']            = 'KHL Regular season'
        record['reference_url']    = response.url
        record['time_unknown']     = 0
        record['participant_type'] = 'team'

        game_dt = extract_data(sel, '//ul[@class="b-match_add_info"]//li[@class="b-match_add_info_item"]\
                                /span[@class="e-num"]//following-sibling::span//text()')
        if 'preview' in response.url:
            game_date_time = get_utc_time(game_dt, '%d.%m.%Y%H:%M', 'US/Eastern')
        else:
            game_date_time = get_utc_time(game_dt, '%d.%m.%Y%H:%M', 'US/Eastern')

        place_info_xpath = '//ul[@class="b-match_add_info"]//li[@class="b-match_add_info_item"]\
                          //span[contains(text(), "J")]//following-sibling::span//text()'
        place_info = extract_data(sel, place_info_xpath)
        city_name = extract_list_data(sel, '//dd[@class="b-details_txt"]//p[@class="e-club_sity"]//text()')[0]
        city_name = city_name.replace('Region', '').replace('region', '').strip()
        stadium_info = place_info.replace(city_name, '').strip('sp').strip()
        stadium = re.sub('\d+$', '', stadium_info).replace(',', '').strip()
        if stadium == 'Ice Palace' and city_name == 'Cherepovets' :
            stadium = 'Ice Palace (Cherepovets)'
        elif stadium == 'Ice Palace' and city_name == 'saint petersburg':
            stadium = 'Ice Palace (Saint Petersburg)'
        if STADIUM_DICT.get(stadium, ''):
            stadium = STADIUM_DICT.get(stadium, '')

        if stadium == '-Arena':
            stadium = city_name + stadium
        elif stadium == 'Arena':
            stadium = stadium + ' ' + city_name

        city_name = city_name.replace('Region', '').replace('region', '').strip()
        if CITY_DICT.get(city_name.lower(), ''):
            city_name = CITY_DICT.get(city_name.lower(), '')
        tz_info = get_tzinfo(city_name)

        game_text = extract_data(sel, '//ul[@class="b-match_add_info"]//li[@class="b-match_add_info_item"]\
                                /span[@class="e-num"]//text()')
        game_name = game_text.replace(u'\u2116', 'NO.').strip().strip('.')

        source_key = ''.join(re.findall('\d+/\d+', response.url)).replace('/', '_').strip()
        teams = extract_list_data(sel, '//div[@class="b-wide_block b-wide_tile m-wide_tile"]\
                                 //dl//dd[@class="b-details_txt"]//h3//text()') or \
                extract_list_data(sel, '//div[@class="b-match_before"]//div//h2//text()')
        try:
            home_sk, away_sk = [team.lower().replace(' ', '_') for team in teams]
        except:
            print 'Teams are unavailable'
        participants = {home_sk: (1, ''), away_sk: (0, '')}
        record['participants']     = participants
        record['source_key']       = source_key
        record['game_datetime']    = game_date_time
        record['tz_info']          = tz_info
        record['rich_data']        = {'game_note': game_name,
                                      'location': {'city': city_name},
                                      'stadium' : stadium}
        scores_text = extract_data(sel, '//div[@class="b-wide_block b-wide_tile m-wide_tile"]\
                                 //dl//dt[@class="b-total_score"]//h3//text()')
        current_date = datetime.datetime.now().date()
        schedule_date = datetime.datetime.strptime(game_date_time, '%Y-%m-%d %H:%M:%S').date()
        if current_date > schedule_date:
            status = 'completed'
        else:
            status = 'scheduled'
        if self.spider_type == 'schedules' and status == 'scheduled':
            record['game_status'] = status
            record['result']      = {}

            yield record
        elif self.spider_type == 'scores' and status != 'scheduled':
            period_scores_text = extract_data(sel, '//dd[@class="b-period_score"]/text()')\
                                       .replace('\n', '').strip()

            home_ot , away_ot, home_period, away_period = [], [], [], []
            home_so , away_so = [], []
            if ';' in period_scores_text:
                period_scores, extra_scores = period_scores_text.split(';')
                period_scores = [score for score in period_scores.strip().split(' ')]
                extra_scores  = [score for score in extra_scores.strip().split(' ')]
            else:
                if u'\xa0' in period_scores_text:
                    period_scores = [score for score in period_scores_text.strip().split(u'\xa0')]
                else:
                    period_scores = [score for score in period_scores_text.strip().split(' ')]
                extra_scores  = []
            for score in period_scores:
                home, away = score.split(':')
                home_period.append(home)
                away_period.append(away)

            for ind, score in enumerate(extra_scores):
                home, away = score.split(':')
                if ind == 0:
                    home_ot.append(home)
                    away_ot.append(away)
                else:
                    home_so.append(home)
                    away_so.append(away)

            result , participants = {}, {}
            home_final, away_final = re.findall('\d+', scores_text)
            home_win = 0
            away_win = 0
            if int(home_final) > int(away_final) : home_win =  1
            elif int(home_final) < int(away_final): away_win = 1

            result = [[[0], [str(home_final)], {'final': [], 'ot':[], \
                     'so':[], 'quarters': [], 'winner': home_win}], [[1], [str(away_final)], \
                     {'final': [], 'ot':[], 'so':[], 'quarters' : []  , 'winner': away_win}]]

            result[0][-1]['final'] = home_final
            result[0][-1]['ot'] = home_ot
            result[0][-1]['quarters'] = home_period
            result[0][-1]['so'] = home_so
            result[1][-1]['final'] = away_final
            result[1][-1]['ot'] = away_ot
            result[1][-1]['so'] = away_so
            result[1][-1]['quarters'] = away_period
            record['result']           = result

            record['game_status']      = status
            yield record
