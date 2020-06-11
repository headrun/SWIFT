import datetime
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

YEAR = datetime.datetime.now().year

RECORD = SportsSetupItem()

def get_time_zone(location):
    tz_info = get_tzinfo(city= location)
    if not tz_info:
        tz_info = get_tzinfo(country="Spain")
    return tz_info

HOME_AWAY_XPATH = './/div[@class="first_two "][%s]//text()'


class FIBASpider(VTVSpider):
    name = "fiba_spider"
    start_urls = ['http://www.fiba.com/basketballworldcup/2014/fullschedule']

    RECORD['source'] = 'fiba_basketball'
    RECORD['affiliation'] = 'fiba'
    RECORD['participant_type'] = 'team'
    RECORD['tournament'] = "FIBA Basketball World Cup"
    RECORD['time_unknown'] = '0'
    RECORD['game'] = "basketball"
    RECORD['season'] = YEAR

    def parse(self, response):
        hxs = Selector(response)
        root_nodes = get_nodes(hxs, \
                '//div[@class="days gmt"]')
        for root_node in root_nodes[:1]:
            nodes = get_nodes(root_node, './/div[@class="day_content"]//\
                    div[@class="score_list"]//a[contains(@class, "score")]')
            for node in nodes:
                game_note = extract_data(node, './@data-group')
                location = extract_data(node, './@data-venues')
                game_link = extract_data(node, './@href')
                teams = extract_data(node, './/@data-countries').split(',')
                home_team = extract_data(node, HOME_AWAY_XPATH % (1))
                away_team = extract_data(node, HOME_AWAY_XPATH % (2))
                g_date = get_utc_time(extract_data(node, './/@data-date'), \
                        "%d-%m-%y %H:%M", "Europe/Madrid")
                if "Group" in game_note:
                    continue
                elif "Round" in game_note:
                    continue
                data = {'game_datetime': g_date,
                        'location': location,
                        'game_note': game_note,
                        'home_sk': str(teams[0]),
                        'away_sk': str(teams[1]),
                        'participants': {str(teams[0]): (1, teams[0]), \
                        str(teams[1]): (0, teams[1])}}

                if game_link == "#":
                    game_sk = home_team + "_" + away_team + "_" + game_link
                    participants = {str(home_team): (1, 'TBD'), str(away_team): (0, 'TBD')}
                    RECORD['game_status'] = "scheduled"
                    RECORD['event'] = ''
                    RECORD['participants'] = participants
                    tz_info = get_time_zone(data['location'])
                    RECORD['tz_info'] = tz_info
                    RECORD['game_datetime'] = data['game_datetime']
                    RECORD['source_key'] = game_sk
                    RECORD['rich_data'] = {'channels': '',
                                           'game_note': data['game_note'],
                                           'location': { "city": data['location'], "country": "Spain"},
                                           'stadium': ''}
                    RECORD['result'] = {str(home_team): {'tdb_title': str(home_team)},
                                        str(away_team): {'tdb_title': str(away_team)}}
                    yield RECORD
                else:
                    game_link = "http://www.fiba.com" + game_link
                    game_link = "http://www.fiba.com/basketballworldcup/2014/1409/USA-Serbia"
                    yield Request(game_link, callback=self.parse_results, \
                            meta={'data': data})


    def parse_results(self, response):
        hxs = Selector(response)
        result = {}

        data = response.meta['data']
        home_sk = data['home_sk']
        away_sk = data['away_sk']
        game_link = response.url
        game_sk = game_link.split('/')[-1] + "_" + game_link.split('/')[-2]
        home_final = extract_data(hxs, '//div[contains(@class, "country_left")]//span[@class="point"]//text()')
        away_final = extract_data(hxs, '//div[contains(@class, "country_right")]//span[@class="point"]//text()')
        print home_final
        if home_final == '-' or home_final == '' and away_final == '-' or away_final == '':
            RECORD['game_status'] = "scheduled"
            RECORD['result'] = {}
        elif home_final != '-' or home_final != '' and away_final != '-' or away_final != '':
            RECORD['result'] = result
            RECORD['game_status'] = "completed"

            home_final_ = extract_data(hxs, '//div[contains(@class, "country_left")]//span[@class="point"]//text()')
            away_final = extract_data(hxs, '//div[contains(@class, "country_right")]//span[@class="point"]//text()')
            nodes = get_nodes(hxs, '//div[@class="results"]//table/tbody/tr')
            for node in nodes:
                points = extract_list_data(node, './/td//text()')
                result.setdefault(home_sk, {})[points[1]] = points[0]
                result.setdefault(away_sk, {})[points[1]] = points[2]
            result.setdefault(home_sk, {})['final'] = home_final_
            result.setdefault(away_sk, {})['final'] = away_final
            score = '-'.join([str(home_final_), str(away_final)])
            if "OT" in points[1]:
                score = score + '(OT)'
            result.setdefault('0', {})['score'] = score
            if str(home_final_)>str(away_final):
                winner = home_sk
            elif str(home_final_) < str(away_final):
                winner = away_sk
            else:
                winner = ''
            result.setdefault('0', {})['winner'] = winner

        RECORD['event'] = ''
        RECORD['participants'] = data['participants']
        tz_info = get_time_zone(data['location'])
        RECORD['tz_info'] = tz_info
        RECORD['game_datetime'] = data['game_datetime']
        RECORD['source_key'] = game_sk
        RECORD['reference_url'] = response.url
        RECORD['rich_data'] = {'channels': '',
                               'game_note': data['game_note'],
                               'location': {"city": data['location'], "country": "Spain"},
                               'stadium': ''}
        import pdb;pdb.set_trace()
        yield RECORD
