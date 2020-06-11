import datetime
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, \
extract_data, extract_list_data, get_nodes, get_utc_time
from vtvspider_new import get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


events = {'fbs': {'ACC': ('atlantic-coast', 'Atlantic Coast Conference'), \
                  'Big 12': ('big-12', 'Big 12 Championship Game'), \
                  'MAC ': ('mid-american', 'Mid-American Conference'), \
                  'MWC': ('mountain-west', 'Mountain West Conference'), \
                  'Pac-12': ('pac-12', 'Pacific-12 Conference'), \
                  'SEC': ('southeastern', 'Southeastern Conference'), \
                  'Sun Belt': ('sun-belt', 'Sun Belt Conference'), \
                  'C-USA': ('conference-usa', 'Conference USA'), \
                  'Big Ten': ('big-ten', 'Big Ten Football Championship Game'), \
                  'IND': ('independent', 'IA Independents Football'), \
                  'AAC': ('american-athletic', 'American Athletic Conference')},
         'fcs': {'Big South': ('big-south', 'Big South Conference'), \
                 'CAA': ('colonial', 'Colonial Athletic Association'), \
                 'Big Sky': ('big-sky', 'Big Sky Conference'), \
                 'Ivy': ('ivy', 'Ivy Football'), \
                 'southland': ('southland' , 'Southland Conference'), \
                 'Missouri Valley': ('mvfc', 'Missouri Valley Football Conference'), \
                 'MEAC': ('mid-eastern', 'Mid-Eastern Athletic Conference'), \
                 'OVC': ('ovc', 'Ohio Valley Conference'), \
                 'patriot': ('patriot', 'Patriot League'), \
                 'SWAC': ('southwestern', 'Southwestern Athletic Conference'), \
                 'NEC': ('northeast', 'Northeast Conference'), \
                 'pioneer': ('pioneer', 'Pioneer Football League'), \
                 'IND': ('independent', 'FCS Independents'), \
                 'southern': ('southern', 'Southern Conference')} }


REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                    'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                    'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                    'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                    'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                    'WV' : 'West Virgina', 'FL' : 'Florida', \
                    'KS' : 'Kansas', 'TN' : 'Tennessee', \
                    'LA' : 'Louisiana', 'MO' : 'Missouri', \
                    'AR' : 'Arkansas', 'SD' : 'South Dakota', \
                    'MS' : 'Mississippi', 'MI' : 'Michigan', \
                    'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                    'ID' : 'Idaho', 'RI' : 'Rhode Island', \
                    'NM' : 'New Mexico', 'MN' : 'Minnesota', \
                    'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
                    'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', \
                    'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado', \
                    'N.M.': 'New Mexico', "Nev.": 'Nevada', \
                    'Ala.': 'Alabama', 'Fla.': 'Florida', 'La.': 'Louisiana', \
                    "Calif.": "California", "N.Y.": "New York", "Md.": "Maryland", \
                    "Mich.": "Michigan", "Ariz.": 'Arizona', "N.C.": "North Carolina", \
                    "Tenn.": "Tennessee", "Ga.": "Georgia", 'Va.': 'Virginia'}
skip_list = []

STADIUM_DICT = {'Bill Snyder Stadium': 'Bill Snyder Family Football Stadium'}

def get_city_state_ctry(venue):
    if len(venue.split(',')) == 2:
        country = 'USA'
        city  = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        state = REPLACE_STATE_DICT.get(venue.split(',')[-1].strip())
        if not state:
            state = ''
    elif len(venue.split(',')) == 3:
        city = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        state = venue.split(',')[1].strip()
        country = 'USA'
    else:
        country = venue.strip()
        if "." in country:
            country = ''
        city, state = '', ''

    return city, state, country


def get_location(venue):
    country = 'USA'
    if "Bowl" in venue:
        stadium = venue.split('Bowl')[0] + "Bowl"
        city = "".join(venue.split('Bowl')[-1].strip().split(' ')[:-1])
        state = venue.split('Bowl')[-1].strip().split(' ')[-1]
        state = REPLACE_STATE_DICT.get(state, '')
    elif "Field" in venue:
        stadium = venue.split('Field')[0] + "Field"
        city = "".join(venue.split('Field')[-1].strip().split(' ')[:-1])
        state = venue.split('Field')[-1].strip().split(' ')[-1]
        state = REPLACE_STATE_DICT.get(state, '')
    elif "Stadium" in venue:
        stadium = venue.split('Stadium')[0] + "Stadium"
        city = "".join(venue.split('Stadium')[-1].strip().split(' ')[:-1])
        state = venue.split('Stadium')[-1].strip().split(' ')[-1]
        state = REPLACE_STATE_DICT.get(state, '')
    else:
        stadium, city, state, country = ''
    return stadium, city, state, country

DOMAIN_URL = "http://www.ncaa.com"

class NCAAFootballSpider(VTVSpider):
    name = 'ncaa_football_spider'
    #allowed_domains = ["www.ncaa.com"]
    start_urls = ['http://www.ncaa.com/scoreboard/football/fcs', \
                 'http://www.ncaa.com/scoreboard/football/fbs', \
                 'http://www.ncaa.com/scoreboard/football/d2', \
                 'http://www.ncaa.com/scoreboard/football/d3']
    start_urls = ['http://www.ncaa.com/scoreboard/football/fcs']

    def parse(self, response):
        hxs = Selector(response)
        event = response.url.split('/')[-2]
        week_nodes = get_nodes(hxs, '//nav[@id="date-browser"]//ul//li/a')
        for week_node in week_nodes[14:]:
            week_url = extract_data(week_node, './/@href')
            week_url = DOMAIN_URL +  week_url + '/' + event
            if "/P/football" in week_url:
                continue
            yield Request(week_url, self.parse_details)


    def parse_details(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="game-contents"]')
        for node in nodes:
            record = SportsSetupItem()
            channel = ''
            channels = items = []
            record['rich_data'] = {}
            game_id = ''
            game_id = extract_data(node, './/ul/li/a[@class="gamecenter"]/@href')
            if "/fcs/" in game_id:
                source_key = game_id.split('/fcs/')[-1]
                record['source_key'] = source_key
                record['tournament'] = "College football"
            elif "/fbs/" in game_id:
                source_key = game_id.split('/fbs/')[-1]
                record['source_key'] = source_key
                record['tournament'] = "College football"
            elif "d2" in game_id:
                source_key = game_id.split('/d2/')[-1]
                record['source_key'] = source_key
                record['tournament'] = "NCAA Division II Football Championship"
            elif "d3" in game_id:
                source_key = game_id.split('/d3/')[-1]
                record['source_key'] = source_key
                record['tournament'] = "NCAA Division III Football Championship"
            elif "d5" in game_id:
                source_key = game_id.split('/d5/')[-1]
                record['source_key'] = source_key
                record['tournament'] = "College football"

            stat_url = "http://www.ncaa.com" + game_id
            game_date = extract_list_data(node, '../preceding-sibling::div/h2/text()')[-1]
            away_sk = extract_data(node, './/table[@class="linescore"]//tr[2]//td/div[@class="team"]/a/@href').split('/schools/')[-1]
            home_sk = extract_data(node, './/table[@class="linescore"]//tr[3]//td/div[@class="team"]/a/@href').split('/schools/')[-1]
            game_time  =  extract_data(node, './/div[contains(@class, "game-status")]/text()').lower()
            game_note  = extract_data(node, './/div[@class="game-championship"]/text()')
            record['game_status'] = 'completed'
            if "tbd" in game_time or "et" in game_time or "tba" in game_time:
                record['game_status'] = 'scheduled'

            if "et" in game_time:
                game_time = '%s %s' % (game_date, ' '.join(game_time.split(' ')[:2]))
                pattern = '%A, %B %d, %Y %I:%M %p'
            else:
                game_time = game_date
                pattern = '%A, %B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
            record['game_datetime'] = date

            record['result'] = {}
            record['game'] = 'football'
            record['participants'] = {home_sk:(1, ''), away_sk: (0, '')}
            record['affiliation'] = 'ncaa-ncf'
            record['participant_type'] = 'team'
            record['rich_data']['channels'] = channels
            record['event'] = ''
            record['source'] = "ncaa_ncf"
            record['rich_data']['game_note'] = game_note
            if not "tbd" in game_time or "tba" not in game_time:
                if record['game_status'] == "completed" and self.spider_type == "scores":
                    yield Request(stat_url, callback=self.parse_scores, meta={'record':record, \
                                        'game_note': game_note,  'game_datetime': date})
                elif record['game_status'] == 'scheduled' and self.spider_type == "schedules":
                    yield Request(stat_url, callback=self.handle_schedule_locations, \
                            meta={'record':record, 'game_note': game_note, 'game_datetime': date})

    def handle_schedule_locations(self, response):
        hxs = Selector(response)
        record  = response.meta['record']
        items = []
        game_datetime = response.meta['game_datetime']
        game_location  = extract_data(hxs, '//p[@class="location"][contains(text(), ",")]/text()')
        if not game_location:
            game_location = extract_data(hxs, '//div[contains(@class, "location")]/text()')

        stadium =  city = state = ''
        stadium = game_location.split(',')[0]
        if '(' in stadium:
            stadium = stadium.split('(')[0].strip()
            venue = ",".join(game_location.split(',')[2:]).strip()
        else:
            stadium = stadium
            venue = ",".join(game_location.split(',')[1:]).strip()
        city, state, country = get_city_state_ctry(venue)

        stadium = stadium.replace('&amp;', '&')
        if STADIUM_DICT.get(stadium, ''):
            stadium = STADIUM_DICT.get(stadium, '')

        if "." in stadium and " ." not in stadium and ". " not in stadium:
            venue = stadium
            stadium, city, state, country = get_location(venue)

        tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
        if not tz_info:
            tz_info = get_tzinfo(city = "New York", game_datetime = game_datetime)
        record['rich_data'] = {'location':{'stadium':stadium, 'city':city, \
                        'state':state, 'country': country}, \
            'game_note': response.meta['game_note']}
        record['time_unknown'] = 0
        record['tz_info'] = tz_info
        record['reference_url'] = response.url
        sportssetupitem = SportsSetupItem()
        for k, v in record.iteritems():
            sportssetupitem[k] = v

        items.append(sportssetupitem)

        for item in items:
            yield item
    def parse_scores(self, response):
        hxs = Selector(response)
        record  = response.meta['record']
        game_datetime = response.meta['game_datetime']
        items  = []
        nodes = get_nodes(hxs,'//div[@class="line-score-container"]//table[@id="linescore"]')
        for node in nodes:
            away_sk = extract_data(node, './/tr[1]//td[@class="school"]//a//@href')
            if "/schools/" in away_sk:
                away_sk = away_sk.split('/')[-2]
            else:
                away_sk = extract_data(node, './/tr[1]//td[@class="school"]//text()')
            if not away_sk:
                away_sk = "TBD"

            home_sk = extract_data(node, './/tr[2]//td[@class="school"]//a//@href')
            if "/schools/" in home_sk :
                home_sk = home_sk.split('/')[-2]
            else:
                home_sk = extract_data(node, './/tr[2]//td[@class="school"]//text()')
            if not home_sk:
                home_sk = "TBD"
            away_scores = extract_list_data(node, './/tr[1]//td[@class="period"]//text()')
            home_scores = extract_list_data(node, './/tr[2]//td[@class="period"]//text()')
            away_total_score = extract_data(node, './/tr[1]//td[@class="score"]//text()')
            home_total_score = extract_data(node, './/tr[2]//td[@class="score"]//text()')

            home_ot = ''
            home_q1 = home_scores[0]
            home_q2 = home_scores[1]
            home_q3 = home_scores[2]
            home_q4 = home_scores[3]
            if len(home_scores) > 4:
                home_ot = home_scores[4]

            away_ot = ''
            away_q1 = away_scores[0]
            away_q2 = away_scores[1]
            away_q3 = away_scores[2]
            away_q4 = away_scores[3]

            if len(away_scores) > 4:
                away_ot = away_scores[4]

            if (int(away_total_score) > int(home_total_score)):
                winner = away_sk
            elif int(away_total_score) < int(home_total_score):
                winner = home_sk
            else:
                 winner = ''

            game_location  = extract_data(hxs, '//p[@class="location"][contains(text(), ",")]/text()')
            if not game_location:
                game_location = extract_data(hxs, '//div[contains(@class, "location")]/text()')

            stadium =  city = state = ''
            stadium = game_location.split(',')[0]
            if '(' in stadium:
                stadium = stadium.split('(')[0].strip()
                venue = ",".join(game_location.split(',')[2:]).strip()
            else:
                stadium = stadium
                venue = ",".join(game_location.split(',')[1:]).strip()
            city, state, country = get_city_state_ctry(venue)

            stadium = stadium.replace('&amp;', '&')

            if "." in stadium and " ." not in stadium and ". " not in stadium:
                venue = stadium
                stadium, city, state, country = get_location(venue)


            game_score = home_total_score + '-' + away_total_score
            if home_ot:

                game_score = game_score + '(OT)'
            record['result'] = {'0': {'winner': winner, 'score': game_score},
                               home_sk: {'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3, \
                                'Q4': home_q4, 'OT1': home_ot, 'final': home_total_score },
                               away_sk: {'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3, \
                                'Q4': away_q4, 'OT1': away_ot, 'final': away_total_score}}
            record['reference_url'] = response.url
            record['participants'] = {home_sk: (1, ''), away_sk: (0, '')}
            record['rich_data'] = {'location': {'stadium':stadium, 'city':city, \
                            'state':state, 'country': country}, \
                            'game_note': response.meta['game_note'] }
            record['time_unknown'] = 0
            tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
            if not tz_info:
                tz_info = get_tzinfo(city = "New York", game_datetime = game_datetime)

            record['tz_info']  = tz_info
            sportssetupitem = SportsSetupItem()
            for k, v in record.iteritems():
                sportssetupitem[k] = v
            items.append(sportssetupitem)
            for item in items:
                return item
