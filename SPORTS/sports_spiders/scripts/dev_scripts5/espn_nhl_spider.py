import time, datetime
import re
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import extract_data, extract_list_data, get_nodes, get_utc_time
from vtvspider_new import get_tzinfo
from vtvspider_new import VTVSpider

true = True
false = False
null = ''

DOMAIN_LINK = "http://espn.go.com"

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', 'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', 'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

def get_location(venue):
    if len(venue.split(',')) == 2:
        country = ''
        city  = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        if "Bonaventure" in city:
            city = "St. Bonaventure"
        state_ = venue.split(',')[-1].strip()
        state = REPLACE_STATE_DICT.get(state_)
        if not state:
            state = state_
    elif len(venue.split(',')) == 3:
        city = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        if "Bonaventure" in city:
            city = "St. Bonaventure"
        state = venue.split(',')[1].strip()
        country = ''
    else:
        country = venue.strip()
        city, state = '', ''
    if city == "Washington":
        city = "Washington"
        country = "USA"
        state = "District of Columbia"

    return city, state, country



class NHLESPNSpider(VTVSpider):
    name = "espn_nhl_spider"
    allowed_domains = []
    start_urls = []

    def start_requests(self):
        next_week_days = []
        urls = []
        top_urls = 'http://espn.go.com/nhl/scoreboard?date=%s'
        if self.spider_type =="schedules":
            for i in range(0, 100):
                next_week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        else:
            for i in range(0, 8):
                next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))

        for wday in next_week_days:
            t_url = top_urls %(wday)
            yield Request(t_url, callback=self.parse)


    def parse(self, response):
        sel = Selector(response)
        game_nodes = get_nodes(sel, '//div[@class="mod-content"]//div[contains(@id, "gameHeader")]')
        for node in game_nodes:
            game_id = extract_data(node, './@id')
            game_id = "".join(game_id.split('-')[0])
            game_status = extract_data(node, './/ul//li//span/text()')
            if not game_status or "ET" not in game_status:
                game_status = extract_list_data(node, './/ul//li//text()')[0].strip()
            print game_status
            game_note = extract_data(node, './/..//p[contains(@id, "gameNote")]//text()')
            event = ''
            
            if "NHL ALL-STAR" in game_note:
                event = "NHL All-Star Game"
            if "WINTER CLASSIC" in game_note:
                event = "NHL Winter Classic"
            if "EASTERN CONFERENCE FIRST ROUND" in game_note:
                event = "NHL Eastern First Round"
                game_note = game_note.replace('EASTERN CONFERENCE FIRST ROUND - ', '').strip()
            if "WESTERN CONFERENCE FIRST ROUND" in game_note:
                event = "NHL Western First Round"
                game_note = game_note.replace('WESTERN CONFERENCE FIRST ROUND - ', '').strip()
            if "EASTERN CONFERENCE SECOND ROUND" in game_note:
                event = "NHL Eastern Conference Semifinals"
                game_note = game_note.replace('EASTERN CONFERENCE SECOND ROUND - ', '').strip()
            if "WESTERN CONFERENCE SECOND ROUND" in game_note:
                event = "NHL Western Conference Semifinals"
                game_note = game_note.replace('WESTERN CONFERENCE SECOND ROUND - ', '').strip()

            if "EASTERN CONFERENCE FINALS" in game_note:
                event = "NHL Eastern Conference Finals"
                game_note = game_note.replace('EASTERN CONFERENCE FINALS - ', '').strip()
            if "WESTERN CONFERENCE FINALS" in game_note:
                event = "NHL Western Conference Finals"
                game_note = game_note.replace('WESTERN CONFERENCE FINALS - ', '').strip()

            if "ET" in game_status:
                status = "scheduled"
            elif "Final" in game_status:
                status = "completed"
            elif "postponed" in game_status.lower():
                status = "postponed"
            elif "cancelled" in game_status.lower():
                status = "cancelled"
            else:
                status = "ongoing"
            home_id = away_id = ''
            away_link = extract_data(node, './/tr[contains(@id, "awayHeader")]//td[@class="team-name"]//a//@href')
            away_name = extract_data(node, './/tr[contains(@id, "awayHeader")]//td[@class="team-name"]//text()')
            home_link = extract_data(node, './/tr[contains(@id, "homeHeader")]//td[@class="team-name"]//a//@href')
            home_name = extract_data(node, './/tr[contains(@id, "homeHeader")]//td[@class="team-name"]//text()')

            if home_link:
                home_id = home_link.split('/')[-2]
                away_id = away_link.split('/')[-2]

            if not home_link:
                home_id = home_name
            if not away_link:
                away_id = away_name

            if status == "scheduled" and self.spider_type=="schedules":
                game_link = 'http://espn.go.com/nhl/boxscore?gameId=%s' %(game_id)
                yield Request(game_link, callback=self.parse_schedules, \
                meta = {'home_id': home_id, 'away_id': away_id, 'game_id': game_id, \
                'event': event, 'game_note': game_note})

            elif status not in ["scheduled", "postponed", "cancelled"] and self.spider_type=="scores":
                game_link = 'http://espn.go.com/nhl/boxscore?gameId=%s' %(game_id)
                yield Request(game_link, callback=self.parse_scores, \
                    meta = {'home_id': home_id, 'away_id': away_id,  \
                    'game_id': game_id, 'game_status': status, \
                    'event': event, 'game_note': game_note})

            elif "postponed" in status or "cancelled" in status:
                game_link = 'http://espn.go.com/nhl/boxscore?gameId=%s' %(game_id)
                yield Request(game_link, callback=self.parse_scores, \
                    meta = {'home_id': home_id, 'away_id': away_id, \
                    'game_id': game_id, 'game_status': status, \
                    'event': event, 'game_note': game_note})


    def parse_schedules(self, response):
        sel = Selector(response)
        data = response.body
        season = re.findall('"seasonType": (\d+)', data)
        game_id = response.meta['game_id']
        home_id = response.meta['home_id'].upper()
        away_id = response.meta['away_id'].upper()
        event = response.meta['event']

        if not event and "2" in season:
            event = "NHL Regular Season"
        if not event and "1" in season:
            event = "NHL Preseason"

        game_note = response.meta['game_note']
        record = SportsSetupItem()
        game_time = extract_data(sel, '//div[@class="game-time-location"]//p[1]//text()').strip()
        if "TBA" in game_time :
            game_time = game_time.split('TBA,')[-1].strip()
            pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
            time_unknown = 1
        elif "TBD" in game_time :
            game_time = game_time.split('TBD,')[-1].strip()
            pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
            time_unknown = 1
        else:
            pattern = '%I:%M %p ET, %B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
            time_unknown = 0

        game_location = extract_data(sel, '//div[@class="game-time-location"]//p[2]//text()').strip()
        stadium = game_location.split(',')[0]
        venue = ",".join(game_location.split(',')[1:]).strip()
        city, state, country = get_location(venue)
        channel = extract_data(sel, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(sel,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
        channel = channel.replace('/', '<>')

        record['tournament']    = 'National Hockey League'
        record['game']          =  "hockey"
        record['participant_type'] = "team"
        record['affiliation']   = "nhl"
        record['source'] = "espn_nhl"
        record['event'] = event
        record['game_status'] = "scheduled"
        record['source_key'] = game_id
        record['reference_url'] = response.url
        record['participants'] = { home_id: (1, ''), away_id: (0, '')}
        record['game_datetime'] = date
        record['time_unknown'] = time_unknown
        record['tz_info'] = get_tzinfo(city = city, game_datetime = date)
        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(city = city, country = country, game_datetime = date)

        record['rich_data'] = {'channels': str(channel), 'game_note': game_note, \
                                'location': { 'city': city, 'state': state}, \
                                'stadium': stadium}
        record['result'] = {}
        yield record


    def parse_scores(self, response):
        sel = Selector(response)
        data = response.body
        season = "".join(re.findall('"seasonType": (\d+)', data))
        record = SportsSetupItem()
        game_status = response.meta['game_status']
        game_note = response.meta['game_note']
        event = response.meta['event']
        game_sk = response.meta['game_id']
        home_sk = response.meta['home_id'].upper()
        away_sk = response.meta['away_id'].upper()

        if not event and "2" in season:
            event = "NHL Regular Season"
        if not event and "1" in season:
            event = "NHL Preseason"
        game_scores = extract_list_data(sel, '//table[@class="linescore"]//tr//td[@style="text-align:center"]//text()')
        if game_scores and game_status in ["completed", "ongoing"]:
            if len(game_scores) == 12:
                home_scores = game_scores[8:12]
                away_scores = game_scores[4:8]
                home_final_score = home_scores[-1]
                away_final_score = away_scores[-1]
            else:
                home_scores = game_scores[10:15]
                away_scores = game_scores[5:10]
                home_final_score = home_scores[-1]
                away_final_score = away_scores[-1]

            home_ot = ''
            home_so = ''
            home_p1 = home_scores[0].strip()
            home_p2 = home_scores[1].strip()
            home_p3 = home_scores[2].strip()
            if len(home_scores) > 4:
                if "OT" in game_scores:
                    home_ot = home_scores[3].strip()
                    home_so = ''
                elif "SO" in game_scores:
                    home_so = home_scores[3].strip()
                    home_ot = ''

            away_ot = ''
            away_so = ''
            away_p1 = away_scores[0].strip()
            away_p2 = away_scores[1].strip()
            away_p3 = away_scores[2].strip()
            if len(away_scores) > 4:
                if "OT" in game_scores:
                    away_ot = away_scores[3].strip()
                    away_so = ''
                elif "SO" in game_scores:
                    away_so = away_scores[3].strip()
                    away_ot = ''

            if (int(away_final_score) > int(home_final_score)):
                winner = away_sk
            elif int(away_final_score) < int(home_final_score):
                winner = home_sk
            else:
                winner = ''
            game_score = home_final_score + '-' + away_final_score
            if home_ot:
                game_score = game_score + '(OT)'
            if home_so:
                game_score = game_score + '(SO)'


        if home_sk == '':
            home_sk = extract_data(sel, '//div[@class="team home"]/div[@class="team-info"]/h3/a/@href').strip()
            home_sk = home_sk.split('/id/')[-1].split('/')[0]
            if home_sk == '':
                home_sk = extract_data(sel, '//div[@class="team home"]/div[@class="team-info"]/h3/text()').strip()
        else:
            home_sk = home_sk
        if away_sk == '':
            away_sk = extract_data(sel, '//div[@class="team away"]/div[@class="team-info"]/h3/a/@href').strip()
            away_sk = away_sk.split('/id/')[-1].split('/')[0]
            if away_sk == '':
                away_sk = extract_data(sel, '//div[@class="team away"]/div[@class="team-info"]/h3/text()').strip()
        else:
            away_sk = away_sk

        game_time = extract_data(sel, '//div[@class="game-time-location"]//p[1]//text()').strip()
        if "TBA" in game_time :
            game_time = game_time.split('TBA,')[-1].strip()
            pattern = '%B %d, %Y'
            game_datetime = get_utc_time(game_time, pattern, 'US/Eastern')
            time_unknown = 1
        elif "TBD" in game_time :
            game_time = game_time.split('TBD,')[-1].strip()
            pattern = '%B %d, %Y'
            game_datetime = get_utc_time(game_time, pattern, 'US/Eastern')
            time_unknown = 1
        else:
            pattern = '%I:%M %p ET, %B %d, %Y'
            game_datetime = get_utc_time(game_time, pattern, 'US/Eastern')
            time_unknown = 0

        game_location = extract_data(sel, '//div[@class="game-time-location"]//p[2]//text()').strip()
        stadium = game_location.split(',')[0]
        if '(' in stadium:
            stadium = stadium.split('(')[0].strip()
            venue = ",".join(game_location.split(',')[2:]).strip()
        else:
            stadium = stadium
            venue = ",".join(game_location.split(',')[1:]).strip()
        city, state, country = get_location(venue)

        channel = extract_data(sel, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(sel,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
        channel = channel.replace('/', '<>')

        record['game']       = "hockey"
        record['participant_type'] = "team"
        record['event'] =  event
        record['tournament']    = "National Hockey League"
        record['affiliation']   =  "nhl"
        record['source']        =  "espn_nhl"
        record['source_key']    = game_sk
        record['game_status']   = game_status
        record['game_datetime'] = game_datetime
        record['time_unknown'] = time_unknown
        record['tz_info'] = get_tzinfo(city = city, game_datetime = game_datetime)
        if game_status == "ongoing":
            winner = ''

        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(city = city, country = country, game_datetime = game_datetime)
        if game_status == "completed" or game_status == "ongoing":

            record['result'] = {'0': {'winner': winner, 'score': game_score},
                               home_sk: {'p1': home_p1, 'p2': home_p2, 'p3': home_p3, 'OT1': home_ot, 'SO1': home_so, 'final': home_final_score },
                               away_sk: {'p1': away_p1, 'p2': away_p2, 'p3': away_p3, 'OT1': away_ot, 'SO1': away_so, 'final': away_final_score}}
        else:
            record['result'] = {}

        record['reference_url'] = response.url
        record['participants'] = {home_sk: (1, ''), away_sk: (0, '')}
        record['rich_data'] = { 'channels': str(channel), 'game_note': game_note, \
                                'location': { 'city': city, 'state': state, 'country': country}, \
                                'stadium': stadium}
        yield record


