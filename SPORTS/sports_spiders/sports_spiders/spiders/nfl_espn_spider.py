import json
import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
from sports_spiders.vtvspider import VTVSpider, extract_data,\
    extract_list_data, get_nodes, get_utc_time, get_tzinfo

def get_location(venue):
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
        city, state = '', ''

    return city, state, country

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                      'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                      'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                      'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                      'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', '\
                      FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
                     'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', \
                     'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
                     'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                     'ID' : 'Idaho', 'RI' : 'Rhode Island', \
                     'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', \
                     'MD' : 'Maryland', 'IN' : 'Indiana', \
                     'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', \
                     'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

days = []
_now = datetime.datetime.now().date()
yday  = (_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
days.append(yday)
today =  _now.strftime("%Y-%m-%d")
days.append(today)
tomo =  (_now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
days.append(tomo)

events_dict = {'NFL Preseason': [1, 5], 'NFL Regular Season': [2, 17], 'NFL Postseason': [3, 5]}

events_dict = {'NFL Regular Season': [2, 17]}

class NFLESPNSpider(VTVSpider):

    name = "nfl_espn_spider"
    allowed_domains = []
    start_urls = []
    game_url   = 'http://scores.espn.go.com/nfl/conversation?gameId=%s'

    def start_requests(self):
        top_url  = 'http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?calendartype=blacklist&limit=100&dates=%s&seasontype=%s&week=%s'
        for key, value in list(events_dict.items()):
            season, weeks = value
            for week in range(1, weeks + 1):
                year = '2018'
                week = '6'
                url = top_url % (year, season, week)
                yield Request(url, self.parse, meta={'event': key})


    def parse(self, response):
        raw_data = json.loads(response.body)
        games    = raw_data.get('events', '')
        for game in games:
            game_id   = game['id']
            game_dt   = game['date']
            status    = game['status']['type']['description'].lower()
            team_data = game['competitions'][0]['competitors']
            game_note = game['competitions'][0]['notes']
            try:
                stadium = game['competitions'][0]['venue']['fullName']
            except:
                stadium = ''

            try:
                city = game['competitions'][0]['venue']['address']['city']
            except:
                city = ''

            try:
                state = game['competitions'][0]['venue']['address']['state']
            except:
                state = ''

            if game_note:
                game_note = game_note[0]['headline']

            teams     = []
            result    = {}
            for data in team_data:
                team_sk = data['team']['abbreviation']
                team_name = data['team']['displayName']
                home_away = data['homeAway']
                if home_away.lower() == 'home':
                    is_home = 1
                    home_sk = team_sk
                elif home_away.lower() == 'away':
                    is_home = 0
                    away_sk = team_sk

                teams.append({'callsign': str(team_sk),
                              'name': str(team_name)})

                location = data.get('location', '')
                if status == 'final':
                    final_score = data['score']
                    line_scores = data['linescores']
                    scores      = [int(list(line_score.values())[0]) for line_score in line_scores]
                    winner      = data['winner']
                    if is_home == 1:
                        home_total = final_score
                        home_scores = scores
                        home_ot    = ''
                        home_q1 = home_scores[0]
                        home_q2 = home_scores[1]
                        home_q3 = home_scores[2]
                        home_q4 = home_scores[3]
                        if len(home_scores) > 4:
                            home_ot = home_scores[4]

                    elif is_home == 0:
                        away_total = final_score
                        away_scores = scores
                        away_ot    = ''
                        away_q1 = away_scores[0]
                        away_q2 = away_scores[1]
                        away_q3 = away_scores[2]
                        away_q4 = away_scores[3]


            scores_link = self.game_url % game_id

            if 'tbd' in team_sk.lower() and 'home' in home_away.lower():
                team_sk = 'TBA1'

            if 'tbd' in team_sk.lower() and 'away' in home_away.lower():
                team_sk = 'TBA2'

            if status not in ['scheduled', 'postponed', 'canceled'] and self.spider_type == 'scores':
                if status == 'final':
                    status = 'completed'
                else:
                    status = 'ongoing'
                game_score = home_total + '-' + away_total
                if home_ot:
                    game_score = home_total + '-' + away_total + ' (OT)'
                if (int(away_total) > int(home_total)):
                    winner = away_sk
                elif int(away_total) < int(home_total):
                    winner = home_sk
                else:
                    winner = ''
                result =  {'0': {'winner': winner, 'score': game_score},
                           home_sk: {'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3, \
                            'Q4': home_q4, 'OT1': home_ot, 'final': home_total},
                           away_sk: {'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3, \
                            'Q4': away_q4, 'OT1': away_ot, 'final': away_total}}


                yield Request(scores_link, callback = self.parse_scores, \
                                 meta = { 'teams': teams,
                                 'status': status,
                                 'game_note': game_note, 'result': result, \
				'game_dt' : game_dt, 'location': location, 'city': city, \
				'state': state, 'stadium': stadium, 'event_text': response.meta['event']})

            elif "postponed" in status.lower() or "canceled" in status.lower():
                result = {}
                yield Request(scores_link, callback = self.parse_scores, \
                    meta = {'teams': teams,
                            'status': status, 'game_note': game_note, 'game_dt' : game_dt, 'location': location, 'city': city, \
                                'state': state, 'stadium': stadium, 'result': result, 'event_text': response.meta['event']})

            elif status.lower() == 'scheduled' and self.spider_type == "schedules":
                yield Request(scores_link, self.parse_scores, \
                        meta = {'teams': teams,
                                'status': status,
                                'game_note': game_note, 'game_dt' : game_dt, 'location': location, 'city': city, \
                                'state': state, 'stadium': stadium, 'event_text': response.meta['event']})


    def get_game_date_time(self, game_time):
        if "TBA" in game_time:
            game_time = game_time.split('TBA,')[-1].strip()
            pattern = '%B %d, %Y'
            time_unknown = 1
            game_dt = get_utc_time(game_time, pattern, 'US/Eastern')
        elif "TBD" in game_time:
            game_time = game_time.split('TBD,')[-1].strip()
            pattern = '%B %d, %Y'
            time_unknown = 1
            game_dt = get_utc_time(game_time, pattern, 'US/Eastern')
        elif "Z" in game_time:
            game_time = game_time.replace('T', ' ').replace('Z', '')
            pattern = '%Y-%m-%d %H:%M'
            time_unknown = 0
            game_dt = get_utc_time(game_time, pattern, 'UTC')
        else:
            pattern = '%I:%M %p ET, %B %d, %Y'
            time_unknown = 0
            game_dt = get_utc_time(game_time, pattern, 'US/Eastern')

        return game_dt, time_unknown


    def parse_scores(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        event_text = response.meta['event_text']
        game_notes = response.meta['game_note']
        game_status = response.meta['status']
        game_text = " vs ".join(extract_list_data(sel, '//div[@class="team-info"]//span[@class="short-name"]//text()'))
        game_note = game_text + " at " + response.meta['stadium']
        game_time = extract_data(sel, '//div[@class="game-time-location"]//p[1]//text()').strip()
        if game_time:
            game_dt, time_unknown   = self.get_game_date_time(game_time)
        else:
            game_dt = ''
            time_unknown = ''

        if response.meta['game_dt']:
            game_dt, time_unknown   = self.get_game_date_time(response.meta['game_dt'])

        game_location = extract_data(sel, '//div[@class="game-time-location"]//p[2]//text()').strip()
        stadium = game_location.split(',')[0]
        if '(' in stadium:
            stadium = stadium.split('(')[0].strip()
            venue = ",".join(game_location.split(',')[2:]).strip()
        else:
            stadium = stadium
            venue = ",".join(game_location.split(',')[1:]).strip()
        city, state, country = get_location(venue)
        if not city:
            city = response.meta['city']
            stadium = response.meta['stadium']
            country = 'USA'
            state_ = response.meta['state']
            state = REPLACE_STATE_DICT.get(state_, '') 
            if not state:
                state = state_
            channel = extract_data(sel, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
            if not channel:
                channel = extract_data(sel,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
            if "/" in channel:
                channel = channel.replace('/', '<>')

            if "HALL OF FAME GAME" in game_notes:
                event_name = "Pro Football Hall of Fame Game"

            elif "AFC WILD CARD PLAYOFF" in game_notes:
                event_name = "AFC Wild Card Round"

            elif "NFC WILD CARD PLAYOFF" in game_notes:
                event_name = "NFC Wild Card Round"

            elif "AFC DIVISIONAL PLAYOFF" in game_notes:
                event_name = "AFC Divisional Round"

            elif "NFC DIVISIONAL PLAYOFF" in game_notes:
                event_name = "NFC Divisional Round"

            elif "AFC CHAMPIONSHIP" in game_notes:
                event_name = "AFC Championship Game"

            elif "NFC CHAMPIONSHIP" in  game_notes:
                event_name = "NFC Championship Game"

            elif "PRO BOWL" in game_notes:
                event_name = "Pro Bowl"

            elif "SUPER BOWL LII" in game_notes:
                event_name =  "Super Bowl"
            else:
                event_name = event_text
            game_sk = response.url.split('=')[-1]
            if stadium == "U of Phoenix Stadium":
                stadium = "University of Phoenix Stadium"
            if response.meta['location']:
                city = response.meta['location']
        
            rich_data = {'channels': str(channel),
                         'location': { 'city': city, 'state': state, 'country': country},
                         'stadium': stadium, 'game_note': game_note}
            record['game']             = "american football"
            record['participant_type'] = "team"
            record['event']            = event_name
            record['tournament']       = "National Football League"
            record['affiliation']      = "nfl"
            record['source']           = "espn_nfl"
            record['source_key']       = game_sk
            record['game_status']      = response.meta['status'].replace('canceled', 'cancelled')
            record['participants']     = response.meta['teams']
            record['game_datetime']    = game_dt
            record['reference_url']    = response.url
            record['time_unknown']     = time_unknown
            if city and game_dt:
                record['tz_info']          = get_tzinfo(city = city, game_datetime = game_dt)
                if not record['tz_info']:
                    record['tz_info']      = get_tzinfo(city = city, country = country, \
                                                    game_datetime = game_dt)

            record['rich_data']        = rich_data

            if response.meta['status'] not in ['scheduled', 'postponed'] and self.spider_type == 'scores':
                record['result']       = response.meta['result']
                yield record
            elif response.meta['status'] not in ['final', 'ongoing'] and self.spider_type == 'schedules':
                record['result'] = {}
                yield record
