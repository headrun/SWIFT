import datetime
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
from sports_spiders.vtvspider import VTVSpider, get_utc_time, get_tzinfo

true = True
false = False
null = ''


STATUS_DICT = {'final': 'completed', 'completed': 'completed',
               'game over': 'completed', 'in progress': 'ongoing',
               'postponed': 'postponed', 'suspended': 'cancelled',
               'cancelled': 'cancelled', 'completed early': 'completed',
               'preview': 'scheduled', 'live': 'ongoing', 'scheduled': 'scheduled'}

COLLEGE_TEAMS = ['Giants Futures', 'Tigres', 'Blue Wahoos', 'Braves Futures',
                 'Tides', 'Biscuits', 'Bats', 'National', 'American', 'NL Lower Seed',
                 'NL Higher Seed', 'AL Champion', 'NL Champion', "TBD", 'NLWC', 'ALWC',
                 'NL', 'AL', 'AL Wild Card', 'NL Wild Card', 'AL Div. Winner', 'NL Div. Winner',
                 'AL Higher Seed', 'AL Lower Seed', 'AL \\u2013 TBD']

STATES_DICT = {'Cleveland': 'Ohio', 'Toronto': 'Ontario', 'Detroit':
               'Michigan', 'Flushing': 'Michigan', 'Cincinnati': 'Ohio', 'Philadelphia':
               'Pennsylvania', 'Washington': 'Washington', 'Boston': 'Massachusetts',
               'Milwaukee': 'Wisconsin', 'Chicago': 'Illinois', 'Arlington': 'Texas',
               'San Francisco': 'California', 'Seattle': 'Washington', 'Los Angeles':
               'California', 'Phoenix': 'Arizona', 'Bronx':  'New York City', 'Baltimore':
               'Maryland', 'Atlanta': 'Georgia', 'Minneapolis': 'Minnesota', 'St. Louis':
               'Missouri', 'Anaheim': 'California', 'San Diego': 'California', 'Miami':
               'Florida', 'Houston': 'Texas', 'Pittsburgh': 'Pennsylvania', 'St. Petersburg': 'Florida', 'Denver': 'Colorado', 'Kansas City': 'Missouri', 'Oakland':
               'California'}


def get_city_state_country(location):
    loc = {}
    if location in STATES_DICT:
        state = STATES_DICT[location]
        country = 'USA'
    else:
        state = ''
        country = 'USA'
    if "Wild Card" in location:
        location = ''
    if "Chi White Sox" in location:
        location = ''

    loc = {'city': location, 'state': state, 'country': country}
    return loc


def get_game_date_time(game_time):
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
        pattern = '%Y-%m-%d %H:%M:%S'
        time_unknown = 0
        game_dt = get_utc_time(game_time, pattern, 'UTC')
    else:
        pattern = '%I:%M %p ET, %B %d, %Y'
        time_unknown = 0
        game_dt = get_utc_time(game_time, pattern, 'US/Eastern')

    return game_dt, time_unknown


def get_timezone_info(loc, date):
    city = loc.get('city', '')
    tz_info = ''
    if city:
        tz_info = get_tzinfo(city=city, game_datetime=date)
        if not tz_info:
            tz_info = get_tzinfo(city=city, country=loc.get(
                'country', ''), game_datetime=date)

    if not tz_info and '2018' in date:
        tz_info = get_tzinfo(country='United States', game_datetime=date)
    return tz_info


def get_game_status(gamestatus):
    game_status = ''
    if gamestatus.lower() in STATUS_DICT:
        game_status = STATUS_DICT[gamestatus.lower()]
    return game_status


def get_tou_event(game_type, division=''):
    event = ''
    tou_name = 'Major League Baseball'
    if "S" in game_type or "E" in game_type:
        event = "MLB Spring Training"
    elif "A" in game_type:
        event = "MLB All-Star Game"
    elif "R" in game_type:
        event = "MLB Regular Season"
    elif "F" in game_type:
        if "nl wild card" in division.lower():
            event = "NL Wild Card"
        elif "al wild card" in division.lower():
            event = "AL Wild Card"
    elif "L" in game_type:
        if division == "NL Championship Series":
            event = "National League Championship Series"
        elif division == "AL Championship Series":
            event = "American League Championship Series"
    elif "D" in game_type:
        if division == "NL Division Series":
            event = "National League Division Series"
        elif division == "AL Division Series":
            event = "American League Division Series"
    elif "W" in game_type:
        event = "World Series"
    else:
        game_type = ''
    return game_type, tou_name, event


def get_result(data):
    result = [[[0], [''], {'winner': '', 'ot': [], 'hits':'', 'errors':'', 'innings_scores':[]}],
              [[1], [''], {'winner': '', 'ot': [], 'hits':'', 'errors':'', 'innings_scores':[]}]]
    home_score = []
    away_score = []
    if "innings" in list(data.keys()):
        total_scores = data['innings']
        for score in total_scores:
            if type(score) is not dict:
                score = {}
            if 'home' in list(score.keys()):
                home_score.append(score['home'].get('runs', 'X'))
            else:
                home_score.append('X')

            if 'away' in list(score.keys()):
                away_score.append(score['away'].get('runs', 'X'))
            else:
                away_score.append('X')

        try:
            away_total_score = data['away']['runs']
            home_total_score = data['home']['runs']
        except:
            away_total_score = data['teams']['away']['runs']
            home_total_score = data['teams']['home']['runs']

        if len(home_score) > 1 and len(away_score) > 1:
            result[0][1], result[1][1] = [home_total_score], [away_total_score]
            result[0][-1]['innings_scores'] = home_score[:9]
            try:
                result[0][-1]['hits'] = data['home'].get('hits', 0)
                result[0][-1]['errors'] = data['home'].get('errors', 0)
            except:
                result[0][-1]['hits'] = data['teams']['home'].get('hits', 0)
                result[0][-1]['errors'] = data['teams']['home'].get(
                    'errors', 0)

            result[0][-1]['ot'] = home_score[9:]

            result[1][-1]['innings_scores'] = away_score[:9]
            try:
                result[1][-1]['hits'] = data['away'].get('hits', 0)
                result[1][-1]['errors'] = data['away'].get('errors', 0)
            except:
                result[1][-1]['hits'] = data['teams']['away'].get('hits', 0)
                result[1][-1]['errors'] = data['teams']['away'].get(
                    'errors', 0)

            result[1][-1]['ot'] = away_score[9:]

            if int(away_total_score) > int(home_total_score):
                result[0][-1]['winner'] = 0
                result[1][-1]['winner'] = 1
            elif int(away_total_score) == int(home_total_score):
                result[1][-1]['winner'] = ''
                result[0][-1]['winner'] = ''
            else:
                result[1][-1]['winner'] = 0
                result[0][-1]['winner'] = 1
    return result


class MLBAPISpider(VTVSpider):
    name = "mlb_api_spider"
    start_urls = []
    game_note_dict = {}

    def __init__(self, *args, **kwargs):
        super(MLBAPISpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        next_week_days = []
        date_time = datetime.datetime.now()
        top_url = 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=%s'
        if self.spider_type == "schedules":
            for i in range(100, 300):
                next_week_days.append(
                    (date_time + datetime.timedelta(days=i)).strftime('%m2F%d2F%Y'))
        else:
            for i in range(0, 10):
                next_week_days.append(
                    (date_time - datetime.timedelta(days=i)).strftime('%m2F%d2F%Y'))

        for wday in next_week_days:
            req_url = top_url % wday.replace('2F', '%2F')
            yield Request(req_url, callback=self.parse)

    def parse(self, response):
        try:
            game_data = eval(response.body).get(
                'dates', [])[0].get('games', [])
        except:
            game_data = []

        for data in game_data:
            game_pk = data['gamePk']
            game_link = 'https://statsapi.mlb.com/api/v1.1/game/%s/feed/live?language=en' % game_pk
            division = data.get('seriesDescription', '')
            game_type = data['gameType']
            event_data = get_tou_event(game_type, division)
            game_datetime, time_unknown = get_game_date_time(data['gameDate'])
            status = get_game_status(data['status']['detailedState'].lower())
            series_number = data.get('seriesGameNumber', '')
            if not series_number:
                series_number = data['teams']['away'].get(
                    'seriesGameNumber', '')

            meta = {
                'game_pk': game_pk, 'event': event_data, 'game_date': game_datetime,
                'status': status, 'time_unknown': time_unknown, 'series_number': series_number
            }

            if 'http' not in game_link:
                game_link = 'http://statsapi.mlb.com' + game_link
            yield Request(game_link, callback=self.parse_game, meta=meta)

    def parse_game(self, response):
        sportssetupitem = SportsSetupItem()
        game_date = response.meta['game_date']
        time_unknown = response.meta['time_unknown']
        game_pk = response.meta['game_pk']
        channels, result = [], []

        data = eval(response.body).get('gameData', '')
        score_data = eval(response.body).get('liveData', '')

        if data:
            link_id = data['game']['id'].replace('/', '_').replace('-', '_')
            reference_url = 'http://mlb.mlb.com/mlb/gameday/index.jsp?gid=' + link_id
            home_tbd_title, away_tbd_title, team_names, = [], [], []
            tbd_tuple = ()

            try:
                home_team_name = data['teams']['home']['name']
                home_name_abbrev = data['teams']['home']['abbreviation']
                home_division = data['teams']['home'].get('division', '')
                away_team_name = data['teams']['away']['name']
                away_name_abbrev = data['teams']['away']['abbreviation']
                away_division = data['teams']['away'].get('division', '')

            except:
                home_team_name = data['teams']['home']['name']['display']
                home_name_abbrev = data['teams']['home']['name']['abbrev']
                home_division = data['teams']['home'].get('division', '')
                away_team_name = data['teams']['away']['name']['display']
                away_name_abbrev = data['teams']['away']['name']['abbrev']
                away_division = data['teams']['away'].get('division', '')

            if home_team_name in COLLEGE_TEAMS and 'all-star game' not in home_division.lower():
                home_name_abbrev = 'tbd1'
                home_tbd_title = (home_name_abbrev, home_team_name)
                team_names.append(
                    {'callsign': home_name_abbrev, 'name': home_team_name})

            elif home_team_name and home_name_abbrev:
                team_names.append(
                    {'callsign': home_name_abbrev, 'name': home_team_name})

            if home_team_name == "Hurricanes":
                home_name_abbrev = "HUR"
            if away_team_name == "Hurricanes":
                away_name_abbrev = "HUR"

            if '#' in home_team_name:
                home_team_name = home_team_name.split('#')[0].strip()
            if '#' in away_team_name:
                away_team_name = away_team_name.split('#')[0].strip()

            if away_team_name in COLLEGE_TEAMS and 'all-star game' not in away_division.lower():
                away_name_abbrev = 'tbd2'
                away_tbd_title = (away_name_abbrev, away_team_name)
                team_names.append(
                    {'callsign': away_name_abbrev, 'name': away_team_name})

            elif away_team_name and away_name_abbrev:
                team_names.append(
                    {'callsign': away_name_abbrev, 'name': away_team_name})

            if home_tbd_title and away_tbd_title:
                tbd_tuple = (home_tbd_title, away_tbd_title)

            elif home_tbd_title and not away_tbd_title:
                tbd_tuple = (home_tbd_title)

            else:
                tbd_tuple = (away_tbd_title)

            game_venue = data['venue']['name']
            game_status = get_game_status(response.meta['status'])

            try:
                game_city = data['venue']['location'].get('city', '')
            except:
                game_city = data['venue']['location'].split(',')[0].strip()

            if self.spider_type == 'schedules':
                home_wins = data['teams']['home']['record']['leagueRecord']['wins']
                home_loss = data['teams']['home']['record']['leagueRecord']['losses']
                away_wins = data['teams']['away']['record']['leagueRecord']['wins']
                away_loss = data['teams']['away']['record']['leagueRecord']['losses']

            if self.spider_type == 'scores':
                home_wins = data['teams']['home']['record']['wins']
                home_loss = data['teams']['home']['record']['losses']
                away_wins = data['teams']['away']['record']['wins']
                away_loss = data['teams']['away']['record']['losses']

                time_ampm = data['datetime']['ampm']
                try:
                    game_date = data['datetime']['timeDate'].replace(
                        '/', '-').strip()

                except:
                    game_date = data['datetime']['originalDate']
                    game_time = data['datetime']['time']
                    game_date = game_date + " " + game_time

                time_unknown = 0

                if "3:33" in game_date:
                    game_date = game_date.replace('3:33', '12:00AM')
                    time_unknown = 1
                else:
                    game_date = game_date + time_ampm

                if time_ampm:
                    game_date = get_utc_time(
                        game_date, '%Y-%m-%d %I:%M%p', 'US/Eastern')
                else:
                    game_date = get_utc_time(
                        game_date, '%Y-%m-%d %I:%M', 'US/Eastern')

                if not game_status:
                    return

                if 'linescore' in list(score_data.keys()) and game_status == 'completed':
                    result = get_result(score_data['linescore'])
                else:
                    print("Game status is not Completed")
                    result = {}

            home_probable = home_team_name + ", " + \
                '(' + str(home_wins) + "-" + str(home_loss) + ")"
            away_probable = away_team_name + ", " + \
                '(' + str(away_wins) + "-" + str(away_loss) + ")"

            game_note = '%s vs %s at %s' % (
                away_probable, home_probable, game_venue)

            loc = get_city_state_country(game_city)
            if loc:
                tz_info = get_timezone_info(loc, game_date)
            else:
                tz_info = data['venue']['timeZone']['offset']

            sportssetupitem['game_datetime'] = str(game_date)

            game_type, tou_name, event = response.meta['event']
            game_note = game_note.replace('TBD, (0-0) vs TBD, (0-0) ', '') \
                                 .replace('TBD, (0-0)', 'TBD').replace('TBD, HP(0-0)', 'TBD') \
                                 .replace('TBD, RHP(0-0)', 'TBD').replace('TBD, LHP(0-0)', 'TBD')

            rich_data = {'video_links': [], 'stadium': game_venue,
                         'location': loc, 'game_note': game_note}

            if channels:
                rich_data['channels'] = channels

            if tbd_tuple:
                rich_data['tbd_title'] = tbd_tuple[0]

            game_data = {'participants': team_names, 'game': 'baseball',
                         'participant_type': 'team', 'tournament': tou_name,
                         'affiliation': 'mlb', 'source': 'mlb', 'event': event,
                         'game_status': game_status, 'result': result,
                         'rich_data': rich_data, 'source_key': game_pk,
                         'tz_info': tz_info, 'reference_url': reference_url,
                         'time_unknown': time_unknown, 'season': '2019-20'}

            if game_status == 'cancelled' or game_status == 'postponed':
                if response.meta['series_number']:
                    game_note = "Series Game %s: %s" % (
                        response.meta['series_number'], game_status)
                    rich_data['game_note'] = game_note

            for key, value in list(game_data.items()):
                sportssetupitem[key] = value
            if sportssetupitem['source_key'] == 565691:
                print(sportssetupitem)
            print(sportssetupitem)
            yield sportssetupitem
