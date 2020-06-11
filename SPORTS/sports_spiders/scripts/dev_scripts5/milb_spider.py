import datetime
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, get_utc_time, get_tzinfo


LOC_DICT = {'AGS': ['Aguascalientes', 'Aguascalientes', 'Parque Alberto Romo Chavez'], \
            'OAX': ['Oaxaca', 'Oaxaca', 'Eduardo Vasconcelos Stadium'], \
            'TIG': ['Cancun', 'Quintana Roo', 'Estadio de Beisbol Beto Avila'], \
            'MVA': ['Monclova', 'Coahuila', 'Estadio De Beisbol Monclova'], \
            'SAL': ['Saltillo', 'Coahuila', 'Estadio de Beisbol Francisco I. Madero'], \
            'YUC': ['Merida', 'Yucatan', 'Parque Kukulcan Alamo'], \
            'CAM': ['Campeche', 'Campeche', 'Estadio Nelson Barrera'], \
            'MEX': ['Mexico City', '', 'Estadio Fray Nano'], \
            'TIJ': ['Tijuana', 'Baja California', 'Estadio Gasmart'], \
            'TAB': ['Villahermosa', 'Tabasco', 'Estadio Centenario 27 de Febrero'], \
            'LAG': ['Torreon', 'Coahuila', 'Estadio Revolucion'], \
            'TAM': ['Reynosa', 'Tamaulipas', 'Estadio Adolfo Lopez Mateos'], \
            'CDC': ['Ciudad del Carmen', 'Campeche', 'Estadio Resurgimiento'], \
            'VER': ['Veracruz', 'Veracruz', 'Estadio Universitario Beto Avila'], \
            'MTY': ['Monterrey', 'Nuevo Leon', 'Estadio de Beisbol Monterrey'], \
            'PUE': ['Puebla', 'Puebla', 'Estadio de Beisbol Hermanos Serdan']
}

STATUS_DICT = {'final': 'completed', 'completed': 'completed', 'game over': \
               'completed', 'in progress': 'ongoing', 'postponed': 'postponed', \
               'suspended': 'cancelled', 'cancelled' : 'cancelled', \
               'completed early' : 'completed',
               'preview': 'scheduled', 'scheduled': 'scheduled'}

ALLSTAR_TEAMS = ['Sur All-Stars', 'Norte All-Stars']




def get_tou_event(game_type):
    event    = ''

    if "S" in game_type or "E" in game_type:
        event = "LMB Preseason"
    elif "A" in game_type:
        event = "LMB All-Star Game"
    elif "R" in game_type:
        event = "LMB Regular Season"
    elif "F" in game_type:
        event = "LMB Wild Card"
    elif "L" in game_type:
        event = "Mexican League Championship Series"
    elif "D" in game_type:
        event = "Mexican League Division Series"
    elif "W" in game_type:
        event = "LMB World Series"
    else:
        game_type = ''
    return game_type, event

class MexicanBaseball(VTVSpider):
    name = "mexican_baseball"
    allowed_domains = []
    start_urls = []

    def start_requests(self):
        week_days = []
        date_time = datetime.datetime.now()
        top_url = 'http://www.milb.com/gen/mashup/epg/%s/schedule.json'
        if  self.spider_type == "schedules":
            for i in range(0, 300):
                week_days.append((date_time + datetime.timedelta(days=i)).strftime('year_%Y/month_%m/day_%d'))
        else:
            for i in range(0, 10):
                week_days.append((date_time - datetime.timedelta(days=i)).strftime('year_%Y/month_%m/day_%d'))
        for wday in week_days:
            top_link = top_url % wday
            yield Request(top_link, callback = self.parse, meta = {'wday': wday})

    def parse(self, response):
        _data = eval(response.body)
        game_data = _data['data']
        record = SportsSetupItem()
        wday = response.meta['wday']
        for game_dat in game_data:
            away_league = game_dat['away_league']

            if away_league != "MEX":
                continue

            away_league = game_dat['away_league']
            away_sk = game_dat['away_team_abbrev']
            home_sk = game_dat['home_team_abbrev']
            game_datetime = game_dat['game_time_et'].replace('T', ' ')
            game_id = game_dat['game_id']
            game_status = game_dat['game_status_text'].lower()
            game_status = STATUS_DICT.get(game_status, '')
            game_datetime = get_utc_time(game_datetime, '%Y-%m-%d %H:%M:%S', 'US/Eastern')
            game_id = game_id.replace('/', '-').strip()
            game_dt = game_id.replace('-', '_').strip()
            stadium = country = state = city = ''
            loc_data = LOC_DICT.get(home_sk, [])
            if loc_data:
                stadium  = loc_data[2]
                state     = loc_data[0]
                city      = loc_data[1]
                country   = "Mexico"
            loc = {'location': {'city': city, 'country': country, \
                                   'state': state, \
                                   'stadium': stadium}}

            record['affiliation'] = 'lmb'
            record['tournament'] = 'Mexican League Baseball'
            record['participant_type'] = "team"
            record['game'] = 'baseball'
            record['time_unknown'] = '0'
            record['source'] = 'milb'

            tz_info = get_tzinfo(city = city,  \
            game_datetime = game_datetime)
            if not tz_info:
                tz_info = get_tzinfo(city = city, country = country, \
                game_datetime = game_datetime)
            if not tz_info:
                tz_info = get_tzinfo(country = country, \
                game_datetime = game_datetime)

            if game_status not in ["completed", "ongoing"] and self.spider_type == "schedules":
                url = "http://www.milb.com/gdcross/components/game/aaa/%s/gid_%s/linescore.json" % (wday, game_dt)

                yield Request(url, callback = self.parse_schedules, \
                meta = {'tz_info': tz_info, \
                        'loc': loc, 'game_datetime': game_datetime, \
                    'game_status': game_status, 'game_id': game_id, \
                    'home_sk': home_sk, 'away_sk': away_sk, 'record': record})

            if game_status != "scheduled"  and self.spider_type == "scores":
                url = "http://www.milb.com/gdcross/components/game/aaa/%s/gid_%s/linescore.json" % (wday, game_dt)

                yield Request(url, callback=self.parse_scores, meta = {'record': record,  \
                    'game_datetime': game_datetime, 'tz_info': tz_info, \
                    'loc': loc, 'game_status': game_status})

    def parse_schedules(self, response):
        _data = eval(response.body)
        game_data = _data['data']['game']
        record = response.meta['record']
        game_type = game_data['game_type']
        home_city = game_data['home_team_city']
        away_city = game_data['away_team_city']

        if home_city in ALLSTAR_TEAMS and game_type == "A":
            participants = {'tbd1': ('1', ''), 'tbd2': ('0', '')}
            results = {'tbd1': {'tbd_title': home_city}, \
            'tbd2': {'tbd_title': away_city}}
            city = ''
        else:
            participants = { response.meta['home_sk']: ('1',''), \
            response.meta['away_sk']: ('0','')}
            results = {}
            city = home_city

        location = game_data.get('location', '')
        stadium  = game_data['venue']
        game_note = game_data.get('series', '')

        game_type, event = get_tou_event(game_type)
        if not location:
            loc = response.meta['loc']
        else:
            state, country = location.split(',')
            loc = {'location': {'city': city, 'country': country.strip(), \
                    'state': state, \
                    'stadium': stadium}}

        record['event'] = event
        record['tz_info'] = response.meta['tz_info']
        record['game_datetime'] = response.meta['game_datetime']
        record['game_status'] = response.meta['game_status']
        record['source_key'] = response.meta['game_id']
        record['participants'] = participants
        record['rich_data'] = loc
        record['rich_data']['game_note'] = game_note
        record['reference_url'] = response.url
        record['result'] = results
        yield record

    def parse_scores(self, response):
        _data = eval(response.body)
        game_data = _data['data']['game']
        record = response.meta['record']
        results = {}

        game_id = response.url.split('gid_')[-1].replace('/linescore.json', '').strip()
        away_sk = game_data['away_name_abbrev']
        home_sk = game_data['home_name_abbrev']
        game_note = game_data.get('series', '')
        game_type = game_data['game_type']
        home_city = game_data['home_team_city']
        away_city = game_data['away_team_city']
        city = home_city


        if home_city in ALLSTAR_TEAMS and game_type == "A":
            home_sk = 'tbd1'
            away_sk = 'tbd2'
            results.setdefault(home_sk, {}).update({'tbd_title': home_city})
            results.setdefault(away_sk, {}).update({'tbd_title': away_city})
            city = ''

        location = game_data['location']
        stadium  = game_data['venue']
        game_type, event = get_tou_event(game_type)

        if not location:
            loc = response.meta['loc']
        else:
            state, country = location.split(',')
            loc = {'location': {'city': city, 'country': country.strip(), \
                    'state': state, \
                    'stadium': stadium}}

        if response.meta['game_status'] == "completed":
            game_scores = game_data['linescore']
            away_team_errors = game_data['away_team_errors']
            home_team_errors = game_data['home_team_errors']
            home_team_hits  = game_data['home_team_hits']
            away_team_hits  = game_data['away_team_hits']
            home_total_scores = game_data['home_team_runs']
            away_total_scores = game_data['away_team_runs']

            if int(home_total_scores) > int(away_total_scores):
                winner = home_sk
            elif int(home_total_scores) < int(away_total_scores):
                winner = away_sk
            else:
                winner = ''
            score = home_total_scores + "-" + away_total_scores

            results.setdefault('0', {}).update({'winner': winner})
            results.setdefault('0', {}).update({'score': score})
            results.setdefault(home_sk, {}).update({'E': home_team_errors})
            results.setdefault(away_sk, {}).update({'E': away_team_errors })
            results.setdefault(home_sk, {}).update({'H': home_team_hits})
            results.setdefault(away_sk, {}).update({'H': away_team_hits})

            for game_score in game_scores:
                away_inning_runs = game_score['away_inning_runs']
                innings          = "I" + game_score['inning']
                home_inning_runs = game_score['home_inning_runs']

                results.setdefault(home_sk, {}).update({innings: home_inning_runs})
                results.setdefault(away_sk, {}).update({innings: away_inning_runs})


        record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
        record['rich_data'] = loc
        record['rich_data']['game_note'] = game_note
        record['reference_url'] = response.url
        record['source_key'] = game_id.replace('_', '-')
        record['result'] = results
        record['event'] = event
        record['game_status'] = response.meta['game_status']
        record['game_datetime'] = response.meta['game_datetime']
        yield record
