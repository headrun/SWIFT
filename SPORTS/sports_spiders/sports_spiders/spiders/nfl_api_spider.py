from scrapy.http import Request
from sports_spiders.items import  SportsSetupItem
from sports_spiders.vtvspider import VTVSpider, log, get_tzinfo
from datetime import datetime, timedelta
import json, requests

TBD_TEAMS = ["AFC Champ", "NFC Champ"]

conference_dict = {'AFC' : ['Texans', 'Raiders', 'Dolphins', 'Steelers'], 'NFC': ['Seahawks', 'Lions', 'Packers', 'Giants', 'Falcons', 'Cowboys']}

conference_dict = {'AFC': ['Titans', 'Chiefs', 'Bills', 'Jaguars', 'Patriots', 'Steelers'], 'NFC': ['Falcons', 'Rams', 'Panthers', 'Saints', 'Vikings', 'Rams', 'Eagles']}

conference_dict = {'AFC':['Ravens','Titans','Texans','Chiefs', 'Bills', 'Patriots'], 'NFC': ['49ers','Packers','Seahawks', 'Saints', 'Vikings', 'Eagles']}


LOC_INFO = {"NE": ('Foxborough', 'Massachusetts'),
            "DEN": ("Denver", 'Colorado'),
            "SEA" : ("Seattle", "Washington"),
            "GB" : ("Green Bay", "Wisconsin"),
            "CIN" : ('Cincinnati', "Ohio"),
            "IND" : ('Indianapolis', "Indiana"),
            "BAL" : ('Baltimore', "Maryland"),
            "PIT" : ('Pittsburgh', 'Pennsylvania'),
            "ARI" : ('Glendale', 'Arizona'),
            "CAR" : ('Charlotte', 'North Carolina'),
            "DET" : ('Detroit', 'Michigan'),
            "DAL" : ('Irving', "Texas"),
            "BUF" : ('Orchard Park', 'New York'),
            "MIA" : ('Miami', 'Florida'),
            "NYJ" : ('East Rutherford', 'New Jersey'),
            "HOU" : ('Houston', 'Texas'),
            "CLE" : ('Cleveland', 'Ohio'),
            "JAC" : ('Jacksonville', 'Florida'),
            "TEN" : ('Nashville', 'Tennessee'),
            "KC" : ('Kansas City', 'Missouri'),
            "SD" : ('San Diego', 'California'),
            "OAK" : ('Oakland', 'California'),
            "PHI" : ('Philadelphia', 'Pennsylvania'),
            "NYG" : ('East Rutherford', 'New Jersey'),
            "WAS" : ('Landover', 'Maryland'),
            "ATL" : ('Atlanta', 'Georgia'),
            "CHI" : ('Chicago', 'Illinois'),
            "JAX" : ('Jacksonville', 'Florida'),
            "LA"  : ('Los Angeles', 'California'),
            "MIN" : ('Minneapolis', 'Minnesota'),
            "NO"  : ('New Orleans', 'Louisiana'),
            "LAC" : ('Los Angeles', 'California'),
            "SF"  : ('Santa Clara', 'California'),
            "TB"  : ('Tampa', 'Florida')
}

STATUS_DICT = {'pregame':'scheduled', 'ingame': 'ongoing', 'halftime':'ongoing',
               'suspended': 'cancelled', 'final': 'completed', 'final_overtime': 'completed'}

events_dict = {'PRE': [5, 'NFL Preseason'], 'REG': [17, 'NFL Regular Season'],
               'POST': [5, 'NFL Postseason'], 'HOF': [1, 'Pro Football Hall of Fame Game'],
               'PRO': [1, 'Pro Bowl'], 'SB': [1, 'Super Bowl']}

#events_dict = {'POST':[5,'NFL Postseason'],'PRO': [1, 'Pro Bowl'], 'SB': [1, 'Super Bowl']}

def get_conference_name(participants):
    try:
        temp_check1 = participants[0]['name']
        temp_check2 = participants[1]['name']
        if temp_check1 in conference_dict['NFC'] or temp_check2 in conference_dict['NFC']: return 'NFC'
        else: return 'AFC'
    except IndexError:
        return ''

def get_tz_info(loc_city, stadium, game_datetime):
    if loc_city and "Wembley Stadium" in stadium:
        _country = "United Kingdom"
    else:
        _country = "USA"
    tz_info = get_tzinfo(city= loc_city, country = _country, game_datetime = str(game_datetime))
    if not tz_info:
        tz_info = get_tzinfo(country= _country, game_datetime = str(game_datetime))
    return tz_info


def get_key():
    headers = {
        'pragma': 'no-cache',
        'origin': 'https://www.nfl.com',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded',
        'accept': '*/*',
        'cache-control': 'no-cache',
        'x-domain-id': '100',
        'authority': 'api.nfl.com',
        'referer': 'https://www.nfl.com/',
        }

    data = {
        'grant_type': 'client_credentials'
        }

    response = requests.post('https://api.nfl.com/v1/reroute', headers=headers, data=data)
    key_data = json.loads(response.text)
    key = key_data.get('token_type', '') + ' ' + key_data.get('access_token', '')
    return key

class NFLAPISpider(VTVSpider):
    name = 'nfl_api_spider'
    start_urls = ['http://www.nfl.com/schedules']

    def __init__(self, *args, **kwargs):
        super(NFLAPISpider, self).__init__(*args, **kwargs)
        self.headers = {
          'Authorization': '',
          'Referer': 'https://www.nfl.com/',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
          'Content-Type': 'application/json',
           }

        key = get_key()
        self.headers.update({'Authorization':key})

    def start_requests(self):
        for key, value in events_dict.items():
            weeks, event = value
            for week in range(1, weeks+1):
                season = 2020
                url = 'https://api.nfl.com/v1/games?s={"$query":{"week.season":%s,"week.seasonType":"%s","week.week":%s}}&fs={week{season,seasonType,week,name},id,gameTime,venue{id,name,location},networkChannels,esbId,gsisId,gameStatus,homeTeam{id,abbr,nickName},visitorTeam{id,abbr,nickName},homeTeamScore,visitorTeamScore}'
                top_url = url % (season, key, week)
                yield Request(top_url, callback = self.parse, headers=self.headers, meta={'event':event})


    @log
    def parse(self, response):
        event_text = response.meta['event']
        _data = json.loads(response.body).get('data', [])

        for game in _data:
            result = {}
            game_sk = game['esbId']
            game_time = game['gameTime']
            season = game['week']['season']
            event_name = game['week']['name']
            home_sk = game['homeTeam']['abbr']
            home_name = game['homeTeam']['nickName']
            away_sk = game['visitorTeam']['abbr']
            away_name = game['visitorTeam']['nickName']
            channels = game['networkChannels']
            game_status = STATUS_DICT.get(game['gameStatus']['phase'].lower(), '')

            game_link = 'https://www.nfl.com/gamecenter/%s/%s/%s/%s@%s' % (game_sk, season, event_name.replace(' ', ''), home_name, away_name)
            if not home_name:
                home_name = 'TBD'
                home_sk = 'TBA1'

            if not away_name:
                away_name = 'TBD'
                away_sk = 'TBA2'
 
            game_note = home_name + ' v ' + away_name

            if not game_status: continue
            if not game_time: continue

            if game_time:
                time_delta = [int(item) for item in game_time.split('-')[-1].split(':')]
                game_datetime = datetime.strptime(game_time.split('.')[0], '%Y-%m-%dT%H:%M:%S') +\
                                timedelta(hours=time_delta[0], minutes=time_delta[-1])

            tz_info = ''
            try:
                stadium_data = game['venue']
            except:
                stadium_data, city, state, stadium = ['']*4
            if stadium_data:
                stadium = stadium_data.get('name', '').replace("Levi's?? Stadium", "Levi's Stadium")
                try:
                    city, state = LOC_INFO[home_sk]
                    #if not city:
                except:
                    city = stadium_data.get('location', {}).get('city', '')
                    state = stadium_data.get('location', {}).get('state', '')

                tz_info = get_tz_info(city, stadium, game_datetime)

                if stadium:
                    game_note = game_note + ' at ' + stadium

            if channels:
                channels = '<>'.join(channels.get('data', []))

            if home_name in TBD_TEAMS:
                home_sk = 'TBA1'
                result[home_sk] = {'tbd_title': home_name}

            if away_name in TBD_TEAMS:
                away_sk = 'TBA2'
                result[away_sk] = {'tbd_title': away_name}

            if "Rice" in home_name:
                home_sk = 'team_rice'
                home_name = 'Team Rice'

            if "Irvin" in away_name:
                away_sk = 'team_irvin'
                away_name = 'Team Irvin'

            if game_status == 'completed':
                home_scores = game.get('homeTeamScore', {})
                away_scores = game.get('visitorTeamScore', {})

                home_final = home_scores.get('pointsTotal', 0)
                away_final = away_scores.get('pointsTotal', 0)

                home_q1 = home_scores.get('pointsQ1', 0)
                home_q2 = home_scores.get('pointsQ2', 0)
                home_q3 = home_scores.get('pointsQ3', 0)
                home_q4 = home_scores.get('pointsQ4', 0)

                away_q1 = away_scores.get('pointsQ1', 0)
                away_q2 = away_scores.get('pointsQ2', 0)
                away_q3 = away_scores.get('pointsQ3', 0)
                away_q4 = away_scores.get('pointsQ4', 0)

                home_ot = home_scores.get('pointsOvertime', {}).get('data', 0)
                if home_ot:
                    home_ot = home_ot[0]

                away_ot = away_scores.get('pointsOvertime', {}).get('data', 0)
                if away_ot:
                    away_ot = away_ot[0]
                
                game_score = str(home_final) + '-' + str(away_final)
                if home_ot:
                    game_score = str(home_final) + '-' + str(away_final) + ' (OT)'

                if int(away_final) > int(home_final):
                    winner = away_sk

                elif int(away_final) < int(home_final):
                    winner = home_sk

                elif int(away_ot) > int(home_ot):
                    winner = away_sk

                elif int(away_ot) < int(home_ot):
                    winner = home_sk

                else:
                    winner = ''

                result =  {'0': {'winner': winner, 'score': game_score},
                           home_sk: {'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3,
                           'Q4': home_q4, 'OT1': home_ot, 'final': home_final},
                           away_sk: {'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3,
                           'Q4': away_q4, 'OT1': away_ot, 'final': away_final}}

            participants = [{'callsign': str(home_sk), 'name': str(home_name)},
                            {'callsign': str(away_sk), 'name': str(away_name)}]

            conf, event = '', ''
            if "Wild Card Weekend" in event_name:
                conf = get_conference_name(participants)
                event = conf + " Wild Card Round"
            elif "Divisional Playoffs" in event_name:
                conf = get_conference_name(participants)
                event = conf + " Divisional Round"
            elif "conference championships" in event_name.lower():
                conf = get_conference_name(participants)
                event =  conf + " Championship Game"
            elif "Pro Bowl" in event_name:
                event = "Pro Bowl"
            elif "Super Bowl" in event_name:
                event = "Super Bowl"
            else:
                event = event_text

            if not conf and 'Wild Card' in event:
                conf = get_conference_name(participants)
                event = conf + " Wild Card Round"

            participants = [{'callsign': str(home_sk), 'name': str(home_name)},
                            {'callsign': str(away_sk), 'name': str(away_name)}]

            record                      = SportsSetupItem()
            record['source_key']        = str(game_sk)
            record['game']              = 'american football'
            record['affiliation']       = 'nfl'
            record['source']            = 'NFL'
            record['participant_type']  = 'team'
            record['game_status']       = game_status
            record['game_datetime']     = str(game_datetime)
            record['participants']      = participants
            record['result']            = result
            record['tournament']        = 'National Football League'
            record['reference_url']     = game_link
            record['event']             = event
            record['time_unknown']      = "0"
            record['rich_data']         = {'channels': str(channels),
                                           'location': {'city': city,
                                                        'state': state,
                                                        'country': 'USA'},
                                           'stadium': stadium, 'game_note': game_note}
            record['tz_info']           = tz_info

            if "NFL Preseason" in event:
                continue

            if "Pro Football Hall of Fame Game" in event:
                continue
 
            if 'scheduled' in game_status and self.spider_type == 'schedules':
                yield record

            if game_status in ['completed', 'ongoing'] and self.spider_type == 'scores':
                yield record

            if game_status not in ['scheduled', 'completed', 'ongoing'] and self.spider_type:
                yield record



