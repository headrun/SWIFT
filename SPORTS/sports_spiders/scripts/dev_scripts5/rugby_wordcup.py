from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, get_utc_time, get_tzinfo

true = True
false = False
null = ''

GAME_NOTE_DICT = ['Winner QF1', 'Winner QF2', 'Winner QF3', 'Winner QF4', 'Loser SF1', 'Loser SF2', 'Winner SF1', 'Winner SF2']

EVENT_DICT = {'Quarter Finals': 'Rugby World Cup Quarterfinals', \
             'Semi Finals': 'Rugby World Cup Semifinals', \
             'Final': 'Rugby World Cup Final'}

class RugbyWorldCup(VTVSpider):
    name = "rugby_worldcup"
    allowd_domains = []
    start_urls = []

    def start_requests(self):
        top_url = "http://cmsapi.pulselive.com/rugby/event/1238/schedule.json"
        yield Request(top_url, callback = self.parse, meta = {})

    def parse(self, response):
        raw_data = response.body
        data = eval(raw_data)
        record = SportsSetupItem()
        tou_name = data.get('event', '').get('label', '')
        nodes = data.get('matches')
        for node in nodes:
            participants = []
            game_id = node.get('matchId', '')
            game_note = node.get('eventPhase', '')
            stadium = node.get('venue', '').get('name', '')
            city = node.get('venue', '').get('city', '')
            country = node.get('venue', '').get('country', '')
            time = node.get('time', '').get('label', '')
            time = time.split('GMT')[0].strip()
            scores = node.get('scores', '')
            home_final= scores[0]
            away_final = scores[1]
            teams = node.get('teams', '')
            for team in teams:
                par_id = team.get('name', '')
                participants.append(par_id)
            home_sk = participants[0].replace(' ', '_')

            away_sk = participants[1].replace(' ', '_')
            if participants[0] in GAME_NOTE_DICT: 
                home_sk = "tbd1"
                away_sk = "tbd2"
            status = node.get('status', '')
            if status == "U":
                status = "scheduled"
            elif status == "C":
                status = "completed"
            else:
                status = "scheduled"
            if "Stadium" not in stadium:
                stadium  = stadium + " Stadium"
            else:
                stadium  = stadium
            game_date = get_utc_time(time, '%a %d %b %Y, %H:%M', 'GMT')
            event_name = EVENT_DICT.get(game_note, '')
            record['game'] = "rugby"
            record['game_status'] = status
            record['affiliation'] = "irb"
            record['source'] = "espn_rugby"
            record['source_key'] = game_id
            record['game_datetime'] = game_date
            record['participants'] = {home_sk: (1, ''), away_sk : (0, '')}
            record['time_unknown'] = 0
            record['participant_type'] = "team"
            record['event'] = event_name
            record['tz_info'] = get_tzinfo(city = city, game_datetime = game_date)

            record['result'] = {}
            record['reference_url'] = response.url
            record['tournament'] = tou_name.replace('2015', '').strip()
            record['rich_data'] = {'location': {'city': city, "state": '', \
                                "country": country, "stadium": stadium }, \
                                            'game_note': game_note}
            if country == "England":
                country = "United Kingdom"
            if not record['tz_info']:
                record['tz_info'] = get_tzinfo(city = city, country = country, game_datetime = game_date)
            if not record['tz_info']:
                record['tz_info'] = get_tzinfo(country = country, game_datetime = game_date) 

            if status == "scheduled":
                record['result'] = {}
            elif status == "completed":
                if int(home_final) > int(away_final):
                    winner = home_sk
                elif int(home_final) < int(away_final):
                    winner = away_sk
                else:
                    winner = ''
                total_score = str(home_final) + "-" + str(away_final) + " FT"
                record['result'] = {'0': {'score': total_score, 'winner': winner}, \
                                home_sk: {'final': home_final},
                                away_sk: {'final': away_final}}
            import pdb;pdb.set_trace()
            yield record
