from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
import re
import datetime

DOMAIN = "http://en.espn.co.uk"

STADIUM_CITY_DICT = {'la rochelle' : ['Stade Marcel-Deflandre', 'Europe', 'France', '', 'La Rochelle'], \
                    'bordeaux begles' : ['Stade Chaban-Delmas', 'Europe', 'France', 'Gironde', 'Bordeaux'], \
                'bayonne': ['Stade Jean Dauger', 'Europe', 'France', 'Aquitaine', 'Bayonne'], \
                'grenoble': ['Stade des Alpes', 'Europe', 'France', '', 'Grenoble'], \
                'oyonnax': ['Stade Charles-Mathon', 'Europe', 'France', '', 'Oyonnax'], \
                'toulouse': ['Stade Ernest-Wallon', 'Europe', 'France', 'Midi-Pyrenees', 'Toulouse'], \
                'racing metro': ['Stade Olympique Yves-du-Manoir', '', 'France', '', 'Colombes'], \
                'castres': ['Stade Pierre-Antoine', 'Europe', 'France', '', 'Castres'], \
                'clermont auvergne': ['Parc des Sports Marcel Michelin', '', 'France', '', 'Clermont-Ferrand'], \
                'montpellier': ['Altrad Stadium', 'Europe', 'France', 'Languedoc-Roussillon', 'Montpellier'], \
                'toulon': ['Stade Mayol', '', 'France', '', 'Toulon'], \
                'brive': ['Stade Amedee-Domenech', 'Europe', 'France', '', 'Brive'], \
                'lyon': ['Matmut Stadium', 'Europe', 'France', 'Rhone-Alpes', 'Lyon'], \
                'stade francais' : ['Stade Jean-Bouin', '', 'France', '', 'Paris'], \
                'newcastle falcons': ['Kingston Park', '', 'England', '', 'Newcastle'],\
                'sale sharks': ['AJ Bell Stadium', '', 'England', '', 'Salford'],\
                'gloucester rugby': ['Kingsholm Stadium', '', 'England', '', 'Gloucester'], \
                'harlequins' : ['Twickenham Stoop', '', 'England', '', 'London'], \
                'bath rugby' : ['Recreation Ground', '', 'England', '', 'Bath'], \
                'london irish': ['Madejski Stadium', '', 'England', '', 'Reading'], \
                'northampton saints': ["Franklin's Gardens", '', 'England', '', 'Northampton'], \
                'london welsh': ['Kassam Stadium', '', 'England', '', 'Oxford'], \
                'leicester tigers': ['Welford Road Stadium', '', 'England', '', 'Leicester'], \
                'saracens': ['Barnet Copthall', '', 'England', '', 'London'], \
                'wasps': ['Ricoh Arena', '', 'England', '', 'Coventry'], \
                'exeter chiefs': ['Sandy Park', '', 'England', '', 'Exeter'], \
                'benetton treviso': ['Stadio Comunale di Monigo', '', 'Italy', '', 'Treviso'], \
                'edinburgh': ['Murrayfield', '', 'Scotland', '', 'Edinburgh'], \
                'leinster': ['Royal Dublin Society', '', 'Ireland', '', 'Dublin'], \
                'scarlets': ['Parc y Scarlets', '', 'Wales', '', 'Llanelli'], \
                'glasgow': ['Scotstoun Stadium', '', 'Scotland', '', 'Glasgow'], \
                'dragons': ['Rodney Parade', '', 'Wales', 'South Wales', 'Newport'], \
                'ospreys': ['Liberty Stadium', '', 'Wales', '', 'Swansea'], \
                'ulster': ['Kingspan Stadium', '', 'Northern Ireland', '', 'Belfast'], \
                'munster': ['Musgrave Park, Cork', '', 'Ireland', '', 'Cork'], \
                'cardiff blues': ['Cardiff Arms Park', '', 'Wales', '', 'Cardiff'], \
                'zebre': ['Stadio XXV Aprile', '', 'Italy', '', 'Parma'], \
                'connacht': ['Galway Sportsground', '', 'Ireland', '', 'Galway'], \
                'Pau': ['Stade du Hameau', '', 'France', '', 'Pau']}



class RugbyEuropeSpider(VTVSpider):
    name = "rugbyeurope_spider"
    #allowed_domains = ["espnscrum.com"]
    start_urls = []
    record = SportsSetupItem()

    def start_requests(self):
        fixtures = []
        top_url = "http://www.espn.co.uk/rugby/schedule/_/league/all/date/%s"
        now = datetime.datetime.now()
        if self.spider_type == "schedules":
            for i in range(0, 300):
                game_date = (now + datetime.timedelta(days=i)).strftime('%Y%m%d')
                fixtures.append(top_url % game_date)
        else:
            for i in range(0, 100):
                game_date = (now - datetime.timedelta(days=i)).strftime('%Y%m%d')
                fixtures.append(top_url % game_date)

        for fixer in fixtures:
            yield Request(fixer, callback = self.parse_details, \
                    meta = {})


    def parse_details(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        if self.spider_type == "schedules":
            nodes = get_nodes(hxs, '//div[@id="sched-container"]//table')
            for node in nodes:
                tou_name = extract_data(node, './/caption//text()').strip()
                game_nodes = get_nodes(node, './/tbody//tr')
                for game_node in game_nodes:
                    date = extract_data(game_node, './/td[@data-behavior="date_time"]//@data-date').strip()
                    team_links = extract_list_data(game_node, './/td//a[@class="team-name"]//@href')
                    team_names = extract_list_data(game_node, './/td//a[@class="team-name"]//text()')

                    if tou_name not in ['Top 14 Orange', 'Aviva Premiership', 'Guinness PRO12', 'European Rugby Champions Cup']:
                        continue

                    home_sk = team_names[0].lower().strip().encode('utf-8')
                    away_sk = team_names[3].lower().strip().encode('utf-8')
                    if "racing 92" in home_sk:
                        home_sk = "racing metro"
                    if "racing 92" in away_sk:
                        away_sk = "racing metro"

                    team_status = extract_data(game_node, './/td//span[@class="record"]//text()')
                    if "v" in team_status:
                        game_status = "scheduled"
                    else:
                        game_status = "completed"
                    game_link = extract_data(game_node, './/td//span[@class="record"]//a//@href')
                    game_time = date.replace('T', ' ').replace('Z', '')
                    pattern = '%Y-%m-%d %H:%M'
                    import pdb;pdb.set_trace()
                    game_datetime = get_utc_time(game_time, pattern, 'GMT')
                    game_sk = game_datetime.split(' ')[0].replace('-', '_') + "_" + home_sk.replace(' ', '_') + "_" +   away_sk.replace(' ', '_')
                    location_details = extract_list_data(game_node, './/td//text()')
                    city = state = country = stadium = ''
                    if len(location_details) == 8:
                        location = location_details[-1]
                    if len(location_details) == 9:
                        location = location_details[-2]

                    if len(location.split(',')) == 2:
                        stadium = location.split(',')[0].strip()
                        city    = location.split(',')[1].strip()
                    elif len(location.split(',')) == 3:
                        stadium = location.split(',')[0]
                        city = location.split(',')[1].strip()
                        country = location.split(',')[2].strip()
                    else:
                        stadium = location.split(',')[0].strip()
                    if country == "Paris":
                        country = "France"
                        city = "Paris"


                    tz_info = get_tzinfo(city = city, game_datetime=game_datetime)
                    if not tz_info:
                        tz_info = get_tzinfo(country = country, game_datetime=game_datetime)
                    record['tz_info'] = tz_info

                    record['affiliation'] = 'irb'
                    record['game_datetime'] = game_datetime
                    record['game'] = 'rugby union'
                    record['source'] = 'espn_rugby'
                    record['game_status'] = game_status
                    if 'Top 14 Orange' in tou_name:
                        tou_name = "France Top 14"
                        record['tournament'] = tou_name
                    elif 'Aviva Premiership' in tou_name:
                        tou_name = 'English Premiership (rugby union)'
                        record['tournament'] = tou_name
                    elif 'Guinness PRO12' in tou_name:
                        record['tournament'] = 'Guinness Pro12'
                    elif 'Rugby Champions Cup' in tou_name:
                        record['tournament'] = "European Rugby Champions Cup"
                    record['event'] = ''
                    record['participant_type'] = "team"
                    record['source_key'] = game_sk
                    record['time_unknown'] = '0'
                    record['reference_url'] = response.url
                    record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
                    record['rich_data'] = {'location': {'city': city, 'country': country, \
                                           'state': state, \
                                           'stadium': stadium}}
                    if self.spider_type == "schedules" and game_status == "scheduled":
                        record['result'] = {}
                        yield record
        else:
            nodes = get_nodes(hxs, '//div[@id="sched-container"]//table')
            for node in nodes:
                tou_name = extract_data(node, './/caption//text()').strip()
                game_nodes = get_nodes(node, './/tbody//tr')
                for game_node in game_nodes:

                    link = extract_data(game_node, './/span[@class="record"]//a//@href')
                    link_id = link.split('gameId=')[-1].split('&')[0]
                    location_details = extract_list_data(game_node, './/td//text()')
                    team_names = extract_list_data(game_node, './/td//a[@class="team-name"]//text()')
                    home_sk = team_names[0].lower().strip().encode('utf-8')
                    away_sk = team_names[3].lower().strip().encode('utf-8')

                    score_link = "http://www.espnscrum.com/scrum/rugby/current/match/" + link_id + ".html?view=matchscore"
                    if tou_name in ['Top 14 Orange', 'Aviva Premiership', 'Guinness PRO12', 'European Rugby Champions Cup']:
                        yield Request(score_link, callback = self.parse_scores,  \
                        meta = {'tou_name': tou_name, 'location_details': location_details, \
                        'home_sk': home_sk, 'away_sk': away_sk})

    def parse_scores(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        tou_name = response.meta['tou_name']
        location_details = response.meta['location_details']
        node = extract_data(hxs, '//td[@class="liveSubNavText"]//text()')
        if len(node.split(','))==4:
            _city = node.split(',')[0].strip()
            if "-" in _city:
                _city = node.split(',')[0].strip().split('- ')[1].strip()
            else:
                _city = ''
            date = node.split(',')[1].strip()
            time = node.split(',')[3].strip()
            date_time = date + " " + time
        else:
            _city = node.split(',')[1].strip().split('- ')[1].strip()

            date = node.split(',')[2].strip()
            time = node.split(',')[4].strip()
            date_time = date + " " + time
        if time:
            pattern =  '%d %B %Y %H:%M GMT'
        else:
            pattern = '%d %B %Y '
        _date = get_utc_time(date_time, pattern, 'GMT')
        scores = extract_data(hxs, '//td[@class="liveSubNavText1"]//text()')
        if "Postponed" in scores:
            game_status = "postponed"
            hm_scores = aw_scores = away_half_time_goals = home_half_time_goals = home_final = away_final = '0'
            home_sk =  scores.split('v')[0].strip().lower()
            away_sk =  scores.split('v')[1].strip().lower().replace('(postponed)', '').strip()
        elif "Postponed" not in scores and '-' not in scores:
            game_status = "completed"
            hm_scores = aw_scores = away_half_time_goals = home_half_time_goals = home_final = away_final = '0'
            home_sk = response.meta['home_sk']
            away_sk = response.meta['away_sk']
        else:
            game_status = 'completed'
            hm_scores =  scores.split(' - ')[0]
            aw_scores = scores.split(' - ')[1]
            home_half_time_goals = re.findall('.*(\(\d+)\)', hm_scores)[0].replace('(', '')
            away_half_time_goals = re.findall('\d+ (\(\d+)\).*', aw_scores)[0].replace('(', '')
            home_final  = re.findall('.* (\d+)', hm_scores)[0]
            away_final =  re.findall('(\d+) .*', aw_scores)[0] 
            home_sk = re.findall('(.*) \(\d+\) \d+', hm_scores)[0].strip().lower().encode('utf-8')
            home_sk = home_sk.replace('\xc3\xa7', '')
            away_sk  = re.findall('.*\) (.* \(FT)\)', aw_scores)[0].split('(FT')[0].strip().lower().encode('utf-8')
            away_sk  = away_sk.replace('\xc3\xa7', '')
        game_sk = _date.split(' ')[0].replace('-', '_') + "_" +  \
        home_sk.replace(' ', '_').encode('utf-8') + "_" +  \
        away_sk.replace(' ', '_').encode('utf-8')
        game_sk = game_sk.replace('\xc3\xa7', '')
        tou_name = extract_data(hxs, '//td[@class="liveSubNavText"]//text()').split('-')[0].strip()

        city = state = country = stadium = ''
        if len(location_details) == 8:
            location = location_details[-1]
        if len(location_details) == 9 or len(location_details) == 10:
            location = location_details[-2]

        if len(location.split(',')) == 2:
            stadium = location.split(',')[0].strip()
            city    = location.split(',')[1].strip()
        elif len(location.split(',')) == 3:
            stadium = location.split(',')[0]
            city = location.split(',')[1].strip()
            country = location.split(',')[2].strip()
        else:
            stadium = location.split(',')[0].strip()
        if country == "Paris":
            country = "France"
            city = "Paris"

        if "Sixways" in stadium:
            stadium = "Sixways Stadium"

        if int(home_final) > int(away_final):
            winner = home_sk
        elif int(home_final) < int(away_final):
            winner = away_sk
        else:
            winner = ''

        totla_score = home_final + "-" + away_final + " FT"
        tz_info = get_tzinfo(city = city, game_datetime = _date)
        record['tz_info'] = tz_info
        if not tz_info:
            if country == "England" or country == "Wales":
                tz_info = get_tzinfo(country = "United Kingdom", game_datetime = _date)
                record['tz_info'] = tz_info
            else:
                tz_info = get_tzinfo(country = country, game_datetime = _date)
                record['tz_info'] = tz_info
        if country == "Wales":
            tz_info = get_tzinfo(country = "United Kingdom", game_datetime = _date)
            record['tz_info'] = tz_info


        record['affiliation'] = "irb"
        record['event'] = ''
        record['game'] = "rugby union"
        record['game_datetime'] = _date
        record['game_status'] = game_status
        record['participant_type'] = "team"
        record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
        record['reference_url'] = response.url
        record['source'] = "espn_rugby"
        record['source_key'] = game_sk
        record['time_unknown'] = '0'
        if 'Top 14 Orange' in tou_name:
            tou_name = "France Top 14"
            record['tournament'] = tou_name
        elif 'Aviva Premiership' in tou_name:
            tou_name = 'English Premiership (rugby union)'
            record['tournament'] = tou_name
        elif 'Guinness PRO12' in tou_name:
            record['tournament'] = 'Guinness PRO12'
        elif 'Rugby Champions Cup' in tou_name:
            record['tournament'] = "European Rugby Champions Cup"
        record['rich_data'] = { 'channels':'', 'location': {'city': city, \
                                'country': country, 'stadium': stadium, \
                                'state': state}}
        record['result'] = {'0': {'score': totla_score, 'winner': winner}, \
                                home_sk: {'H1': home_half_time_goals, \
                                'final': home_final},
                                away_sk: {'H1': away_half_time_goals, \
                               'final': away_final}}
        yield record

