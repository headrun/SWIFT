import time, datetime
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import extract_data, extract_list_data, get_nodes, get_utc_time
from vtvspider_new import get_tzinfo
from vtvspider_new import VTVSpider

true = True
false = False
null = ''


events = {'basketball': {'Am. East': ('1','America East Conference'), 'A 10': ('3', 'Atlantic 10 Conference'), 'ACC': ('2', 'Atlantic Coast Conference'), 'A-Sun': ('46', 'Atlantic Sun Conference'), 'Big 12': ('8', 'Big 12 Conference'), 'Big East': ('4', "Big East Men's Basketball Tournament"), 'Big Sky': ('5', "Big Sky Conference Men's Basketball Tournament"), 'Big South': ('6', 'Big South Conference'), 'Big Ten': ('7', 'Big Ten Conference'), 'Big West': ('9', 'Big West Conference'), 'CAA': ('10', 'Colonial Athletic Association'), 'C-USA': ('11', 'Conference USA'), 'Independent': ('43', 'Division I Independents'), 'Great West': ('57', 'Great West'), 'Horizon': ('45', 'Horizon League'), 'Ivy': ('12', 'Ivy League'), 'MAAC': ('13', 'Metro Atlantic Athletic Conference'), 'MAC': ('14', 'Mid-American Conference'), 'MEAC': ('16', 'Mid-Eastern Athletic Conference'), 'MVC': ('18', 'Missouri Valley Conference'), 'MWC': ('44', 'Mountain West Conference'), 'NEC': ('19', 'Northeast Conference'), 'OVC': ('20', 'Ohio Valley Conference'), 'Pac-12': ('21', 'Pacific-12 Conference'), 'Patriot': ('22', 'Patriot League'), 'SEC': ('23', 'Southeastern Conference'), 'Southern': ('24', 'Southern Conference'), 'SWAC': ('26', 'Southwestern Athletic Conference'), 'Summit': ('49', 'Summit League'), 'Sun Belt': ('27', 'Sun Belt Conference'), 'WCC': ('29', 'West Coast Conference'), 'WAC': ('30', 'Western Athletic Conference'), 'Southland': ('25', 'Southland Conference'), 'Ncaa Tourney': ('100', "NCAA Men's Basketball Championship"), 'NIT': ('50', "National Invitation Tournament"), 'CBI': ('55', "College Basketball Invitational"), 'CIT': ('56', "CollegeInsider.com Postseason Tournament"), 'AAC':('62', "Atlantic Athletic Conference")}}

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', 'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', 'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

def get_city_state_ctry(venue):
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
        state = ""

    return city, state, country



class NCBSpider(VTVSpider):
    name = "ncb_spider"
    #allowed_domains = ["http://scores.espn.go.com/"]
    start_urls = []


    def start_requests(self):
        next_week_days = []
        urls = []
        #top_urls ='http://scores.espn.go.com/ncb/scoreboard?date=%s&confId=%s'
        top_urls = 'http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?lang=en&region=us&calendartype=blacklist&limit=300&dates=%s&groups=%s&league=mens-college-basketball'
        if self.spider_type =="schedules":
            for i in range(0, 300):
                next_week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        else:
            for i in range(0, 10):
                next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))
        for wday in next_week_days:
            for aka, event in events['basketball'].iteritems():
                t_url = top_urls %(wday, event[0])
                event_name = event[1]
                event_aka = aka

                yield Request(t_url, callback=self.parse, meta = {'event_name': event_name, 'event_aka': event_aka})
    def parse(self, response):
        hxs = Selector(response)
        data_ = eval(response.body)
        record = SportsSetupItem()
        nodes = data_['events']
        event_name = response.meta['event_name']
        for node in nodes:
            game_nodes = node['competitions']

            for game_node in game_nodes:
                game_id = game_node['id']
                if not game_id:
                    continue
                game_date = game_node['date']
                status = game_node['status']['type']['description']
                team_details = game_node['competitors']
                stadium = game_node['venue']['fullName']
                city = game_node['venue']['address']['city']
                state = game_node['venue']['address']['state']
                for team_det in team_details:
                    team_id = team_det['id']
                    type_ = team_det['type']
                    team_type = team_det['homeAway']
                    if team_type == "home" and type_ == "team":
                        home_sk = team_id
                        #home_final_scores = team_det['score']
                        #home_scores = team_det['linescores']
                    if team_type == "away" and type_ == "team":
                        away_sk = team_id
                        #away_final_scores = team_det['score']
                        #away_scores = team_det['linescores']

            date = game_date.replace('T', ' ').replace('Z', ":00")

            if "scheduled" in status.lower() and self.spider_type =="schedules":

                info_link = "http://scores.espn.go.com/ncb/conversation?gameId=%s" % game_id
                yield Request(info_link, callback=self.parse_schedules, meta ={'home_id': home_sk, \
                                'away_id': away_sk, \
                                'game_status': status.lower(), 'game_id': game_id, 'date': date, 'city': city, \
                                'state': state, 'stadium': stadium
                                })

            elif "final" in status.lower() and self.spider_type =="scores":
                game_status = "completed"

                video_link = "http://scores.espn.go.com/ncb/boxscore?gameId=%s" % game_id
                yield Request(video_link, callback=self.parse_scores, meta ={'home_id': home_sk, \
                        'away_id': away_sk,
                        'game_status': game_status, 'game_id': game_id, 'date': date, 'city': city, \
                                'state': state, 'stadium': stadium})

            elif "postponed" in status.lower() or "cancelled" in status.lower():
                if "postponed" in status.lower():
                    game_status = "postponed"
                else:
                    game_status =  "cancelled"
                away_team_score = []
                home_team_score = []
                away_ot_score = []
                home_ot_score = []
                home_final_score = []
                away_final_score = []
                video_link = "http://scores.espn.go.com/ncb/boxscore?gameId=%s" % game_id
                yield Request(video_link, callback=self.parse_scores, meta ={'home_id': home_sk,
                            'away_id': away_sk,
                            'game_status': game_status, 'game_id': game_id, 'date': date, 'city': city, \
                                'state': state, 'stadium': stadium})



    def parse_scores(self, response):

        hxs = Selector(response)
        record = SportsSetupItem()
        game_status = response.meta['game_status']
        game_note = ''
        game_sk = response.meta['game_id']
        home_sk = response.meta['home_id']
        away_sk = response.meta['away_id']
        date = response.meta['date']
        city = response.meta['city']
        state = response.meta['state']
        stadium = response.meta['stadium']
        game_scores = extract_list_data(hxs, '//table[@class="linescore"]//tr//td//text()')
        if not game_scores:
            game_scores = extract_list_data(hxs, '//table[@id="linescore"]//tr//td//text()')
        if game_scores and game_status == "completed":
            if len(game_scores) == 8:
                home_scores = game_scores[5:8]
                away_scores = game_scores[1:4]
                home_final_score = home_scores[-1]
                away_final_score = away_scores[-1]
            else:
                home_scores = game_scores[6:11]
                away_scores = game_scores[1:5]
                home_final_score = home_scores[-1]
                away_final_score = away_scores[-1]

            home_ot = ''
            home_h1 = home_scores[0].strip()
            home_h2 = home_scores[1].strip()
            if len(home_scores) > 3:
                home_ot = home_scores[2].strip()

            away_ot = ''
            away_h1 = away_scores[0].strip()
            away_h2 = away_scores[1].strip()
            if len(away_scores) > 3:
                away_ot = away_scores[2].strip()

            if (int(away_final_score) > int(home_final_score)):
                winner = away_sk
            elif int(away_final_score) < int(home_final_score):
                winner = home_sk
            else:
                winner = ''
            game_score = home_final_score + '-' + away_final_score
            if home_ot:
                game_score = game_score + '(OT)'


        if home_sk == '':
            home_sk = extract_data(hxs, '//div[@class="team home"]/div[@class="team-info"]/h3/a/@href').strip()
            home_sk = home_sk.split('/id/')[-1].split('/')[0]
            if home_sk == '':
                home_sk = extract_data(hxs, '//div[@class="team home"]/div[@class="team-info"]/h3/text()').strip()
        else:
            home_sk = home_sk
        if away_sk == '':
            away_sk = extract_data(hxs, '//div[@class="team away"]/div[@class="team-info"]/h3/a/@href').strip()
            away_sk = away_sk.split('/id/')[-1].split('/')[0]
            if away_sk == '':
                away_sk = extract_data(hxs, '//div[@class="team away"]/div[@class="team-info"]/h3/text()').strip()
        else:
            away_sk = away_sk

        if game_status != "completed":
            videos_link = ''
        else:
            videos_link = response.url
        tournament = "NCAA Men's Division I Basketball"
        if game_note and "MEN'S BASKETBALL CHAMPIONSHIP" in game_note:
            tournament = "NCAA Men's Basketball Championship"
            if "REGION - 1ST ROUND" in game_note:
                event = "NCAA Basketball: The Men's First Round"
            elif "REGION - 2ND ROUND" in game_note:
                event = "NCAA Basketball: The Men's Second Round"
            elif "REGION - 3RD ROUND" in game_note:
                event = "NCAA Basketball: The Men's Third Round"
            elif "SWEET 16" in game_note:
                event = "NCAA Basketball: The Men's Regional Semifinals"
            elif "ELITE 8" in game_note:
                event = "NCAA Basketball: The Men's Regional Finals"
            elif "The Regional Finals" in game_note.title() or "FINAL FOUR" in game_note:
                event = "NCAA Basketball: The Men's National Semifinals"
            elif "NATIONAL CHAMPIONSHIP" in game_note:
                event = "NCAA Basketball: The Men's National Championship"
        else:
            event = ''
        game_time = extract_data(hxs, '//div[@class="game-time-location"]//p[1]//text()').strip()
        if game_time:
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
        else:
            game_datetime = date
            time_unknown = 0

        country = ''
        game_location = extract_data(hxs, '//div[@class="game-time-location"]//p[2]//text()').strip()
        if game_location:
            stadium = game_location.split(',')[0]
            if '(' in stadium:
                stadium = stadium.split('(')[0].strip()
                venue = ",".join(game_location.split(',')[2:]).strip()
            else:
                stadium = stadium
                venue = ",".join(game_location.split(',')[1:]).strip()
            city, state, country = get_city_state_ctry(venue)

        if stadium == "Event Center":
            stadium = "Event Center Arena"
            city, state, country = "San Jose", "California" , "USA"

        if "Memorial Hall (Dover" in stadium:
            stadium = "Memorial Hall (Delaware State)"
            city, state, country = "Dover", "Delaware", "USA"

        channel = extract_data(hxs, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(hxs,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
        channel = channel.replace('/', '<>')

        record['game']       = "basketball"
        record['participant_type'] = "team"
        record['event'] = event
        record['tournament']    = tournament
        record['affiliation']   =  "ncaa-ncb"
        record['source']        =  "espn_ncaa-ncb"
        record['source_key']    = game_sk
        record['game_status']   = game_status
        record['game_datetime'] = game_datetime
        record['time_unknown'] = time_unknown
        record['tz_info'] = get_tzinfo(city = city, game_datetime = game_datetime)

        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(city = city, country = "USA", game_datetime = game_datetime)
        if game_status == "completed":

            record['result'] = {'0': {'winner': winner, 'score': game_score},
                               home_sk: {'H1': home_h1, 'H2': home_h2, 'OT1': home_ot, 'final': home_final_score },
                               away_sk: {'H1': away_h1, 'H2': away_h2, 'OT1': away_ot, 'final': away_final_score}}
        else:
            record['result'] = {}

        record['reference_url'] = response.url
        record['participants'] = {home_sk: (1, ''), away_sk: (0, '')}
        record['rich_data'] = {'channels': str(channel), \
                                'location': { 'city': city, 'state': state, 'country': country}, \
                                'stadium': stadium}
        yield record

    def parse_schedules(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        game_status = response.meta['game_status']
        game_sk = response.meta['game_id']
        home_sk = response.meta['home_id']
        away_sk = response.meta['away_id']
        date = response.meta['date']
        city = response.meta['city']
        state = response.meta['state']
        stadium = response.meta['stadium']
        game_note = ''
        game_time = extract_data(hxs, '//div[@class="game-time-location"]//p[1]//text()').strip()
        if game_time:
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
        else:
            date  = date
            time_unknown = 0

        tournament = "NCAA Men's Division I Basketball"
        if game_note and "MEN'S BASKETBALL CHAMPIONSHIP" in game_note:
            tournament = "NCAA Men's Basketball Championship"
            if "REGION - 1ST ROUND" in game_note:
                event = "NCAA Basketball: The Men's First Round"
            elif "REGION - 2ND ROUND" in game_note:
                event = "NCAA Basketball: The Men's Second Round"
            elif "REGION - 3RD ROUND" in game_note:
                event = "NCAA Basketball: The Men's Third Round"
            elif "SWEET 16" in game_note:
                event = "NCAA Basketball: The Men's Regional Semifinals"
            elif "ELITE 8" in game_note:
                event = "NCAA Basketball: The Men's Regional Finals"
            elif "The Regional Finals" in game_note.title() or "FINAL FOUR" in game_note:
                event = "NCAA Basketball: The Men's National Semifinals"
            elif "NATIONAL CHAMPIONSHIP" in game_note:
                event = "NCAA Basketball: The Men's National Championship"
        else:
            event = ''

        country = ''
        game_location = extract_data(hxs, '//div[@class="game-time-location"]//p[2]//text()').strip()
        if game_location:
            stadium = game_location.split(',')[0]
            venue = ",".join(game_location.split(',')[1:]).strip()
            city, state, country = get_city_state_ctry(venue)

        if stadium == "Event Center":
            stadium = "Event Center Arena"
            city, state, country = "San Jose", "California" , "USA"

        if "Memorial Hall (Dover" in stadium:
            stadium = "Memorial Hall (Delaware State)"
            city, state, country = "Dover", "Delaware", "USA"

        if state == "Japan":
            city = city
            state = ''
            country = "Japan"
        channel = extract_data(hxs, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(hxs,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(hxs,'//div[@class="game-status"]//span[@class="network"]//text()').strip()
        if home_sk == '':
            home_sk = extract_data(hxs, '//div[@class="team home"]/div[@class="team-info"]/h3/a/@href').strip()
            home_sk = home_sk.split('/id/')[-1].split('/')[0]
            if home_sk == '':
                home_sk = extract_data(hxs, '//div[@class="team home"]/div[@class="team-info"]/h3/text()').strip()
        else:
            home_sk = home_sk
        if away_sk == '':
            away_sk = extract_data(hxs, '//div[@class="team away"]/div[@class="team-info"]/h3/a/@href').strip()
            away_sk = away_sk.split('/id/')[-1].split('/')[0]
            if away_sk == '':
                away_sk = extract_data(hxs, '//div[@class="team away"]/div[@class="team-info"]/h3/text()').strip()
        else:
            away_sk = away_sk
        record['tournament']    = tournament
        record['game']          =  "basketball"
        record['participant_type'] = "team"
        record['affiliation']   = "ncaa-ncb"
        record['source'] = "espn_ncaa-ncb"
        record['event'] = event
        record['game_status'] = game_status
        record['source_key'] = game_sk
        record['reference_url'] = response.url
        record['participants'] = { home_sk: (1, ''), away_sk: (0, '')}
        record['game_datetime'] = date
        record['time_unknown'] = time_unknown
        record['tz_info'] = get_tzinfo(city = city, game_datetime = date)
        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(city = city, country = "USA", game_datetime = date)

        record['rich_data'] = {'channels': str(channel), \
                                'location': { 'city': city, 'state': state, 'country': country}, \
                                'stadium': stadium}
        record['result'] = {}
        yield record

