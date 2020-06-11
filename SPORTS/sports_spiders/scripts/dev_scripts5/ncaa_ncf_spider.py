from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data, get_utc_time

events = {'football': {'ACC': ('1', 'ACC Championship Game'), 'Big 12': ('4', 'Big 12 Championship'), 'Big East': ('10', 'Big East Conference'), \
                        'C-USA': ('12', 'Conference USA'), 'Big Ten': ('5', 'Big Ten Conference'), 'IA Indep.': ('18', 'IA Independents'), \
                        'MAC ': ('15', 'MAC Championship Game'), 'MWC': ('17', 'Mountain West Conference Football Championship Game'), \
                        'Pac-12': ('9', 'Pacific-12 Football Championship Game'), \
                        'SEC': ('8', 'SEC Championship Game'), 'Sun Belt': ('37', 'Sun Belt Conference'), 'WAC': ('16', 'Western Athletic Conference'), \
                        'Big Sky': ('20', 'Big Sky Conference'), 'Big South': ('40', 'Big South Conference'), 'CAA': ('48', 'Colonial Athletic Association'), \
                        'Great West': ('43', 'Great West Conference'), 'IAA Indep.': ('32', 'IAA Independents'), 'Ivy': ('22', 'Ivy League'), \
                        'MEAC': ('24', 'Mid-Eastern Athletic Conference'), 'MVC': ('21', 'Missouri Valley Football Conference'), \
                        'NEC': ('25', 'Northeast Conference'), 'OVC': ('26', 'Ohio Valley Conference'), 'Patriot': ('27', 'Patriot League'), \
                        'Pioneer': ('28', 'Pioneer Football League'), 'Southern': ('29', 'Southern Conference'), 'Southland': ('30', 'Southland Conference'), \
                        'SWAC': ('31', 'Southwestern Athletic Conference'), 'Div II/III': ('35' , 'NCAA Football Division II/III'), \
                        'AAC': ('151', 'Atlantic Athletic Conference')}}

BOWL_TOURNAMENTS = ["Poinsettia Bowl", "Super Bowl", "Pro Bowl", "New Mexico Bowl", "Idaho Potato Bowl ", "New Orleans Bowl", "Beef 'O' Brady's Bowl",
                    "Maaco Bowl Las Vegas", "Hawaii Bowl", "Independence Bowl", "Little Caesars Pizza Bowl", "Belk Bowl", "Military Bowl", "Holiday Bowl",
                    "Russell Athletic Bowl", "Alamo Bowl", "Armed Forces Bowl", "Pinstripe Bowl", "Music city Bowl", "Buffalo Wild Wings Bowl",
                    "Meineke Car Care Bowl", "Sun Bowl",
                    "Hunger Bowl", "Liberty Bowl", "Chick-fil-a Bowl", "Heart Of Dallas Bowl", "Capital one Bowl", "Outback Bowl", "Gator Bowl",
                    "Rose Bowl", "Fiesta Bowl", "Sugar Bowl", "Orange Bowl", "Cotton Bowl", "Bbva Compass Bowl", "Godaddy.com Bowl", "BCS National Championship Game" ]

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', 'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', 'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

MAIN_LINK = "http://scores.espn.go.com/ncf/scoreboard"

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
        city, state = '', ''

    return city, state, country

class NcfSpider(VTVSpider):
    name = "ncf_spider"
    allowed_domains = ["espn.go.com", "scores.espn.go.com"]
    start_urls = []
    def start_requests(self):
        top_url = 'http://scores.espn.go.com/ncf/scoreboard?confId=%s&seasonYear=2014&seasonType=2'

        for aka, event in events['football'].iteritems():
            top_urls = top_url % (event[0])
            event_name = event[1]
            yield Request(top_urls, callback = self.parse, meta = {'event_name': event_name})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        event_name = response.meta['event_name']
        game_weeks = get_nodes(hxs, '//div[@class="week"]//a')
        for week in game_weeks:
            week_link = MAIN_LINK + extract_data(week, './@href')
            yield Request(week_link, callback = self.parse_week, meta = {'event_name': event_name})

    def parse_week(self, response):
        hxs = HtmlXPathSelector(response)
        game = "football"
        affiliation = "ncaa-ncf"
        event_name = response.meta['event_name']
        game_nodes = get_nodes(hxs, '//div[@class="gameDay-Container"]//div[contains(@id, "gameContainer")]')
        for game_node in game_nodes:

            game_id = extract_data(game_node, './/div[@class="mod-content"]/span[@class="sort"]//text()').strip()
            if not game_id:
                continue

            home_team_id = extract_data(game_node, './/div[@class="team home"]//div[@class="team-capsule"]/p[@class="team-name"]/span/a/@href').strip()
            home_team_id  = home_team_id.split('/id/')[-1].split('/')[0]
            away_type = extract_data(game_node, './/div[@class="team visitor"]/div[@class="team-capsule"]//p[@class="record"]').replace('\n', '').replace('\r', '').strip()
            away_type = away_type.split(' ', 2)[-1].strip(')')
            away_team_id  = extract_data(game_node, './/div[@class="team visitor"]//div[@class="team-capsule"]/p[@class="team-name"]/span/a/@href').strip()
            away_team_id = away_team_id.split('/id/')[-1].split('/')[0]
            home_type = extract_data(game_node, './/div[@class="team home"]/div[@class="team-capsule"]//p[@class="record"]').strip()
            home_type = home_type.split(' ', 2)[-1].strip(')').strip()
            about_game = extract_data(game_node, './/div[@class="game-notes"]//text()').strip()

            status = extract_data(game_node, './/div[@class="game-status"]//text()').strip()

            if "final" in status.lower():
                if not self.spider_type == "scores" :
                    continue
                game_status = "completed"
                home_scores = extract_list_data(game_node, './/div[@class="team home"]//ul[@class="score"]//li[contains(@id, "Score")]//text()')
                home_final_score = extract_data(game_node, './/div[@class="team home"]//ul[@class="score"]//li[contains(@class, "final")]//text()')
                away_scores      = extract_list_data(game_node, './/div[@class="team visitor"]//ul[@class="score"]//li[contains(@id, "Score")]//text()')
                away_final_score = extract_data(game_node, './/div[@class="team visitor"]//ul[@class="score"]//li[contains(@class, "final")]//text()')
                home_team_score = home_scores[:4]
                away_team_score = away_scores[:4]
                video_link = "http://scores.espn.go.com/ncf/video?gameId=%s" % game_id
                yield Request(video_link, callback = self.parse_video, meta = { 'home_id': home_team_id, 'away_id': away_team_id, 'home_score': home_team_score, \
                              'away_score': away_team_score, 'game_status': game_status, 'game_id': game_id, 'about_game': about_game, \
                              'home_final_score': home_final_score, 'away_final_score': away_final_score, 'event_name': event_name})

            elif "postponed" in status.lower():
                game_status = "Hole"
                away_team_score = []
                home_team_score = []

                video_link = "http://scores.espn.go.com/ncf/video?gameId=%s" % game_id

                yield Request(video_link, callback = self.parse_video, meta = { 'home_id': home_team_id, 'away_id': away_team_id,
                'home_score': home_team_score, 'away_score': away_team_score, \
                'game_status': game_status, 'game_id': game_id, 'about_game': about_game, \
                'event_name': event_name})

            elif "et" in status.lower() or "TBD" in status:
                if not self.spider_type == "schedules" :
                    continue
                game_status = "scheduled"
                info_link = "http://scores.espn.go.com/ncf/conversation?gameId=%s" % game_id
                yield Request(info_link, self.parse_schedules, meta={ 'home_id': home_team_id, 'away_id': away_team_id, 'game': game, \
                                      'affiliation': affiliation, 'game_status': game_status, 'game_id': game_id, \
                                      'about_game': about_game, 'event_name': event_name})

        next_pages = extract_data(hxs, '//div[@class="week"]//a[@class=" selected"]/following-sibling::a/@href')

        if next_pages:
            next_page = next_pages[0]
            next_page = "http://scores.espn.go.com/ncf/scoreboard"+next_page
            yield Request (next_page, callback = self.parse, meta = { 'event_name': event_name})

    def parse_video(self, response):

        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        game_note = response.meta['about_game']
        game_status = response.meta['game_status']
        home_scores = response.meta['home_score']
        away_scores = response.meta['away_score']
        game_sk = response.meta['game_id']
        home_sk = response.meta['home_id']
        away_sk = response.meta['away_id']
        home_final_score = response.meta['home_final_score']
        away_final_score = response.meta['away_final_score']
        event_name = response.meta['event_name']
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
        tournament = 'NCAA College Football'

        if game_note:
            bowl_event  = [i.strip() for i in BOWL_TOURNAMENTS if i.strip().lower() in game_note.lower()]
            if bowl_event:
                event = bowl_event[0]
            else:
                event = event_name
        else:
            event = event_name
        game_time = extract_data(hxs, '//div[@class="game-time-location"]//p[1]//text()').strip()
        if "TBA" in game_time :
            game_time = game_time.split('TBA,')[-1].strip()
            pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
        elif "TBD" in game_time :
            game_time = game_time.split('TBD,')[-1].strip()
            pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
        else:
            pattern = '%I:%M %p ET, %B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')

        game_location = extract_data(hxs, '//div[@class="game-time-location"]//p[2]//text()').strip()
        stadium = game_location.split(',')[0]
        if '(' in stadium:
            stadium = stadium.split('(')[0].strip()
            venue = ",".join(game_location.split(',')[2:]).strip()
        else:
            stadium = stadium
            venue = ",".join(game_location.split(',')[1:]).strip()
        city, state, country = get_city_state_ctry(venue)
        channel = extract_data(hxs, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(hxs,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()

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
        if game_status == "completed":
            if (int(away_final_score) > int(home_final_score)):
                winner = away_sk
            elif int(away_final_score) < int(home_final_score):
                winner = home_sk
            else:
                winner = ''

        game_score = home_final_score + '-' + away_final_score
        if home_ot:
            game_score = game_score + '(OT)'

        record['game']       = "football"
        record['participant_type'] = "team"
        record['event'] = event
        record['tournament']    = tournament
        record['affiliation']   =  "ncaa-ncf"
        record['source']        =  "espn_ncaa-ncf"
        record['source_key']    = game_sk
        record['game_status']   = game_status
        record['participants']  = {home_sk:(1, ''), away_sk:(0, '')}
        record['game_datetime'] = date
        record['reference_url'] = response.url
        record['rich_data']     = {'channels': str(channel), 'location': { 'city': city, 'state': state, 'country': country}, 'stadium': stadium, 'game_note': game_note}
        record['result'] = {'0': { 'winner': winner, 'score': game_score}, home_sk: { 'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3, \
                                    'Q4': home_q4, 'OT1': home_ot, 'final': home_final_score}, away_sk: { 'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3, 'Q4': away_q4, \
                                    'OT1': away_ot, 'final': away_final_score}}
        yield record

    def parse_schedules(self, response):

        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        game_note = response.meta['about_game']
        game_status = response.meta['game_status']
        game_sk = response.meta['game_id']
        home_sk = response.meta['home_id']
        away_sk = response.meta['away_id']
        event_name = response.meta['event_name']
        tournament = 'NCAA College Football'

        if game_note:
            bowl_event  = [i.strip() for i in BOWL_TOURNAMENTS if i.strip().lower() in game_note.lower()]
            if bowl_event:
                event = bowl_event[0]
            else:
                event = event_name
        else:
            event = event_name
        src = "espn_ncaa-ncf"
        game_time = extract_data(hxs, '//div[@class="game-time-location"]//p[1]//text()').strip()

        if "TBA" in game_time :
            game_time = game_time.split('TBA,')[-1].strip()
            pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
        elif "TBD" in game_time :
            game_time = game_time.split('TBD,')[-1].strip()
            pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
        else:
            pattern = '%I:%M %p ET, %B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')

        game_location = extract_data(hxs, '//div[@class="game-time-location"]//p[2]//text()').strip()
        stadium = game_location.split(',')[0]
        venue = ",".join(game_location.split(',')[1:]).strip()
        city, state, country = get_city_state_ctry(venue)
        channel = extract_data(hxs, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()

        if not channel:
            channel = extract_data(hxs,'//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
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
        record['game']          =  "football"
        record['participant_type'] = "team"
        record['affiliation']   = "ncaa-ncf"
        record['source'] = src
        record['event'] = event
        record['game_status'] = game_status
        record['source_key'] = game_sk
        record['reference_url'] = response.url
        record['participants'] = { home_sk: (1, ''), away_sk: (0, '')}
        record['game_datetime'] = date
        record['rich_data'] = {'channels': str(channel), 'location': { 'city': city, 'state': state, 'country': country}, 'stadium': stadium, 'game_note': game_note}
        record['result'] = {}
        yield record
