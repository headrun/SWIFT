import time, datetime
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import extract_data, extract_list_data, get_nodes, get_utc_time, get_tzinfo
from vtvspider_new import VTVSpider

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'Ala' : 'Alabama', 'NC' : 'North Carolina', \
'SC' : 'South Carolina', 'GA' : 'Georgia', 'Kan': 'Kansas', 'Ind': 'Indiana', \
'OR' : 'Oregon', 'Ore': 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', \
'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

def get_city_state_ctry(venue):
    if len(venue.split(',')) == 2:
        country = 'USA'
        city  = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        state_ = venue.split(',')[-1].strip().replace('.', '')
        state = REPLACE_STATE_DICT.get(state_)
        if not state:
            state = state_
    elif len(venue.split(',')) == 3:
        city = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        state = venue.split(',')[1].strip().replace('.', '')
        country = 'USA'
    else:
        country = venue.strip()
        if "." in country:
            country = ''
        city, state = '', ''
    if city == "Washington":
        city = "Washington, D.C."
        country = "USA"
        state = ""


    return city, state, country


class NCAASpider(VTVSpider):
    name = "ncaa_spider"
    allowed_domains = ["www.ncaa.com"]

    events_men = {"First Four": "NCAA Basketball: The Men's First Round",
                "First Round": "NCAA Basketball: The Men's First Round",
                "Second Round": "NCAA Basketball: The Men's Second Round",
                "Third Round": "NCAA Basketball: The Men's Third Round",
                "Sweet Sixteen": "NCAA Basketball: The Men's Regional Semifinals",
                "Sweet 16" : "NCAA Basketball: The Men's Regional Semifinals",
                "Elite Eight": "NCAA Basketball: The Men's Regional Finals",
                "Final Four": "NCAA Basketball: The Men's National Semifinals",
                "National Championship": "NCAA Basketball: The Men's National Championship", \
                "Championship": "NCAA Basketball: The Men's National Championship"}

    events_women = {"First Round": "NCAA Basketball: The Women's First Round",
                "Second Round": "NCAA Basketball: The Women's Second Round",
                "Third Round" : "NCAA Basketball: The Women's Regional Semifinals",
                "Quarterfinals": "NCAA Basketball: The Women's Regional Finals",
                "Semifinals": "NCAA Basketball: The Women's National Semifinals",
                "Championship": "NCAA Basketball: The Women's National Championship"}

    start_urls = []

    def start_requests(self):
        top_urls = {}
        print top_urls
        ncaa_dict = { 'women': 'http://www.ncaa.com/scoreboard/basketball-women/d1/%s'}
                      #'men': 'http://www.ncaa.com/scoreboard/basketball-men/d1/%s' }

        now = datetime.datetime.now()
        if self.spider_type == "schedules":
            for gender, reference in ncaa_dict.iteritems():
                for i in range(0, 300):
                    game_date = (now + datetime.timedelta(days=i)).strftime('%Y/%m/%d')
                    top_urls[reference % game_date] = gender

        elif self.spider_type == "scores":
            for gender, reference in ncaa_dict.iteritems():
                for i in range(0, 7):
                    game_date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
                    top_urls[reference % game_date] = gender

        for url, gender in top_urls.iteritems():
            yield Request(url, self.parse, meta = {'gender' : gender})


    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        gender = response.meta['gender']

        game_date = "".join(hxs.select('//div[@class="day-wrapper"]/h2/text()').extract()).strip()

        if not game_date:
            return
        game_date = game_date.split(',', 1)[1].strip().replace('\n', ' ')

        nodes = get_nodes(hxs, '//div[@class="game-contents"]')
        for node in nodes:
            team_names = {}
            record = SportsSetupItem()
            channel = ''
            channels = items = []
            record['rich_data'] = {}

            game_id = ''
            game_id = extract_data(node, './/ul/li/a[@class="gamecenter"]/@href')
            if "/game/basketball-women/d1/2014/12/16/-southern-utah" in game_id:
                continue
            if "/game/basketball-men/d1/2014/12/22/-eastern-wash" in game_id:
                continue
            stat_url = "http://www.ncaa.com" + game_id

            event = extract_data(node, './/div[@class="game-championship"]/text()')
            if "Sweet 16" in event:
                event = "Sweet 16"
            if "Elite Eight" in event:
                event = "Elite Eight"
            if "FINAL FOUR" in event:
                event = "Final Four"
            if "Championship" in event:
                event = "Championship"
            if gender == 'men':
                tournament = "NCAA Men's Division I Basketball"
                record['tournament'] = tournament
                event = self.events_men.get(event, '')
                if event:
                    record['tournament'] = "NCAA Men's Basketball Championship"
                record['source'] = 'ncaa_ncb'
                record['event'] = event
                record['affiliation'] = "ncaa-ncb"

            elif gender == 'women':
                tournament = "NCAA Women's Division I Basketball"
                record['tournament'] = tournament
                record['source'] = 'ncaa_ncw'
                event = self.events_women.get(event, '')
                if event:
                    record['tournament'] = "NCAA Women's Division I Basketball Championship"
                record['event'] = event
                record['affiliation'] = "ncaa-ncw"
            else:
                event = ''

            away_sk = extract_data(node, './/table[@class="linescore"]//tr[2]//td/div[@class="team"]/a/@href')
            if "/schools/" in away_sk:
                away_sk = away_sk.split('/')[-1]
                away_team = away_sk.replace('-', ' ')
            else:
                away_sk = extract_data(node, './/table[@class="linescore"]//tr[2]//td/div[@class="team"]//text()')

            if not away_sk:
                away_team = ""
                away_sk   = ""

            home_sk = extract_data(node, './/table[@class="linescore"]//tr[3]//td/div[@class="team"]/a/@href')
            if "/schools/" in home_sk:
                home_sk   = home_sk .split('/schools/')[-1]
                home_team = home_sk.replace('-', ' ')
            else:
                home_sk = extract_data(node, './/table[@class="linescore"]//tr[3]//td/div[@class="team"]//text()')

            if not home_sk:
                home_team = ""
                home_sk   = ""

            result = {}
            game_time  =  extract_data(node, './/div[contains(@class, "game-status")]/text()').lower()

            record['game_status'] = 'completed'
            if "tbd" in game_time or "et" in game_time or "tba" in game_time:
                record['game_status'] = 'scheduled'
            elif "postponed" in game_time:
                record['game_status'] = 'postponed'
            elif "cancelled" in game_time:
                record['game_status'] = 'cancelled'

            if "et" in game_time:
                game_time = '%s %s' % (game_date, ' '.join(game_time.split(' ')[:2]))
                pattern = '%B %d, %Y %I:%M %p'
            else:
                game_time = game_date
                pattern = '%B %d, %Y'
            date = get_utc_time(game_time, pattern, 'US/Eastern')
            record['game_datetime'] = date

            record['result'] = {}
            record['game'] = 'basketball'
            record['participants'] = {home_sk:(1, ''), away_sk: (0, '')}
            record['participant_type'] = 'team'
            record['rich_data']['channels'] = channels
            record['source_key'] = game_id
            record['event'] = event
            record['time_unknown'] = '0'

            if not "tbd" in game_time or "tba" not in game_time:
                if record['game_status'] == "completed" and self.spider_type == "scores":
                    yield Request(stat_url, callback=self.parse_scores, meta={'record':record, 'date': date})
                elif record['game_status'] == 'scheduled' and self.spider_type == "schedules":
                    yield Request(stat_url, callback=self.handle_schedule_locations, meta={'record':record, 'date': date})

    def handle_schedule_locations(self, response):
        hxs = HtmlXPathSelector(response)
        record  = response.meta['record']
        items = []
        date = response.meta['date']
        game_location  = extract_data(hxs, '//p[@class="location"][contains(text(), ",")]/text()')
        if not game_location:
            game_location  = extract_data(hxs, '//div[@class="round-location"]//div[@class="location"]//text()')
        stadium = game_location.split(',')[0]
        if '(' in stadium:
            stadium = stadium.split('(')[0].strip()
            venue = ",".join(game_location.split(',')[2:]).strip()
        else:
            stadium = stadium
            venue = ",".join(game_location.split(',')[1:]).strip()
        if venue == ",":
            city = state = country = ''
        else:
            city, state, country = get_city_state_ctry(venue)

        stadium = stadium.replace('&amp;', '&')

        record['rich_data'] = {'location':{'city':city, 'state':state, \
        'country': country}, 'stadium': stadium}
        record['reference_url'] = response.url
        tz_info = get_tzinfo(city = city, game_datetime = date)
        if not tz_info:
            tz_info = get_tzinfo(city = "New York", game_datetime = date)
        record['tz_info'] = tz_info

        sportssetupitem = SportsSetupItem()
        for k, v in record.iteritems():
            sportssetupitem[k] = v

        items.append(sportssetupitem)

        for item in items:
            yield item

    def parse_scores(self, response):
        hxs = HtmlXPathSelector(response)
        record  = response.meta['record']
        items  = []
        date = response.meta['date']
        nodes = get_nodes(hxs,'//div[@class="line-score-container"]//table[@id="linescore"]')
        for node in nodes:
            away_sk = extract_data(node, './/tr[2]//td[@class="school"]//a//@href')
            if not away_sk :
                away_sk = extract_data(node, './/tr[2]//td[@class="school"]//text()')
            else:
                away_sk = away_sk.replace('/basketball-women', '').replace('/schools', '').replace('/basketball-men', '').replace('/', '').strip()

            home_sk = extract_data(node, './/tr[3]//td[@class="school"]//a//@href')
            if not home_sk:
                home_sk = extract_data(node, './/tr[3]//td[@class="school"]//text()')
            if not home_sk:
                home_sk = extract_data(node, './/tr[1]//td[@class="school"]//a//@href')
                home_sk = home_sk.replace('/basketball-women', '').replace('/schools', '').replace('/basketball-men', '').replace('/', '').strip()
                if not home_sk:
                    home_sk = extract_data(node, './/tr[1]//td[@class="school"]//text()')
            else:
                home_sk = home_sk.replace('/basketball-women', '').replace('/schools', '').replace('/basketball-men', '').replace('/', '').strip()
            if not home_sk:
                home_sk = ''
            if not away_sk:
                away_sk = ''
            away_scores = extract_list_data(node, './/tr[2]//td[@class="period"]//text()')
            home_scores = extract_list_data(node, './/tr[3]//td[@class="period"]//text()')
            if not home_scores:
                home_scores = extract_list_data(node, './/tr[1]//td[@class="period"]//text()')
            away_total_score = extract_data(node, './/tr[2]//td[@class="score"]//text()')
            home_total_score = extract_data(node, './/tr[3]//td[@class="score"]//text()')
            if not home_total_score:
                home_total_score = extract_data(node, './/tr[1]//td[@class="score"]//text()')
            home_ot =  home_ot2 = home_h1 = home_h2 = ''
            away_ot = away_ot2 = away_h1 =  away_h2 = ''
            if home_scores:
                home_h1 = home_scores[0]
                home_h2 = home_scores[1]
                if len(home_scores) > 2:
                    home_ot = home_scores[2]
                if len(home_scores) == 4:
                    home_ot2 = home_scores[3]
            if away_scores:
                away_h1 = away_scores[0]
                away_h2 = away_scores[1]
                if len(away_scores) > 2:
                    away_ot = away_scores[2]
                if len(away_scores) == 4:
                    away_ot2 = away_scores[3]
            if (int(away_total_score) > int(home_total_score)):
                winner = away_sk
            elif int(away_total_score) < int(home_total_score):
                winner = home_sk
            else:
                winner = ''

            game_location = extract_data(hxs, '//p[@class="location"][contains(text(), ",")]/text()')
            if not game_location:
                game_location  = extract_data(hxs, '//div[@class="round-location"]//div[@class="location"]//text()')

            stadium = game_location.split(',')[0]
            if '(' in stadium:
                stadium = stadium.split('(')[0].strip()
                venue = ",".join(game_location.split(',')[2:]).strip()
            else:
                stadium = stadium
                venue = ",".join(game_location.split(',')[1:]).strip()
            if venue == ",":
                city = state = country = ''
            else:
                city, state, country = get_city_state_ctry(venue)

            if "Arena " in stadium:
                city = stadium.split('Arena')[-1].strip()
                stadium = stadium.split('Arena')[0] + "Arena"
                state_ = game_location.split(',')[-1].strip()
                state = REPLACE_STATE_DICT.get(state_)
                if not state:
                    state = state_
                country = "USA"
            if "Center " in stadium:
                city = stadium.split('Center')[-1].strip()
                stadium = stadium.split('Center')[0] + "Center"
                state_ = game_location.split(',')[-1].strip()
                state = REPLACE_STATE_DICT.get(state_)
                if not state:
                    state = state_
                country ="USA"
            if city == "Washington":
                city = "Washington, D.C."
                country = "USA"
                state = ""


            stadium = stadium.replace('&amp;', '&')
            if "Lucas Oil Stadium Indianapolis" in stadium:
                stadium = "Lucas Oil Stadium"
            game_score = home_total_score + '-' + away_total_score
            if home_ot:
                game_score = game_score + '(OT)'
            record['result'] = {'0': {'winner': winner, 'score': game_score},
                               home_sk: {'H1': home_h1, 'H2': home_h2, 'OT1': home_ot, 'OT2': home_ot2, 'final': home_total_score },
                               away_sk: {'H1': away_h1, 'H2': away_h2, 'OT1': away_ot, 'OT2': away_ot2, 'final': away_total_score}}
            record['reference_url'] = response.url
            record['participants'] = {home_sk: (1, ''), away_sk: (0, '')}
            record['rich_data'] = {'location': {'city':city, 'state':state, \
            'country': country}, 'stadium': stadium}
            tz_info = get_tzinfo(city = city, game_datetime = date)
            if not tz_info:
                tz_info = get_tzinfo(city= "New York", game_datetime = date)
            record['tz_info'] = tz_info
            sportssetupitem = SportsSetupItem()
            for k, v in record.iteritems():
                sportssetupitem[k] = v
            items.append(sportssetupitem)
            for item in items:
                return item
