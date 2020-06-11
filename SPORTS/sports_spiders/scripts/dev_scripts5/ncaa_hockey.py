import datetime
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, \
extract_data, extract_list_data, get_nodes, get_utc_time
from vtvspider_new import get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


REPLACE_STATE_DICT = {'Mass.' : 'Massachusetts', \
                      'R.I.': 'Rhode Island', \
                      'Ind.': 'Indiana', \
                      'N.H.': 'New Hampshire', \
                      'N.D.': 'North Dakota'}

def get_location(venue):
    if len(venue.split(',')) == 2:
        country = 'USA'
        city  = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        state_ = venue.split(',')[-1].strip()
        state = REPLACE_STATE_DICT.get(state_)
        if not state:
            state = state_
    elif len(venue.split(',')) == 3:
        city = venue.split(',')[0].strip()
        city = city[0].upper() + city[1:].lower()
        state = venue.split(',')[1].strip()
        country = 'USA'
    else:
        country = venue.strip()
        city, state = '', ''

    return city, state, country

class NCAAHockey(VTVSpider):
    name = "ncaa_hockey"
    allowed_domains = []
    start_urls = []

    def start_requests(self):
        top_urls = {}
        ncaa_dict = { 'men': 'http://www.ncaa.com/scoreboard/icehockey-men/d1/%s' }
        now = datetime.datetime.now()
        if self.spider_type == "schedules":
            for gender, reference in ncaa_dict.iteritems():
                for i in range(0, 10):

                    game_date = (now + datetime.timedelta(days=i)).strftime('%Y/%m/%d')
                    top_urls[reference % game_date] = gender

        elif self.spider_type == "scores":
            for gender, reference in ncaa_dict.iteritems():
                for i in range(0, 150):
                    game_date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
                    print game_date
                    top_urls[reference % game_date] = gender

        for url, gender in top_urls.iteritems():
            yield Request(url, self.parse, meta = {'gender' : gender})


    def parse(self, response):
        import pdb;pdb.set_trace()
        hxs = Selector(response)
        game_date = "".join(hxs.select('//div[@class="day-wrapper"]/h2/text()').extract()).strip()

        if not game_date:
            return
        game_date = game_date.split(',', 1)[1].strip().replace('\n', ' ')

        nodes = get_nodes(hxs, '//div[@class="game-contents"]')
        for node in nodes:
            record = SportsSetupItem()
            record['rich_data'] = {}
            game_id = extract_data(node, './/ul/li/a[@class="gamecenter"]/@href')
            stat_url = "http://www.ncaa.com" + game_id
            away_sk = extract_data(node, './/table[@class="linescore"]//tr[2]//td/div[@class="team"]/a/@href').split('/schools/')[-1]
            away_team = away_sk.replace('-', ' ')
            if not away_team:
                away_team = "TBD"
                away_sk   = "tbd2"

            home_sk = extract_data(node, './/table[@class="linescore"]//tr[3]//td/div[@class="team"]/a/@href').split('/schools/')[-1]
            home_team = home_sk.replace('-', ' ')

            if not home_team:
                home_team = "TBD"
                home_sk   = "tbd1"

            game_time  =  extract_data(node, './/div[contains(@class, "game-status")]/text()').lower()
            game_note = extract_data(node, './/div[@class="game-championship"]/text()')
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
            record['game'] = 'hockey'
            record['participants'] = {home_sk:(1, ''), away_sk: (0, '')}
            record['participant_type'] = 'team'
            record['source_key'] = game_id
            record['event'] = ""
            record['source'] = "ncaa_nch"
            record['rich_data']['game_note'] = game_note
            record['time_unknown'] = '1'
            record['affiliation'] = "ncaa-nch"
            record['tournament'] = "NCAA College Hockey"
            today_date = str(datetime.datetime.utcnow())
            if today_date > date and home_sk == "tbd1":
                record['game_status'] = "Hole"
            if today_date > date and away_sk == "tbd2":
                record['game_status'] = "Hole"


            if not "tbd" in game_time or "tba" not in game_time:
                if record['game_status'] == "completed" and \
                    self.spider_type == "scores":
                    yield Request(stat_url, callback = self.parse_scores, \
                                    meta={'record':record, 'date': date})
                elif record['game_status'] == 'scheduled' and \
                self.spider_type == "schedules":
                    yield record

    def parse_scores(self, response):
        hxs = Selector(response)
        record  = response.meta['record']
        date = response.meta['date']
        nodes = get_nodes(hxs,'//div[contains(@class, "line-score-container")]//table[@id="linescore"]')
        for node in nodes:
            home_sk = extract_data(node, './/tr[2]//td[@class="school"]//a//@href')
            if not home_sk :
                home_sk = extract_data(node, './/tr[2]//td[@class="school"]//text()')
            else:
                home_sk = home_sk.replace('icehockey-men', ''). \
                    replace('/schools', '').replace('/', '').strip()

            away_sk = extract_data(node, './/tr[3]//td[@class="school"]//a//@href')
            if not away_sk:
                away_sk = extract_data(node, './/tr[3]//td[@class="school"]//text()')
            if not away_sk:
                away_sk = extract_data(node, './/tr[1]//td[@class="school"]//a//@href')
                away_sk = away_sk.replace('/icehockey-men', ''). \
                    replace('/schools', '').replace('/', '').strip()
            else:
                away_sk = away_sk.replace('/schools', ''). \
                replace('/icehockey-men', '').replace('/', '').strip()
            if not home_sk:
                home_sk = 'tbd1'
            if not away_sk:
                away_sk = 'tbd2'
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
                city, state, country = get_location((venue))

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
                country = "USA"
            if "Garden " in stadium:
                city = stadium.split('Garden')[-1].strip()
                stadium = stadium.split('Garden')[0] + "Garden"
                state_ = game_location.split(',')[-1].strip()
                state = REPLACE_STATE_DICT.get(state_)
                if not state:
                    state = state_
                country = "USA"


            home_scores = extract_list_data(node, './/tr[2]//td[@class="period"]//text()')
            away_scores = extract_list_data(node, './/tr[3]//td[@class="period"]//text()')
            if not away_scores:
                away_scores = extract_list_data(node, './/tr[1]//td[@class="period"]//text()')
            home_total_score = extract_data(node, './/tr[2]//td[@class="score"]//text()')
            away_total_score = extract_data(node, './/tr[3]//td[@class="score"]//text()')
            if not away_total_score:
                away_total_score = extract_data(node, './/tr[1]//td[@class="score"]//text()')
            home_ot =  home_ot2 = home_p1 = home_p2 = home_p3 = ''
            away_ot = away_ot2 =  away_p1 =  away_p2 =  away_p3 = ''
            if home_scores and len(home_scores) !=2:
                home_p1 = home_scores[0]
                home_p2 = home_scores[1]
                home_p3 = home_scores[2]
                if len(home_scores) > 3:
                    home_ot = home_scores[3]
                if len(home_scores) == 5:
                    home_ot2 = home_scores[4]
            if away_scores and len(away_scores) !=2:
                away_p1 = away_scores[0]
                away_p2 = away_scores[1]
                away_p3 = away_scores[2]
                if len(away_scores) > 3:
                    away_ot = away_scores[3]
                if len(away_scores) ==5:
                    away_ot2 = away_scores[4]
            if home_scores and len(home_scores) ==2:
                home_p1 = home_scores[0]
                home_p2 = home_scores[1]

            if away_scores and len(away_scores) == 2:
                away_p1 = away_scores[0]
                away_p2 = away_scores[1]

            if (int(away_total_score) > int(home_total_score)):
                winner = away_sk
            elif int(away_total_score) < int(home_total_score):
                winner = home_sk
            else:
                winner = ''
            game_score = home_total_score + '-' + away_total_score
            if home_ot:
                game_score = game_score + '(OT)'
            record['rich_data']['location'] = {'city':city, 'state':state, \
            'country': country}
            record['rich_data']['stadium'] =  stadium
            tz_info = get_tzinfo(city = city, game_datetime = date)
            record['tz_info'] = tz_info


            record['result'] = {'0': {'winner': winner, 'score': game_score},
                               home_sk: {'P1': home_p1, 'P2': home_p2, \
                               'P3': home_p3, \
                               'OT': home_ot, 'OT2': home_ot2, \
                               'final': home_total_score },
                               away_sk: {'P1': away_p1, 'P2': away_p2, \
                               'P3': away_p3, \
                               'OT': away_ot, 'OT2': away_ot2, \
                                'final': away_total_score}}
            record['reference_url'] = response.url
            record['participants'] = {home_sk: (1, ''), away_sk: (0, '')}
            yield record
 
