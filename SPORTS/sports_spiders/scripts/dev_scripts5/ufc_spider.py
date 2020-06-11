from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request, FormRequest
from scrapy_spiders_dev.items import SportsSetupItem
import re
from vtvspider_dev import VTVSpider, extract_data, get_nodes, get_utc_time, log, get_tzinfo

true    = True
false   = False
null    = ''


REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', 'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', 'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

class UfcSpider(VTVSpider):
    name            = "ufcgames"
    start_urls      = []
    record          = SportsSetupItem()

    def start_requests(self):
        if self.spider_type == "schedules":
            top_url = 'http://www.ufc.com/schedule/event'
            yield Request(top_url, callback=self.parse_scheduled, meta = {'type': self.spider_type})
        else:
            top_url = 'http://www.ufc.com/event/Past_Events'
            yield Request(top_url, callback=self.parse_completed, meta = {'type': self.spider_type})

    def parse_scheduled(self, response):
        hxs     = HtmlXPathSelector(response)
        nodes   = get_nodes(hxs, '//div[@id="event_content"]//div//table/tr/td[@class="upcoming-events-image"] \
                                //div[@class="event-title"]/a')
        for node in nodes:
            schedule_link = extract_data(node, './@href')
            event_link = schedule_link + '#oddsContainer'
            if "http:" not in event_link:
                event_link = "http://www.ufc.com" + event_link

            yield FormRequest(event_link, callback=self.parse_game_info, \
                        meta = {'type': self.spider_type, 'schedule_link': schedule_link})

    @log
    def parse_completed(self, response):
        hxs     = HtmlXPathSelector(response)
        nodes   = get_nodes(hxs, '//table[@id="past-events-table"]/tr[@class="event-row"]')
        for node in nodes[:30]:
            date_ = extract_data(node, './/td[@class="event-date"]//text()')
            location = extract_data(node, './/td[3]//text()')
            _link =  extract_data(node, './td[@class="event-title"]/a/@href')
            if _link or not "http:" in _link:
                _link = "http://www.ufc.com" + _link

                yield Request(_link, callback=self.parse_scores, \
                        meta = {'type': self.spider_type, '_date':date_, 'location': location})

    @log
    def parse_game_info(self, response):
        #import pdb; pdb.set_trace()
        hxs     = HtmlXPathSelector(response)
        date    = hxs.select('//div[@id="titleArea"]/text()').extract()
        _date   = date[1].strip()
        location = extract_data(hxs, '//div[@id="subtitleArea"]//span[not(contains(@class, "event-time"))]//text()')

        events_dict = {}
        event_nodes = get_nodes(hxs, '//div[contains(@id, "Module")]//a')
        for event in event_nodes:
            _sk = extract_data(event, './/@data-fight-id')
            event_title = extract_data(event, './/div[@class="title-container"]/div[@class="fight-title"]/text()')
            events_dict[_sk] = event_title

        nodes = get_nodes(hxs, '//div[contains(@id, "card-")]//div[@class="fight-card-container"]//a[@target="_self"]')
        for node in nodes:
            game_sk = extract_data(node, './div/@data-fight-id').strip()
            _event  = events_dict.get(game_sk)
            game_link = "http://www.ufc.com/event/fightDetails/"+game_sk
            time = extract_data(node, './/..//..//preceding-sibling::div[@class="module-title-bar"] \
                                      /div[@class="fight-card-time"]/text()').strip()

            import pdb; pdb.set_trace()
            if "TBA" in time:
                time = time.split('/')[0]
                date_time = _date+ " "+time.replace('ETPT ETPT', 'ETPT')
                game_datetime = get_utc_time(date_time, "%a. %b. %d, %Y TBA ETPT", 'US/Eastern')
            elif "TBD" in time:
                time = time.split('/')[0]
                date_time = _date+ " "+time.replace('ETPT ETPT', 'ETPT')
                game_datetime = get_utc_time(date_time, "%a. %b. %d, %Y TBD ETPT", 'US/Eastern')

            else:
                time = time.split('/')[0]+" "+time.split('/')[-1].split(' ')[-1]
                date_time = _date+ " "+time.replace('ETPT ETPT', 'ETPT')
                if ":" not in time and time != "ETPT ETPT":
                    game_datetime = get_utc_time(date_time, "%a. %b. %d, %Y %I%p ETPT", 'US/Eastern')
                elif time == "ETPT ETPT":
                    game_datetime = get_utc_time(date_time, "%a. %b. %d, %Y ETPT", 'US/Eastern')

                else:
                    game_datetime = get_utc_time(date_time, "%a. %b. %d, %Y %I:%M%p ETPT", 'US/Eastern')

            if game_link:
                yield Request(game_link, callback=self.parse_game_details, meta = \
                            {'type': self.spider_type, 'date_time': game_datetime, \
                                'game_sk': game_sk, 'event': _event, 'location': location})

    @log
    def parse_game_details(self, response):
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        replace_words = ["Bout", "Women", "Vacant", "Title Fight", "5 Round", "Fight Pass"]
        tou_name = "Ultimate Fighting Championship"
        event = response.meta['event']
        for word in replace_words:
            if 'Women' in event:
                event = event.replace(word, "Women's").strip()
            else:
                event = event.replace(word, "").strip()
        event = "UFC - " + event.replace(" Women's's", '')

        home_sk = extract_data(hxs, '//div[@class="stat-section fighter-info"] \
                                /a[@class="fighter-name fighter-left"]/@href').strip()
        home_sk = home_sk.split('/')[-1]
        away_sk = extract_data(hxs, '//div[@class="stat-section fighter-info"] \
                                /a[@class="fighter-name fighter-right"]/@href').strip()
        away_sk = away_sk.split('/')[-1]

        date_time = response.meta['date_time']
        game_sk = response.meta['game_sk']
        location = response.meta['location']
        city = state = ''
        if location:
            city = location.split(',')[0].strip()
            state = location.split(',')[-1].strip()
            state = REPLACE_STATE_DICT.get(state)
            if not state:
                state = state
        ref = response.url
        record['affiliation'] = "ufc"
        record['event'] = event
        record['game'] = "martial arts"
        record['game_datetime'] = date_time
        record['game_status'] = "scheduled"
        record['participant_type'] = "player"
        record['tournament'] = tou_name
        record['source'] = 'ufc'
        record['source_key'] = game_sk
        record['reference_url'] = ref
        record['time_unknown'] = 0
        record['tz_info'] = get_tzinfo(city = city)
        record['participants'] = {home_sk:(1,''), away_sk:(0,'')}
        record['rich_data'] = {'channel':'', 'location':{'stadium':'',
        'city': city, 'state': state, 'country': ''}, 'game_note':''}
        record['result'] = {}
        yield record

    @log
    def parse_scores(self, response):
        hxs = HtmlXPathSelector(response)
        _date = response.meta['_date']
        location = response.meta['location']
        nodes = get_nodes(hxs, '//div[contains(@id, "card-")]//div[@class="fight-card-container"]//a[@target="_self"]')
        for node in nodes:
            game_sk = extract_data(node, './div/@data-fight-id')
            game_stat_id = extract_data(node, './div/@data-fight-stat-id')
            time = extract_data(node, './/..//..//preceding-sibling::div[@class="module-title-bar"] \
                                        /div[@class="fight-card-time"]/text()').strip()
            if "TBD" in time or time == '':
                time = time.split('/')[0]
                date_time = _date
                game_datetime = get_utc_time(date_time, "%b %d %Y", 'US/Eastern')

            else:
                time = time.split('/')[0]+" "+time.split('/')[-1].split(' ')[-1]
                date_time = _date+ " "+time
                if ":" not in time:
                    game_datetime = get_utc_time(date_time, "%b %d %Y %I%p ETPT", 'US/Eastern')

                elif "AM" not in time and "PM" not in time:
                    game_datetime = get_utc_time(date_time, "%b %d %Y %I:%M ETPT", 'US/Eastern')

                else:
                    game_datetime = get_utc_time(date_time, "%b %d %Y %I:%M%p ETPT", 'US/Eastern')

            winner = extract_data(node, './/span[contains(@class, "fighter-name fighter-name")]//span[@class="win"]\
                                       //following-sibling::text()')
            _id = "".join(re.findall('document.refreshURL = \'http://liveapi.fightmetric.com/V1/(.*)\/Fnt.json\';', \
                                    response.body)).strip()
            game_link = "http://liveapi.fightmetric.com/V2/%s/%s/Stats.json" % (_id, game_stat_id)

            if game_link:
                yield Request(game_link, callback=self.parse_final, meta =  \
                            {'type': self.spider_type, 'game_sk': game_sk, \
                            'winner' : winner, 'game_datetime': game_datetime, 'location': location})

    @log
    def parse_final(self, response):
        record = SportsSetupItem()
        game_sk = response.meta["game_sk"]
        winner = response.meta["winner"]
        game_datetime = response.meta['game_datetime']
        location = response.meta['location']
        jsonresponse = eval(response.body)
        event = jsonresponse['FMLiveFeed']['WeightClass']
        home_node = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]
        home_takedown_attempts = home_node["Grappling"]["Takedowns"]["Attempts"]
        home_takedown_landed = home_node["Grappling"]["Takedowns"]["Landed"]

        home_standups_landed = home_node["Grappling"]["Standups"]["Landed"]

        home_control_time = home_node["TIP"]["Control Time"]
        home_knock_downs = home_node["Strikes"]["Knock Down"]["Landed"]

        home_total_strikes = home_node["Strikes"]["Total Strikes"]["Landed"]
        home_total_attempts = home_node["Strikes"]["Total Strikes"]["Attempts"]

        home_significant_srikes = home_node["Strikes"]["Significant Strikes"]["Landed"]
        home_significant_attempts = home_node["Strikes"]["Significant Strikes"]["Attempts"]


        home_submissions_attempts = home_node["Grappling"]["Submissions"]["Attempts"]

        home_sk = jsonresponse["FMLiveFeed"]["Fighters"]["Red"]["Name"].lower().replace(' ', "-")
        away_node = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]
        away_takedown_attempts = away_node["Grappling"]["Takedowns"]["Attempts"]
        away_takedown_landed = away_node["Grappling"]["Takedowns"]["Landed"]

        away_standups_landed = away_node["Grappling"]["Standups"]["Landed"]

        away_control_time = away_node["TIP"]["Control Time"]
        away_knock_downs = away_node["Strikes"]["Knock Down"]["Landed"]

        away_total_strikes = away_node["Strikes"]["Total Strikes"]["Landed"]
        away_total_attempts = away_node["Strikes"]["Total Strikes"]["Attempts"]

        away_significant_srikes = away_node["Strikes"]["Significant Strikes"]["Landed"]
        away_significant_attempts = away_node["Strikes"]["Significant Strikes"]["Attempts"]


        away_submissions_attempts = away_node["Grappling"]["Submissions"]["Attempts"]

        away_sk = jsonresponse["FMLiveFeed"]["Fighters"]["Blue"]["Name"].lower().replace(' ', "-")

        if winner.lower() in away_sk.replace('-', ' '):
            winner = away_sk
            winner = winner
        elif winner.lower() in home_sk.replace('-', ' '):
             winner = home_sk
             winner = winner
        else:
            winner = winner
        city = location.split(',')[0]
        state = location.split(',')[1]
        state = REPLACE_STATE_DICT.get(state) 
        tz_info = get_tzinfo(city = city)

        record['affiliation'] = "ufc"
        record['game'] = "martial arts"
        record['game_status'] = "completed"
        record['game_datetime'] = game_datetime
        record['participant_type'] = "player"
        record['event'] = "UFC - " + event
        record['tournament'] = "Ultimate Fighting Championship"
        record['source'] = "ufc"
        record['source_key'] = game_sk
        record['reference_url'] = response.url
        record['time_unknown'] = 0
        record['tz_info'] = tz_info
        record['participants'] = {home_sk: (1,''), away_sk: (0,'') }
        record['rich_data'] = {'channels':'', 'location':{'stadium':'',
        'city': city, 'state': state}, 'game_node':''}
        record['result'] = {'0': {'score': '', 'winner': winner},
        home_sk:{ 'attempts': home_total_attempts,
                  'controltime': home_control_time, 'knockdowns': home_knock_downs,
                  'significant_attempts': home_significant_attempts,
                  'significant_srikes': home_significant_srikes,
                  'standups': home_standups_landed, 'strikes':home_total_strikes,
                  'submissions': home_submissions_attempts,
                  'takedown_attempts': home_takedown_attempts,
                  'takedown_landed': home_takedown_landed}, away_sk: {'attempts': away_total_attempts,
                  'controltime': away_control_time, 'knockdowns': away_knock_downs,
                  'significant_attempts': away_significant_attempts,
                  'significant_srikes': away_significant_srikes,
                  'standups': away_standups_landed,
                  'strikes': away_total_strikes,
                  'submissions': away_submissions_attempts,
                  'takedown_attempts': away_takedown_attempts,
                  'takedown_landed': away_takedown_landed}}
        yield record

