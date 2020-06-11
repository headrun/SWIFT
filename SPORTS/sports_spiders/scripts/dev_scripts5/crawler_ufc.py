import time
import datetime
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request, FormRequest
#from ufc_games.items import UfcGamesItem
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider import VTVSpider
import re
import pytz
from vtvspider import get_nodes, extract_data, extract_list_data

true    = True
false   = False
null    = ''



class UfcGamesCrawler(VTVSpider):
    ''' Class main '''
    name            = "ufcgames"
    allowed_domains = ["ufc.com", "fightmetric.com"]
    start_urls      = []
    scores_outfile  = open('UFC_SCORES', 'w+')
    schedules_outfile = open('UFC_SCHEDULES', 'w+')
    cookies         = {"end_user_id" : "oeu1394875134747r0.5665066712148423"}

    def start_requests(self):
        req = []
        if self.spider_type == "scheduled":
            top_url = 'http://www.ufc.com/schedule/event'
            yield Request(top_url, callback=self.parse_scheduled, meta = {'type': self.spider_type})
        else:
            top_url = 'http://www.ufc.com/event/Past_Events'
            yield Request(top_url, callback=self.parse_completed, meta = {'type': self.spider_type})

    def parse_scheduled(self, response):
        hxs     = HtmlXPathSelector(response)
        nodes   = get_nodes(hxs, '//div[@id="event_content"]//div//table/tr/td[@class="upcoming-events-image"]//div[@class="event-title"]/a')
        for node in nodes[:1]:
            schedule_link = extract_data(node, './@href') 
            event_link = schedule_link + '#oddsContainer'
            if "http:" not in event_link:
                event_link = "http://www.ufc.com" + event_link

            yield FormRequest(event_link, callback=self.parse_game_info, cookies=self.cookies, meta = {'type': self.spider_type, 'schedule_link': schedule_link})

    def parse_completed(self, response):
        hxs     = HtmlXPathSelector(response)
        nodes   = get_nodes(hxs, '//table[@id="past-events-table"]/tr[@class="event-row"]/td[@class="event-title"]')
        for node in nodes[:1]:
            _link =  extract_data(node, './a/@href')
            if _link or not "http:" in _link:
                _link = "http://www.ufc.com" + _link
                #winner = "".join(node.select('./div//span[contains(@class, "fighter-name fighter-name")]/span[@class="win"]').extract()).strip()
                yield Request(_link, callback=self.parse_scores, meta = {'type': self.spider_type})


    def parse_game_info(self, response):
        hxs     = HtmlXPathSelector(response)
        cur_year = datetime.datetime.now().year
        date    = hxs.select('//div[@id="titleArea"]/text()').extract()
        _date   = date[1].strip() +" " + str(cur_year)

        events_dict = {}
        event_nodes = get_nodes(hxs, '//a[@class="fight"]')
        for event in event_nodes:
            _sk = extract_data(event, './@data-fight-id')
            event_title = extract_data(event, './/div[@class="title-container"]/div[@class="fight-title"]/text()')
            events_dict[_sk] = event_title

        nodes = hxs.select('//div[contains(@id, "card-")]//div[@class="fight-card-container"]//a[@target="_self"]')
        for node in nodes:
            game_sk = "".join(node.select('./div/@data-fight-id').extract()).strip()
            _event  = events_dict.get(game_sk)
            game_link = "http://www.ufc.com/event/fightDetails/"+game_sk
            time = "".join(node.select('.//..//..//preceding-sibling::div[@class="module-title-bar"]/div[@class="fight-card-time"]/text()').extract()).strip()
            if "TBD" in time:
                time = time.split('/')[0]
                date_time = _date
                game_time = datetime.datetime.strptime(date_time, "%a. %b. %d %Y")
                utc = pytz.utc
                eastern=pytz.timezone('US/Eastern')
                date_eastern=eastern.localize(game_time,is_dst=None)
                date_utc = date_eastern.astimezone(utc)
                game_datetime = date_utc.strftime("%Y-%m-%d %H:%M:%S")

            else:
                time = time.split('/')[0]+" "+time.split('/')[-1].split(' ')[-1]
                date_time = _date+ " "+time
                if ":" not in time:
                    game_time = datetime.datetime.strptime(date_time, "%a. %b. %d %Y %H%p ETPT")
                    utc = pytz.utc
                    eastern=pytz.timezone('US/Eastern')
                    date_eastern=eastern.localize(game_time,is_dst=None)
                    date_utc = date_eastern.astimezone(utc)
                    game_datetime = date_utc.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    game_time = datetime.datetime.strptime(date_time, "%a. %b. %d %Y %H:%M%p ETPT")
                    utc = pytz.utc
                    eastern=pytz.timezone('US/Eastern')
                    date_eastern=eastern.localize(game_time,is_dst=None)
                    date_utc = date_eastern.astimezone(utc)
                    game_datetime = date_utc.strftime("%Y-%m-%d %H:%M:%S")

            if game_link:
                yield Request(game_link, callback=self.parse_game_details, meta = {'type': self.spider_type, 'date_time': game_datetime, 'game_sk': game_sk, 'event': _event})

    def parse_game_details(self, response):
        replace_words = ["Bout", "Women", "Vacant", "Title Fight", "5 Round", "Fight Pass"]
        record = {}
        tou_name = "Ultimate Fighting Championship"
        hxs = HtmlXPathSelector(response)
        #event = "".join(hxs.select('//div[@class="module-title-bar"]/text()').extract()).strip().lower()
        event = response.meta['event']
        print event
        for word in replace_words:
            if 'Women' in event:
                #import pdb; pdb.set_trace()
                event = event.replace(word, "Women's").strip()
            else:
                event = event.replace(word, "").strip()
        event = "UFC - " + event.replace(" Women's's", '')

        home_sk = "".join(hxs.select('//div[@class="stat-section fighter-info"]/a[@class="fighter-name fighter-left"]/@href').extract()).strip()
        home_sk = home_sk.split('/')[-1]
        away_sk = "".join(hxs.select('//div[@class="stat-section fighter-info"]/a[@class="fighter-name fighter-right"]/@href').extract()).strip()
        away_sk = away_sk.split('/')[-1]

        date_time = response.meta['date_time']
        game_sk = response.meta['game_sk']
        ref = response.url
        record['home_id'] = home_sk
        record['away_id'] = away_sk
        record['game_date'] = date_time
        record['tou_name'] = tou_name
        record['stadium'] = ''
        record['channel'] = ''
        record['reference'] = ref
        record['location'] = ''
        record['event_name']= event
        record['game_sk'] = game_sk
        record['game'] = 'martial arts'
        record['status'] = 'scheduled'
        record['source'] = 'ufc'
        self.schedules_outfile.write('%s\n' %(repr(record)))

    def parse_scores(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//div[contains(@id, "card-")]//div[@class="fight-card-container"]//a[@target="_self"]')
        for node in nodes:
            game_sk = extract_data(node, './div/@data-fight-id')
            game_stat_id = extract_data(node, './div/@data-fight-stat-id')
            winner = extract_data(node, './/span[contains(@class, "fighter-name fighter-name")]//span[@class="win"]\
                        //following-sibling::text()')
            _id = "".join(re.findall('document.refreshURL = \'http://liveapi.fightmetric.com/V1/(.*)\/Fnt.json\';', response.body)).strip()
            game_link = "http://liveapi.fightmetric.com/V2/%s/%s/Stats.json" % (_id, game_stat_id)

            if game_link:
                yield Request(game_link, callback=self.parse_final, meta = {'type': self.spider_type, 'game_sk': game_sk, 'winner' : winner})

    def parse_final(self, response):
        hxs = HtmlXPathSelector(response)
        game_sk = response.meta["game_sk"]
        winner = response.meta["winner"]
        print "response", response.url
        record = {}
        home_scores = {}
        away_scores = {}
        jsonresponse = eval(response.body)
        
        home_takedown_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Grappling"]["Takedowns"]["Attempts"]
        home_takedown_landed = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Grappling"]["Takedowns"]["Landed"]

        home_standups_landed = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Grappling"]["Standups"]["Landed"]

        home_control_time = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["TIP"]["Control Time"]
        home_knock_downs = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Strikes"]["Knock Down"]["Landed"]

        home_total_strikes = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Strikes"]["Total Strikes"]["Landed"]
        home_total_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Strikes"]["Total Strikes"]["Attempts"]

        home_significant_srikes = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Strikes"]["Significant Strikes"]["Landed"]
        home_significant_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Strikes"]["Significant Strikes"]["Attempts"]


        home_submissions_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Red"]["Grappling"]["Submissions"]["Attempts"]

        home_sk = jsonresponse["FMLiveFeed"]["Fighters"]["Red"]["Name"].lower().replace(' ', "-")
        
        away_takedown_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Grappling"]["Takedowns"]["Attempts"]
        away_takedown_landed = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Grappling"]["Takedowns"]["Landed"]

        away_standups_landed = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Grappling"]["Standups"]["Landed"]

        away_control_time = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["TIP"]["Control Time"]
        away_knock_downs = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Strikes"]["Knock Down"]["Landed"]

        away_total_strikes = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Strikes"]["Total Strikes"]["Landed"]
        away_total_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Strikes"]["Total Strikes"]["Attempts"]

        away_significant_srikes = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Strikes"]["Significant Strikes"]["Landed"]
        away_significant_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Strikes"]["Significant Strikes"]["Attempts"]


        away_submissions_attempts = jsonresponse["FMLiveFeed"]["FightStats"]["Blue"]["Grappling"]["Submissions"]["Attempts"]

        away_sk = jsonresponse["FMLiveFeed"]["Fighters"]["Blue"]["Name"].lower().replace(' ', "-")

        if winner.lower() in away_sk.replace('-', ' '):
            winner = away_sk
            record['winner'] = winner
        elif winner.lower() in home_sk.replace('-', ' '):
            winner = home_sk
            record['winner'] = winner
        else:
            winner = winner
            record['winner'] = winner


        away_scores["takedown_landed"] = away_takedown_landed
        home_scores["takedown_landed"] = home_takedown_landed
        home_scores["takedown_attempts"] = home_takedown_attempts
        away_scores["takedown_attempts"] = away_takedown_attempts
        away_scores["standups"] = away_standups_landed
        home_scores["standups"] = home_standups_landed
        home_scores["controltime"] = home_control_time
        away_scores["controltime"] = away_control_time
        away_scores["submissions"] = away_submissions_attempts
        home_scores["submissions"] = home_submissions_attempts
        home_scores["knockdowns"] = home_knock_downs
        away_scores["knockdowns"] = away_knock_downs
        away_scores["strikes"] = away_total_strikes
        home_scores["strikes"] = home_total_strikes
        away_scores["significant_srikes"] = away_significant_srikes
        home_scores["significant_srikes"] = home_significant_srikes
        away_scores["attempts"] = away_total_attempts
        home_scores["attempts"] = home_total_attempts
        home_scores["significant_attempts"] = home_significant_attempts
        away_scores["significant_attempts"] = away_significant_attempts
        #away_scores["final"] = away_final
        #home_scores["final"] = home_final
        record["game_sk"] = game_sk
        record["status"] = "completed"
        record["reference"] = response.url
        record["home_id"] = home_sk
        record["away_id"] = away_sk
        record["event_name"] = ''
        record["tou_name"] = "Ultimate Fighting Championship"
        record["game"] = "martial arts"
        record["location"] = ''
        record["stadium"] = ''
        ##record["total_score"] = total_score
        record["away_scores"] = away_scores
        record["home_scores"] = home_scores
        record["source"] = "ufc"
        print "record>>", record
        self.scores_outfile.write('%s\n' %(repr(record)))
