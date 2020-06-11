from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import time
import pytz

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', 'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', 
        'NJ' : 'New Jersey', \
        'IL' : 'Illinois', 'AL' : 'Alabama', 'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', 'OR' : 'Oregon',  
        'DE' : 'Delaware', 'IA' : 'Iowa', \
        'WV' : 'West Virgina', 'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', 'LA' : 'Louisiana', 'MO' : 'Missouri', 
        'AR' : 'Arkansas', \
        'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', 'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 
        'ID' : 'Idaho', 'RI' : 'Rhode Island', \
        'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'AZ': 'Arizona', 'CA': 'California', \
        'IN' : 'Indiana', 'DC' : 'District of Columbia', 'CT': 'Connecticut', 'WA': 'Washington'}



def get_event(game_note):
    WNBA_EVENTS = {'CONFERENCE FINALS': 'WNBA Conference Finals', 'CONFERENCE SEMIFINALS': 'WNBA Playoffs', 'WNBA FINALS': 'WNBA Finals', 'WNBA ALL-STAR': 'WNBA All-Star Game'}
    for event in WNBA_EVENTS.keys():
        if event in game_note:
            event_name = WNBA_EVENTS[event]
            break
        else:
            event_name = ''
    return event_name


def get_utc_time(date_val, pattern):
   utc          = pytz.utc
   eastern      = pytz.timezone('US/Eastern')
   fmt          = '%Y-%m-%d %H:%M:%S'
   _date         = datetime.datetime.strptime(date_val, pattern)
   _date_eastern = eastern.localize(_date,is_dst=None)
   _date_utc     = _date_eastern.astimezone(utc)
   utc_date     = _date_utc.strftime(fmt)
   return utc_date

class WnbaGamesSpider(VTVSpider):
    name             = "wnba_games_back"
    allowed_domains = ["espn.go.com"]
    start_urls       = []
    record           = SportsSetupItem()

    def start_requests(self):
        next_week_days = []
        top_url = 'http://scores.espn.go.com/wnba/scoreboard?date=%s'
        if self.spider_type == "schedules":
            for i in range(0, 20):
                next_week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        else:
            for i in range(0, 20):
                next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))
        print next_week_days
        for wday in next_week_days:
            top_urls = top_url %(wday)
            yield Request (top_urls,  callback = self.parse)

    def parse(self,response):
        requests = []
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//div[contains(@id, "gameHeader")]')
        game = "basketball"
        for node in nodes:
            game_id = extract_data(node, './@id')
            game_id = "".join(game_id.split('-')[0])
            game_status = extract_data(node, './/div[@class="game-header"]/div[@class="game-status"]/p/text()')
            if "ET" in game_status:
                status = "scheduled"
            elif "Final" in game_status:
                status = "completed"
            elif "postponed" in game_status.lower():
                status = "postponed"
            else:
                status = "ongoing"

            home_id = extract_data(node, './/div[@class="team home"]//div[@class="team-capsule"]//p[@class="team-name"]//a[contains(@href, "team=")]/@href')
            home_id = home_id.split('team=')[-1]
            home_id = "".join(home_id)
            away_id = extract_data(node, './/div[@class="team visitor"]//div[@class="team-capsule"]//p[@class="team-name"]//a[contains(@href, "team=")]/@href')
            away_id = away_id.split('team=')[-1]
            away_id = "".join(away_id)

            home_name = extract_data(node, './/div[@class="team home"]//div[@class="team-capsule"]//p[@class="team-name"]//text()')
            print home_name
            if "West All-Stars" in home_name:
                home_id = 'WCF'
            elif "East All-Stars" in home_name:
                home_id = 'ECF'

            away_name = extract_data(node, './/div[@class="team visitor"]//div[@class="team-capsule"]//p[@class="team-name"]//text()')
            print away_name
            if "East All-Stars" in away_name:
                away_id = 'ECF'
            elif "West All-Stars"  in away_name:
                away_id = 'WCF'

            home_scores      = get_nodes(node, './/div[@class="team home"]//ul[@class="score"]//li[not(@class)]')
            home_scores      = [extract_data(home_score, './text()')  for home_score in home_scores]
            home_final_score = extract_data(node, './/div[@class="team home"]//ul[@class="score"]//li[@class="final"]//text()')

            away_scores      = get_nodes(node, './/div[@class="team visitor"]//ul[@class="score"]//li[not(@class)]')
            away_scores      = [extract_data(away_score, './text()')  for away_score in away_scores]
            away_final_score = extract_data(node, './/div[@class="team visitor"]//ul[@class="score"]//li[@class="final"]//text()')
            if len(home_scores)>4:
                hm_ot_scores = home_scores[4:]
                home_scores = home_scores[:4]
                print hm_ot_scores
            else:
                home_scores = home_scores[:4]
                hm_ot_scores = ''
            if len(away_scores)>4:
                aw_ot_scores = away_scores[4:]
                away_scores = away_scores[:4]
                print aw_ot_scores
            else:
                away_scores = away_scores[:4]
                aw_ot_scores = ''

            url = extract_data(node, './/div[@class="expand-gameLinks"]//a[contains(text(), "Conversation")]/@href')
            if 'http:' not in url:
               link = 'http://espn.go.com'+ url
            print link

            game_note = extract_data(node, './/div[contains(@class, "game-note")]/text()')
            print game_note

            event_name = get_event(game_note)
            print event_name

            if not home_id or not away_id: continue

            if self.spider_type == "schedules" and status =="scheduled":
                yield Request(link, callback=self.parse_scheduled, meta={'game_id': game_id, 'home_id': home_id, 'away_id': away_id, 'game_status': status, 
                                                                            'game_note': game_note, 'event_name': event_name, 'game': game})
            elif self.spider_type == "scores" and status!="scheduled":
                yield Request(link, callback=self.parse_scores, meta = {'game_id': game_id, 'home_id': home_id, 'away_id': away_id,'game_status': status,
                                                                          'game_note': game_note, 'event_name': event_name, 'game': game,'home_scores': home_scores, 
                                                                          'home_final_score': home_final_score, 'away_scores': away_scores, 
                                                                          'away_final_score': away_final_score, 'hm_ot_scores':hm_ot_scores,'aw_ot_scores':aw_ot_scores})

    def parse_scores(self,response):
        hxs              = HtmlXPathSelector(response)
        record           = SportsSetupItem()
        link             = response.url
        away_final_score = response.meta['away_final_score']
        home_final_score = response.meta['home_final_score']
        home_scores      = response.meta['home_scores']
        away_scores      = response.meta['away_scores']
        game_status      = response.meta['game_status']
        home_id          = response.meta['home_id']
        away_id          = response.meta['away_id']
        game             = response.meta['game']
        game_note        = response.meta['game_note']
        event_name       = response.meta['event_name']
        game_id          = response.meta['game_id']
        aw_ot_scores     = response.meta['aw_ot_scores']
        hm_ot_scores     = response.meta['hm_ot_scores']
        game_datetime    = extract_data(hxs, '//div[@class="game-time-location"]//p[1]//text()')
        game_location    = extract_data(hxs, '//div[@class="game-time-location"]//p[2]//text()')
        stadium          = game_location.split(',')[-3].strip()
        city             = game_location.split(',')[-2].strip()
        state            = game_location.split(',')[-1].strip()
        state            = REPLACE_STATE_DICT.get(state)
        country          = 'USA'
        channel = extract_data(hxs, '//div[@class="game-vitals"]/p[contains(text(),"coverage:")]//strong//text()')
        if not channel:
            channel = extract_data(hxs, '//div[@class="game-vitals"]//div/p[contains(text(),"coverage:")]//strong//text()')
        result_list = [[[0], [], {'quarters': [], 'ot': [], 'winner': ''}], [[1], [], {'quarters': [], 'ot': [], 'winner': ''}]]

        game_datetime = get_utc_time(game_datetime, '%I:%M %p ET, %B %d, %Y')
        print game_datetime

        tournament = "wnba basketball"
        source = 'espn_wnba'
        self.record['result'] = {}
        values = {}
        if game_status =="completed" or game_status =="ongoing":
            _hs, _hs_ot = [], []
            if home_scores:
                for home_score in home_scores:
                    if str(home_score) == '-' :
                        _hs.append('0')
                    else:
                        _hs.append(str(home_score))
            if hm_ot_scores:
                for hm_ot_score in hm_ot_scores:
                    if hm_ot_score and str(hm_ot_score) == '0':
                        _hs_ot.append('0')
                    else:
                        _hs_ot.append(hm_ot_score)
            result_list[0][-1]['quarters'] = _hs
            result_list[0][-1]['ot'] = _hs_ot
            _aw, _aw_ot = [], []
            
            if away_scores:
                for away_score in away_scores:
                    if str(away_score) == '-' :
                        _aw.append('0')
                    else:
                        _aw.append(str(away_score))
            if aw_ot_scores:
                for aw_ot_score in aw_ot_scores:
                    if aw_ot_score and str(aw_ot_score) == '0':
                        _aw_ot.append('0')
                    else:
                        _aw_ot.append(aw_ot_score)

            result_list[1][-1]['quarters'] = _aw
            result_list[1][-1]['ot'] = _aw_ot
            result_list[0][1], result_list[1][1] = [home_final_score], [away_final_score]
            if game_status == "completed":
                if (int(away_final_score) > int(home_final_score)):
                    result_list[1][-1]['winner'] = 1
                    result_list[0][-1]['winner'] = 0
                elif int(away_final_score) < int(home_final_score):
                    result_list[0][-1]['winner'] = 1
                    result_list[1][-1]['winner'] = 0
                else:
                    result_list[0][-1]['winner'] = 0
                    result_list[1][-1]['winner'] = 0
            record['result'] = result_list

            record['affiliation'] = 'wnba'
            record['event'] = event_name
            record['game'] = game
            record['game_datetime'] = game_datetime
            record['game_status'] = game_status
            record['participant_type'] = "team"
            record['participants'] = {away_id:(0, ''), home_id:(1, '')}
            record['rich_data'] = {'channels':channel, 'location':{'city':city, 'country':country, 'stadium':stadium, 'state':state}}
            record['source'] = source
            record['source_key'] = game_id
            record['tournament'] = tournament
            record['reference_url'] = link

            yield record

    def parse_scheduled(self,response):
        record = SportsSetupItem()
        hxs = HtmlXPathSelector(response)
        link = response.url
        game_status = response.meta['game_status']
        game = response.meta['game']
        event_name = response.meta['event_name']
        game_note = response.meta['game_note']
        home_id = response.meta['home_id']
        away_id = response.meta['away_id']
        game_id = response.meta['game_id']
        game_datetime = extract_data(hxs, '//div[@class="game-time-location"]//p[1]/text()')
        game_location = extract_data(hxs, '//div[@class="game-time-location"]//p[2]//text()')
        stadium  = game_location.split(',')[-3].strip()
        city = game_location.split(',')[-2].strip()
        state = game_location.split(',')[-1].strip()
        state = REPLACE_STATE_DICT.get(state)
        country = 'USA'

        channel = extract_data(hxs, '//div[@class="game-vitals"]//p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(hxs, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        game_datetime = get_utc_time(game_datetime, '%I:%M %p ET, %B %d, %Y')
        print game_datetime
        tournament = "WNBA Basketball"
        source = 'espn_wnba'

        record['affiliation'] = 'wnba'
        record['event'] = event_name
        record['game'] = game
        record['game_datetime'] = game_datetime
        record['game_status'] = game_status
        record['participant_type'] = "team"
        record['participants'] = {away_id:(0, ''), home_id:(1, '')}
        record['rich_data'] = {'channels':channel, 'location':{'city':city, 'country':country, 'stadium':stadium, 'state':state} }
        record['tournament'] = tournament
        record['source'] = source
        record['source_key'] = game_id
        record['reference_url'] = link
        
        record['result'] = {}
        yield record


