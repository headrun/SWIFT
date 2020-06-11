from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, get_nodes, get_utc_time
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import datetime

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                      'TX' : 'Texas', 'OK' : 'Oklahoma', 'NJ' : 'New Jersey', \
                      'IL' : 'Illinois', 'AL' : 'Alabama', 'NY' : 'New York', \
                      'NC' : 'North Carolina', 'GA' : 'Georgia', \
                      'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                      'WV' : 'West Virgina', 'KS' : 'Kansas', 'UT' : 'Utah', \
                      'LA' : 'Louisiana', 'ID' : 'Idaho', 'MD' : 'Maryland', \
                      'MO' : 'Missouri',  'AR' : 'Arkansas', \
                      'SD' : 'South Dakota', 'MS' : 'Mississippi', \
                      'MI' : 'Michigan', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                      'RI' : 'Rhode Island', 'NM' : 'New Mexico', \
                      'MN' : 'Minnesota', 'PA' : 'Pennsylvania', \
                      'AZ': 'Arizona', 'CA': 'California', 'IN' : 'Indiana', \
                      'DC' : 'District of Columbia', 'CT': 'Connecticut', \
                      'WA': 'Washington', 'SC' : 'South Carolina', \
                      'FL' : 'Florida'}

def get_event(game_note):
    wnba_events = {'CONFERENCE FINALS': 'WNBA Conference Finals', \
                   'CONFERENCE SEMIFINALS': 'WNBA Playoffs', \
                   'WNBA FINALS': 'WNBA Finals', \
                   'WNBA ALL-STAR': 'WNBA All-Star Game'}

    for event in wnba_events.keys():
        if event in game_note:
            event_name = wnba_events[event]
            break
        else:
            event_name = ''
    return event_name

class WnbaSpider(VTVSpider):

    name             = "wnba_games"
    start_urls       = []
    record           = SportsSetupItem()

    def start_requests(self):
        next_week_days = []
        top_url = 'http://scores.espn.go.com/wnba/scoreboard?date=%s'
        if self.spider_type == "schedules":
            for i in range(0, 30):
                next_week_days.append((datetime.datetime.now() + \
                datetime.timedelta(days=i)).strftime('%Y%m%d'))
        else:
            for i in range(0, 5):
                next_week_days.append((datetime.datetime.now() - \
                datetime.timedelta(days=i)).strftime('%Y%m%d'))

        for wday in next_week_days:
            top_urls = top_url % (wday)
            yield Request (top_urls,  callback = self.parse)

    def parse(self, response):

        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//div[contains(@id, "gameHeader")]')
        game = "basketball"

        for node in nodes:
            game_id = extract_data(node, './@id')
            game_id = "".join(game_id.split('-')[0])
            game_status = extract_data(node, './/div[@class="game-header"] \
                                       /div[@class="game-status"]/p/text()')
            if "ET" in game_status:
                status = "scheduled"
            elif "Final" in game_status:
                status = "completed"
            elif "postponed" in game_status.lower():
                status = "postponed"
            else:
                status = "ongoing"

            home_id = extract_data(node, './/div[@class="team home"] \
                                  //div[@class="team-capsule"] \
                                  //p[@class="team-name"] \
                                  //a[contains(@href, "team=")]/@href')

            home_id = home_id.split('team=')[-1]
            home_id = "".join(home_id)
            away_id = extract_data(node, './/div[@class="team visitor"] \
                                  //div[@class="team-capsule"] \
                                  //p[@class="team-name"] \
                                  //a[contains(@href, "team=")]/@href')

            away_id = away_id.split('team=')[-1]
            away_id = "".join(away_id)

            home_name = extract_data(node, './/div[@class="team home"] \
                                    //div[@class="team-capsule"] \
                                   //p[@class="team-name"]//text()')

            if "West All-Stars" in home_name:
                home_id = 'WCF'
            elif "East All-Stars" in home_name:
                home_id = 'ECF'

            away_name = extract_data(node, './/div[@class="team visitor"] \
                                     //div[@class="team-capsule"] \
                                     //p[@class="team-name"]//text()')
            if "East All-Stars" in away_name:
                away_id = 'ECF'
            elif "West All-Stars"  in away_name:
                away_id = 'WCF'

            home_scores      = get_nodes(node, './/div[@class="team home"] \
                                        //ul[@class="score"]//li[not(@class)]')
            home_scores      = [extract_data(home_score, './text()')  \
                               for home_score in home_scores]
            home_final_score = extract_data(node, './/div[@class="team home"] \
                                           //ul[@class="score"] \
                                           //li[@class="final"]//text()')

            away_scores      = get_nodes(node, './/div[@class="team visitor"] \
                                        //ul[@class="score"]//li[not(@class)]')
            away_scores      = [extract_data(away_score, './text()')  \
                               for away_score in away_scores]
            away_final_score = extract_data(node, \
                                           './/div[@class="team visitor"] \
                                          //ul[@class="score"] \
                                          //li[@class="final"]//text()')


            url = extract_data(node, './/div[@class="expand-gameLinks"]// \
                               a[contains(text(), "Conversation")]/@href')
            if 'http:' not in url:
                link = 'http://espn.go.com'+ url

            game_note = extract_data(node, './/div[contains(@class, \
                                     "game-note")]/text()')

            event_name = get_event(game_note)

            if not home_id or not away_id: continue

            if self.spider_type == "schedules" and status == "scheduled":
                data = {'game_id': game_id, 'home_id': home_id, \
                        'away_id': away_id, 'game_status': status, \
                        'game_note': game_note, 'event_name': event_name, \
                        'game': game}
                yield Request(link, callback=self.parse_scheduled, \
                              meta= {'data': data})

            elif self.spider_type == "scores" and status != "scheduled":
                data = {'game_id': game_id, 'home_id': home_id, \
                        'away_id': away_id, 'game_status': status, \
                        'game_note': game_note, 'event_name': event_name, \
                        'game': game, 'home_scores': home_scores, \
                        'home_final_score': home_final_score, \
                        'away_scores': away_scores, \
                        'away_final_score': away_final_score}
                yield Request(link, callback=self.parse_scores, \
                              meta = {'data': data})

    def parse_scores(self, response):

        hxs              = HtmlXPathSelector(response)
        record           = SportsSetupItem()
        away_final_score = response.meta['data']['away_final_score']
        home_final_score = response.meta['data']['home_final_score']
        home_scores      = response.meta['data']['home_scores']
        away_scores      = response.meta['data']['away_scores']
        game_status      = response.meta['data']['game_status']
        home_id          = response.meta['data']['home_id']
        away_id          = response.meta['data']['away_id']

        game_datetime = extract_data(hxs, '//div[@class="game-time-location"] \
                                    //p[1]//text()')
        game_datetime = get_utc_time(game_datetime, '%I:%M %p ET, %B %d, %Y', \
                                    'US/Eastern')

        game_location = extract_data(hxs, '//div[@class="game-time-location"] \
                                    //p[2]//text()')
        stadium       = game_location.split(',')[-3].strip()
        city          = game_location.split(',')[-2].strip()
        state         = game_location.split(',')[-1].strip()
        state         = REPLACE_STATE_DICT.get(state)
        country       = 'USA'
        channel = extract_data(hxs, '//div[@class="game-vitals"]/ \
                            p[contains(text(),"coverage:")]//strong//text()')
        if not channel:
            channel = extract_data(hxs, '//div[@class="game-vitals"]//div \
                                  /p[contains(text(),"coverage:")]//strong \
                                  //text()')

        values = {}
        if game_status == "completed" or game_status == "ongoing":

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
            if game_status == "completed":
                if (int(away_final_score) > int(home_final_score)):
                    winner = away_id
                elif int(away_final_score) < int(home_final_score):
                    winner = home_id
                else:
                    winner = ''

            game_score = home_final_score + '-' + away_final_score
            if home_ot:
                game_score = game_score + '(OT)'

            record['affiliation'] = 'wnba'
            record['event'] = response.meta['data']['event_name']
            record['game'] = response.meta['data']['game']
            record['game_datetime'] = game_datetime
            record['game_status'] = response.meta['data']['game_status']
            record['participant_type'] = "team"
            record['participants'] = {response.meta['data']['away_id'] : \
                                     (0, ''), response.meta['data']['home_id'] \
                                     : (1, '')}
            record['rich_data'] = {'channels' : channel, 'location' : \
                                   {'city' : city, 'country' : country, \
                                   'stadium' : stadium, 'state' : state, \
                                   'game_note' : response.meta['data'] \
                                   ['game_note']}}
            record['source'] = 'espn_wnba'
            record['source_key'] = response.meta['data']['game_id']
            record['tournament'] = 'wnba basketball'
            record['reference_url'] = response.url
            record['result'] = {'0': { 'winner': winner, 'score': game_score}, home_id: {'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3, 'Q4': home_q4, 'OT1': home_ot, 'final': home_final_score}, away_id: { 'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3, 'Q4': away_q4, 'OT1': away_ot, 'final': away_final_score}}
            import pdb;pdb.set_trace()
            yield record

    def parse_scheduled(self, response):

        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()

        game_datetime = extract_data(hxs, '//div[@class="game-time-location"] \
                                    //p[1]/text()')
        game_datetime = get_utc_time(game_datetime, '%I:%M %p ET, %B %d, %Y', \
                                    'US/Eastern')

        game_location = extract_data(hxs, '//div[@class="game-time-location"] \
                                    //p[2]//text()')
        stadium  = game_location.split(',')[-3].strip()
        city = game_location.split(',')[-2].strip()
        state = game_location.split(',')[-1].strip()
        state = REPLACE_STATE_DICT.get(state)
        country = 'USA'

        channel = extract_data(hxs, '//div[@class="game-vitals"]// \
                               p[contains(text(), "Coverage:")]/strong \
                               //text()').strip()
        if not channel:
            channel = extract_data(hxs, '//div[@class="game-vitals"]//div/ \
                                   p[contains(text(), "Coverage:")]/strong// \
                                   text()').strip()

        record['affiliation'] = 'wnba'
        record['event'] = response.meta['data']['event_name']
        record['game'] = response.meta['data']['game']
        record['game_datetime'] = game_datetime
        record['game_status'] = response.meta['data']['game_status']
        record['participant_type'] = "team"
        record['participants'] = {response.meta['data']['away_id'] : (0, ''), \
                                 response.meta['data']['home_id'] : (1, '')}
        record['rich_data'] = {'channels' : channel, 'location' : \
                               {'city' : city, 'country' : country, \
                               'stadium' : stadium, 'state' : state, \
                               'game_note' : response.meta['data'] \
                               ['game_note']}}
        record['tournament'] = "WNBA Basketball"
        record['source'] = "espn_wnba"
        record['source_key'] = response.meta['data']['game_id']
        record['reference_url'] = response.url
        record['result'] = {}
        yield record
