import re
from scrapy.http import request
from scrapy.selector import Selector
import datetime
from vtvspider_new import VTVSpider, extract_list_data, extract_data, get_utc_time, get_nodes, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import time

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                    'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                    'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                    'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                    'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                    'WV' : 'West Virgina', 'FL' : 'Florida', \
                    'KS' : 'Kansas', 'TN' : 'Tennessee', \
                    'LA' : 'Louisiana', 'MO' : 'Missouri', \
                    'AR' : 'Arkansas', 'SD' : 'South Dakota', \
                    'MS' : 'Mississippi', 'MI' : 'Michigan', \
                    'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                    'ID' : 'Idaho', 'RI' : 'Rhode Island', \
                    'NM' : 'New Mexico', 'MN' : 'Minnesota', \
                    'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
                    'CA' : 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', \
                    'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado', \
                    'AZ' : "Arizona", 'N.M.': 'New Mexico', "Nev.": 'Nevada', \
                    'Ala.': 'Alabama', 'Fla.': 'Florida', 'La.': 'Louisiana', \
                    "Calif.": "California", "N.Y.": "New York", "Md.": "Maryland", \
                    "Mich.": "Michigan", "Ariz.": 'Arizona', "N.C.": "North Carolina", \
                    "Tenn.": "Tennessee", "Ga.": "Georgia"}

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



def normalize(text):
    text = text.replace("\t", '').replace('\n', '').strip()
    return text

def get_location_tzinfo(loc_details, game_datetime):
    tz_info = ''
    if "," in loc_details:
        loc_city = loc_details.split(',')[0].strip(). \
        replace('Albequerque', 'Albuquerque'). \
        replace('Ft. Worth', 'Fort Worth'). \
        replace('Bronx', "The Bronx")
        loc_state = loc_details.split(',')[1].strip()
        loc_cou = REPLACE_STATE_DICT.get(loc_state.strip(), '')
        if loc_cou == '':
            loc_cou = loc_state
    else:
        loc_city = loc_details
        loc_cou = ''
    location = {"city": loc_city, "state": loc_cou, 'country': "USA"}

    tz_info = get_tzinfo(city= loc_city, game_datetime = game_datetime)
    if not tz_info:
        tz_info = get_tzinfo(city= loc_city, country = "USA", game_datetime = game_datetime)
    return tz_info, location

IMAGES = {"Pac-12" : "http://www.cbssports.com/images/collegefootball/Pac-12logo.png",
          "MWC" : "http://college-football.findthedata.org/sites/default/files/2077/media/images/MWC_410059_i0.gif",
          "MAC" : "http://d193sqwirurshj.cloudfront.net/sites/default/files/styles/large/public/MAC-logo.png",
          "Tulane" : "http://assets.bizjournals.com/bizjournals/on-numbers/scott-thomas/CFB-Tulane*304.jpg",
          "Louisiana-Lafayette" : "http://assets.bizjournals.com/bizjournals/on-numbers/scott-thomas/CFB-LouisianaLafayette*304.jpg",
          "C-USA" : "http://www.cbssports.com/images/collegefootball/CUSA_logo2.jpg",
          "American" : "http://lastwordonsports.com/wp-content/uploads/2013/08/American-Athletic-Conference-Logo.jpg",
          "Big Ten" : "http://cdn.bleacherreport.net/images_root/images/photos/001/344/937/BIG_TEN_CONFERENCE_LOGO_t607_crop_340x234.jpg",
          "Army" : "http://3.bp.blogspot.com/-mUCF-n-ntis/T9zld_-N5BI/AAAAAAAAAIA/kET0UlS1QYA/s1600/army%2Bfootball%2Blogo.png",
          "ACC" : "http://acc.blogs.starnewsonline.com/files/2013/06/acc-logo.jpg",
          "Navy": "http://collegefootballzealots.com/images/stories/Navy.gif",
          "SEC" : "http://www.wcbi.com/wordpress/wp-content/uploads/SEC_new_logo.png",
          "BCS" : "http://media.campustimes.org/2010/12/BCS-Rankings-Logo.jpg",
          "Big 12" : "http://guthriesports.files.wordpress.com/2010/11/big_12.jpg",
          "BYU" : "http://images.picturesdepot.com/photo/b/byu_mascot-10052.gif",
          "C-USA/American" : "http://www.cbssports.com/images/collegefootball/CUSA_logo2.jpg",
          "BCs" : "http://media.campustimes.org/2010/12/BCS-Rankings-Logo.jpg",
          "Sun Belt" : "http://college-football.findthedata.org/sites/default/files/2077/media/images/SUN%20BELT_410062_i0.gif",
          "BCS No. 1" : "http://media.campustimes.org/2010/12/BCS-Rankings-Logo.jpg",
          "BCS No. 2" : "http://media.campustimes.org/2010/12/BCS-Rankings-Logo.jpg"}

TOU_DICT = {"Potato": "Famous Idaho Potato Bowl",
            "Cactus": "Cactus Bowl",
            "GoDaddy": "GoDaddy Bowl",
            "Popeyes": "Bahamas Bowl",
            "Championship Game": "College Football Playoff National Championship",
            "CFP Champ. Game": "College Football Playoff National Championship", \
            "CFP National Championship": "College Football Playoff National Championship", \
            "Cotton": "Cotton Bowl Classic", \
            "Rose": "Rose Bowl Game"}

STD_DICT = {'Camellia Bowl': "Cramton Bowl", "Celebration Bowl": "Georgia Dome", \
            "Cure Bowl": "Orlando Citrus Bowl", "Las Vegas Bowl": "Sam Boyd Stadium", \
            "New Mexico Bowl": "University Stadium", "New Orleans Bowl": "Mercedes-Benz Superdome", \
            "Miami Beach Bowl": "Marlins Park", "Famous Idaho Potato Bowl": "Albertsons Stadium", \
            "Boca Raton Bowl": "FAU Stadium", "GoDaddy Bowl": "Ladd-Peebles Stadium", \
            "Poinsettia Bowl": "Qualcomm Stadium", "Bahamas Bowl": "Thomas Robinson Stadium", \
            "Hawaii Bowl": "Aloha Stadium", "Foster Farms Bowl": "Levi's Stadium", \
            "Heart of Dallas Bowl": "Cotton Bowl", "Independence Bowl": "Independence Stadium", \
            "Pinstripe Bowl": "Yankee Stadium", "St. Petersburg Bowl": "Tropicana Field", \
            "Sun Bowl": "Sun Bowl", "Quick Lane Bowl": "Ford Field", "Military Bowl": "Navy-Marine Corps Memorial Stadium", \
            "Armed Forces Bowl": "Amon G. Carter Stadium", "Russell Athletic Bowl": "Orlando Citrus Bowl", \
            "Texas Bowl": "NRG Stadium", "Belk Bowl": "Bank of America Stadium", \
            "Birmingham Bowl": "Legion Field", "Holiday Bowl": "Qualcomm Stadium", \
            "Music City Bowl": "Nissan Stadium", "Cotton Bowl Classic": "AT&T Stadium", \
            "Peach Bowl": "Georgia Dome", "Orange Bowl": "Sun Life Stadium", \
            "Fiesta Bowl": "University of Phoenix Stadium", "Outback Bowl": "Raymond James Stadium", \
            "Citrus Bowl": "Orlando Citrus Bowl", "Rose Bowl Game": "Rose Bowl", \
            "Sugar Bowl": "Mercedes-Benz Superdome", "Alamo Bowl": "Alamodome", \
            "Cactus Bowl": "Chase Field", "Liberty Bowl": "Liberty Bowl Memorial Stadium", \
            "TaxSlayer Bowl": "EverBank Field", "College Football Playoff National Championship": "University of Phoenix Stadium", \
            "Arizona Bowl": "Arizona Stadium"}

def get_event(event):
    tou = TOU_DICT.get(event, '')
    if tou == "":
        tou = event.replace('* ', '') + " Bowl"
    return tou

class NCAABowl(VTVSpider):
    start_urls = ['http://www.ncaa.com/sports/football/bowls']
    name = "ncaa_bowl"

    record = SportsSetupItem()
    record['source'] = "ncaa_ncf"
    record['participant_type'] = "team"
    record['game'] = "football"
    record['affiliation'] = 'ncaa-ncf'
    record['time_unknown'] = '0'
    record['result'] = {}
    record['tournament'] = "College football"

    def get_game_date(self, event_date, et_time):
        event_date = event_date + " " + et_time
        game_date = event_date.replace('TBD', '00:00 am').replace('.', '').\
                    replace('Noon', '12:00 pm').replace('\n', ' ')
        if "Dec" in game_date: game_datetime = "2015" + " " + game_date
        if "Jan" in game_date: game_datetime = "2016" + " " + game_date
        if not ":" in game_datetime:
            game_dt = get_utc_time(game_datetime, '%Y %b %d %I %p', "US/Eastern")
            sk_date = str((datetime.datetime(*time.strptime(\
                      game_datetime.strip(), '%Y %b %d %I %p')[0:6])).date())
        else:
            game_dt = get_utc_time(game_datetime, '%Y %b %d %H:%M %p', "US/Eastern")
            sk_date = str((datetime.datetime(*time.strptime(\
                      game_datetime.strip(), '%Y %b %d %H:%M %p')[0:6])).date())
        return game_dt, sk_date

    def parse(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="inline-left"]//tbody//tr')
        for node in nodes:
            data = {}
            result = {}
            participants = {}
            tbd_tuple = ()
            bowl_details = extract_list_data(node, './/td//text()')
            game_link = extract_data(node, './/td[5]//a//@href')
            tou = extract_data(node, './/td[1]/text()')
            if "(" in tou:
                tou = tou.split('(')[0].strip()
            teams = extract_data(node, './/td[5]//text()')
            location = extract_data(node, './/td[4]/text()')
            et_time = extract_data(node, './/td[3]/text()')
            #et_time = 'TBD'
            event_date = extract_data(node, './/td[2]/text()')
            channel = extract_data(node, './/td[6]/text()')
            if "vs." in teams:
                tou_teams = teams.replace('.', '').split('vs')
                status = ''
            else:
                tou_teams = extract_data(node, './/td[2]//text()').replace('.', '').split(',')
                status = "completed"
            if "TBD" in teams:
                for ind, team in enumerate(tou_teams):
                    team_sk = "tbd" + str((ind + 1))
                    participants[team_sk] = (0, team.strip())
            else:
                home_team = tou_teams[-1].strip()
                away_team = tou_teams[0].strip()
                home_sk = home_team.lower().replace(' ', '-'). \
                replace('state', 'st').replace('michigan', 'mich')
                away_sk = away_team.lower().replace(' ', '-'). \
                replace('state', 'st').replace('michigan', 'mich')
                participants = {home_sk: (1, ''), away_sk: (0, '')}

            if "*" in tou:
                tournament = "College Football Playoff"
                self.record['tournament'] = "College Football Playoff"
            else:
                tournament = "College football"
            print tou

            if et_time and event_date:
                game_dt, sk_date = self.get_game_date(event_date, et_time)
                compare_date = game_dt.split(' ')[0]

            if location:
                tz_info, loc = get_location_tzinfo(location, game_dt)
            else:
                tz_info = ""
                loc = ""

            if status != "completed":
                today_date = str(datetime.datetime.now().date())
                if compare_date > today_date:
                    status = "scheduled"
                else:
                    status = "ongoing"
            date_sk = sk_date.replace('-', '_')
            sk = tou.replace(' ', '_').replace('* ', '')
            game_id = sk +  "_" + date_sk
            self.record['game_datetime'] = game_dt

            event = get_event(tou)
            if "semifinal" in event:
                continue

            if event in ['Citrus Bowl', 'Rose Bowl Game', 'Citrus Bowl', 'Fiesta Bowl', 'Sugar Bowl', 'College Football Playoff National Championship', 'FCS title game']:

                status = "completed"

            if event in ['Las Vegas Bowl', 'Citrus Bowl', 'Pinstripe Bowl']:
                channel = "ABC"
            if event == "Sun Bowl":
                channel = "CBS"
            if event == "Cure Bowl":
                channel = "CBSSN"
            if event in ['Outback Bowl']:
                channel = "ESPN2"
            if event in ['Arizona Bowl']:
                channel = "ASN"
            if not channel:
                channel ="ESPN"

            stadium = STD_DICT.get(event, '')
            rich_data = {'channels': str(channel), 'stadium': stadium, 'location': loc}
            self.record['rich_data'] = rich_data
            self.record['game_status'] = status
            self.record['event'] = normalize(event)
            self.record['participants']  = participants
            self.record['tz_info'] = tz_info
            self.record['location_info'] = loc
            self.record['reference_url'] = response.url
            participants = {home_sk: (1, ''), away_sk: (0, '')}
            self.record['participants']  = participants
            self.record['source_key'] = game_id
            data = {'tournament': tournament, 'rich_data': rich_data,
                    'status': status, 'event': event,
                    'participants': participants,'tz_info': tz_info,
                    'location': location, 'tou': tou, 'ref_url': game_link, \
                    'game_id': game_id, 'game_dt': game_dt, 'rich_data': rich_data}

            if status == "ongoing":
                self.record['source_key'] = game_id
                res_data = game_link.split('fbs')[-1]
                req_url = "http://data.ncaa.com/jsonp/game/football/fbs%s/gameinfo.json" % (res_data)
                data.update({'game_dt': game_dt, 'game_id': game_id, 'location': location})
                yield Request(req_url, self.parse_ongoing, meta={'data': data})
            elif status == "scheduled":
                self.record['result'] = {}
                yield self.record
            elif status == "completed":
                res_data = game_link.split('fbs')[-1]
                #req_url = "http://data.ncaa.com/jsonp/game/football/fbs%s/gameinfo.json" % (res_data)
                req_url = "http://www.ncaa.com/game/football/fbs%s" %(res_data)
                yield Request(req_url, self.parse_result, meta={'data': data})

    def parse_result(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs,'//div[@class="line-score-container"]//table[@id="linescore"]')
        data = response.meta['data']
        record = SportsSetupItem()
        items  = []
        for node in nodes:
            home_sk = extract_data(node, './/tr[2]//td[@class="school"]//a//@href')
            if "/schools/" in home_sk:
                home_sk = home_sk.split('/')[-2]
            else:
                home_sk = extract_data(node, './/tr[2]//td[@class="school"]//text()')
            if not home_sk:
                home_sk = "TBD"

            away_sk = extract_data(node, './/tr[1]//td[@class="school"]//a//@href')
            if "/schools/" in away_sk :
                away_sk = away_sk.split('/')[-2]
            else:
                away_sk = extract_data(node, './/tr[1]//td[@class="school"]//text()')
            if not away_sk:
                away_sk = "TBD"

            home_scores = extract_list_data(node, './/tr[2]//td[@class="period"]//text()')
            away_scores = extract_list_data(node, './/tr[1]//td[@class="period"]//text()')
            home_total_score = extract_data(node, './/tr[2]//td[@class="score"]//text()')
            away_total_score = extract_data(node, './/tr[1]//td[@class="score"]//text()')

            home_ot = ''
            home_q1 = home_scores[0]
            home_q2 = home_scores[1]
            home_q3 = home_scores[2]
            home_q4 = home_scores[3]
            if len(home_scores) > 4:
                home_ot = home_scores[4]

            home_ot2 = ''
            if len(home_scores) == 6:
                home_ot2 = home_scores[5]

            away_ot = ''
            away_q1 = away_scores[0]
            away_q2 = away_scores[1]
            away_q3 = away_scores[2]
            away_q4 = away_scores[3]

            if len(away_scores) > 4:
                away_ot = away_scores[4]

            away_ot2 = ''
            if len(away_scores) == 6:
                away_ot2 = away_scores[5]

            if (int(away_total_score) > int(home_total_score)):
                winner = away_sk
            elif int(away_total_score) < int(home_total_score):
                winner = home_sk
            else:
                 winner = ''

            game_location  = extract_data(hxs, '//p[@class="location"][contains(text(), ",")]/text()')
            stadium =  city = state = ''
            stadium = game_location.split(',')[0]
            if '(' in stadium:
                stadium = stadium.split('(')[0].strip()
                venue = ",".join(game_location.split(',')[2:]).strip()
            else:
                stadium = stadium
                venue = ",".join(game_location.split(',')[1:]).strip()
            city, state, country = get_city_state_ctry(venue)

            stadium = stadium.replace('&amp;', '&')

            game_score = home_total_score + '-' + away_total_score
            game_score_ = game_score
            if home_ot:
                game_score_ = game_score + '(OT)'

            if home_ot2:
                game_score_ = game_score + '(2OT)'
            
            self.record['result'] = {'0': {'winner': winner, 'score': game_score_},
                               home_sk: {'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3, \
                                'Q4': home_q4, 'OT1': home_ot, 'OT2': home_ot2, 'final': home_total_score },
                               away_sk: {'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3, \
                                'Q4': away_q4, 'OT1': away_ot, 'OT2': away_ot2, 'final': away_total_score}}
            data['rich_data'].update({'game_note': "Final"})
            self.record['rich_data'] = data['rich_data']
            self.record['source'] = "ncaa_ncf"
            self.record['tournament'] = data['tournament']
            self.record['participant_type'] = "team"
            self.record['game'] = "football"
            self.record['affiliation'] = 'ncaa-ncf'
            self.record['time_unknown'] = '0'
            self.record['game_datetime'] = data['game_dt']
            self.record['game_status'] = data['status']
            self.record['event'] = normalize(data['event'])
            self.record['participants']  = {home_sk: (1, ''), away_sk: (0, '')}
            self.record['tz_info'] = data['tz_info']
            self.record['source_key'] = data['game_id']
            self.record['location_info'] = data['location']
            self.record['reference_url'] = response.url
            yield self.record

    def parse_ongoing(self, response):
        hxs = Selector(response)
        raw_data = response.body
        data = response.meta['data']
        record = SportsSetupItem()
        game_data = eval(raw_data.replace('\n', '').replace('\t', '').replace('callbackWrapper', '').replace(';', ''))
        if data['status'] == "completed":
            game_dt = game_data.get('startDate', '')
            game_time = game_data.get('startTime', '').replace('ET', '').strip()
            game_dtetime = game_dt + game_time
            game_date = get_utc_time(game_dtetime, "%Y-%m-%d%H:%M %p", "US/Eastern")
            game_id = data['tou']
            date_sk = game_dt.replace('-', '_')
            sk = data['tou'].replace(' ', '_').replace('* ', '')
            game_id = sk +  "_" + date_sk
            data.update({'game_dt': game_date, 'game_id': game_id})
            home_sk = game_data.get('home', '').get('nameSeo', '')
            away_sk = game_data.get('away', '').get('nameSeo', '')
            teams = {home_sk:  (1, ''), away_sk: (0, '')}
            data.update({'participants': teams})

        home_score = game_data.get('home', '').get('scoreBreakdown', '')
        home_final = game_data.get('home', '').get('currentScore', '')
        home_sk = game_data.get('home', '').get('nameSeo', '')
        away_score = game_data.get('away', '').get('scoreBreakdown', '')
        away_final = game_data.get('away', '').get('currentScore', '')
        away_sk = game_data.get('away', '').get('nameSeo', '')
        status = game_data.get('gameState', '').lower()
        if status != "pre":
            home_ot1 = away_ot1 = home_ot2 = away_ot2 = ""
            if len(home_score) == 5:
                home_q1, home_q2, home_q3, home_q4, home_ot1 = home_score
            elif len(home_score) == 6:
                home_q1, home_q2, home_q3, home_q4, home_ot1, home_ot2 = home_score
            else:
                home_q1, home_q2, home_q3, home_q4 = home_score
            if len(away_score)== 5:
                away_q1, away_q2, away_q3, away_q4, away_ot1 = away_score
            elif len(away_score) == 6:
                away_q1, away_q2, away_q3, away_q4, away_ot1, away_ot2 = away_score
            else:
                away_q1, away_q2, away_q3, away_q4 = away_score
            game_round = game_data.get('currentPeriod', '')
            game_status = game_data.get('gameStatus', '')
            game_note = game_round + " " + game_status
            if status == "final":
                self.record['game_status'] = "completed"
                data['rich_data'].update({'game_note': "Final"})
                winner = self.get_winner(home_final, away_final, home_sk, away_sk)
            else:
                self.record['game_status'] = "ongoing"
                data['rich_data'].update({'game_note': game_note})
                winner = ""
            game_score = home_final + " - " + away_final


            self.record['result'] = {'0': {'winner': winner, 'score': game_score},
                                home_sk: {'Q1': home_q1, 'Q2': home_q2, 'Q3': home_q3, \
                                'Q4': home_q4, "OT1": home_ot1, 'final': home_final },
                                away_sk: {'Q1': away_q1, 'Q2': away_q2, 'Q3': away_q3, \
                                'Q4': away_q4, "OT1": away_ot1, 'final': away_final}}
            self.record['rich_data'] = data['rich_data']
            self.record['tournament'] = data['tournament']
            self.record['game_datetime'] = data['game_dt']
            self.record['event'] = normalize(data['event'])
            self.record['participants']  = data['participants']
            self.record['tz_info'] = data['tz_info']
            self.record['source_key'] = data['game_id']
            self.record['location_info'] = data['location']
            self.record['reference_url'] = data['ref_url']
            yield self.record

    def get_winner(self, home_final, away_final, home_sk, away_sk):
        if int(home_final) > int(away_final):
            winner = home_sk
        elif int(away_final) > int(home_final):
            winner = away_sk
        else:
            winner = ''
        return winner
