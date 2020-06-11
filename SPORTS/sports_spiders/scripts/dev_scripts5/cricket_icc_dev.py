'''Spider for cricket schedules and scores
'''
import os
import re
import time
import datetime
from vtvspider_dev import VTVSpider, extract_data, get_nodes, \
extract_list_data, log, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem

skipped_matches = ['730587', '730589', '730081', '756031', '756033', '754715', '760779', '779097', '754733', '754735', '722327', '810279', '754745', '766925', '766927']
cancelled_matches = ['770131', '780257', '770133', '770135', '770137', '754733', '722327', '810279', '766925', '766927', '829755']
tbd_ids = ['3600', '3760']


class IccCricketDEV(VTVSpider):
    '''Class function
    '''
    name            = "cricket_iccdev"
    allowed_domains = ["www.espncricinfo.com"]
    winners_file    = open('winners_misplaced', 'w')

    file_existance  = False
    if not os.path.isfile('teams_dict'):
        games_dict  = open('teams_dict', 'w')
    else:
        file_existance  = True
        games_dict      = open('teams_dict', 'r')

    teams_tuple = ("india", "west indies", "sri lanka", "australia", "pakistan", "zimbambwe", "new zealand", \
                  "england", "south africa", "bangladesh", "canada", "scotland", "netherlands", "ireland", \
                  "kenya", "bermuda", "tba")

    status      = False
    start_urls  = ["http://www.espncricinfo.com/bangladesh-v-india-2015/engine/series/870723.html"]

    def get_team_name(self, team):
        teams_tuple = ("india", "west indies", "sri lanka", "australia", "pakistan", "zimbambwe", "new zealand", \
                      "england", "south africa", "bangladesh", "canada", "scotland", "netherlands", "ireland", \
                      "kenya", "bermuda", "tba", "hong kong", "papua new guinea", "namibia", "united arab emirates")
        for tm in teams_tuple:
            if tm in team.lower():
                return tm
        return team

    def data_replacement(self, data):
        '''Refining correct string
        '''
        replace_dict = {'Sri Lankans': 'Sri Lanka', 'Indians': 'India', 'home': '', '\n': '', \
                        'Player of the match': '', 'Players of the match': '', 'Players of the series': '', \
                        'Player of the series': '', 'innings': '', '1st': '', '2nd': '', '&amp;': '&', \
                        'New Zealanders': 'N Zealanders','England Lions': 'Eng Lions', 'Worcestershire': 'Worcs', \
                        'United Arab Emirates': 'U.A.E.', 'South Africans': 'Sth Africans', \
                        "Sri Lanka Cricket Board President's XI" : 'SL BP XI', 'Faisalabad Wolves': 'Wolves',\
                        'Sunrisers Hyderabad' : 'Sunrisers', 'Rajasthan Royals': 'Royals', 'Mumbai India': \
                        'Mum India', 'Brisbane Heat' : 'Heat', 'Trinidad & Tobago': 'Trinidad & T', \
                        'Chennai Super Kings': 'Super Kings', 'Perth Scorchers': 'Scorchers', \
                        'Kandurata Maroons': 'Kandurata Ma', "UWI Vice Chancellor's": 'UWI VC', 'T20' : '',\
                        'Jamaica Select' : 'Jam Sel', 'Northamptonshire' : 'Northants', \
                        "Sri Lanka Board President's" : "SL Pres.", "Leicestershire" : "Leics", \
                        'Gloucestershire' : 'Gloucs', 'Nottinghamshire' : 'Notts', 'Southern Express' :'Express',
                        'Northern Districts' : 'Northern D', 'Kolkata Knight Riders' : 'KKR', "St Kittsevis": "St Kitts & N", 
                        'Hobart Hurricanes' : 'Hurricanes', 'Kings XI Punjab' : 'Kings', 'XI' : '', 'Barbados Tridents' : 'Tridents', \
                        'Cricket Australia': '', 'Bangladesh Cricket Board XI': '', 'Bangladesh Cricket Board': '', \
                        'Cricket Australia XI': '', "New Zealand Chairman's" : 'N Zealanders', 'Delhi Daredevils': 'Daredevils', 'Royal Challengers Bangalore': 'RCB'}
        for key, val in replace_dict.iteritems():
            data = data.replace(key, val).strip()
        return data

    def get_tournament(self, data):
        '''Returns exact tournament name
        '''
        tou_dict = {"test" : "Test cricket", "t20i" : "Twenty20 Cricket", \
                    "asia-cup" : "Asia Cup", "world-t20" : "Icc World Twenty20", "west indies tri" :\
                    "ODI Cricket", "icc champions trophy" : "ICC Champions Trophy", "test series" :\
                    "Test Cricket", "odi" : "ODI Cricket", "natwest series" :\
                    "odi cricket", "rsa challenge" : "ODI Cricket", "the ashes" : "Test cricket", \
                    "champions league twenty20" : "Twenty20 Cricket", "pataudi trophy" : "Test Cricket", \
                    "icc intercontinental" : "ODI Cricket", "icc world cricket league" : "ODI Cricket", \
                    "tour match" : "Warm Up Match", "tour" : "Warm Up Match", "world t20" : "Icc World Twenty20",\
                    "indian-premier-league" : "Indian Premier League", 'indian t20 league' : 'Indian Premier League',
                    'natwest t20 blast' : 'Twenty20 Cricket', 'zimbabwe triangular series' : 'ODI Cricket', \
                    'dubai triangular series': 'ODI Cricket', 'wcl-championship': 'ICC World Cricket League Championship', 'icc-intercontinental-cup': 'ICC Intercontinental Cup', 'icc intercontinental cup': 'ICC Intercontinental Cup', \
                    'lv= county championship division one' : "English County cricket", "lv= county championship division two": "English County cricket", "natwest t20 blast ": "English County cricket", 'royal london one-day cup': "English County cricket"}

        tou = [val for key, val in tou_dict.iteritems() if key in data.lower()]
        if tou:
            tou = tou[0]
            return tou
        else:
            return data

    def get_event(self, data):
        '''Returns exact event name
        '''
        event_dict = {"west indies tri" : "West Indies Tri-Nation series", "the ashes" : "The Ashes", \
                      "champions league twenty20" : "Champions League Twenty20", "Qualifier" : "Indian Premier League Playoffs",\
                      "Eliminator" : "Indian Premier League Playoffs", "zimbabwe triangular series" : "Tri Series Cricket", \
                      "carlton mid odi tri": "Carlton Mid Triangular Series", 'dubai triangular series': "Dubai Triangular Series"}
        event = [val for key, val in event_dict.iteritems() if key in data]
        if event:
            event = event[0]
        else:
            event = ''
        return event

    def get_game_status(self, status_text):
        '''Returns game status
        '''
        game_status_dict = {"draw" : "completed", "no result" : "no_result", "Match tied" : "completed", \
                            "abandoned" : "Hole", "won the toss" : "ongoing", "require another" : \
                            "ongoing", "trail by" : "ongoing", "lead by" : "ongoing", "won by" : "completed", \
                            "match tied" : "completed", "match drawn" : "completed", "scheduled" : "scheduled", \
                            "cancelled" : "Hole"}
        status_text = [val for key, val in game_status_dict.iteritems() if key in status_text]
        if status_text: status_text = status_text[0]
        return status_text

    @log
    def parse(self, response):
        '''Creating doc for the main url
        '''
        hxs     = Selector(response)
        if not self.status:
            self.status         = True
            cricket_tournaments = [ 'http://www.espncricinfo.com/bangladesh-v-india-2015/engine/series/870723.html',  \
                                    'http://www.espncricinfo.com/england-v-new-zealand-2015/engine/series/743909.html', \
                                    'http://www.espncricinfo.com/west-indies-v-australia-2015/engine/series/810415.html', \
                                    'http://www.espncricinfo.com/sri-lanka-v-pakistan-2015/engine/series/860091.html', \
                                    'http://www.espncricinfo.com/the-ashes-2015/engine/series/743911.html', \
                                    'http://www.espncricinfo.com/icc-intercontinental-cup-2015-17/engine/series/870857.html', \
                                    'http://www.espncricinfo.com/south-africa-v-england-2015-16/engine/series/800431.html', \
                                    'http://www.espncricinfo.com/county-championship-div1-2015/engine/series/803387.html', \
                                    'http://www.espncricinfo.com/county-championship-div2-2015/engine/series/803389.html', \
                                    'http://www.espncricinfo.com/natwest-t20-blast-2015/engine/series/803393.html', \
                                    'http://www.espncricinfo.com/royal-london-one-day-cup-2015/engine/series/803391.html', \
                                    'http://www.espncricinfo.com/wcl-championship-2015-17/engine/series/870869.html', \
                                    'http://www.espncricinfo.com/ci/engine/series/876457.html', \
                                    'http://www.espncricinfo.com/bangladesh-v-south-africa-2015/engine/series/817095.html', \
                                    'http://www.espncricinfo.com/zimbabwe-v-india-2015/engine/series/885951.html', \
                                    'http://www.espncricinfo.com/south-africa-v-new-zealand-2015/engine/series/848811.html', \
                                    'http://www.espncricinfo.com/ci/engine/series/875977.html', \
                                    'http://www.espncricinfo.com/south-africa-v-england-2015-16/engine/series/800431.html', \
                                    'http://www.espncricinfo.com/south-africa-australia-v--2015-16/engine/series/884343.html']
            #cricket_tournaments = [ 'http://www.espncricinfo.com/bangladesh-v-india-2015/engine/series/870723.html']

            for tournaments in cricket_tournaments:
                yield Request(tournaments, callback=self.parse_second)


        nodes = get_nodes(hxs, '//div[contains(@id, "fix_fix")]/div[contains(@class, "divFix")]')
        for node in nodes:
            tours = extract_data(node, './/p[@class="ciPhotoWidgetLink"]/text()')
            if tours not in ('International tours'):
                continue
            reference_urls  = get_nodes(node, './/p[@class="wallpaperbrowsultext"]//@href')
            for reference in reference_urls:
                reference   = "".join(reference.extract()).replace('/content/', '/engine/')
                if not "http" in reference:
                    reference = "http://www.espncricinfo.com" + reference
                yield Request(reference, callback=self.parse_second)

    @log
    def parse_second(self, response):
        '''Creating doc for the tournament link
        '''
        hxs     = Selector(response)
        record  = SportsSetupItem()
        series_name = extract_data(hxs, '//h1[@class="SubnavSitesection"]//text()').strip().split(',')[0]
        print series_name
        nodes   = get_nodes(hxs, '//p[@class="potMatchHeading"]|//h2')
        game_date = extract_list_data(hxs,  '//p[@class="potMatchHeading"]//text()')
        if game_date:
            print len(game_date)
            print game_date
            season_start = game_date[4].strip()
            season_start = season_start.split(',')[0].split('-')[1].strip() + season_start.split(',')[-1]
            season_start = datetime.datetime.strptime(season_start, '%b %d %Y').strftime('%Y-%m-%d %H:%M:%S')
            season_end = game_date[-1].strip()
            season_end = season_end.split(',')[0].split('-')[1].strip() +season_end.split(',')[-1].replace("'", '')
            season_end = datetime.datetime.strptime(season_end, '%b %d %Y').strftime('%Y-%m-%d %H:%M:%S')
        else:
            game_date = extract_list_data(hxs,  '//div[@class="news-list large-20 medium-20 small-20"]//ul//li//h2/text()')
            season_start = game_date[1].strip()
            season_start = season_start.split(',')[0].split('-')[1].strip() + season_start.split(',')[-1]
            season_start = datetime.datetime.strptime(season_start, '%b %d %Y').strftime('%Y-%m-%d %H:%M:%S')
            season_end = game_date[-1].strip()
            season_end =  season_end.split(',')[0].split('-')[1].strip() + season_end.split(',')[-1]
            season_end = datetime.datetime.strptime(season_end, '%b %d %Y').strftime('%Y-%m-%d %H:%M:%S')
            series_name =''
        for node in nodes:
            rich_data   = {}
            match_link  = extract_data(node, './span/a/@href')
            match_title = extract_data(node, './/text()').\
                                replace('\n', '').replace('\t', '').replace('  ', '').strip()
            match_string = extract_data(node, './span/a/text()')
            game_data = extract_data(node, './text()')
            game_date = game_data.rsplit('- ', 1)[-1].strip()
            game_time = extract_data(node, './following-sibling::p[@class="potMatchText mat_status"][1]//text()|./following-sibling::span[@class="show-for-small"][1]//text()')
            game_time = game_time.split('(')[-1].split(')')[0].strip()
            time_unknown = 0
            if game_date or game_time:
                if "Match scheduled" in game_time:
                    game_time = game_time.split(' at ')[1]
                ####Converting game_daetime to datetime format###
                if "GMT" in game_time:
                    if '-'in game_date:
                        game_dt = (game_date.split('-')[0]).strip()+', '+game_date.split('-')[-1]\
                                                        .split(',')[-1].strip() + ' ' + game_time
                        _dt     = get_utc_time(game_dt, "%b %d, %Y %H:%M GMT", 'GMT')
                        #_dt     = time.strftime("%Y-%m-%d %H:%M", time.strptime(game_dt, "%b %d, %Y %H:%M GMT"))
                    else:
                        game_datetime = game_date + ' ' + game_time
                        _dt = get_utc_time(game_datetime, "%b %d, %Y %H:%M GMT", 'GMT')
                        #_dt = time.strftime("%Y-%m-%d %H:%M", time.strptime(game_datetime, "%b %d, %Y %H:%M GMT"))

                elif game_date:
                    if '-'in game_date:
                       game_dt  = (game_date.split('-')[0]).strip()+', '+game_date.split('-')[-1].split(',')[-1].strip()
                       _dt      = time.strftime("%Y-%m-%d", time.strptime(game_dt, "%b %d, %Y"))
                       #_dt = get_utc_time(game_dt, "%b %d, %Y", "GMT")
                    elif 'Dec 0' in game_date:
                        _dt = ''
                    else:
                         _dt      = time.strftime("%Y-%m-%d", time.strptime(game_date, "%b %d, %Y")) 
                        #_dt = get_utc_time(game_date, "%b %d, %Y", "US/Eastern")
                    time_unknown = 1
                else:
                    _dt = ''

            affiliation = ''
            if "champions-league-twenty20" in response.url:
                affiliation = 'clt20'
            elif "indian-premier-league" in response.url:
                affiliation = 'ipl'
            else:
                affiliation = 'icc'
            tou     = "".join(re.findall(r' (\w+):', match_title)).strip()
            event   = ''
            if len(tou) < 2:
                tou     = extract_data(hxs, '//h1[@class="SubnavSitesection"]/text()').replace('\n', '').split(',')[0].strip()
                event   = "".join(re.findall(r' (.*):', match_title)).strip()
                if "asia-cup" in response.url:
                    tou = "Asia cup"
                elif "world-t20" in response.url:
                    tou = "Icc World Twenty20"
                elif "indian-premier-league" in response.url:
                    tou = "Indian Premier League"
                elif "wcl-championship" in response.url:
                    tou = "ICC World Cricket League Championship"
                elif "icc-intercontinental-cup" in response.url:
                    tou = "ICC Intercontinental Cup"
                elif "county-championship" in response.url:
                    tou = "English County cricket"
                elif "natwest-t20-blast" in response.url:
                    tou = "English County cricket"

                if "Group" in event:
                    event   = "".join(re.findall(r', (.*)', event))
                    event   = tou + ' ' + event
                else:
                    event   = tou + event.replace('-', '') + 's' 
            if match_link and game_time:
                match_link = match_link+"?view=scoreboard"
                if not "http" in match_link:
                    match_link = "http://www.espncricinfo.com" + match_link
                source_key = "".join(re.findall(r'/(\d+).html', match_link))
                if "wcl-championship" in response.url:
                    tou = "ICC World Cricket League Championship"
                if "icc-intercontinental-cup" in response.url:
                    tou = "ICC Intercontinental Cup"
                if "county-championship" in response.url:
                    tou = "English County cricket"
                if "natwest-t20-blast" in response.url:
                    tou = "English County cricket"
                if "royal-london-one-day-cup" in response.url:
                    tou = "English County cricket"
                rich_data['match_string']   = match_string
                record['affiliation']       = affiliation
                record['tournament']        = tou
                record['event']             = event
                record['rich_data']         = rich_data
                tou_link = response.url
                yield Request(match_link, callback = self.parse_schedules, \
                              meta={'rich_data' : rich_data, 'record' : record, 'games_dict' : self.games_dict, \
                              'winners_file' : self.winners_file, 'dt' : _dt, 'file_existance' : self.file_existance, \
                              'match_title': match_title, 'time_unknown': time_unknown, 'affiliation': affiliation, 'ref': match_link, 'tou': tou, 'tou_link': tou_link, 'series_name': series_name, 'season_start': season_start, 'season_end': season_end})

            elif match_link and not game_time:
                if 'indian-premier-league' in response.url:
                    tou = 'indian premier league'
                elif 'wcl-championship' in response.url:
                    tou = "wcl-championship"
                elif "icc-intercontinental-cup" in response.url:
                    tou = "ICC Intercontinental Cup"
                elif "natwest-t20-blast" in response.url:
                    tou = "English County cricket"
                elif "royal-london-one-day-cup" in response.url:
                    tou = "English County cricket"
                match_link = "http://www.espncricinfo.com" + match_link + "?view=scoreboard"
                rich_data['match_string']   = match_string
                rich_data['game_date']      = game_date
                rich_data['dt']             = _dt
                rich_data['tou_link']       = response.url
                yield Request(match_link, callback=self.parse_schedule_details, \
                                 meta= {'rich_data' : rich_data, 'affiliation' : affiliation, \
                                 'match_title': match_title, 'tou' : tou, 'time_unknown': time_unknown, \
                                 'series_name': series_name, 'season_start': season_start, 'season_end': season_end})     #Passing game_date match link to schedule_details
    @log
    def parse_schedule_details(self, response):
        '''Creating doc for the games not having game_time
        '''
        hxs         = Selector(response)
        record      = SportsSetupItem()
        meta_data   = response.meta['rich_data']
        record['rich_data'] = {}
        loc         =  extract_data(hxs, '//div[@class="archiveCardContainer"]//\
                        div[@class="headRightDiv"]//li[2]/a/text()')
        if not loc:
            loc         = extract_data(hxs, '//div[contains(@class, "match-information")]//a[contains(@href, "ground")]//text()')
        if loc:
            loc     = loc.split(',')
            stadium = loc[0].strip()
            city = state = ''
            if len(loc) == 2:
                city    = loc[1].strip()
                state   = ''
            elif len(loc) == 3:
                city = loc[2]
        else:
            stadium = city = state = ''

        tzinfo = ''
        if city:
            tzinfo = get_tzinfo(city=city)

        location = {'city' : city, 'state' :  state}
        if not city:
            city = response.meta['match_title'].split('-')[0].split(' at ')[-1].strip()

        source_key = "".join(re.findall(r'/(\d+).html', response.url))
        if response.meta['tou']:
            tou = self.get_tournament(response.meta['tou'])
        else:
            tou = "".join(re.findall(r' (\w+):', response.meta['match_title'])).strip()
            tou = self.get_tournament(tou)
            if not tou:
                tou = self.get_tournament(meta_data['tou_link'])
                if not tou:
                    tou = self.get_tournament(response.meta['match_title'].lower())

        event   = "".join(re.findall(r' (.*):', response.meta['match_title'])).strip()
        if "-Final" in response.meta['match_title']:
            event   = tou + ' ' + event.replace('-', '') + 's'
        elif "Final" in response.meta['match_title']:
            event   = tou + ' ' + "".join(re.findall(r'(.*):', response.meta['match_title']))
        else:
            event = self.get_event(response.meta['match_title'])

        if "Champions League" in response.meta['tou']:
            event = response.meta['tou']
        if "triangular-series" in meta_data['tou_link']:
            event = "zimbabwe triangular series"
        if "natwest-t20-blast" in meta_data['tou_link']:
            event = "natwest t20 blast"
        if "royal-london-one-day-cup" in meta_data['tou_link']:
            event = "royal london one-day cup"
        if ":" in response.meta['match_title']:
            teams = "".join(re.findall(r': (.*) at', response.meta['match_title'])).split('v')
        else:
            teams = "".join(re.findall(r'(.*) at', response.meta['match_title'])).split('v')
        if teams:
            team1 = self.get_team_name(teams[0].strip())
            team2 = self.get_team_name(teams[1].strip())
        else:
            team1 = "tbd1"
            team2 = "tbd2"

        if "tba" in team1:
            team1 = "tbd1"
        elif team1 in self.teams_tuple:
            team1 = team1 + " national cricket team"

        if "tba" in team2:
            team2 = "tbd2"
        elif team2 in self.teams_tuple:
            team2 = team2 + " national cricket team"

        if "tbd1" in team1 and "tbd2" not in team2:
            participants = {team1 : ('0', 'TBA'), '0' : ('0', team2)}
        elif "tbd1" not in team1 and  "tbd2" in team2:
            participants = {'0' : ('0', team1), team2 : ('0', 'TBA')}
        elif "tbd1" in team1 and "tbd2" in team2:
            participants = {team1 : ('0', 'TBA'), team2 : ('0', 'TBA')}
        else:
            participants = {'0' : ('0', team1), '00' : ('0', team2)}

        if response.meta['series_name'] == "":
            series_name = extract_data(hxs, '//div[@class="match-information-strip"]//text()').split(',')[0].strip()

        else:
            series_name = response.meta['series_name']

        if "tour" in series_name:
            country  = series_name.split(' tour of ')[-1].replace(' and ', "<>")
            record['series_name'] = {'series_name': series_name, \
            'season_start': response.meta['season_start'], \
            'season_end': response.meta['season_end'], 'country': country}

        if not record.has_key('result') : record['result'] = {}

        record['result']['0']       = {'match_title': response.meta['match_title']}
        record['game_status']       = 'scheduled'
        record['game']              = 'cricket'
        record['game_datetime']     = meta_data['dt']
        record['time_unknown']      = 1
        record['source']            = 'espn_cricket'
        record['participants']      = participants
        record['tz_info']           = tzinfo
        record['tournament']        = tou
        record['event']             = event
        record['reference_url']     = response.url
        record['source_key']        = source_key
        record['affiliation']          = response.meta['affiliation']
        if source_key in cancelled_matches:
            record['game_status'] = "Hole"

        record['time_unknown']      = response.meta['time_unknown']
        record['affiliation']       = response.meta['affiliation']
        record['rich_data']['location']         = {'city': city, 'state': state}
        record['rich_data']['stadium'] = stadium
        yield record

    @log
    def parse_schedules(self, response):
        '''Creating doc for the games having game_datetime
        '''
        hxs             = Selector(response)

        max_scores      = {}
        max_wickets     = {}
        final_scores    = {}
        record          = SportsSetupItem()
        event_name      = innings = ''
        ground_ = extract_data(hxs, '//ul//li//a[contains(@href, "ground")]//@href')
        if "/ci/content/ground/index.html" in ground_:
            ground_ = ''
        if "http" not in ground_:
            ground = "http://www.espncricinfo.com" +ground_
        innings_dict    = {'1st': '1', '2nd': '2', '3rd': '3', '4th': '4'}
        skipped_status  = ['match drawn', 'match tied', 'no result', '']
        sereies_name = extract_list_data(hxs, '//div[@class="match-information-strip"]//text()')[0] \
                            .split(',')[0].strip()
        #team_sks        = {"335977" : 624080, "335974" : 623917, "628333": 624084, "335978" : 624074}
        rich_data       = response.meta['record']['rich_data']

        teams   = extract_list_data(hxs, '//div[@class="large-11 medium-11 columns"]//a[contains(@href, "/content/team/")]/@href')
        tnames  = [self.data_replacement(i) for i in\
                  extract_list_data( hxs, '//div[@class="large-11 medium-11 columns"]//a[contains(@href, "/content/team/")]/text()')]
        if not teams and not tnames:
            teams   = extract_list_data(hxs, '//table[@class="rhTableLink"]//li/a[contains(@href, "/content/team")]/@href')
            tnames  = [self.data_replacement(i) for i in\
                     extract_list_data(hxs, '//table[@class="rhTableLink"]//li/a[contains(@href, "/content/team")]/text()')]


        tou_name    = (extract_data(hxs, '//p[@class="seriesText"]//text()').split('-')[0].strip() or\
                      extract_data(hxs, '//div[@class="large-13 medium-13 columns innings-information"]\
                      //a[@class="headLink"]/text()').split('-')[0].strip().replace('Pepsi ', '').strip())
        team1 = team2 = ''
        if not tou_name:
            tou_name = extract_data(hxs, '//h1[@class="SubnavSitesection"]//text()').split(',')[0].split('-')[0].strip()
            if not tou_name:
                tou_name = extract_data(hxs, '//div[@class="match-information-strip"]/text()')
                if "Indian Premier League" in tou_name:
                    tou_name = "Indian Premier League"
        try:
            team1   = teams[0].split('/')[-1].replace('.html', '').strip()

            team2   = teams[1].split('/')[-1].replace('.html', '').strip()
        except:
            if "Zimbabwe v India" in tou_name:
                tnames  = ['Zimbabwe', 'India']
                team1   = '9'
                team2   = '6'
            if "Namibia v Hong Kong" in tou_name:
                tnames  = ['Namibia', 'Hong Kong']
                team1   = '19'
                team2   = '28'
        if not teams and "TBA" in response.meta['match_title']:
            team1   = 'tbd1'
            team2   = 'tbd2'
        if not teams and "TBA" not in response.meta['match_title']:
            if ":" in response.meta['match_title']:
                teams = "".join(re.findall(r': (.*) at', response.meta['match_title'])).split('v')
                tnames = teams
            else:
                teams = response.meta['match_title'].split(' at ')[0].split(' v ')
                tnames = teams
            team1 = self.get_team_name(teams[0].strip())
            team2 = self.get_team_name(teams[1].strip())
        tournament      = self.get_tournament(tou_name.lower())
        if " v " in tournament:
            tournament = response.meta['record']['tournament']
        event_name      = self.get_event(tou_name.lower())
        if not tou_name:
            tournament    = response.meta['record']['tournament']
            event_name  = response.meta['record']['event']
        loc =  extract_data(hxs, '//div[@class="archiveCardContainer"]//div[@class="headRightDiv"]//li[2]/a/text()') or\
              extract_data(hxs, '//div[contains(@class, " match-information")]/div/following-sibling::div[contains(text(), "Played at")]/a/text()')
        if loc:
            loc     = loc.split(',')
            stadium = loc[0].strip()
            city    = state = ''
            if len(loc) == 2:
                city    = loc[1].strip()
                state   = ''
            elif len(loc) == 3:
                city    = loc[2].strip()
        else:
            stadium = city = state = ''

        if city == '':
            city =  response.meta['match_title'].split(' at ')[-1].split('-')[0]
            if "," in city:
                city = city.split(',')[0]
                state = ''
            else:
                city = city
                state = ''
        if city == "TBA":
            city = ''

        location = {'city' : city, 'state' : state}
        tzinfo = ''
        if city:
            tzinfo = get_tzinfo(city = city)

        _id     = response.url.split('/')[-1].split('.html')[0].strip()
        if _id in skipped_matches:
            return

        if len(teams)==2:
            pts     = {team1: (0, tnames[0].strip()), team2: (0, tnames[1].strip())}
            pt_name = {tnames[0]: team1, tnames[1]: team2}
        elif len(teams)==1:
            pts     = {team1: (0, tnames[0]), "tbd2": (0, "TBA2")}
            pt_name = {tnames[0]: team1, "TBA2": "tbd2"}

        elif 'tbd1' in team1:
            pts     = {team1: (0,"TBA1"), team2: (0, "TBA2")}
            pt_name = {"TBA1": team1, "TBA2": team2}
        else:
            pts     = {'team1 not found' : (0, 'team1'), 'team2 not found' : (0, 'team2')}
            return

        if not response.meta['file_existance']:
            response.meta['games_dict'].write(str(pt_name) + '\n')

        player_of_match = player_of_series = ''
        try:
            status_text = extract_list_data(hxs, '//p[@class="statusText"]/text()')
            if status_text:
                status_text = "".join(status_text[0]).strip().replace('\n', ' ')
            if not status_text:
                status_text = extract_data(hxs, '//div[@class="innings-requirement"]/text()').replace('\n', ' ')
        except:
            status_text = ''

        status = ''
        break_text = extract_data(hxs, '//div[@class="archiveCardContainer"]//div[@class="headLeftDiv"]\
                                                            /p[@class="breakText"]/text()')
        if not break_text:
            break_text = extract_data(hxs, '//div[contains(@class, "break-status")]/h6/text()')

        status = self.get_game_status(status_text.lower())
        if not status_text or status == 'scheduled':
            record['rich_data'] = rich_data
            if break_text:
                status_text = break_text
        elif status == 'completed':
            record = SportsSetupItem()
            record['rich_data'] = rich_data
            #match_player = extract_data(hxs, '//div[@class="bold space-top-bottom-10"]/../text()').split('\n')
            match_player = extract_list_data(hxs, '//div[contains(text(),"Player of the match")]//text()')
            if not match_player:
                match_player = extract_list_data(hxs, '//div[contains(text(),"Players of the match")]//text()')
            game_note = extract_data(hxs, '//tr[@class="notesRow"]/td/b[contains(text(),"Series")]/following-sibling::text()').strip()
            if not game_note:
                game_note = extract_data(hxs, '//div[@class="match-information"]//span[contains(text(), "series")]//text()').strip()

            rich_data['game_note'] = game_note
            if not game_note:
                rich_data['game_note'] =''

            if not match_player or match_player[0] == '':
                match_player = extract_data(hxs, '//tr[@class="notesRow"]/td/b[contains(text(),"Players of the match")]/../text()')
            if match_player:
                if len(match_player)==2:
                    player_of_match = match_player[1]
                    player_of_match = self.data_replacement(player_of_match)
                    player_of_series = ''
                else:
                    player_of_match = match_player[1]
                    player_of_match = self.data_replacement(player_of_match)
                    player_of_series = match_player[3]
                    player_of_series =self.data_replacement(player_of_series)

            elif not player_of_match:
                match_player = extract_list_data(hxs, '//tr[@class="notesRow"]/td/b[contains(text(),"Player of the match")]/following-sibling::text()')
                if match_player:
                    player_of_match = "".join(match_player[0]).strip().replace('\n', '').strip()
                if not player_of_match and (len(match_player) == 2):
                    player_of_match = "".join(match_player[1]).strip().replace('\n', '').strip()
                if not player_of_match and (len(match_player) == 4 or len(match_player) == 5):
                    player_of_match = "".join(match_player[-1]).strip().replace('\n', '').strip()
                if not player_of_match and len(match_player) == 7:
                    player_of_match = "".join(match_player[-3]).strip().replace('\n', '').strip()

            elif not player_of_match:
                player_of_match = ''
                match_player    = extract_data(hxs, '//tr[@class="notesRow"]/td/b[contains(text(),"Player of the match")]/../text()').split('\n')
                if match_player:
                    for player in match_player:
                        if 'Player of the match' in player:
                            player_of_match = player.strip('Player of the match').strip()

            if not player_of_match:
                game_keys = extract_list_data(hxs, '//div[@class="match-information"]/div[@class="bold space-top-bottom-10"][2]/text()')
                game_values = extract_list_data(hxs, '//div[@class="match-information"]/div[@class="bold space-top-bottom-10"][2]/span[@class="normal"]/text()')
                for ind, key in enumerate(game_keys):
                    if "player of the match" in key.strip().lower():
                        player_of_match = game_values[ind]
                    elif not player_of_series and ("player of the series" in key.strip().lower()):
                        player_of_series = game_values[ind]

            if not player_of_series:
                try:
                    player_of_series = extract_list_data(hxs, '//tr[@class="notesRow"]/td/b[contains(text(),"Player of the series")]/following-sibling::text()')
                    if player_of_series:
                        player_of_series = player_of_series[0].replace('\n', '')
                    if len(player_of_series) == 7:
                        player_of_series = "".join(player_of_series[-1]).replace('\n', '').strip()
                    else:
                        player_of_series = "".join(player_of_series[1:]).replace('\n', '').strip()
                except:
                    pass

        if not status:
            status = 'scheduled'
            game_note = ''

        final_scores = {}
        record['source_key']           = _id
        record['game']                 = 'cricket'
        record['source']               = 'espn_cricket'
        record['tournament']           = tournament
        rich_data['location']          = location
        rich_data['stadium']           = stadium
        record['game_datetime']        = response.meta['dt']
        record['reference_url']        = response.url
        record['participants']         = pts
        record['game_status']          = status
        record['affiliation']          = response.meta['affiliation']
        if response.meta['series_name'] == "":
            series_name = extract_data(hxs, '//div[@class="match-information-strip"]//text()').split(',')[0].strip()
        else:
            series_name = response.meta['series_name']
        if "tour" in series_name.lower():
            country  = series_name.split(' tour of ')[-1].replace(' and ', "<>")
            record['series_name'] = {'series_name': series_name, 'season_start': response.meta['season_start'], \
            'season_end': response.meta['season_end'], 'country': country}
        if record['game_status'] == "scheduled":
            rich_data['game_note']                      = ''
            game_note                                   = ''
        elif record['game_status'] == "ongoing":
            rich_data['game_note']                      = ''
            game_note                                   = ''
        elif record['game_status'] == "no_result":
            rich_data['game_note']                      = ''
            game_note                                   = ''
        if _id  in cancelled_matches:
            record['game_status']      = "Hole"
            game_note                  = status_text
        if "natwest-t20-blast-2015" in response.meta['tou_link']:
            event_name = "Nntwest t20 blast"
            tournament = "English County cricket"
            record['tournament']           = tournament
        if "royal-london-one-day-cup" in response.meta['tou_link']:
            tournament = "English County cricket"
            record['tournament']           = tournament
            event_name = "Royal London One-Day Cup"
        record['event']                = event_name
        record['time_unknown']         = response.meta['time_unknown']
        record['tz_info']              = tzinfo

        if '0' not in final_scores:
            final_scores['0'] = {}

        if status != "scheduled":
            record['rich_data'] = rich_data
            scores = (get_nodes(hxs, '//td[@class="inningsDetails"]/b[contains(text(), "Total")]/../..')
                        or get_nodes(hxs, '//td[@class="total"]/b[contains(text(), "Total")]/../..'))
            inn_count = 0
            for score in scores:
                team_name = (extract_list_data(score, './/preceding::td[@colspan="2"]/span/../text()') or \
                            extract_list_data(score, './preceding::tr[@class="tr-heading"]/th[@colspan]/span[@class="uppercase"]/text()|./preceding::tr[@class="tr-heading"]/th[@colspan]/text()') or \
                            extract_list_data(score, '/preceding::tr[contains(@class, "inningsHead")]/td[contains(@colspan, "2")]/text()')\
                            or extract_list_data(score, './/preceding::td[contains(@colspan, "2")]/text()'))

                if len(team_name) == 3 and '\t' in team_name[2]:
                    team_name = ["".join(team_name[:2])]
                elif len(team_name) == 2  and '\t' in team_name[1]:
                    tm_name = []
                    tm_name.append("".join(team_name[:1]))
                    team_name = tm_name
                elif len(team_name) == 4 and '\t' in team_name[3]:
                    tm_name = []
                    tm_name.append("".join(team_name[0]))
                    tm_name.append("".join(team_name[2]))
                    team_name = tm_name

                elif len(team_name) == 6 and '\t' in team_name[5]:
                    tm_name = []
                    tm_name.append("".join(team_name[:2]))
                    tm_name.append("".join(team_name[3:5]))
                    team_name = tm_name
                elif len(team_name) == 8 and '\t' in team_name[7]:
                    tm_name = []
                    tm_name.append("".join(team_name[:2]))
                    tm_name.append("".join(team_name[3:5]))
                    tm_name.append("".join(team_name[6]))
                    team_name = tm_name

                elif len(team_name) == 9:
                    tm_name = []
                    tm_name.append("".join(team_name[:2]))
                    tm_name.append("".join(team_name[3:5]))
                    tm_name.append("".join(team_name[6:8]))
                    team_name = tm_name
                elif len(team_name) == 12:
                    tm_name = []
                    tm_name.append("".join(team_name[:2]))
                    tm_name.append("".join(team_name[3:5]))
                    tm_name.append("".join(team_name[6:8]))
                    tm_name.append("".join(team_name[9:11]))
                    team_name = tm_name
                if team_name: team_name = (team_name[-1]).replace('innings', '').strip().split('(')[0]
                for d in innings_dict:
                    if d in team_name:
                        team_name = team_name.replace(d, '').strip()
                        innings = innings_dict[d]
                        break

                team_name = self.data_replacement(team_name)
                if team_name:
                    pid     = pt_name[team_name]

                runs = (extract_data(score, './/td[@class="battingRuns"]//text()') or\
                        extract_data(score, './/td[@class="bold"]//text()')).strip().replace('\n', ' ')

                overs_wickets = extract_data(score, './/td[@class="battingDismissal"]/text()')\
                or extract_data(score, './/td[@class="total-details"]/text()')
                overs = re.findall(';(.*?)overs', overs_wickets)
                if overs: overs = overs[0].strip()
                wickets = re.findall('(.*?);', overs_wickets)
                if wickets:
                    if "wickets dec" in wickets[0]:
                        runs = runs + 'd'

                    wickets = wickets[0].strip('(').split('wicket')[0].strip()
                if "all out" in wickets: wickets = 10
                inn_count += 1
                if innings:
                    if not final_scores.has_key(pid): final_scores[pid] = {}
                    if inn_count <= 2:
                        inn = ''
                    else:
                        inn = '2'
                    final_scores[pid].update({'score%s' % inn : runs, 'overs%s' % inn: overs, 'wickets%s' % inn : wickets})
                    final_scores['0']['innings%s' % inn_count] = pid
                else:
                    final_scores[pid] = {'score': runs, 'overs': overs, 'wickets': wickets}

        if status == "completed":
            tot_scores, pid = {}, ''
            all_teams = hxs.select('//table[contains(@id,"inningsBat")]//tr[@class="inningsHead"]/td[@colspan]/text()').extract()
            details = hxs.select('//table[contains(@id,"inningsBat")]//tr[@class="inningsRow"]')
            for i, detail in enumerate(details):
                t_name = "".join(detail.select('./..//tr[@class="inningsHead"]/td[@colspan]/text()')[-1].extract()).strip().split('(')[0]
                team_name = self.data_replacement(t_name)
                pid = pt_name[team_name]
                player_name = extract_data(detail, './/td[@class="playerName"]/a/text()')
                pl_sk = re.findall(r'player/(\d+).html', extract_data(detail, './/td[@class="playerName"]/a/@href'))
                player_runs = extract_data(detail, './/td[@class="battingRuns"]/text()')
                player_ball = extract_data(detail, './/td[@class="battingDetails"][2]/text()')
                if not tot_scores.has_key(t_name):
                    tot_scores[t_name] = {}
                if pl_sk and player_runs.isdigit():
                    tot_scores[t_name][pl_sk[0]] = [int(player_runs), int(player_ball), int(pid)]

            max_scores = {}
            for i, j in tot_scores.iteritems():
                _max = [0, 0, 0]
                player = ''
                max_scores[i] = {}
                for key, val in j.iteritems():
                    if int(val[0]) > _max[0]:
                        _max = [int(val[0]), int(val[1]), int(val[2])]
                        max_scores[i] = {}
                        max_scores[i][key] = _max
                    elif int(val[0]) == _max[0]:
                        max_scores[i][key] = _max

            main_teams = {}
            wikcets = {}
            max_wickets = {}
            wikets = get_nodes(hxs, '//table[contains(@id,"inningsBow")]//tr[@class="inningsRow"]')
            for wic in wikets:
                nid = "".join(re.findall(r'inningsBowl(\d+)', extract_data(wic, './../@id')))
                for key, val in enumerate(all_teams):
                    main_teams[key] = val.strip()
                if int(nid) == 1:
                    t_name = main_teams.get(1)
                if int(nid) == 2:
                    t_name = main_teams.get(0)
                if int(nid) == 3:
                    t_name = main_teams.get(3)
                if int(nid) == 4:
                    t_name = main_teams.get(2)
                if t_name:
                    team_name = self.data_replacement(t_name)
                pid = pt_name[team_name]
                player_name = extract_data(wic, './/td[@class="playerName"]/a/text()')
                pl_sk = "".join(re.findall(r'player/(\d+).html', extract_data(wic, './/td[@class="playerName"]/a/@href')))
                player_wikets = extract_data(wic, './/td[@class="bowlingDetails"][4]/text()')
                if not wikcets.has_key(t_name): wikcets[t_name] = {}
                if player_name and player_wikets:
                    if wikcets[t_name].has_key(pl_sk):
                        wikcets[t_name][pl_sk] = [player_wikets, int(pid)]
                    else:
                        wikcets[t_name][pl_sk] = [player_wikets, int(pid)]

            max_wickets = {}
            for i, j in wikcets.iteritems():
                _max = [0, 0]
                max_wickets[i] = {}
                for key, val in j.iteritems():
                    if int(val[0]) > _max[0]:
                        _max = [int(val[0]), int(val[1])]
                        max_wickets[i] = {}
                        max_wickets[i][key] = _max
                    elif int(val[0]) == _max[0]:
                        max_wickets[i][key] = _max

        if final_scores and status == 'completed':
            if int(final_scores[team1]['score'].split('d')[0]) > int(final_scores[team2]['score'].split('d')[0]):
                winner = team1
            else:
                winner = team2

        else: winner = ''
        if 'won' in status_text.lower() and 'won the toss' not in status_text.lower():
            points =  pts
            if points:
                for key, value in points.iteritems():
                    if value[1].lower() == status_text.lower().split('won')[0].strip():
                        if winner != key:
                            response.meta['winners_file'].write(str(_id) + ' ' + str(winner) + ' ' + str(key) + '\n')
                        winner = key

        if status_text.lower() in skipped_status or "match tied" in status_text.lower():
            winner = ''

        if max_scores:
            for inn, results in max_scores.iteritems():
                innings = re.findall(r' (\d+).* innings', inn)
                if innings: innings = innings[0].strip()
                stat_count = 0
                for psk, rec in results.iteritems():
                    stat_count += 1
                    #final_scores[str(rec[2])]['max_runs_id_%s%s' %(innings, stat_count)] = psk
                    values = str(rec[0]) + "(" + str(rec[1]) + ")"
                    #final_scores[str(rec[2])]['max_runs_values_%s%s' %(innings, stat_count)] = values

        if max_wickets:
            for inn, results in max_wickets.iteritems():
                try:
                    innings = re.findall(r' (\d+).* innings', inn)
                except:
                    pass
                if innings: innings = innings[0].strip()
                stat_count = 0
                for psk, rec in results.iteritems():
                    stat_count += 1
                    #final_scores[str(rec[1])]['max_wickets_id_%s%s' %(innings, stat_count)] = psk
                    values = str(rec[0])
                    #final_scores[str(rec[1])]['max_wickets_values_%s%s' %(innings, stat_count)] = values

        final_scores['0'].update({'match_result': status_text, 'session_text': break_text, 'man_of_the_match'\
                                : player_of_match.strip(), 'man_of_the_series': player_of_series, 'match_title'\
                                : response.meta['match_title'], 'winner': winner})

        for key, value in pts.iteritems():
            if key in tbd_ids:
                final_scores[key].update({'tbd_title' : value[1]})
        record['result'] = final_scores
        rich_data['game_id'] = _id
        record['participant_type'] = 'team'
        if not ground_:
            yield record
        yield Request(ground, callback =self.parse_location, dont_filter=True, meta = {'record': record, 'game_note': game_note})

    def parse_location(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        game_note = response.meta['game_note']
        rich_data  = response.meta['record']['rich_data']
        location =  extract_data(hxs, '//p[@class="loc"]//text()')
        country = location.split(',')[-1].strip()
        city = location.split(',')[-2].strip()
        if city == "TBA":
            city = ''
        tz_info = extract_data(hxs, '//div[@id="stats"]/text()')
        tz_info = tz_info.split(',')[-1].split('UTC')[-1].split('00)')[0].strip().replace('1030', '10:30').replace(')', '').replace('0530', '05:30').replace('0100', '01')
        stadium = extract_data(hxs, '//div[@id="head"]//h1/text()').strip()
        if "M.Chinnaswamy Stadium" in stadium:
            stadium = "M. Chinnaswamy Stadium"
        if "MA Chidambaram Stadium" in stadium:
            stadium = "M. A. Chidambaram Stadium"
        record['tz_info'] = tz_info
        rich_data['stadium'] =  stadium
        rich_data['game_note'] = game_note
        rich_data['location'] = {'city': city, 'country': country}
        yield record

