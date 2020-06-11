'''Spider for cricket schedules and scores
'''
import os
import re
import time
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data, log, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from scrapy_spiders_dev.items import SportsSetupItem

skipped_matches = ['730587', '730589', '730081', '756031', '756033', '754715', '760779', '779097', '754733', '754735', '722327', '810279', '754745']
cancelled_matches = ['770131', '780257', '770133', '770135', '770137', '754733', '722327', '810279']
tbd_ids = ['3600', '3760']


class IccCricketWorld(VTVSpider):
    '''Class function
    '''
    name            = "cricket_iccworld"
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
    start_urls  = ["http://www.espncricinfo.com/ci/content/current/match/fixtures_futures.html"]

    def get_team_name(self, team):
        teams_tuple = ("india", "west indies", "sri lanka", "australia", "pakistan", "zimbambwe", "new zealand", \
                      "england", "south africa", "bangladesh", "canada", "scotland", "netherlands", "ireland", \
                      "kenya", "bermuda", "tba")
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
                        'Cricket Australia XI': '', "New Zealand Chairman's" : 'N Zealanders'}
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
                    'dubai triangular series': 'ODI Cricket'}

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
        hxs     = HtmlXPathSelector(response)
        if not self.status:
            self.status         = True
            cricket_tournaments = ['http://www.espncricinfo.com/icc-champions-trophy-2013/engine/series/566910.html', \
                                    'http://www.espncricinfo.com/tri-nation-west-indies-2013/engine/series/597920.html', \
                                    'http://www.espncricinfo.com/england-v-new-zealand-2013/engine/series/566893.html', \
                                    'http://www.espncricinfo.com/champions-league-twenty20-2013/engine/series/629521.html', \
                                    'http://www.espncricinfo.com/world-t20/engine/current/series/628368.html', \
                                    'http://www.espncricinfo.com/indian-premier-league-2014/engine/current/series/695871.html',\
                                    'http://www.espncricinfo.com/ci/engine/series/566905.html']
            cricket_tournaments = ['http://www.espncricinfo.com/champions-league-twenty20-2014/engine/series/763543.html',
                                   'http://www.espncricinfo.com/ci/engine/current/series/691693.html',
                                   'http://www.espncricinfo.com/zimbabwe-triangular-series-2014/engine/series/736429.html',
                                   'http://www.espncricinfo.com/carlton-mid-triangular-series-2015/engine/series/754623.html',
                                   'http://www.espncricinfo.com/pakistan-v-new-zealand-2014/engine/series/742601.html', \
                                   'http://www.espncricinfo.com/australia-v-india-2014-15/engine/series/656517.html', \
                                   'http://www.espncricinfo.com/icc-cricket-world-cup-2015/engine/series/509587.html', \
                                   'http://www.espncricinfo.com/ci/engine/series/812621.html', \
                                   'http://www.espncricinfo.com/ci/engine/series/803351.html'
                                   ]
            cricket_tournaments = ['http://www.espncricinfo.com/icc-cricket-world-cup-2015/engine/series/509587.html']
            for tournaments in cricket_tournaments:
                print tournaments
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
                    reference = "http://www.espncricinfo.com/icc-cricket-world-cup-2015/engine/series/509587.html"
                yield Request(reference, callback=self.parse_second)

    @log
    def parse_second(self, response):
        '''Creating doc for the tournament link
        '''
        hxs     = HtmlXPathSelector(response)
        record  = SportsSetupItem()
        nodes   = get_nodes(hxs, '//p[@class="potMatchHeading"]|//h2')
        for node in nodes:
            rich_data   = {}
            match_link  = extract_data(node, './span/a/@href')
            match_title = extract_data(node, './/text()').\
                                replace('\n', '').replace('\t', '').replace('  ', '').strip()
            match_string = extract_data(node, './span/a/text()')
            game_data = extract_data(node, './text()')
            game_date = game_data.rsplit('- ', 1)[-1].strip()
            game_time = extract_data(node, './following-sibling::p[@class="potMatchText mat_status"][1]//text()|./following-sibling::span[@class="show-for-small"][1]//text()')
            game_time = game_time.split('at')[-1].split('local time')[0].strip()
            time_unknown = 0
            if  ":" not in game_time:
                game_time = ""
            else:
                game_time = game_time

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

                if "Group" in event:
                    event   = "".join(re.findall(r', (.*)', event))
                    event   = tou + ' ' + event
                else:
                    event   = tou + event.replace('-', '') + 's' 
            if match_link:
                match_link = match_link+"?view=scoreboard"
                if not "http" in match_link:
                    match_link = "http://www.espncricinfo.com" + match_link
                rich_data['match_string']   = match_string
                record['affiliation']       = affiliation
                record['tournament']        = tou
                record['event']             = event
                record['rich_data']         = rich_data
                yield Request(match_link, callback = self.parse_schedules, \
                              meta={'rich_data' : rich_data, 'record' : record, 'games_dict' : self.games_dict, \
                              'winners_file' : self.winners_file, 'date' : game_date, 'time': game_time, 'file_existance' : self.file_existance, \
                              'match_title': match_title, 'time_unknown': time_unknown})
    @log
    def parse_schedules(self, response):
        '''Creating doc for the games having game_datetime
        '''

        hxs             = HtmlXPathSelector(response)
        max_scores      = {}
        max_wickets     = {}
        final_scores    = {}

        record      = SportsSetupItem()
        event_name  = innings = ''
        ground      = extract_data(hxs, '//ul//li//a[contains(@href, "ground")]//@href')
        ground      = "http://www.espncricinfo.com" +ground

        innings_dict    = {'1st': '1', '2nd': '2', '3rd': '3', '4th': '4'}
        skipped_status  = ['match drawn', 'match tied', 'no result', '']
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
                      //a[@class="headLink"]/text()').split('-')[0].strip())

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
        if not teams:
            team1   = 'tbd1'
            team2   = 'tbd2'


        tournament      = self.get_tournament(tou_name.lower())

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

        location = {'city' : city, 'state' : state}
        if city == '':
            city =  response.meta['match_title'].split(' at ')[-1].split('-')[0]
            if "," in city:
                city = city.split(',')[0]
            else:
                city = city
        if response.meta['time']:
            game_date = response.meta['date'].strip() + " " + response.meta['time']
            pattern = "%b %d, %Y %H:%M"
        else:
            game_date = response.meta['date'].strip()
            pattern = "%b %d, %Y"
        if city == "Melbourne":
            game_datetime = get_utc_time(game_date, pattern, 'Australia/Melbourne')
        elif city in ["Auckland", "Wellington", "Napier", "Nelson", "Hamilton", "Dunedin", "Christchurch"]:
            game_datetime = get_utc_time(game_date, pattern, 'Pacific/Auckland')
        elif "Perth" in city:
            game_datetime = get_utc_time(game_date, pattern, 'Australia/Perth')
        elif "Hobart" in city or "Canberra" in city:
            game_datetime = get_utc_time(game_date, pattern, 'Australia/Hobart')
        elif "Sydney" in city:
            game_datetime = get_utc_time(game_date, pattern, 'Australia/Sydney')
        elif "Adelaide" in city:
            game_datetime = get_utc_time(game_date, pattern, 'Australia/Adelaide')
        elif "Brisbane" in city:
            game_datetime = get_utc_time(game_date, pattern, 'Australia/Brisbane')
        tzinfo = ''
        if city:
            tzinfo = get_tzinfo(city = city)

        _id     = response.url.split('/')[-1].split('.html')[0].strip()
        if _id in skipped_matches:
            return
        if len(teams)==2:
            pts     = {team1: (0, tnames[0]), team2: (0, tnames[1])}
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
                player_of_match = extract_data(hxs, '//div[@class="match-information"]/div[@class="bold space-top-bottom-10"][2]/span[@class="normal"][2]/text()')

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
        record['game_datetime']        = game_datetime
        record['reference_url']        = response.url
        record['participants']         = pts
        record['game_status']          = status
        if record['game_status'] == "scheduled":
            rich_data['game_note']                      = ''
        elif record['game_status'] == "ongoing":
            rich_data['game_note']                      = ''
        if _id  in cancelled_matches:
            record['game_status']      = "Hole"
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
                                : player_of_match, 'man_of_the_series': player_of_series, 'match_title'\
                                : response.meta['match_title'], 'winner': winner})

        for key, value in pts.iteritems():
            if key in tbd_ids:
                final_scores[key].update({'tbd_title' : value[1]})

        record['result'] = final_scores
        rich_data['game_id'] = _id
        record['participant_type'] = 'team'
        yield Request(ground, callback =self.parse_location, dont_filter=True, meta = {'record': record, 'city': city})

    def parse_location(self, response):
        hxs = HtmlXPathSelector(response)
        record = response.meta['record']
        rich_data  = response.meta['record']['rich_data']
        location =  extract_data(hxs, '//p[@class="loc"]//text()')
        country = location.split(',')[-1].strip()
        city = location.split(',')[0].strip()
        tz_info = extract_data(hxs, '//div[@id="stats"]/text()')
        tz_info = tz_info.split(',')[-1].split('UTC')[-1].split('00)')[0].strip().replace('1030', '10:30').replace(')', '').replace('0530', '05:30')
        stadium = extract_data(hxs, '//div[@id="head"]//h1/text()').strip()
        if "W.A.C.A. Ground" in stadium:
            stadium = "WACA Ground"
        record['tz_info'] = tz_info
        rich_data['stadium'] =  stadium
        rich_data['location'] = {'city': city, 'country': country}
        import pdb;pdb.set_trace()
        yield record
