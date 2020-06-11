from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import  SportsSetupItem
from vtvspider_dev import VTVSpider, get_nodes, extract_data, \
extract_list_data, log, get_utc_time, get_tzinfo
import urllib2
import urllib
import re

TBD_TEAMS = ["Irvin", "Carter", "AFC Champ", "NFC Champ"]

LOC_INFO = {"NE": ('Foxborough', 'Massachusetts'), \
            "DEN": ("Denver", 'Colorado'), \
            "SEA" : ("Seattle", "Washington"), \
            "GB" : ("Green Bay", "Wisconsin"), \
            "CIN" : ('Cincinnati', "Ohio"), \
            "IND" : ('Indianapolis', "Indiana"), \
            "BAL" : ('Baltimore', "Maryland"), \
            "PIT" : ('Pittsburgh', 'Pennsylvania'), \
            "ARI" : ('Glendale', 'Arizona'), \
            "CAR" : ('Charlotte', 'North Carolina'), \
            "DET" : ('Detroit', 'Michigan'), \
            "DAL" : ('Irving', "Texas"), \
            "BUF" : ('Orchard Park', 'New York'), \
            "MIA" : ('Miami', 'Florida'), \
            "NYJ" : ('East Rutherford', 'New Jersey'), \
            "HOU" : ('Houston', 'Texas'), \
            "CLE" : ('Cleveland', 'Ohio'), \
            "JAC" : ('Jacksonville', 'Florida'), \
            "TEN" : ('Nashville', 'Tennessee'), \
            "KC" : ('Kansas City', 'Missouri'), \
            "SD" : ('San Diego', 'California'), \
            "OAK" : ('Oakland', 'California'), \
            "PHI" : ('Philadelphia', 'Pennsylvania'), \
            "NYG" : ('East Rutherford', 'New Jersey'), \
            "WAS" : ('Landover', 'Maryland')}

def player_sks(players, name):
    for i, j in players.iteritems():
        if i == name:
            pl_url = "http://www.nfl.com/players/profile?id=%s" % (j)
            req = urllib2.Request(pl_url)
            res = urllib2.urlopen(req)
            pid = "".join(re.findall(r'(\d+)/profile', res.geturl()))
            return pid

class NFLSpider(VTVSpider):
    name = 'nflspider'
    start_urls = []

    def start_requests(self):
        top_urls = []
        urls = []
        reg_reference = 'http://www.nfl.com/schedules/2015/REG%s'
        pre_reference = 'http://www.nfl.com/schedules/2015/PRE%s'
        post_reference = 'http://www.nfl.com/schedules/2015/POST'

        for i in range(1, 18):
            top_urls.append(reg_reference%i)

        for i in range(5):
            top_urls.append(pre_reference%i)

        if "/POST" in post_reference:
            top_urls.append(post_reference)
        for ref in top_urls:
            if "POST" in ref:
                year = "2016"
            else:
                year = "2015"
        if self.spider_type == 'schedules':
            yield Request(ref, self.parse, meta={'year': year})

        else:
            top_url = 'http://www.nfl.com/scores'
            yield Request(top_url, self.parse, meta={'year': year})

    def get_teams(self, team_name):
        teams_dict = {'Sanders' : 'NFC', 'Rice' : 'AFC'}
        team = teams_dict.get(team_name, '')
        if not team:
            return team_name
        return team

    def replacing_time(self, string):
        replacing_time  = ['st', 'nd', 'rd', 'th', '\r', '\n', \
        '\t', 'amp;', ' ET', '  ']
        for r_time in replacing_time:
            date    = string.replace(r_time, '').strip()
        return date

    def get_tz_info(self, loc_city, stadium):
        if loc_city and stadium == "Wembley Stadium":
            _country = "United Kingdom"
        else:
            _country = "USA"
        tz_info = get_tzinfo(city= loc_city[0])
        if not tz_info:
            tz_info = get_tzinfo(country= _country)
        return tz_info

    @log
    def parse(self, response):
        hxs = Selector(response)
        match_date      = ''
        year = response.meta['year']
        tou_name        = ''
        other_data      = response.body
        conference = re.findall('"(.*)" : {.* "conference": "(.*)", "division"', other_data)

        if 'schedules'  in self.spider_type:
            game   = get_nodes(hxs, '//li[contains(@class, "schedules-list-")]')
            count = 0
            status = 0
            #radio_nodes = extract_list_data(hxs, '//div[@class="schedules-header-links-icons"]//a/@title')
            #radio_ = radio_nodes[1] + ' ' + "Radio"
            #online_ = radio_nodes[2] + "<>" + radio_nodes[3]

            for game_node in game:
                result = {}
                _date = extract_data(game_node, \
                        './/span[@class=""]/span/text()')
                tou_name = extract_data(game_node, \
                         './/span[@class="post"]//text()')
                tou_name = 'NFL Football'
                if 'Next Game' in _date:
                    continue

                if _date and tou_name:
                    match_date  = self.replacing_time(_date)
                    continue

                home_team = extract_data(game_node, \
                            './/span[contains(@class, \
                            "team-name home")]/text()').strip()
                away_team = extract_data(game_node, './/span[contains(@class, \
                            "team-name away")]/text()').strip()
                home_team = self.get_teams(home_team)
                away_team = self.get_teams(away_team)

                match_time = extract_data(game_node, \
                            './/div[@class="list-matchup-row-time"]/\
                            span[@class="time"]/text()').strip()
                am_pm = extract_data(game_node, \
                        './/div[@class="list-matchup-row-time"]/\
                        span[@class="suff"]/span[1]/text()')
                channels = extract_list_data(game_node, './/div/span[contains(@class, "nflicon")]/@title')
                if channels:
                    channel_info = '<>'.join(channel for channel in channels if channel)
                if "FINAL" in match_time or not match_date:
                    continue

                event_info = extract_data(game_node, './/preceding-sibling::li/span[@class="post"]/text()')


                if match_time and "ET" in match_time:
                    match_time = match_time.replace("ET", "").strip()
                channel = extract_data(game_node, './/div/span[contains(@class, "nflicon")][1]/@title')

                if match_date:
                    fdate = match_date + " " + year + " " + match_time + " " + am_pm
                    game_datetime = get_utc_time(fdate, \
                                    '%A, %B %d %Y %I:%M %p', 'US/Eastern')
                status += 1
                game_link = extract_data(game_node, './/div[@class="list-matchup-row-gc-link"]/a/@href')
                if game_link and "gamecenter" in game_link:
                    game_id = re.findall(r'gamecenter/(.*)icampaign', \
                              game_link)[0].replace('?', '').strip()
                else:
                    game_link = extract_data(game_node, './/div[contains(@class, "pre expandable")]/@data-gc-url')
                    if game_link:
                        game_id = re.findall(r'gamecenter/(.*)', game_link)[0].\
                              replace('?', '').strip()
                channel_info = channel_info
                #rich_data       = {"game_note": "", 'location': ''}
                home_callsign = extract_list_data(game_node, \
                                './/following-sibling::div[contains(@class, \
                                "schedules-list-content")]/@data-home-abbr')
                away_callsign = extract_list_data(game_node, \
                                './/following-sibling::div[contains(@class, \
                                "schedules-list-content")]/@data-away-abbr')
                if home_team in TBD_TEAMS:
                    home_callsign = ['TBA1']
                    result[home_callsign[0]] = {'tbd_title': home_team}
                if away_team in TBD_TEAMS:
                    away_callsign = ['TBA2']
                    result[away_callsign[0]] = {'tbd_title': away_team}

                if not home_team:
                    home_team = "TBD"
                    home_callsign = ['TBA1']
                if not away_team:
                    away_team = "TBD"
                    away_callsign = ['TBA2']


                participants = [{'callsign': str(home_callsign[0]),
                                'name': str(home_team)},
                                {'callsign': str(away_callsign[0]),
                                'name': str(away_team)}]

                record                      = SportsSetupItem()
                #record['rich_data']         = rich_data
                record['game']              = 'football'
                record['affiliation']       = 'nfl'
                record['source']            = 'NFL'
                record['participant_type']  = 'team'
                record['game_status']       = 'scheduled'
                record['game_datetime']     = game_datetime
                record['participants']      = participants
                record['result']            = result
                record['tournament']        = tou_name
                record['reference_url']     = game_link
                record['time_unknown']      = "0"
                event = ''
                for values in conference:
                    key, value = values
                    if str(away_callsign[0]) == key:
                        conf = value
                if "/PRE" not in response.url:
                    continue
                if response.url.endswith('/PRE0') and status == 1:
                    event = "Pro Football Hall of Fame Game"
                elif response.url.endswith('/REG1') and status == 1:
                    event = "NFL Kickoff Game"
                elif '/PRE' in response.url:
                    event = "NFL Preseason"
                elif '/REG' in response.url:
                    event = "NFL Regular Season"
                elif "Wild Card Weekend" in event_info:
                    event = conf + " Wild Card Round"
                elif "Divisional Playoffs" in event_info:
                    event = conf + " Divisional Round"
                elif "Conference Championships" in event_info:
                    event =  conf + " Championship Game"
                elif "Pro Bowl" in event_info:
                    event = "Pro Bowl"
                elif "Super Bowl" in event_info:
                    event = "Super Bowl"
                record['event'] = event
                if game_link:
                    record['source_key']        = game_id
                    yield Request(game_link, callback=self.parse_location_details, \
                                  meta = {'top_url' : response.url, \
                                  'record' : record, 'status' : status, 'channel_info': channel_info, 'team_sk': str(away_callsign[0])})
                else:
                    game_id = game_datetime.split(' ')[0] + "_" + event.replace(' ', '-') + "_" + home_team + "_" + away_team
                    if game_id == "2015-01-18_NFL-Conference-Championships_TBD_TBD":
                        count += 1
                        game_id = "2015-01-18_NFL-Conference-Championships_TBD_TBD_%s" % (count)
                    city = state = ''
                    for key, value in LOC_INFO.iteritems():
                        if str(home_callsign[0]) in key:
                            city, state = value
                    record['source_key']        = game_id
                    record['rich_data'] = {'channels': str(channel_info), \
                    'location': {'city': city, 'state': state, 'country': "USA"}}
                    record['tz_info'] = get_tzinfo(city = city)
                    #yield record
                    season_years        = extract_list_data(hxs, '//ol[@class="year-selector"]//li/@data-value')
                    active_season_games = extract_list_data(hxs, '//div[@class="scores-seasons-wrapper"]\
                            //a[contains(@class, "active")]/following-sibling::ul//li/a/@href')
        else:
            game   = get_nodes(hxs, '//li[contains(@class, "schedules-list-")]')
            season_years        = extract_list_data(hxs, '//ol[@class="year-selector"]//li/@data-value')
            active_season_games = extract_list_data(hxs, '//div[@class="scores-seasons-wrapper"]\
                                    //a[contains(@class, "active")]/following-sibling::ul//li/a/@href')
            for page_url in active_season_games:
                page_url = 'http://www.nfl.com' + page_url
                yield Request(page_url, callback = self.parse_details, \
                        meta={'season_year': season_years[0], 'conference': conference})

    @log
    def parse_location_details(self, response):
        items = []
        record = response.meta['record']
        link = response.url
        other_data      = response.body
        location = re.findall('city\t\t: "(.*?)",', other_data)
        tz_info = ''
        channel = response.meta['channel_info']
        other_data      = re.sub('chapter.caption.txt.*', '', other_data)
        stadium_details = re.findall('stadium\t\t: "(.*?)",', other_data)
        if location:
            loc_city = location[0]
            tz_info = self.get_tz_info(loc_city, stadium_details)
        sign_id         = re.findall('ASSET_BUILD:.*"(.*?)",', other_data)
        js_page = "http://combine.nflcdn.com/yui/min2/index.php?5.9" + sign_id[0]\
                 + "&b=yui3%2Fstatic%2F5.9%2Fscripts%2Fgamecenter&ss=anyraw&f=app/view/gameinfo.js"
        js_page         = urllib.urlopen(js_page).read()

        channel_info    = re.findall('_GC_landing" target="_blank">(.*?)</a>', js_page)
        if channel_info :
            channels =  channel + "<>" + channel_info[0]
        else:
            channels = channel

        radio_home      = re.findall("radio.home;desc='(.*?)';if", js_page)
        if radio_home:
            radio_home  = radio_home[0]

        if stadium_details:
            stadium_details = stadium_details[0]


        record['tz_info'] = tz_info
        record['rich_data'] = {'channels': str(channels), 'Radio': radio_home}
        record['rich_data']['stadium']  = stadium_details
        record['reference_url'] = link
        if record['game_datetime']:
            sportssetupitem = SportsSetupItem()
            for key, val in record.iteritems():
                sportssetupitem[key] = val
            items.append(sportssetupitem)
            for item in items:
                import pdb;pdb.set_trace()
                yield item
    @log
    def parse_details(self, response):
        data  = {}
        season_year     = response.meta['season_year']
        hxs             = Selector(response)

        online          = ''
        radio_home      = ''
        stadium_details = ''
        channel_info    = ''
        score_boards = get_nodes(hxs, \
            '//div[@id="score-boxes"]//div[contains(@id, "sb-wrapper")]')
        if score_boards:
            for score_board in score_boards:
                _hs     = []
                _as     = []
                _hs_ot  = []
                _aw_ot  = []

                home_team = extract_data(score_board, \
                            './/div[@class="home-team"]//\
                            p[@class="team-name"]//text()')
                home_team_callsign  = extract_data(score_board, \
                                './/div[@class="home-team"]//\
                                p[@class="team-name"]//a/@href').\
                                split('=')[-1].strip()

                away_team = extract_data(score_board, \
                            './/div[@class="away-team"]//\
                            p[@class="team-name"]//text()')
                away_team_callsign  = extract_data(score_board, \
                                     './/div[@class="away-team"]//\
                                     p[@class="team-name"]/a/@href').\
                                     split('=')[-1].strip()

                participants_info = {'home': home_team_callsign, \
                                     'away' : away_team_callsign}
                time_left = extract_data(score_board, \
                            './/span[@class="time-left"]//text()').strip()
                other_url = extract_data(score_board, \
                            './/div[@class="game-center-area"]/\
                            a[contains(@class, "game-center-link")]/@href').\
                            strip()
                game_id   = other_url.split('/', 2)[-1]
                _id       = "".join(re.findall(r'(\d+)/2014', game_id))
                other_ref = "http://www.nfl.com" + other_url
                date      = extract_data(score_board, \
                            './/span[@class="date"]//text()').strip()

                home_scores = extract_list_data(score_board, \
                              './/div[@class="home-team"]//\
                              div[@class="team-data"]//\
                              p[@class="quarters-score"]\
                              //span[not(contains(@class, "ot-qt"))]//text()')
                away_scores = extract_list_data(score_board, \
                              './/div[@class="away-team"]//\
                              div[@class="team-data"]//\
                              p[@class="quarters-score"] \
                              //span[not(contains(@class, "ot-qt"))]//text()')
                home_ot_scores = extract_list_data(score_board, \
                                './/div[@class="home-team"]//\
                                div[@class="team-data"]//\
                                p[@class="quarters-score"]\
                                //span[@class="ot-qt"]//text()')
                away_ot_scores = extract_list_data(score_board, \
                                './/div[@class="away-team"]//\
                                div[@class="team-data"]//\
                                p[@class="quarters-score"]\
                                //span[@class="ot-qt"]//text()')
                if home_scores:
                    for score in home_scores:
                        score = "".join(score)
                        if score:
                            _hs.append(str(score))
                if away_scores:
                    for score in away_scores:
                        score = "".join(score)
                        if score:
                            _as.append(str(score))

                if home_ot_scores:
                    for score in home_ot_scores:
                        score = "".join(score).strip()
                        if score:
                            _hs_ot.append(str(score))
                if away_ot_scores:
                    for score in away_ot_scores:
                        score = "".join(score).strip()
                        if score:
                            _aw_ot.append(str(score))

                channels = ''
                weeknum = extract_data(hxs, \
                          '//h2[@class="week-title"]/text()').strip()
                channel_info = extract_list_data(score_board, \
                          './/span[@class="network"]/text()')
                if channel_info:
                    channels = '<>'.join(channel for channel in channel_info)

                participants    = [{'home_team_callsign': str(home_team_callsign), \
                                    'name': str(home_team)}, \
                                   {'away_team_callsign': str(away_team_callsign), \
                                   'name': str(away_team)}]
                total_as = extract_data(score_board, \
                          './/div[@class="away-team"]//\
                          div[@class="team-data"]//\
                          p[@class="total-score"]//text()').strip()
                total_hs = extract_data(score_board, \
                          './/div[@class="home-team"]//\
                          div[@class="team-data"]//\
                          p[@class="total-score"]//text()').strip()

                result = [[[0], [total_hs], {'quarters': _hs, 'ot': _hs_ot,
                         'winner': ''}], [[1], [total_as], {'quarters': _as,
                         'ot': _aw_ot, 'winner': ''}]]

                data = {'other_url'         : other_url,
                        'source_key'        : game_id,
                        '_id'               : _id,
                        'other_ref'         : other_ref,
                        'participants'      : participants,
                        'result'            : result,
                        'participants_info' : participants_info,
                        'season_year'       : season_year,
                        'time_left'         : time_left,
                        'online'            : online,
                        'radio_home'        : radio_home,
                        'stadium_details'   : stadium_details,
                        'channels'          : channels,
                        'date'              : date,
                        'weeknum'           : weeknum,
                        'home_team_callsign': str(home_team_callsign),
                        'away_team_callsign': str(away_team_callsign),
                        'total_as'          : [total_as],
                        'total_hs'          : [total_hs]}

                total_as        = extract_data(score_board, './/div[@class="away-team"]//div[@class="team-data"]//p[@class="total-score"]//text()').strip()
                total_hs        = extract_data(score_board, './/div[@class="home-team"]//div[@class="team-data"]//p[@class="total-score"]//text()').strip()

                result          = [[[0], [total_hs], {'quarters': _hs, 'ot': _hs_ot, 'winner': ''}], \
                                   [[1], [total_as], {'quarters': _as, 'ot': _aw_ot, 'winner': ''}]]

                data = {
                        'other_url'         : other_url,            'source_key'                : game_id, \
                        '_id'               : _id,                  'other_ref'                 : other_ref, \
                        'participants'      : participants,         'result'                    : result, \
                        'participants_info' : participants_info,    'season_year'               : season_year, \
                        'time_left'         : time_left,            'online'                    : online, \
                        'radio_home'        : radio_home,           'stadium_details'           : stadium_details, \
                        'channels'          : channels,             'date'                      : date, \
                        'weeknum'           : weeknum
                        }

                yield Request(other_ref, callback = self.parse_next_details, meta = {'data' : data, 'score_board' : score_board})
    @log
    def parse_next_details(self, response):
        other_data      = response.body
        tz_info = ''

        score_board     = response.meta['score_board']
        data            = response.meta['data']
        time_left       = data['time_left'].replace('ET', '').strip()
        other_data      = re.sub('chapter.caption.txt.*', '', other_data)
        stadium_details = re.findall('stadium\t\t: "(.*?)",', other_data)
        city = re.findall('city\t\t: "(.*?)",', other_data)
        if city:
            tz_info = self.get_tz_info(city, stadium_details[0])

        sign_id         = re.findall('ASSET_BUILD:.*"(.*?)",', other_data)
        js_page = "http://combine.nflcdn.com/yui/min2/index.php?5.9" + sign_id[0]\
                 + "&b=yui3%2Fstatic%2F5.9%2Fscripts%2Fgamecenter&ss=anyraw&f=app/view/gameinfo.js"
        js_page         = urllib.urlopen(js_page).read()

        channel_info    = re.findall('_GC_landing" target="_blank">(.*?)</a>', js_page)
        if data['channels']:
            channel_info = data['channels'].upper() + "<>" + channel_info[0]
            data.update({'channels': channel_info})
        radio_home      = re.findall("radio.home;desc='(.*?)';if", js_page)
        if radio_home:
            radio_home  = radio_home[0]

        if stadium_details:
            stadium_details = stadium_details[0]

        year_       = data['season_year']
        datetime_   = ''

        date = extract_data(score_board, './/span[@class="date"]/text()').strip()
        if "Jan" in date and "2014" not in str(year_) and date:
            year_ = int(str(data['season_year'])) + 1

        game_datetime = ''
        if date:
            if ":" in time_left:
                matchtime   = date + " " + str(year_) + " " + time_left
                if "AM  PM" in matchtime:
                    matchtime = matchtime.replace('AM  PM', 'AM').\
                                replace('ET', '').strip()
                matchtime   = self.replacing_time(matchtime)
                game_datetime = get_utc_time(matchtime, '%a, %b %d %Y %I:%M  %p', 'US/Eastern')
            else:
                game_date = year_ + ' ' + date
                game_datetime = get_utc_time(game_date, '%Y %a, %b %d', 'US/Eastern')

        if "FINAL" in time_left:
            json_link = 'http://www.nfl.com/liveupdate/game-center/' + data['_id'] + '/' + '%s_gtd.json' % (data['_id'])
            data.update({'year_': year_, 'time_left': time_left,
                         'date': date, 'game_datetime': game_datetime,
                         'stadium_details' : stadium_details,
                         'tz_info': tz_info, 'radio_home': radio_home})
            yield Request(json_link, callback = self.parse_json_details, \
                    meta = {'data' : data})

    @log
    def parse_json_details(self, response):
        _dict       = {}
        tds_dict    = {}
        items       = []
        data        = response.meta['data']
        time_left   = data['time_left']
        result      = data['result']
        total_as    = data['result'][1][1][0]
        total_hs    = data['result'][0][1][0]
        _id         = data['_id']
        details     = response.body
        null        = "''"
        true        = "''"
        false       = "''"
        results     = eval(details)
        if results:
            tds = results.get(_id, '').get('scrsummary', {})
        for key, value in tds.iteritems():
            if value.get('type', '') == "TD":
                quarter = value.get('qtr', '')
                team    = value.get('team', '')
                _type   = value.get('type', '')
                desc    = value.get('desc', '')
                players = value.get('players' , '')
                if "pass" in desc:
                    rece_player    = "".join(re.findall('(^[a-zA-Z].*?) [0-9]', desc))
                    pass_player    = "".join(re.findall(r'pass from (.*)$', desc)).split("(")[0].strip()
                    pass_time      = "".join(re.findall(r'yards in (.*)', desc))
                    passing_yards  = "".join(re.findall(r'(\d+) yd.', desc))
                    rc_pid         = player_sks(players, rece_player)
                    ps_pid         = player_sks(players, pass_player)
                    td_list = {}
                    td_list.update({'rc_pid': rc_pid,
                                    'rece_player': rece_player,
                                    'ps_pid': ps_pid,
                                    'pass_player': pass_player,
                                    'team': team, 'pass_time': pass_time,
                                    'passing_yards': passing_yards})
                    tds_dict[quarter]   = td_list
                    _dict[_type]        = tds_dict
                    data.update({'tds_dict' : _dict})
                elif "run" in desc:
                    rush_player     = "".join(re.findall('(^[a-zA-Z].*?) [0-9]', desc))
                    rush_time       = "".join(re.findall(r'yards in (.*)', desc))
                    rush_yards      = "".join(re.findall(r'(\d+) yd.', desc))
                    rs_pid          = player_sks(players, rush_player)
                    td_list = {}
                    td_list.update({'rs_pid': rs_pid,
                    'rush_player': rush_player, 'team': team,
                    'rush_time': rush_time, 'rush_yards': rush_yards})

                    tds_dict[quarter]   = td_list
                    _dict['rushing']        = tds_dict
                    data.update({'tds_dict': _dict})

                elif value.get('type', '') == "FG":
                    quarter     = value.get('qtr', '')
                    team        = value.get('team', '')
                    _type       = value.get('type', '')
                    desc        = value.get('desc', '')
                    players = value.get('players' , '')
                    field_player = "".join(re.findall('(^[a-zA-Z].*?) [0-9]', desc))
                    field_time   = "".join(re.findall(r'yards in (.*)', desc))
                    field_yards  = "".join(re.findall(r'(\d+) yd.', desc))
                    fd_id        = player_sks(players, field_player)
                    td_list = {}
                    td_list.update({'fd_id': fd_id,
                    'field_player': field_player, 'team': team,
                    'field_time' : field_time, 'field_yards': field_yards})

                    tds_dict[quarter]   = td_list
                    _dict['field']      = tds_dict
                    data.update({'tds_dict' : _dict})

        live_status     = 0
        status_result   = {}
        data.update({'status_result' : status_result})
        if "FINAL" not in time_left:
            live_scores = 'http://www.nfl.com/liveupdate/scores/scores.json'
            yield Request(live_scores, callback = self.parse_live, \
                    meta = {'data' : data})
        else:
            if "FINAL" in time_left:
                if int(data['result'][1][1][0]) > int(data['result'][0][1][0]):
                    result[1][-1]['winner'] = 1
                    result[0][-1]['winner'] = 0
                elif int(data['result'][1][1][0]) == int(data['result'][0][1][0]):
                    result[1][-1]['winner'] = ''
                    result[0][-1]['winner'] = ''
                else:
                    result[0][-1]['winner'] = 1
                    result[1][-1]['winner'] = 0

            if "," in data['channels']:
                channels = data['channels'].replace(', ', "<>")
            else:
                channels = data['channels']
            rich_data = {'channels': str(channels), 'Online': '',
                         'Radio': str(data['radio_home']),
                         'stadium': data['stadium_details']}

            record = SportsSetupItem()
            record['rich_data']                 = rich_data
            record['participants']              = data['participants']
            record['result']                    = data['result']
            record['source_key']                = data['source_key']
            record['game']                      = 'football'
            record['affiliation']               = 'nfl'
            record['participant_type']          = 'team'
            record['source']                    = 'NFL'
            record['tournament']                = 'NFL Football'
            record['game_datetime']             = data['game_datetime']
            record['reference_url']             = data['other_ref']
            record['tz_info']                   = data['tz_info']
            record['time_unknown']              = "0"

            if "FINAL" in time_left:
                rich_data['video_links'] = ''
                record['rich_data'] = rich_data
                record['game_status'] = "completed"

                if self.spider_type == 'clips':
                    expand_id   = data['source_key'].split('/')[0]
                    expand_url  = "http://www.nfl.com/widget/gc/2011/tabs/cat-post-watch?gameId=%s" % expand_id
                    yield Request(expand_url, callback = self.parse_clips, \
                            meta = {'record': record})
                else:
                    sportssetupitem = SportsSetupItem()
                    for key, val in record.iteritems():
                        sportssetupitem[key] = val

                    items.append(sportssetupitem)
                    for item in items:
                        import pdb;pdb.set_trace()
                        yield item

    @log
    def parse_live(self, response):
        items = []
        true    = "true"
        false   = "false"
        null    = "null"
        data = response.meta['data']
        status_result = data['status_result']
        live_scores         = response.body
        away_team_callsign  = data['away_team_callsign']
        home_team_callsign  = data['home_team_callsign']
        result              = data['result']
        total_as            = data['total_as']
        total_hs            = data['total_hs']
        now                 = data['source_key'].split('/')[0]
        live_scores_dict    = eval(live_scores)
        live                = live_scores_dict.get(now, '')
        live_status = ""
        if live:
            away_abbr   = live['away']['abbr']
            home_abbr   = live['home']['abbr']
            total_as    = ''
            total_hs    = ''
            if away_abbr == away_team_callsign and \
                home_abbr == home_team_callsign:
                away_score  = live['away']['score']
                home_score  = live['home']['score']

                result[0][-1]['quarters']   = [home_score['1'], \
                                              home_score['2'], \
                                              home_score['3'], \
                                              home_score['4']]
                result[1][-1]['quarters']   = [away_score['1'], \
                                              away_score['2'], \
                                              away_score['3'], \
                                              away_score['4']]
                result[0][-1]['ot']         = [home_score['5']]
                result[1][-1]['ot']         = [away_score['5']]

                total_as = away_score['T']
                total_hs = home_score['T']

                result[0][1], result[1][1] = [str(total_hs)], [str(total_as)]

                live_status = "1"
                clock = live['clock']
                down = live['down']
                y_link = live['yl']
                togo = live['togo']
                if str(down) == '1':
                    down = str(down) + "st"
                elif str(down) == '2':
                    down = str(down) + "nd"
                elif str(down) == "3":
                    down = str(down) + "rd"
                elif str(down) == "4":
                    down = str(down) + "th"
                match_result = str(down)  + " & " + str(togo) + " "+ str(y_link)
                status_result['match_result'] = match_result
                status_result['status_text'] = clock

        if "FINAL" in data['time_left']:
            if int(total_as[0]) > int(total_hs[0]):
                result[1][-1]['winner'] = 1
                result[0][-1]['winner'] = 0
            elif int(total_as[0]) == int(total_hs[0]):
                result[1][-1]['winner'] = ''
                result[0][-1]['winner'] = ''
            else:
                result[0][-1]['winner'] = 1
                result[1][-1]['winner'] = 0

        rich_data = {'game_id': data['source_key'], 'channels': str(data['channels']),
                     'Online': '', 'Radio': str(data['radio_home']),
                    'stadium': data['stadium_details'],
                    'stats': data['tds_dict'], 'status_result': status_result}

        record = SportsSetupItem()
        record['rich_data']                 = rich_data
        record['participants']              = data['participants']
        record['result']                    = data['result']
        record['source_key']                = data['source_key']
        record['game']                      = 'football'
        record['affiliation']               = 'nfl'
        record['source']                    = 'NFL'
        record['participant_type']          = 'team'
        record['tournament']                = 'NFL Football'
        record['game_datetime']             = data['game_datetime']
        if "FINAL" in data['time_left'] or live_status :
            rich_data['video_links'] = ''
            record['rich_data'] = rich_data
            record['game_status'] = "completed"
            if live_status:
                record['game_status'] = "ongoing"

            if self.spider_type == 'clips':
                expand_id = data['game_id'].split('/')[0]
                expand_url = "http://www.nfl.com/widget/gc/2011/tabs/cat-post-watch?gameId=%s" % expand_id
                yield Request(expand_url, callback = self.parse_clips, \
                    meta = {'record': record})
            else:
                sportssetupitem = SportsSetupItem()
                for key, val in record.iteritems():
                    sportssetupitem[key] = val

                items.append(sportssetupitem)
                for item in items:
                    import pdb;pdb.set_trace()
                    yield item

    def parse_clips(self, response):
        pass
