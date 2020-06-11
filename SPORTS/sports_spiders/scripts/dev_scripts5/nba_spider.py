import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import json
from vtvspider_new import VTVSpider, get_nodes, extract_data
from vtvspider_new import extract_list_data, log, get_utc_time, get_tzinfo


EASTERN_TEAMS = ['NYK', 'BKN', 'MIA', 'MIL', \
                 'IND', 'TOR', 'PHI', 'ORL', \
                 'WAS', 'CHA', 'CHI', 'CLE', \
                 'DET', 'ATL', 'BOS']

WESTERN_TEAMS = ['NOH', 'MIN', 'LAL', 'MEM',
                 'LAC', 'HOU', 'SAS', 'SAC',
                 'POR', 'PHO', 'UTA', 'DAL',
                 'DEN', 'GSW', 'OKC']

FIRST_DATE    = ['2015-04-19', '2015-04-20', '2015-04-21', '2015-04-22', '2015-04-23', '2015-04-24', '2015-04-25', '2015-04-26', '2015-04-27', '2015-04-28', '2015-04-29', '2015-04-30', '2015-05-01', '2015-05-02']

COF_DATES = ['2015-05-04', '2015-05-05', '2015-05-06', '2015-05-07', '2015-05-08', '2015-05-09', '2015-05-10', '2015-05-11', '2015-05-12', '2015-05-13', '2015-05-14', '2015-05-15', '2015-05-16', '2015-05-17', '2015-05-18', '2015-05-19', '2015-05-03']

COF_FINALS = ['2015-05-21', '2015-05-23', '2015-05-25', '2015-05-27', '2015-05-29', '2015-05-31', '2015-06-02', '2015-05-20', '2015-05-22', '2015-05-24', '2015-05-26', '2015-05-28', '2015-05-30', '2015-06-01']

REG_START = '2015-10-26'
REG_END   = '2016-04-14'



class NBASpider(VTVSpider):
    name = "nbaspider"
    allowed_domains = ["nba.com"]
    quarter_info = ('1Q', '2Q', '3Q', '4Q', '1OT')

    playoffs_date = '20140601'
    game_link = 'http://www.nba.com%s'
    game_sk_format = '/games/%s/gameinfo.html'

    #nba_finals    = {"NBA Finals": ["MIA", "SAS"]}
    Eastern_teams = {"NBA Eastern First Round": ['NYK', 'BKN', 'MIA', 'MIL', \
                                                 'IND', 'TOR', 'PHI', 'ORL', \
                                                 'WAS', 'CHA', 'CHI', 'CLE', \
                                                 'DET', 'ATL', 'BOS']}
    Western_teams = {"NBA Western First Round": ['NOH', 'MIN', 'LAL', 'MEM', \
                                                 'LAC', 'HOU', 'SAS', 'SAC', \
                                                 'POR', 'PHO', 'UTA', 'DAL', \
                                                 'DEN', 'GSW', 'OKC']}

    Eastern_teams_cs = {"NBA Eastern Conference Semifinals": ['NYK', 'BKN', \
                                                 'MIA', 'MIL', 'IND', 'TOR', \
                                                 'PHI', 'ORL', 'WAS', 'CHA', \
                                                 'CHI', 'CLE', 'DET', \
                                                 'ATL', 'BOS']}
    Western_teams_cs = {"NBA Western Conference Semifinals": ['NOH', 'MIN', \
                                                'LAL', 'MEM', 'LAC', 'HOU', \
                                                'SAS', 'SAC', 'POR', 'PHO', \
                                                'UTA', 'DAL', 'DEN', 'GSW', \
                                                'OKC']}
    Eastern_teams_final = {"NBA Eastern Conference Finals": ['NYK', 'BKN', \
                                                'MIA', 'MIL', 'IND', 'TOR', \
                                                'PHI', 'ORL', \
                                                'WAS', 'CHA', 'CHI', 'CLE', \
                                                'DET', 'ATL', 'BOS']}
    Western_teams_final = {"NBA Western Conference Finals": ['NOH', 'MIN', \
                                                'LAL', 'MEM', 'LAC', 'HOU', \
                                                'SAS', 'SAC', \
                                                'POR', 'PHO', 'UTA', 'DAL', \
                                                'DEN', 'GSW', 'OKC']}


    name_callsign_map = { "Boston Celtics": "BOS", "Chicago Bulls": "CHI",
                          "Atlanta Hawks" : "ATL", "New Jersey Nets" : "BKN",
                          "Cleveland Cavaliers" : "CLE",
                          "Charlotte Bobcats": "CHA", "New York Knicks": "NYK",
                          "Detroit Pistons" : "DET", "Miami Heat" : "MIA",
                          "Philadelphia 76ers": "PHI", "Indiana Pacers": "IND",
                          "Orlando Magic" : "ORL", "Toronto Raptors" : "TOR",
                          "Milwaukee Bucks": "MIL", "Washington Wizards": "WAS",
                          "Dallas Mavericks" : "DAL", "Denver Nuggets" : "DEN",
                          "Golden State Warriors" : "GSW",
                          "Houston Rockets" : "HOU",
                          "Minnesota Timberwolves" : "MIN",
                          "Los Angeles Lakers": "LAL",
                          "Memphis Grizzlies": "MEM",
                          "Portland Trail Blazers" : "POR",
                          "Los Angeles Clippers" : "LAC",
                          "New Orleans Hornets" : "NOH",
                          "Oklahoma City Thunder" : "OKC",
                          "Phoenix Suns" : "PHX", "San Antonio Spurs": "SAS",
                          "Utah Jazz" : "UTA", "Sacramento Kings" : "SAC",
                          "FBU": "FBU", "RMD": "RMD", "MPS": "MPS",
                          "EAM": "EAM", "FCB": "FCB", "MAC": "MAC",
                          "ALB": "ALB", "New Orleans Pelicans": "NOP",
                          'Eastern Conference' : 'ECF',
                          'Western Conference' : 'WCF',
                          'TBD2': 'HIL', 'TBD1': 'WEB', "Brooklyn Nets": 'BKN',
                          "Maccabi Elite": "MTA",
                          "Rio de Janeiro Flamengo": "FLA",
                          "WLD": "tbd2", "USA": "tbd1",
                          'Orlando White': 'ORW',
                          'Orlando Blue': 'ORB', 'FBU': 'FEN',
                          'NBA D-League Select': 'DLS'}

    euro_teams = """
    Efes Pilsen
    Lottomatica Roma
    Maccabi Elite
    MMT Estudiantes
    Panathinaikos
    Real Madrid
    Unicaja Malaga
    Zalgiris Kaunas
    """
    world_teams = """
    Chinese National Team
    """
    euro_teams = euro_teams.split('\n')
    euro_teams = [a.strip() for a in euro_teams if a.strip()]

    world_teams = world_teams.split('\n')
    world_teams = [a.strip() for a in world_teams if a.strip()]

    other_teams = world_teams + euro_teams

    start_urls = []

    game_note_dict = {}

    def start_requests(self):
        week_days =  []
        events_dict = {}
        urls = []

        top_url = 'http://data.nba.com/data/1h/json/cms/noseason/scoreboard/%s/games.json'
        for i in open('./GAME_EVENT_MAPPING', 'r').readlines():
            i = i.strip().split('<>')
            events_dict[i[0]] = i[1]

        if self.spider_type == "schedules":
            for i in range(0, 300):
                week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        else:
            for i in range(0, 5):
                week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))
        self.week_days = week_days
        for wday in week_days:
            url = top_url % wday
            max_date = repr(datetime.datetime(2013, 04, 17, 23, 59, 00))
            data = Request(url, self.parse, meta = {'events_dict': events_dict, 'max_date' : max_date, 'type' : self.spider_type})
            urls.append(data)
        return urls

    def get_channels(self, game):
        broadcasters = game.get('broadcasters', '')
        channel = ''
        radio = ''
        if broadcasters:
            channels = broadcasters.get('tv', '')
            if channels:
                channels = channels.get('broadcaster', '')
                channel = '<>'.join([channel['display_name'] for channel in channels])
            radio = broadcasters.get('radio', '')
            if radio:
                radios = radio.get('broadcaster', '')
                radio = '<>'.join([rad['display_name'] for rad in radios])
        return channel, radio

    @log
    def parse(self, response):
        raw_data = json.loads(response.body)
        games = raw_data['sports_content']['games']['game']
        event_text = raw_data['sports_content']['sports_meta']['season_meta']['display_season']
        year =  raw_data['sports_content']['sports_meta']['season_meta']['display_year']
        for game in games:
            import pdb;pdb.set_trace()
            game_id = game.get('game_url', '')
            stadium = game.get('arena', '')
            city = game.get('city', '')
            state = game.get('state', '')
            country = game.get('country', '')
            status = game['period_time']['game_status']
            game_note = game['period_time']['period_status']
            if game_note.lower() == 'final':
                game_note = ''
            channels, radio = self.get_channels(game)
            home_team = game['home']['team_key']
            away_team = game['visitor']['team_key']

            game_date = game['date']
            game_time = game['time']
            if game_time:
                game_dt = game_date + ' ' + game_time
                game_datetime = get_utc_time(game_dt, '%Y%m%d %H%M', 'US/Eastern')
                time_unknown = 0
            else:
                game_datetime = get_utc_time(game_date, '%Y%m%d', 'US/Eastern')
                time_unknown = 1

            home_nickname = game['home'].get('nickname', '')
            home_city = game['home'].get('city', '')
            away_nickname = game['visitor'].get('nickname', '')
            away_city = game['visitor'].get('city', '')
            home_name = home_city + ' ' + home_nickname
            away_name = away_city + ' ' + away_nickname
            teams = {home_team: (1, home_name), away_team: (0, away_name)}

            sportssetupitem = {}
            record = SportsSetupItem()
            items = []
            game_id = self.game_sk_format % game_id
            game_url = self.game_link % game_id
            if "&amp;" in stadium:
                stadium = stadium.replace("&amp;", "&")
            record['rich_data'] = {}
            record['rich_data']['stadium'] = stadium
            record['rich_data']['game_note'] = game_note
            record['rich_data']['city'] = city
            record['rich_data']['state'] = state
            record['rich_data']['channels'] = str(channels)
            record['rich_data']['Radio'] = str(radio)
            record['game_datetime'] = game_datetime
            record['source_key'] = game_id
            record['affiliation'] = 'nba'
            record['game'] = 'basketball'
            record['source'] = 'NBA'
            record['participants'] = teams
            record['reference_url'] = game_url
            record['time_unknown'] = time_unknown
            record['participant_type'] = 'team'
            event = response.meta['events_dict'].get(game_id, '')
            if "summer league" in event_text.lower():
                record['tournament'] = 'NBA Summer League'
            else:
                record['tournament'] = 'National Basketball Association'
            if "nba league" not in event_text.lower() and "playoff" in event_text.lower():
                event = "NBA Playoffs"
            elif "pre season" in event_text.lower():
                event = "NBA Preseason"
            elif "regular season" in event_text.lower():
                event = 'NBA Regular season'
            if 'EST' in teams.keys():
                event = 'NBA All-Star Game'
            if 'USA' in teams.keys():
                event = "NBA BBVA Compass Rising Stars Challenge"

            record['event'] = event
            if status == '1':
                status = 'scheduled'
            elif status == '3':
                status = 'completed'
            elif status == '2':
                status = 'ongoing'
            record['game_status'] = status
            if status != 'scheduled':
                stat_url, record = self.handle_completed_game(game, record, game_id, game_datetime)
                yield Request(stat_url, callback = self.parse_stats, meta={'record' : record, 'node' : game})
                continue

            team_key1 = (home_team, away_team)
            team_key2 = (away_team, home_team)

            final_game_note = game_note = ''

            if "nba league" not in event_text.lower() and "playoff" in event_text.lower() and self.playoffs_date in self.week_days:
                if team_key1 not in self.game_note_dict and team_key2 not in self.game_note_dict:
                    self.game_note_dict.setdefault(team_key1, 1)
                else:
                    try1 = self.game_note_dict.get(team_key1, '')
                    try2 = self.game_note_dict.get(team_key2, '')
                    if try1:
                        counter = try1 + 1
                        self.game_note_dict[team_key1] = counter
                    elif try2:
                        counter = try2 + 1
                        self.game_note_dict[team_key2] = counter

                if team_key1 in self.game_note_dict:
                    game_note = self.game_note_dict[team_key1]
                if team_key2 in self.game_note_dict:
                    game_note = self.game_note_dict[team_key2]

                if game_note:
                    final_game_note = 'Game %s' % str(game_note)
                    #final_game_note = None

            if home_team == "WST":
                home_team = "WCF"
            elif away_team == "WST":
                away_team = "WCF"
            if away_team == "EST":
                away_team = "ECF"
            elif home_team == "EST":
                home_team = "ECF"
            if home_team == "USA":
                home_team = "USA"
            if away_team == "WLD":
                away_team = "WLD"
            teams = {home_team: (1, home_name), away_team: (0, away_name)}
            team_names = []
            callsign_map_teamname = dict(zip(self.name_callsign_map.values(), self.name_callsign_map))
            for callsign in (home_team, away_team):
                #if callsign in self.nba_finals.values()[0]:
                #    event = self.nba_finals.keys()[0]

                if callsign not in callsign_map_teamname:
                    if callsign in self.other_teams:
                        team_names.append({'name': callsign})
                        #if callsign in self.nba_finals.values()[0]:
                        #    event = self.nba_finals.keys()[0]
                else:
                    team_names.append({'callsign': callsign, 'name': callsign_map_teamname[callsign]})

            if home_team in EASTERN_TEAMS and game_dt.split(' ')[0] in FIRST_DATE:
                record['event']      = "NBA Eastern First Round"
            elif home_team in WESTERN_TEAMS and game_dt.split(' ')[0] in FIRST_DATE:
                record['event']       = "NBA Western First Round"
            elif home_team in EASTERN_TEAMS and game_dt.split(' ')[0] in COF_DATES:
                record['event']      = "NBA Eastern Conference Semifinals"
            elif home_team in WESTERN_TEAMS and game_dt.split(' ')[0] in COF_DATES:
                record['event']      = "NBA Western Conference Semifinals"
            elif home_team in EASTERN_TEAMS and game_dt.split(' ')[0] in COF_FINALS:
                record['event']      = "NBA Eastern Conference finals"
            elif home_team in WESTERN_TEAMS and game_dt.split(' ')[0] in COF_FINALS:
                record['event']      = "NBA Western Conference finals"

            record['game'] = 'basketball'
            record['tournament'] = 'National Basketball Association'
            record['participants'] = teams
            record['affiliation'] = 'nba'
            record['participant_type'] = 'team'
            if final_game_note:
                record['rich_data']['game_note'] = final_game_note
            else:
                record['rich_data']['game_note'] = ''
            record['source_key'] = game_id
            record['rich_data']['update'] = datetime.datetime.now()
            record['source'] = 'nba'
            record['reference_url'] = self.game_link % game_id

            if not "TBD" in game_time:
                if record['game_status'] == "completed" and self.spider_type == 'scores':
                    sportssetupitem = SportsSetupItem()
                    for key, val in record.itemitems():
                        sportssetupitem[key] = val

                    items.append(sportssetupitem)
                    for item in items:
                        yield item
            if record['game_status'] == 'scheduled' and self.spider_type == 'schedules':
                record['result'] = {}
                stat_url = self.game_link % game_id
                yield Request(stat_url, callback=self.handle_schedule_locations, meta={'record':record})

    @log
    def handle_schedule_locations(self, response):
        hxs = Selector(response)
        record  = response.meta['record']
        sportssetupitem = {}
        items = []
        event_link = extract_data(hxs, '//div[@class="nbaGImatchup"]/a/@href')
        record['reference_url'] = response.url

        game_venue  = extract_data(hxs, '//div[@id="nbaGIStation"]//span[@class="nbaGILocat"]/text()')
        if len(game_venue.split(',')) == 3:
            stadium = "".join(game_venue.split(',')[0]).strip()
            city = "".join(game_venue.split(',')[1]).strip()
            state = "".join(game_venue.split(',')[2]).strip()
        elif len(game_venue.split(',')) == 2:
            stadium = "".join(game_venue.split(',')[0]).strip()
            city = "".join(game_venue.split(',')[1]).strip()
            state = ''
        else:
            stadium =  city = state = ''

        if '&amp;' in stadium:
            stadium = stadium.replace('&amp;', '&')
        else:
            stadium = stadium
        record['rich_data']['stadium'] = stadium
        record['rich_data']['city'] = city
        record['rich_data']['state'] = state
        record['rich_data']['game_note']= extract_data(hxs, '//div[@class="nbaGImatchup"]/text()')
        record['time_unknown'] = "0"
        tz_info = get_tzinfo(city = city, game_datetime= record['game_datetime'])
        if not tz_info :
            tz_info = get_tzinfo(country = 'USA', game_datetime= record['game_datetime'])
        record['tz_info'] =  tz_info
        sportssetupitem = SportsSetupItem()
        for key, val in record.iteritems():
            sportssetupitem[key] = val

        items.append(sportssetupitem)
        for item in items:
            yield item

    def handle_completed_game(self, game, record, game_id, game_date):
        record = record
        callsign_map_teamname = dict(zip(self.name_callsign_map.values(), self.name_callsign_map))
        team_names = []
        result_list = [[[0], [], {'quarters': [], 'ot': [], 'winner': ''}], [[1], [], {'quarters': [], 'ot': [], 'winner': ''}]]

        home_final = game['home'].get('score', '')
        away_final = game['visitor'].get('score', '')
        home_score = game['home'].get('linescores', '')
        away_score = game['visitor'].get('linescores', '')
        game_status = game['period_time']['game_status']
        home_ot = []
        away_ot = []

        if home_score:
            home_quarters = [score['score'] for score in home_score['period']]
        if away_score:
            away_quarters = [score['score'] for score in away_score['period']]

        result_list[0][-1]['quarters'] = home_quarters
        result_list[0][-1]['ot'] = home_ot

        result_list[1][-1]['quarters'] = away_quarters
        result_list[1][-1]['ot'] = away_ot

        result_list[0][1], result_list[1][1] = [home_final], [away_final]
        if game_status == '3':
            if (int(away_final) > int(home_final)):
                result_list[1][-1]['winner'] = 1
                result_list[0][-1]['winner'] = 0
            elif int(away_final) < int(home_final):
                result_list[0][-1]['winner'] = 1
                result_list[1][-1]['winner'] = 0
            else:
                result_list[0][-1]['winner'] = 0
                result_list[1][-1]['winner'] = 0

        record['result'] = result_list

        stat_url = self.game_link % game_id
        return stat_url, record

    @log
    def parse_stats(self, response):
        sportssetupitem = {}
        items = []
        record = response.meta['record']
        doc = response.meta['node']
        hxs = Selector(response)
        league = extract_data(hxs, '//tr[@class="nbaGITvImg"]//td[@id="nbaGITvFirst"]//text()').replace('\n', '').strip()
        quarter_time = extract_data(hxs, '//div[@id="nbaLineScoreAJAX"]/div[@id="nbaGIGmeLve"]/div[@id="nbaGITmeQtr"]/h2/text()').strip()
        quarters = extract_data(hxs, '//div[@id="nbaLineScoreAJAX"]/div[@id="nbaGIGmeLve"]/div[@id="nbaGITmeQtr"]/p/text()').strip()
        if quarters and quarters in self.quarter_info and quarter_time != 'FINAL':
            game_note = (quarters + ' - ' + quarter_time).strip()
        elif quarter_time == "FINAL":
            game_note = extract_data(hxs, '//div[@class="nbaGImatchup"][contains(text(), "Series")]/text()').strip()
        else:
            game_note = ''
        game_venue = extract_data(hxs, '//div//span[@class="nbaGILocat"]/text()')
        game_venue = game_venue.split(',')
        if len(game_venue) == 3:
            stadium = "".join(game_venue[0]).strip()
            city = "".join(game_venue[1]).strip()
            state = "".join(game_venue[2]).strip()
        elif len(game_venue) == 2:
            stadium = "".join(game_venue[0]).strip()
            city = "".join(game_venue[1]).strip()
            state = ''
        else:
            stadium = city = state = ''
        tz_info = get_tzinfo(city = city, game_datetime = record['game_datetime'])
        if not tz_info:
            tz_info = get_tzinfo(country = "USA", game_datetime = record['game_datetime'])

        stat_nodes = get_nodes(hxs, '//div[@id="nbaGIboxscore"]//table[@id="nbaGITeamStats"]')
        stats, steals,  rebounds,  points = {}, {}, {}, {}
        for stat_node in stat_nodes:
            max_rebs = max_st = max_pts = 0
            team = extract_data(stat_node, './thead[contains(@class,"nbaGI")]//text()').split("(")[0].strip()
            team = self.name_callsign_map.get(team, team)
            if team == 'PHX':
                team = 'PHO'
            if team == 'NOP':
                team = 'NOH'
            details = get_nodes(stat_node, './/tr[@class="odd"]|.//tr[@class="even"]')
            for dt_node in details:
                pl_nm = "".join(re.findall(r'playerfile/(\w+)', extract_data(dt_node, './/td[@id="nbaGIBoxNme"]/a/@href')))
                rebs = extract_data(dt_node, './/td[10]/text()')
                st = extract_data(dt_node, './/td[13]/text()')
                pts = extract_data(dt_node, './/td[17]/text()')
                if not pl_nm:
                    continue
                if rebs and int(rebs) > int(max_rebs):
                    max_rebs = rebs
                    if not rebounds.has_key(team):
                        rebounds[team] = []
                    rebounds[team] = [(pl_nm, max_rebs)]
                elif rebs and int(rebs) == int(max_rebs):
                    max_rebs = rebs
                    if not rebounds.has_key(team):
                        rebounds[team] = []
                    rebounds[team].append((pl_nm, max_rebs))

                if st and int(st) > int(max_st):
                    max_st = st
                    if not steals.has_key(team):
                        steals[team] = []
                    steals[team] = [(pl_nm, max_st)]
                elif st and int(st) == int(max_st):
                    max_st = st
                    if not steals.has_key(team):
                        steals[team] = []
                    steals[team].append((pl_nm, max_st))

                if pts and int(pts) > int(max_pts):
                    max_pts = pts
                    if not points.has_key(team):
                        points[team] = []
                    points[team] = [(pl_nm, max_pts)]
                elif pts and int(pts) == int(max_pts):
                    max_pts = pts
                    if not points.has_key(team):
                        points[team] = []
                    points[team].append((pl_nm, max_pts))

        stats['max_rebounds_'] = rebounds
        stats['max_points_'] = points
        stats['max_steals_'] = steals

        if "summer" in league.lower():
            record['tournament'] = 'NBA Summer League'
        else:
            record['tournament'] = 'National Basketball Association'
        record['rich_data']['game_id'] = record.get('game_id')
        record['rich_data']['update'] = datetime.datetime.now()
        record['rich_data']['game_note'] = game_note
        record['rich_data']['stats'] = stats
        record['tz_info'] = tz_info

        clips_url = extract_data(hxs, './/div[@class="nbaMnStatsFtr"]/a/@href').strip()
        if clips_url and record['game_status'] == 'completed'  and self.spider_type == "clips":
            if "http:" not in clips_url:
                clips_url = "http://www.nba.com" + clips_url
            media_urls = []
            nodes = get_nodes(hxs, '//div[@id="nbaGIPlayList"]/div[@id="nbaGIPLItems"]')
            for node in nodes:
                clipdict = {}
                url = extract_list_data(node, './a/@onclick')
                url = re.findall('playVideo\(\'(.*?)\'\);', url)[0]
                url = 'http://www.nba.com' + url + '.xml'
                yield Request(url, callback = self.parse_recap, meta={'record' : record, 'clipdict' : clipdict, 'media_urls' : media_urls})

        sportssetupitem = SportsSetupItem()
        for key, value in record.iteritems():
            sportssetupitem[key] = value

        items.append(sportssetupitem)
        status = record['game_status']

        if status == 'scheduled' and self.spider_type == "schedules":
            for item in items:
                yield item
        elif status == 'completed' and self.spider_type == "scores":
            for item in items:
                import pdb;pdb.set_trace()
                yield item
        elif status == 'ongoing' and self.spider_type == "scores":
            for item in items:
                yield item

    @log
    def parse_recap(self, response):
        hxs = Selector(response)
        clipdict = response.meta['clipdict']
        media_urls = response.meta['media_urls']
        record = response.meta['record']
        sportssetupitem = {}
        items = []

        clipdict['title'] = extract_data(hxs, '//headline/text()').strip()
        clipdict['desc'] = extract_data(hxs, '//description/text()').strip()
        clipdict['sk'] = extract_data(hxs, '//video/@id').strip()
        clipdict['reference'] = extract_data(hxs, '//pageLink/text()').strip()
        clipdict['img_link'] = extract_data(hxs, '//images//image[@name="400x300"]/text()').strip()

        video_urls = {}
        nodes = get_nodes(hxs, '//files/file')
        for node in nodes:
            bitrate = extract_data(node, './@bitrate').strip()
            url = extract_data(node, './text()').strip()
            if not url.startswith('http://www.nba.com/'):
                url = 'http://nba.cdn.turner.com/nba/big' + url
            video_urls[bitrate] =  url

        if video_urls:
            btrs = video_urls.keys()
            btrs.sort(reverse=True)
            clipdict['high'] = {'url': video_urls['556'], 'mimetype': 'video/x-flv'}
            clipdict['low'] = {'url': video_urls['420'], 'mimetype': 'video/x-flv'}
            media_urls.append(clipdict)

        record['rich_data']['video_links'] = media_urls

        sportssetupitem = SportsSetupItem()

        for key, val in record.iteritems():
            sportssetupitem[key] = val

        items.append(sportssetupitem)
        for item in items:
            yield item
