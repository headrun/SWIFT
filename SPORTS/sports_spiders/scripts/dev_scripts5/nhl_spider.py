import datetime
import re
import urllib
from vtvspider_new import VTVSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import extract_data, \
extract_list_data, get_nodes, log, \
get_utc_time, get_tzinfo

true = True
false = False
null = ''

EASTERN_FIRST = ['OTT', 'MTL', 'DET', 'TBL', 'PIT', 'NYR', 'NYI', 'WSH']
WESTERN_FIRST = ['MIN', 'STL', 'CHI', 'NSH', 'WPG', 'ANA', 'CGY', 'VAN']
FINAL_LIST    = ['TBL', 'NYR', 'CHI', 'ANA']

FIRST_DATE    = ['2015-04-15', '2015-04-16', '2015-04-17', '2015-04-18', '2015-04-19', '2015-04-20', '2015-04-21', '2015-04-22', '2015-04-23', '2015-04-24', '2015-04-25', '2015-04-26', '2015-04-27', '2015-04-28', '2015-04-29']

COF_DATES = ['2015-04-30', '2015-05-01', '2015-05-02', '2015-05-03', '2015-05-04', '2015-05-05', '2015-05-06', '2015-05-07', '2015-05-08', '2015-05-09', '2015-05-10', '2015-05-11', '2015-05-12', '2015-05-13', '2015-05-14']

COF_SEMI_DATES = ['2015-05-15', '2015-05-16', '2015-05-17', '2015-05-18', '2015-05-19', '2015-05-20', '2015-05-21', '2015-05-22', '2015-05-23', '2015-05-24', '2015-05-25', '2015-05-26', '2015-05-27', '2015-05-28', '2015-05-29', '2015-05-30']

FINAL_DATES = ['2015-06-03', '2015-06-04', '2015-06-05', '2015-06-06', '2015-06-07', '2015-06-08', '2015-06-09', '2015-06-10', '2015-06-11', '2015-06-12', '2015-06-13', '2015-06-14', '2015-06-15', '2015-06-16', '2015-06-17', '2015-06-18']

def get_period_suffix(period):
    pdict = {'1':'st', '2':'nd', '3': 'rd'}
    if period[-1] in pdict:
        return period + pdict[period]
    else:
        return period+'th'
TBD_TEAMS = ['Team Foligno', 'Team Toews']

class NHLSpider(VTVSpider):
    name    = "nhlspider"
    allowed_domains = ['nhl.com']

    start_urls = []
    events_dict = {}

    teamsDict = {
    "Devils":"NJD",\
    "Islanders":"NYI",\
    "Rangers":"NYR",\
    "Flyers":"PHI",\
    "Penguins":"PIT",\
    "Bruins":"BOS",\
    "Sabres":"BUF",\
    "Canadiens":"MON",\
    "Senators":"OTT",\
    "Maple Leafs":"TOR",\
    "Thrashers":"ATL",\
    "Hurricanes":"CAR",\
    "Panthers":"FLO",\
    "Lightning":"TAM",\
    "Capitals":"WAS",\
    "Blackhawks":"CHI",\
    "Blue Jackets":"CLB",\
    "Red Wings":"DET",\
    "Predators":"NAS",\
    "Blues":"STL",\
    "Flames":"CAL",\
    "Avalanche":"COL",\
    "Oilers":"EDM",\
    "Wild":"MIN",\
    "Canucks":"VAN",\
    "Mighty Ducks":"ANA",\
    "Ducks":"ANA",\
    "Stars":"DAL",\
    "Kings":"LOS",\
    "Coyotes":"PHO",\
    "Sharks":"SAN"
    }
    newteamsDict = {
    "Blue Jackets" : "CBJ",
    "Predators" : "NSH" ,
    "Flames": "CGY" ,
    "Canadiens": "MTL" ,
    "Panthers": "FLA" ,
    "Kings": "LAK" ,
    "Lightning": "TBL" ,
    "Coyotes": "PHX" ,
    "Capitals": "WSH" ,
    "Sharks": "SJS" ,
    "Jets": "WPG",
    "Arizona" : "ARI",
    "Metropolitan": "Team Metropolitan",
    "Atlantic": "Team Atlantic",
    "Pacific": "Team Pacific",
    "Central": "Team Central",
    "East": "All-stars East",
    "West": "All-stars West"
    }

    teamsDict.update(newteamsDict)

    part_to_full_name_map = { \
    'Ducks' : 'Mighty Ducks of Anaheim', \
    'Red Wings': 'Detroit Red Wings' \
    }

    stadiums_dict = {'CHI' : ('United Center', 'Chicago', 'IL'), 'PIT' : ('CONSOL Energy Center', 'Pittsburgh', 'PA'), \
                     'CAR' : ('PNC Arena', 'Raleigh', 'NC'), 'BUF' : ('First Niagara Center', 'Buffalo', 'NY'), \
                     'STL' : ('Scottrade Center', 'Saint Louis', 'MO'), 'WSH' : ('Verizon Center', 'Washington', 'DC'), \
                     'BOS' : ('TD Garden', 'Boston', 'MA'), 'PHX' : ('Jobing.com Arena', 'Glendale', 'AZ'), \
                     'COL' : ('Pepsi Center', 'Denver', 'CO'), 'DAL' : ('American Airlines Center', 'Dallas', 'TX'), \
                     'FLA' : ('BB&T Center', 'Sunrise', 'FL'), 'TBL' : ('Amalie Arena', 'Tampa', 'FL'), \
                     'NYI' : ('Nassau Coliseum', 'Uniondale', 'NY'), 'NYR' : ('Madison Square Garden', 'New York', 'NY'), \
                     'ANA' : ('Honda Center', 'Anaheim', 'CA'), 'SJS' : ('SAP Center at San Jose', 'San Jose', 'CA'), \
                     'LAK' : ('Staples Center', 'Los Angeles', 'CA'), 'DET' : ('Joe Louis Arena', 'Detroit', 'MI'), \
                     'NJD' : ('Prudential Center', 'Newark', 'NJ'), 'PHI' : ('Wells Fargo Center', 'Philadelphia', 'PA'), \
                     'MIN' : ('Xcel Energy Center', 'Saint Paul', 'MN'), 'CGY' : ('Scotiabank Saddledome', 'Calgary', 'AB'), \
                     'NSH' : ('Bridgestone Arena', 'Nashville', 'TN'), 'EDM' : ('Rexall Place', 'Edmonton', 'AB'), \
                     'VAN' : ('Rogers Arena', 'Vancouver', 'BC'), 'CBJ' : ('Nationwide Arena', 'Columbus', 'OH'), \
                     'TOR' : ('Air Canada Centre', 'Toronto', 'CA'), 'MTL': ('Bell Centre', 'Montreal', 'QC'), \
                     'Team Foligno' : ('Nationwide Arena', 'Columbus', 'Ohio'), 'WPG': ('MTS Centre', 'Winnipeg', 'Manitoba'), \
                     'OTT': ('Canadian Tire Centre', 'Ottawa', 'Ontario')}


    '''def start_requests(self):
        req = []
        urlformat = "http://www.nhl.com/ice/scores.htm?date=%s"
        dates_to_crawl = []
        if self.spider_type == "schedules":
            for i in range(0, 300):
                dates_to_crawl.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%m/%d/%Y'))
        else:
            for i in range(0, 6):
                dates_to_crawl.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%m/%d/%Y'))

        print "dates_to_crawl", dates_to_crawl
        for date in dates_to_crawl:
            url = urlformat % date
            r   = Request(url, self.parse)
            req.append(r)
        return req'''

    def start_requests(self):
        url = 'https://statsapi.web.nhl.com/api/v1/game/2015020756/feed/live'
        headers = { 'Origin': 'https://www.nhl.com',
                    'Accet-Encoding': 'gzip, deflate, sdch',
                    'Accept-Language': 'en-US,en;q=0.8',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36',
                    'Accept': '*/*',
                    'Referer': 'https://www.nhl.com/gamecenter/buf-vs-mtl/2016/02/03/2015020756',
                    'Connection': 'keep-alive'}
        yield Request(url, self.parse, headers=headers)

    @log
    def parse(self, response):
        data = eval(response.body)
        import pdb;pdb.set_trace()
        hxs = HtmlXPathSelector(response)
        events_dict = {}
        for i in open('GAME_EVENT_MAPPING', 'r').readlines():
            i = i.strip().split('<>')
            events_dict[i[0]] = i[1]

        record = SportsSetupItem()
        callsignDict = {}
        for key, val in self.teamsDict.items():
            callsignDict[val] = key
        game_date = response.url.split('date=')[-1]

        nodes = get_nodes(hxs, '//div[@class="sbGame"]')
        import pdb;pdb.set_trace()
        for node in nodes:
            items           = []
            home_team_cs    = extract_data(node, './/table//tr[@class="homeTeam"]//td[@class="team left"]//a/@rel')
            away_team_cs     = extract_data(node, './/table//tr[not(@class)]//td[@class="team left"]//a/@rel')
            if not home_team_cs:
                home_team_cs = extract_data(node, './/table//tr[@class="homeTeam"]//td[@class="team left"]//text()')
                away_team_cs = extract_data(node, './/table//tr[not(@class)]//td[@class="team left"]//text()')
            event_name = extract_data(node, './/div[@class="gcLinks"]/a/text()')

            #event = self.stanley_cup_finals.get(home_team_cs)
            #if not event:
            #    event = self.stanley_cup_finals.get(away_team_cs)
            stadium = city = state = ''
            #stadium = extract_data(node, './div[@class="gcLinks"]/text()').split(':')[1].strip()

            data = self.stadiums_dict.get(home_team_cs, '')
            stadium = city = state = ''
            if data:
                stadium     = data[0]
                city        = data[1]
                state       = data[2]

            elif not data:
                away_data   = self.stadiums_dict.get(away_team_cs, '')
                if away_data:
                    stadium = away_data[0]
                    city    = away_data[1]
                    state   = away_data[2]

            else:
                stadium = city = state = ''
            game_time   = extract_data(node, './/table//tr//th[@class="left"]//text()')
            game_time   = game_time.replace('*', '').strip()
            channels    = extract_list_data(node, './/div[@class="tvBroadcasts"]//text()')
            if channels:
                channel_info = '<>'.join(channel for channel in channels if channel)
            else:
                channels  = extract_list_data(node, './/div[@class="tvBroadcasts"]//div//@alt')
                channel_info = '<>'.join(channel for channel in channels if channel)
            if home_team_cs in TBD_TEAMS:
                participants     = [{'callsign': 'tbd1', 'name': str(home_team_cs)}, \
                                {'callsign': 'tbd2', 'name': str(away_team_cs)}]
            else:
                participants     = [{'callsign': str(home_team_cs), 'name': callsignDict[str(home_team_cs)]}, \
                                    {'callsign': str(away_team_cs), \
                                    'name': callsignDict[str(away_team_cs)]}]


            rich_data        = {'stadium' : stadium, 'city' : city, 'state' : state, \
                                'channels' : str(channel_info), \
                                    'video_links' : []}
            record['participants']              = participants
            record['tournament']                = 'National Hockey League'
            record['game']                      = 'hockey'
            record['source']                    = 'nhl'
            record['participant_type']          = 'team'
            record['affiliation']               = 'nhl'
            record['event']                     = event_name
            record['reference_url']             = response.url

            if "TBD" in game_time:
                record['game_status']  = 'scheduled'
                record['time_unknown'] = 1
                game_id = extract_data(node, './/a[contains(@href, "/preview?")]/@href')
                if not game_id:
                    game_id = extract_data(node, './/a[contains(@href, "/boxscore?")]/@href')
                game_id = game_id.split('=')[-1]
                record['source_key']       = game_id
                game_datetime    = game_date
                date             = get_utc_time(game_datetime, '%m/%d/%Y', 'US/Eastern')
                print "Game time not yet set in site.. %s" % game_time

            is_live = extract_list_data(node, './/a[@title="Watch live online with NHL GameCenter LIVE"]')
            if  is_live:
                is_live = is_live[0]
            else:
                is_live = extract_list_data(node, './/a[contains(@href, "gamecenterlive.htm")]/..//text()')
                if is_live: is_live = is_live[0]
                else: is_live = ''

            if is_live and "Watch Now" in is_live:
                record['game_status']  = 'ongoing'
                game_id = extract_data(node, './/a[contains(@href, "/preview?")]/@href')
                game_note = extract_data(node, './/table//tr[1]/th[1]/text()').split(' ')
                if len(game_note) == 2:
                    game_note = game_note[1] + 'Q' + ' - ' + game_note[0]
                    rich_data['game_note'] = game_note

                if not game_id:
                    game_id = extract_data(node, './/a[contains(@href, "/boxscore?")]/@href')
                game_id = game_id.split('=')[-1]
                _hs     = []
                _as     = []

                hm_scores         = extract_data(node, './/tr[@class="homeTeam"]/td[not(@class)]/text()', '\n')
                aw_scores         = extract_data(node, './/tr[not(@class)]/td[not(@class)]/text()', '\n')
                aw_scores         = aw_scores.split('\n')
                hm_scores         = hm_scores.split('\n')
                away_total_score  = extract_data(node, './/table//tr[not(@class)]//td[@class="total"]//text()')
                home_total_score  = extract_data(node, './/table//tr[@class="homeTeam"]//td[@class="total"]//text()')
                result = [[[0], [str(home_total_score)], {'quarters': [], 'ot':[], \
                            'so':[], 'winner':0}], [[1], [str(away_total_score)], \
                                    {'quarters': [], 'ot':[], 'so':[], 'winner': 0}]]

                result[1][-1]['quarters']  = aw_scores
                result[0][-1]['quarters']  = hm_scores

                record['result']           = result
                record['source_key']       = game_id

                rich_data['event']    = self.events_dict.get(game_id, '')

                stats       = {}
                goal_scores = get_nodes(node, './/div[@class="scoringInfo"]//div[@class="goalDetails"]')
                try:
                    for gs in goal_scores:
                        periods = extract_list_data(gs, './preceding-sibling::b[1][contains(text(), "Period") or contains(text(), "1")\
                                            or contains(text(), "2") or contains(text(), "3") or \
                                            contains(text(), "OVERTIME")]/text()')[0].replace(' Period:', ''). \
                                            replace('st', '').replace('nd', '').replace('rd', '').replace('th', '').strip()
                        pn      = extract_data(gs, './/text()').replace('\n', ' ').split(' - ')
                        if ":" in pn[0]:
                            details = pn[0].replace(' (PPG)', '').replace(' (SHG)', '').split(' ')
                        else:
                            details = pn[0].replace('(PPG)', '').replace('(SHG)', '').split(' ' )
                        p_sk    = re.findall(r'id=(\d+)', extract_list_data(gs, './a/@href')[0])
                        details.append(pn[1])
                        details.append(p_sk)
                        if not stats.has_key(periods): stats[periods] = ()
                        results         = (tuple(details), )
                        stats[periods]  = stats[periods] + results
                except:
                    pass

                rich_data['stats']  = stats
                record['rich_data'] = rich_data

                sportssetupitem     = SportsSetupItem()
                for k, v in record.iteritems():
                    sportssetupitem[k] = v

                items.append(sportssetupitem)
                for item in items:
                    yield item

                continue

            elif "FINAL" not in game_time and "ET" in game_time: #Scheduled games
                game_datetime  = game_date+ ' ' + game_time
                game_datetime  = game_datetime.replace('ET', '').strip()
                date             = get_utc_time(game_datetime, '%m/%d/%Y %I:%M %p', 'US/Eastern')
                game_id        = extract_data(node, './/a[contains(@href, "preview")]/@href')
                game_id        = game_id.split('=')[-1]
                if not game_id :
                    game_id = date.split(' ')[0] + "_" + home_team_cs + "_" + away_team_cs
                result = {}
                if home_team_cs in TBD_TEAMS:
                    home_callsign = ['tbd1']
                    result[home_callsign[0]] = {'tbd_title': home_team_cs}
                if away_team_cs in TBD_TEAMS:
                    away_callsign = ['tbd2']
                    result[away_callsign[0]] = {'tbd_title': away_team_cs}


                record['result']       = result
                record['game_status']  = 'scheduled'
                record['source_key']   = game_id
                record['time_unknown'] = 0
            elif "FINAL" in game_time:
                game_datetime    = game_date
                date             = get_utc_time(game_datetime, '%m/%d/%Y', 'US/Eastern')
                game_id          = extract_data(node, './/a[contains(@href, "/recap")]/@href')
                if not game_id:
                    game_id      = extract_data(node, './/a[contains(@href, "/boxscore")]/@href')
                game_id          = game_id.split('=')[-1]

                record['source_key'] = game_id
                _hs = []
                _as = []

                hm_scores         = extract_data(node, './/tr[@class="homeTeam"]/td[not(@class)]//text()', '\n')
                aw_scores         = extract_data(node, './/tr[not(@class)]/td[not(@class)]//text()', '\n')
                hm_scores         = hm_scores.split('\n')
                aw_scores         = aw_scores.split('\n')
                hm_scores         = map(str, hm_scores)
                aw_scores         = map(str, aw_scores)
                away_total_score  = extract_data(node, './/table//tr[not(@class)]//td[@class="total"]//text()')
                home_total_score  = extract_data(node, './/table//tr[@class="homeTeam"]//td[@class="total"]//text()')
                result            = [[[0], [str(home_total_score)], {'quarters': [], 'ot':[], \
                'so':[], 'winner':0}], [[1], [str(away_total_score)], \
                                        {'quarters': [], 'ot':[], 'so':[], 'winner': 0}]]
                if int(home_total_score) > int(away_total_score):
                    result[0][-1]['winner'] = 1
                else:
                    result[1][-1]['winner'] = 1

                record['game_status'] = 'completed'

                stats = {}
                goal_scores = get_nodes(node, './/div[@class="scoringInfo"]//div[@class="goalDetails"]')
                try:
                    for gs in goal_scores:
                        periods = extract_list_data(gs, './preceding-sibling::b[1][contains(text(), "Period") or contains(text(), "1")\
                                            or contains(text(), "2") or contains(text(), "3") or contains(text(), "OVERTIME")]/text()')[0].\
                                            replace(' Period:', '').replace('st', '').replace('nd', '').\
                                            replace('rd', '').replace('th', '').strip()
                        pn = extract_data(gs, './/text()').replace('\n', '').split(' - ')

                        if ":" in pn[0]:
                            details = pn[0].replace(' (PPG)', '').replace(' (SHG)', '').split(' ')
                        else:
                            details = pn[0].replace('(PPG)', '').replace('(SHG)', '').split(' ')
                        p_sk = re.findall(r'id=(\d+)', extract_list_data(gs, './a/@href')[0])
                        details.append(pn[1])
                        details.append(p_sk)
                        if not stats.has_key(periods): stats[periods] = ()
                        results = (tuple(details), )
                        stats[periods] = stats[periods] + results
                except:
                    pass

                #rich_data['stats']  = stats
                record['rich_data'] = rich_data

            if "PPD" in game_time:
                game_datetime         = game_date
                date                  = get_utc_time(game_datetime, '%m/%d/%Y', 'US/Eastern')
                game_id               = extract_data(node, './/a[contains(@href, "preview")]/@href')
                game_id               = game_id.split('=')[-1]
                record['result']      = [[[0], [u''], {'quarters':[], 'ot': [], \
                'winner': 0}], [[1], [u''], {'quarters':[], 'ot': [], 'winner': 0}]]
                record['source_key']  = game_id

                record['game_status']      = 'postponed'

            Dt = date
            record['tz_info']  = get_tzinfo(city = city, game_datetime = Dt)
            if city == "San Jose" and not record['tz_info']:
                record['tz_info']  = get_tzinfo(city = city, country= "United States (USA)", game_datetime = Dt)

            record['game_datetime']   = Dt
            if str(home_team_cs) in EASTERN_FIRST and Dt.split(' ')[0] in FIRST_DATE:
                record['event']      = "NHL Eastern First Round"
            elif str(home_team_cs) in WESTERN_FIRST and Dt.split(' ')[0] in FIRST_DATE:
                record['event']       = "NHL Western First Round"
            elif str(home_team_cs) in EASTERN_FIRST and Dt.split(' ')[0] in COF_DATES:
                record['event']      = "NHL Eastern Conference Semifinals"
            elif str(home_team_cs) in WESTERN_FIRST and Dt.split(' ')[0] in COF_DATES:
                record['event']      = "NHL Western Conference Semifinals"
            elif str(home_team_cs) in EASTERN_FIRST and Dt.split(' ')[0] in COF_SEMI_DATES:
                record['event']      = "NHL Eastern Conference Finals"
            elif str(home_team_cs) in WESTERN_FIRST and Dt.split(' ')[0] in COF_SEMI_DATES:
                record['event']      = "NHL Western Conference Finals"
            elif str(home_team_cs) in FINAL_LIST and Dt.split(' ')[0] in FINAL_DATES:
                record['event']      = "Stanley Cup Finals"
            elif "2015-09" in Dt.split(' ')[0] or Dt.split(' ')[0] in ['2015-10-01', '2015-10-02', '2015-10-03', '2015-10-04', '2015-10-05']:
                record['event']      =  "NHL Preseason"
            elif "2015-10" in Dt.split(' ')[0] or "2015-11" in Dt.split(' ')[0] or "2015-12" in Dt.split(' ')[0] or "2016" in Dt.split(' ')[0]:
                record['event']      = "NHL Regular Season"

            if event_name == "NHL Winter Classic":
                record['event']      =  "NHL Winter Classic"
            if event_name == "NHL All-Star Game" :
                record['event'] =  "NHL All-Star Game"
                rich_data = {'stadium' : "Bridgestone Arena", 'city' : "Nashville", 'state' : "Tennessee", \
                                'channels' : str(channel_info)}
                record['tz_info'] = get_tzinfo(city = "Nashville", game_datetime = Dt)

            rich_data['update']       = datetime.datetime.now()
            #rich_data['event_name']   = self.events_dict.get(game_id, '')
            record['rich_data']       = rich_data

            if "PPD" in game_time:
                sportssetupitem = SportsSetupItem()
                for k, v in record.iteritems():
                    sportssetupitem[k] = v

                items.append(sportssetupitem)
                for item in items:
                    yield item


            if not "FINAL" in game_time  and  self.spider_type == "schedules":
                game_note = extract_data(node, './div[@class="gcLinks"]/text()').split(':')[0].strip()
                game_series = extract_data(node, './div[@class="gcLinks"]//span/text()').encode('utf-8')
                game_series = game_series.replace('\xe2\x80\xba', '').strip()
                game_note = game_note  + " " + game_series
                record['rich_data']['game_note'] = game_note.title()
                sportssetupitem = SportsSetupItem()
                for k, v in record.iteritems():
                    sportssetupitem[k] = v

                items.append(sportssetupitem)
                for item in items:
                    yield item

            elif "FINAL" in game_time and self.spider_type != "schedules":
                game_note = extract_data(node, './div[@class="gcLinks"]/text()').split(':')[0].strip()
                game_series = extract_data(node, './div[@class="gcLinks"]//span/text()').encode('utf-8')
                game_series = game_series.replace('\xe2\x80\xba', '').strip()
                game_note = game_note  + " " + game_series
                record['rich_data']['game_note'] = game_note.title()
                if len(aw_scores) == 6 and len(hm_scores) == 6:
                    result[1][-1]['quarters'] = aw_scores[:3]
                    result[1][-1]['quarters'] = aw_scores[:3]
                    result[0][-1]['quarters'] = hm_scores[:3]
                    result[1][-1]['ot']       = [aw_scores[3]]
                    result[1][-1]['so']       = [aw_scores[5].split('(')[0].strip()]
                    result[0][-1]['ot']       = [hm_scores[3]]
                    result[0][-1]['so']       = [hm_scores[5].split('(')[0].strip()]
                elif len(aw_scores) == 5 and len(hm_scores) == 5:
                    result[1][-1]['quarters'] = aw_scores[:3]
                    result[0][-1]['quarters'] = hm_scores[:3]
                    result[1][-1]['ot']       = [aw_scores[3]]
                    result[1][-1]['so']       = [aw_scores[4].split('(')[0].strip()]
                    result[0][-1]['ot']       = [hm_scores[3]]
                    result[0][-1]['so']       = [hm_scores[4].split('(')[0].strip()]
                elif len(aw_scores) == 4 and len(hm_scores) == 4:
                    result[1][-1]['quarters'] = aw_scores[:3]
                    result[0][-1]['quarters'] = hm_scores[:3]
                    result[1][-1]['ot']       = [aw_scores[3]]
                    result[0][-1]['ot']       = [hm_scores[3]]
                else:
                    result[1][-1]['quarters'] = aw_scores
                    result[0][-1]['quarters'] = hm_scores

                record['result']              = result
                recap_url = ''

                sportssetupitem = SportsSetupItem()
                for k, v in record.iteritems():
                    sportssetupitem[k] = v

                items.append(sportssetupitem)
                for item in items:
                    import pdb;pdb.set_trace()
                    yield item

                if "clips" in self.spider_type:
                    video_page_url      = extract_data(node, './/a[contains(@href, "videocenter/console?hlg=")]/@href')
                    video_page_url      = re.findall('\?hlg=(.*?)&', video_page_url)
                    if not video_page_url:
                        continue

                    video_page_url          = video_page_url[0].split(',')
                    season, _type, number   = (video_page_url)
                    video_page_url          = "http://video.nhl.com/videocenter/highlights"
                    recap_url               = 'http://www.nhl.com/ice/recap.htm?id=%s' % game_id

                    params = {
                                "xml"   : 1,
                                "season": season,
                                "type"  : _type,
                                "number": number
                            }

                    params = urllib.urlencode(params)
                    video_page_url = video_page_url + "?" + params
                    yield Request(video_page_url, callback = self.parse_newclips, \
                    meta = {'record': record, 'recap_url': recap_url})
                    break
                else:
                    sportssetupitem = SportsSetupItem()
                    for k, v in record.iteritems():
                        sportssetupitem[k] = v

                    items.append(sportssetupitem)
                    for item in items:
                        yield item
            else:
                continue

    @log
    def parse_newclips(self, response):
        items       = []
        record      = response.meta['record']
        recap_url   = response.meta['recap_url']

        hxs         = HtmlXPathSelector(response)
        season      = extract_data(hxs, '//game/season/text()', ' ')
        _type       = extract_data(hxs, '//game/game-type/text()', ' ')
        number      = extract_data(hxs, '//game/game-number/text()', ' ')

        goal_nodes = get_nodes(hxs, '//goals/goal')
        for node in goal_nodes:
            event_id                = extract_data(node, './event-id/text()')
            team                    = extract_data(node, './scoring-team-abbreviation/text()')
            name                    = extract_data(node, './scoring-player-name/text()')
            period                  = extract_data(node, './period/text()')
            period                  = get_period_suffix(period)
            _time                   = extract_data(node, './time/text()')
            title                   = 'GOAL: (%s) %s (%s in %s)' % (team, name, _time, period)
            clipDict = {}
            clipDict['sk']          = season + "-" + _type + "-" + number+ "-" + event_id
            clipDict['title']       = title
            clipDict['reference']   = recap_url
            image                   = extract_data(node, './/video-clip-thumbnail/@src')
            clipDict['img_link']    = image
            clip_url                = extract_data(node, './/alt-video-clip/text()')
            clipDict['a']           = {'url':clip_url, 'mimetype':'video/mp4'}
            record['rich_data']['video_links'].append(clipDict)

        hits_nodes  = get_nodes(hxs, '//hits/hit')
        for node in hits_nodes:
            event_id                = extract_data(node, './event-id/text()')
            hitter                  = extract_data(node, './hitting-player-name/text()')
            gothit                  = extract_data(node, './player-hit-name/text()')
            period                  = extract_data(node, './period/text()')
            period                  = get_period_suffix(period)
            _time                   = extract_data(node, './time/text()')
            title                   = 'HIT: %s on %s (%s in %s)' % (hitter, gothit, _time, period)
            clipDict = {}
            clipDict['sk']          = season + "-" + _type + "-" + number+ "-" + event_id
            clipDict['title']       = title
            clipDict['reference']   = recap_url
            image                   = extract_data(node, './/video-clip-thumbnail/@src')
            clipDict['img_link']    = image
            clip_url                = extract_data(node, './/alt-video-clip')
            clipDict['a']           = {'url':clip_url, 'mimetype':'video/mp4'}
            record['rich_data']['video_links'].append(clipDict)

        save_nodes  = get_nodes(hxs, '//saves/save')
        for node in save_nodes:
            event_id                = extract_data(node, './event-id/text()')
            goalie                  = extract_data(node, './goalie-name/text()')
            shooter                 = extract_data(node, './shooter-name/text()')
            period                  = extract_data(node, './period/text()')
            period                  = get_period_suffix(period)
            _time                   = extract_data(node, './time')
            title                   = 'SAVE: %s on %s (%s in %s)' % (goalie, shooter, _time, period)
            clipDict = {}
            clipDict['sk']          = season + "-" + _type + "-" + number+ "-" + event_id
            clipDict['title']       = title
            clipDict['reference']   = recap_url
            image                   = extract_data(node, './/video-clip-thumbnail')
            clipDict['img_link']    = image
            clip_url                = extract_data(node, './/alt-video-clip')
            clipDict['a']           = {'url':clip_url, 'mimetype':'video/mp4'}
            record['rich_data']['video_links'].append(clipDict)

        sportssetupitem = SportsSetupItem()
        for k, v in record.iteritems():
            sportssetupitem[k] = v

        items.append(sportssetupitem)
        for item in items:
            yield item
