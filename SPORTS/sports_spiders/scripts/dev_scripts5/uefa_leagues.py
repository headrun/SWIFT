import re
import time
import datetime
import urllib
from vtvspider_dev import VTVSpider, get_nodes, extract_data, log, get_utc_time, extract_list_data, get_tzinfo
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


def clean_text(data):
    data = data.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').strip()
    data = data.rstrip().lstrip().strip()
    return data

tou_dict = {'Belgian First League'   : 'Belgian First League',      'German Bundesliga'     : 'German Bundesliga',
            'Swedish First Division' : 'Swedish First Division',    'French Ligue 1'        : 'French Ligue 1',
            'Russian Premier League' : 'Russian Premier League',    'Spanish Liga'          : 'Spanish Liga',
            'Italian Serie A'        : 'Italian Serie A',       'Portuguese First Division' : 'Portuguese First Division',
            'English Premier League' : 'English Premier League',    'Scottish Premiership'  : 'Scottish Premier League',
            'Greek Super League'     : 'Greek Super League',        'Danish Super League'   : 'Danish Super League',
            'Dutch First Division'   : 'Dutch First Division',      'Turkish Super League'  : 'Turkish Super League',
            'Austrian Bundesliga'    : 'Austrian Bundesliga',    'Swiss Super League'       : 'Swiss Super League',
            'Romanian First Division'   : 'Romanian First Division', 'Polish First Division': 'Polish First Division',
            'Ukrainian Premier League'  : 'Ukrainian Premier League'}

#req_leagues = ['aut','eng','den','por','sco','ger','gre','esp','sui',
               #'tur','rus','fra','bel','swe','ned','ita', 'rou', 'ukr', 'pol']
req_leagues = ['sui']

required_tournaments = ['Spanish Liga', 'Italian Serie A', 'Belgian First League', 'Portuguese First Division', 'French Ligue 1', 'German Bundesliga', 'Russian Premier League', 'Swedish First Division', 'English Premier League', 'Scottish Premiership', 'Greek Super League', 'Dutch First Division', 'Turkish Super League', 'Austrian Bundesliga', 'Swiss Super League', 'Danish Super League', 'Polish First Division', 'Ukrainian Premier League', 'Romanian First Division']

sks = {"1652": "White Hart Lane", "52682":"Old Trafford",
       "2600537" : "KC Stadium", "7889" : "Anfield", "52919" : "Etihad Stadium",
       "52659" : "Liberty Stadium", "52914" : "Stamford Bridge",
       "52923" : "St. Mary's Stadium", "52916" : "Selhurst Park", "53358" : "Upton Park",
       "52683" : "Villa Park", "53356" : "Britannia Stadium",
       "50095" : "Cardiff City Stadium", "52921" : "Carrow Road",
       "52281" : "Goodison Park", "59324" : "St. James Park",
       "75386" : "Craven Cottage", "53360" : "Stadium Of Light",
       "53359" : "The Hawthorns", "52280" : "Emirates Stadium",
       "74070" : "Estadio de Vallecas", "50124" : "Vicente Calderon",
       "53042" : "El Sadar", "87960" : "Coliseum Alfonso Perez", "53043" : "Balaidos",
       "89561" : "Manuel Martinez Valero", "2602893" : "Los Carmenes",
       "50125" : "San Mames", "52714" : "Ramon Sanchez Pizjuan" ,
       "52265" : "Benito Villamarin", "70691" : "El Madrigal",
       "50080" : "Camp Nou" , "50051" : "Santiago Bernabeu",
       "52801" : "Jose Zorrilla", "52268" : "Mestalla",
       "2600292" : "Juegos Mediterraneos", "87959" : "Ciutat de Valencia",
       "54189" : "Cornella-El Prat", "50123" : "Anoeta", "74069" : "La Rosaleda",
       "51310" : "Friuli", "50139" : "Juventus Stadium", "50136" : "San Paolo",
       "52973" : "Olimpico", "52969" : "Renato Dall'Ara",
       "50029" : "Olimpico di Torino", "2600632" : "Stadio Citta del Tricolore",
       "54262" : "Ennio Tardini", "88183" : "Armando Picchi",
       "52816" : "A. Azzurri d'Italia", "50089" : "Luigi Ferraris",
       "50138" : "San Siro", "50137" : "Olimpico", "93736" : "Angelo Massimino",
       "52972" : "Luigi Ferraris", "59013" : "Stadio Is Arenas",
       "52817" : "Artemio Franchi", "50058" : "San Siro", "77904" : "M. Bentegodi",
       "52301" : "M. Bentegodi", "50072" : "Commerzbank-Arena",
       "50107" : "Mercedes-Benz Arena", "50037" : "Allianz Arena", "50109" : "BayArena",
       "52758" : "Signal Iduna Park", "57388" : "Veltins-Arena",
       "59880" : "MAGE SOLAR Stadion", "5451" : "Olympiastadion",
       "64332" : "Volkswagen Arena", "50040" : "Weserstadion",
       "57488" : "AWD-Arena", "50108" : "Grundig-Stadion",
       "52757" : "Signal Iduna Park", "52167" : "Imtech Arena",
       "2600978" : "SGL Arena", "65172" : "Eintracht-Stadion",
       "2600431" : "Rhein-Neckar-Arena", "70853" : "Coface Arena",
       "52355" : "Municipal du Ray", "52747" : "Parc des Princes",
       "52748" : "Stade Velodrome", "5312" : "Stade Gerland",
       "50127" : "Stade Chaban-Delmas", "50022" : "Geoffroy-Guichard",
       "52693" : "La Beaujoire", "75797" : "Grand Stade Lille Metropole",
       "92381" : "Stade du Hainaut", "55031" : "Stade de la Route de Lorient",
       "52813" : "Auguste Bonal", "50023" : "Stade Louis II",
       "52934" : "Municipal Toulouse", "68499" : "Stade du Moustoir",
       "61567" : "Armand Cesari", "50128" : "La Mosson",
       "64165" : "Auguste Delaune", "79942" : "Francois Coty",
       "60442" : "Du Roudourou", "2601670" : "Parc des Sports Annecy"}


class UefaLeagues(VTVSpider):
    name            = "uefa_leagues"
    start_urls      = ['http://www.uefa.com/memberassociations/index.html']

    #@log
    def parse(self, response):
        hdoc  =  HtmlXPathSelector(response)
        leagues   = get_nodes(hdoc, '//div[@class="hdr-list-wrap hdr-wrap-ma"]/ul[@class="hdr-layer-list hdr-list-ma"]/li[@class="hdr-layer-item hdr-item-ma"]')

        for league in leagues:
            league_name = clean_text(extract_data(league, './a//text()')).strip()
            league_name = league_name.decode('utf-8').encode('iso-8859-1')
            league_link = extract_data(league, './/a[contains(@href,"memberassociations")]/@href').strip()
            league_link = league_link.decode('utf-8').encode('iso-8859-1')
            league      = league_link.split("/association=")[1].split("/")[0]

            if league in req_leagues:
                league_link = "http://www.uefa.com" + league_link
                yield Request(league_link, callback = self.parse_first, meta={'league'  : league, 'leagues_list' : required_tournaments, 'flag' : 0})

    #@log
    def parse_first(self, response):
        hdoc  =  HtmlXPathSelector(response)
        if not response.meta['flag']:
            link = extract_data(hdoc, '//li[@id="dml3_league"]/a/@href')
            if link:
                new_link      = "http://www.uefa.com"+ link
                yield Request(new_link, callback=self.parse_first, meta = {'league' : response.meta['league'],
                                                                           'flag': 1, 'leagues_list' : response.meta['leagues_list']})

        elif response.meta['flag'] == 1:
            matches_link  = "http://www.uefa.com" + extract_data(hdoc, '//li[@id="dml3_matches"]/a/@href')

            if matches_link:
                yield Request(matches_link, callback=self.parse_first, meta = {'league' : response.meta['league'],
                                                                               'flag': 2, 'leagues_list' : response.meta['leagues_list']})


        elif response.meta['flag'] == 2:
            tou             = extract_data(hdoc, '//div/h2[@class="bigTitle"]/text()')
            tournament_name = tou.split(' 20')[0]
            games           = []

            t_url  =  "http://www.uefa.com/memberassociations/association=%s/domesticleague/matches/month=%s/_includematch.html"
            d       = datetime.datetime.today()
            m       = d.month
            MONTHS  = ['1', '2', '3', '4', '5','6','7','8', '9', '10','11', '12']
            if self.spider_type == 'scores':
                if not m == '1':
                    month_list   = [m, m - 1]
                else:
                    month_list   = [m, '12']
            else:
                if not m > '9':
                    month_list  = MONTHS[m:] + ['1', '2', '3', '4', '5', '6'] + [str(d.month)]
                else:
                     month_list  = MONTHS[m:] + [str(d.month)]

            allowed_soccer_tournaments = {response.meta['league'] : month_list}

            if tournament_name and tournament_name in response.meta['leagues_list']:
                for country, month in allowed_soccer_tournaments.iteritems():
                    if country == response.meta['league']:
                        for m in month:
                            games.append(t_url % (country, m))

                        for game_link in games:
                            if game_link and tournament_name in response.meta['leagues_list']:
                                yield Request(game_link, callback = self.parse_schedules, meta = {'tournament_name': tournament_name,\
                                                       'league':response.meta['league'] ,'country': country})

    #@log
    def parse_schedules(self, response):
        hdoc  =  HtmlXPathSelector(response)
        tournament_name = response.meta['tournament_name']
        #nodes           = hdoc.select('//tr[@class=" odd"]|//tr[@class=""]|//tr[contains(@class,"fs15 nbb npd-")]')
        nodes           = get_nodes(hdoc, '//tr[@class=" odd"]|//tr[@class=""]|//tr[contains(@class,"fs15 nbb npd-")]|//tr[contains(@class,"mtc-venue")]')
        sportssetupitem = SportsSetupItem()
        game_note = ''
        for node in nodes:
            match_status = extract_data(node, './/preceding-sibling::td[@class="headInfo"][1]//text()').replace("\n", "")
            if not match_status:
                match_status = extract_data(node, './following-sibling::tr[3][@class="reasonwin odd"][1]/td/text()')

            home_scores = extract_data(node, './/following::tr[contains(@class,"mtc-scorers")][1]//td[@class="r"]//text()').strip()
            away_scores = extract_data(node, './/following::tr[contains(@class,"mtc-scorers")][1]//td[@class="l"]//text()').strip()

            hme_scores, awy_scores = [], []
            home_half_time = 0
            for scr in home_scores.split(","):
                scr = re.findall('(\d+)', scr)
                if scr:
                    if int(scr[0]) <= 45:
                        home_half_time += 1

            away_half_time = 0
            for scr in away_scores.split(","):
                scr = re.findall('(\d+)', scr)
                if scr:
                     if int(scr[0]) <= 45:
                         away_half_time += 1
            tzinfo = ''
            if node:
                location = extract_data(node, './following-sibling::tr[2][@class="mtc-venue npd-tb"]/td[contains(text(), "Venue:")]/text()').strip()
                if location:
                    location = "".join(re.findall('Venue: (.*)', location))
                    tzinfo = get_tzinfo(city = location)

            home_id = away_id = ''
            home = extract_data(node, './/td[@class="r"]/a[contains(@href,"/teams/club=")]/@href')
            if home:
                home_id = re.findall(r'/teamsandplayers/teams/club=(\d+)/domestic/index.html', home)

            away = extract_data(node, './/td[@class="l"]/a[contains(@href,"/teams/club=")]/@href')
            if away:
                away_id = re.findall(r'/teamsandplayers/teams/club=(\d+)/domestic/index.html', away)

            game_time   = extract_data(node, './/td[@class="fs19 b nw"]/a/text()')

            link        = extract_data(node, './/td[@class="fs19 b nw"]/a/@href').strip() or extract_data(node, './/p[@class="mloglink"]/a/@href').strip()
            half_time   = {}
            game_link   = ''
            game_sk     = ''
            if link:
                game_sk   = link.split("match=")[1].split("/")[0]
                game_link = "http://www.uefa.com"+link
                if not match_status:
                    match_status = extract_data(node, './/p[@class="mloglink"]/a/text()')
                half_time = {'home_half' : home_half_time, 'away_half' : away_half_time}

            if node:
                date = extract_data(node, './/p[@class="mlogdate"]/span/text()')
                if '- ' in date:
                    game_note = date.split('- ')[1].strip()

            if date:
                orginal_date = date.strip('- Second round').strip('- First round').strip('- Play-off').strip()
                date_object = datetime.datetime(*time.strptime(orginal_date, '%d/%m/%Y')[0:6])
                game_date   = date_object.strftime('%Y-%m-%d')
                now         = datetime.datetime.utcnow()
                date_now    = now.strftime("%Y-%m-%d %H:%M:%S")

            channel     = ''
            location    = ''
            stadium     = ''
            event_name  = ''
            utc_dt_time = ''
            record_data = {}

            for site_name, db_name in tou_dict.iteritems():
                if site_name == tournament_name:
                    tou_name = db_name

                    if ":" in game_time:
                        game_date   = game_date + " " +  game_time
                        date_object = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M')[0:6])
                        game_date   = date_object.strftime('%Y-%m-%d %H:%M:%S')
                        now         = datetime.datetime.utcnow()
                        date_now    = now.strftime("%Y-%m-%d %H:%M:%S")

                        game_dt_time    = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M:%S')[0:6])
                        utc_dt_time_format     = game_dt_time - datetime.timedelta(hours=2)
                        #utc_game_date   = time.strftime("%Y-%m-%d %H:%M:%S", utc_dt_time.timetuple())
                        utc_dt_time = get_utc_time(game_date, '%Y-%m-%d %H:%M:%S', 'CET')


                        if away_id == '50151' and home_id == '53025' and game_sk == '200105195':
                            game_sk = str(game_sk) + "1"

                        for home_sk, stad in sks.iteritems():
                            if home_id == home_sk:
                                stadium = stad
                                record_data['stadium'] = stadium

                        if "schedules" in self.spider_type and utc_dt_time > str(now):
                            items           =   []
                            if home_id and away_id:
                                participants    =   {home_id[0]: (1, ''), away_id[0]: (0, '')}
                            rich_data       =   {'location' : location, 'game_note' : game_note}

                            sportssetupitem['source']           = "uefa_soccer"
                            sportssetupitem['source_key']       = game_sk
                            sportssetupitem['game_status']      = "scheduled"
                            sportssetupitem['tournament']       = tou_name
                            sportssetupitem['game']             = "soccer"
                            sportssetupitem['participants']     = participants
                            sportssetupitem['reference_url']    = game_link
                            sportssetupitem['game_datetime']    = utc_dt_time
                            sportssetupitem['game']             = "soccer"
                            sportssetupitem['affiliation']      = "uefa"
                            sportssetupitem['participant_type'] = "team"
                            sportssetupitem['rich_data']        = rich_data
                            sportssetupitem['event']            = event_name
                            sportssetupitem['tz_info'] = tzinfo

                            items.append(sportssetupitem)
                            for item in items:
                                yield item

                        elif "scores" in self.spider_type and utc_dt_time <= str(now)\
                                    and now < (utc_dt_time_format + datetime.timedelta(hours=1)) and game_link:
                            #game_date   = date_object.strftime('%Y-%m-%d %H:%M:%S')
                            game_date = get_utc_time(game_date, "%Y-%m-%d %H:%M:%S", 'US/Eastern')
                            status      = "ongoing"
                            yield Request(game_link, callback = self.parse_scores,
                                          meta = {'game_sk': game_sk, 'game': 'soccer', \
                                          'home_id' : home_id, 'away_id'    : away_id, 'status': status,\
                                          'tou_name': tou_name, 'game_date' : utc_dt_time,
                                          'league'  : response.meta['league'],
                                          'location'  : location, 'half_time'  : half_time,\
                                          'game_note' : game_note, 'tz_info' : tzinfo})

                        elif "scores" in self.spider_type and utc_dt_time < str(now) and\
                                        now > (utc_dt_time_format + datetime.timedelta(hours=1)) and game_link:
                            #game_date   = date_object.strftime('%Y-%m-%d %H:%M:%S')
                            game_date = get_utc_time(game_date, "%Y-%m-%d %H:%M:%S", 'US/Eastern')
                            status      = "completed"
                            yield Request(game_link, callback = self.parse_scores, \
                                          meta = {'game_sk'   : game_sk, 'game'   : 'soccer',
                                                'home_id'   : home_id, 'away_id': away_id,
                                                'status'    : status, 'tou_name': tou_name,
                                                'game_date' : utc_dt_time, 'game_note' : game_note,
                                                'league'    : response.meta['league'], 'tz_info' : tzinfo,
                                                'location'  : location, 'half_time'  : half_time})

                        elif "scores" in self.spider_type  and utc_dt_time < str(now) and game_link:
                            #game_date   = date_object.strftime('%Y-%m-%d %H:%M:%S')
                            game_date = get_utc_time(game_date, "%d/%m/%Y %H:%M:%S", 'US/Eastern')
                            status      = "completed"
                            yield Request(game_link, callback = self.parse_scores,
                                          meta = {'game_sk': game_sk, 'game': 'soccer',
                                          'home_id' : home_id, 'away_id'    : away_id,
                                          'status'  : status, 'tou_name': tou_name,
                                          'game_date' : utc_dt_time, 'game_note' : game_note,
                                          'league'  : response.meta['league'], 'tz_info' : tzinfo,
                                          'location'  : location, 'half_time'  : half_time})

                    elif "abandoned" in match_status and game_link:
                        status = "postponed"
                        game_date = get_utc_time(orginal_date, '%d/%m/%Y', 'US/Eastern')
                        yield Request(game_link, callback = self.parse_scores,
                                        meta={'game_sk': game_sk, 'game'  : 'soccer',
                                        'home_id' : home_id, 'away_id'    : away_id,
                                        'status'  : status, 'tou_name'    : tou_name,
                                        'game_date'   : game_date, 'game_note' : game_note,
                                        'league'  : response.meta['league'], 'location'  : location, 
                                        'half_time'  : half_time, 'tz_info' : tzinfo})

                    elif match_status == "-" and "postponed" in match_status and game_link:
                        status = "postponed"
                        game_date = get_utc_time(orginal_date, '%d/%m/%Y', 'US/Eastern')
                        yield Request(game_link, callback = self.parse_scores,
                                      meta={'game_sk'   : game_sk, 'game': 'soccer',
                                      'home_id' : home_id, 'away_id'    : away_id,
                                      'status'  : status, 'tou_name': tou_name,
                                      'game_date' : game_date, 'game_note' : game_note,
                                      'league'  : response.meta['league'], 'tz_info' : tzinfo, 
                                      'location'  : location, 'half_time'  : half_time})

                    elif "-" in game_time and "scores" in self.spider_type and game_date < date_now and game_link:
                        #game_date   = date_object.strftime('%Y-%m-%d %H:%M:%S')
                        game_date = get_utc_time(orginal_date, '%d/%m/%Y', 'US/Eastern')
                        status      = "completed"
                        yield Request(game_link, callback = self.parse_scores,
                                        meta={'game_sk': game_sk,
                                        'game'      : 'soccer', 'home_id'   : home_id,
                                        'away_id'   : away_id, 'status'     : status,
                                        'tou_name'  : tou_name, 'game_date' : game_date, 'tz_info' : tzinfo,
                                        'league'    : response.meta['league'], 'game_note' : game_note,
                                        'location'  : location, 'half_time'  : half_time})


    #@log
    def parse_scores(self, response):
        hdoc = HtmlXPathSelector(response)

        final_link = response.url
        tour_name  = extract_data(hdoc, '//div[@class="hidden-title"]/text()')

        total_scores = extract_data(hdoc, '//div[@class="score"]//div[contains(@class, "score-element")]//text')
        match_status = extract_data(hdoc, '//div[@class="match-state"]//text()')
        status = ''
        if "postponed" in match_status:
            status = "postponed"
        if "Full Time" in match_status:
            status = "completed"

        game_id     = response.url.split("match=")[1].split("/")[0]
        home_id     = extract_list_data(hdoc, '//div[@class="main-view"]//div[@class="home-team"]/@data-team')
        away_id     = extract_list_data(hdoc, '//div[@class="main-view"]//div[@class="away-team"]/@data-team')
        home_name   = extract_data(hdoc, '//div[@class="main-view"]//div[@class="home-team"]//h3/span/text()')
        away_name   = extract_data(hdoc, '//div[@class="main-view"]//div[@class="away-team"]//h3/span/text()')

        if home_id: home_id = home_id[0]
        if away_id: away_id = away_id[0]

        home_goals = away_goals = final_score = ''
        final_score = extract_data(hdoc, '//div[@class="main-view"]//div[@class="score"]//div[@class="score-element main-view-score rounded"]/text()')
        if '-' in final_score:
            goals = final_score.split('-')
            home_goals = goals[0]
            away_goals = goals[1]

        final_link  = extract_data(hdoc, '//script[contains(@src, "/season=")]/@src').strip()

        if final_link and status != 'postponed':
            link        = final_link.split("/config")[0]
            scores_link = "http://www.uefa.com/livecommon" + link + "/feed/teams.match.json"
            if not status:
                status  = response.meta['status']

            stad = ''
            if sks.has_key(home_id):
                stad = sks.get(home_id, '')

            yield Request(scores_link, callback = self.parse_final,
                                meta = {'game_id'   : game_id, 'game'   : 'soccer',
                                        'home_id'   : home_id, 'away_id': away_id,
                                        'status'    : response.meta['status'],
                                        'game_date' : response.meta['game_date'],
                                        'final_link': final_link, 'game_note' : response.meta['game_note'],
                                        'tou_name'  : response.meta['tou_name'],
                                        'location'  : response.meta['location'],
                                        'stadium'   : stad, 'half_time' : response.meta['half_time'],
                                        'final_score' : final_score, 'home_goals' : home_goals,
                                        'away_goals' : away_goals, 'tz_info' : response.meta['tz_info']})

        elif status == 'postponed':
            record = SportsSetupItem()
            record['rich_data'] = {}
            record['source_key'] = game_id
            record['game'] = 'soccer'
            record['participants'] = {str(home_id): (1, ''), str(away_id): (0, '')}
            record['reference_url'] = response.url
            record['game_datetime'] = response.meta['game_date']
            record['tournament'] = response.meta['tou_name']
            record['affiliation']      = "uefa"
            record['rich_data']['game_note'] = response.meta['game_note']
            record['participant_type'] = 'team'
            record['game_status'] = status
            record['tz_info'] = response.meta['tz_info']
            record['source'] = 'uefa_soccer'
            yield record



    #@log
    def parse_final(self, response):
        hdoc = HtmlXPathSelector(response)
        result  = {}

        sportssetupitem = SportsSetupItem()
        info            = eval(urllib.urlopen(response.url).read())
        away            = info.get('Away', '')
        MatchStat       = away.get('MatchStat', '')
        away_shots      = MatchStat.get('TotalAttempts', '')

        if away_shots:
            away_shots = away_shots
        else:
            away_shots = ''
        away_shots_on_goal = MatchStat.get('AttempsOn','')

        if away_shots_on_goal:
            away_shots_on_goal= away_shots_on_goal
        else:
            away_shots_on_goal = ''
        away_cornerkicks = MatchStat.get('Corners', '')

        if away_cornerkicks:
            away_cornerkicks = away_cornerkicks
        else:
            away_cornerkicks = ''
        away_saves = MatchStat.get('Saves', '')

        if away_saves:
            away_saves = away_saves
        else:
            away_saves = ''

        away_yellow_cards = MatchStat.get('YellowCard', '')
        if away_yellow_cards:
            away_yellow_cards = away_yellow_cards
        else:
            away_yellow_cards = ''

        away_red_cards = MatchStat.get('RedCard', '')
        if away_red_cards:
            away_red_cards = away_red_cards
        else:
            away_red_cards = ''

        away_team_id = MatchStat.get('TeamId','')
        if away_team_id:
            away_team_id = str(away_team_id)
        else:
            away_team_id = ''

        #away_goals = MatchStat.get('GoalsScored','')
        away_goals = response.meta['away_goals']
        away_fouls = MatchStat.get('FoulsCommitted','')
        if away_fouls:
            away_fouls = away_fouls
        else:
            away_fouls = ''

        away_offsides = MatchStat.get('Offside','')
        if away_offsides:
            away_offsides = away_offsides
        else:
            away_offsides = away_offsides

        away_possession = MatchStat.get('BallPossession', '')
        if away_possession:
            away_possession = away_possession
        else:
            away_possession = ''

        home        = info.get('Home', '')
        h_MatchStat = home.get('MatchStat', '')
        home_shots  = h_MatchStat.get('TotalAttempts', '')

        if home_shots:
            home_shots = home_shots
        else:
            home_shots = ''

        home_shots_on_goal = h_MatchStat.get('AttempsOn','')
        if home_shots_on_goal:
            home_shots_on_goal = home_shots_on_goal
        else:
            home_shots_on_goal = ''

        home_cornerkicks = h_MatchStat.get('Corners', '')
        if home_cornerkicks:
            home_cornerkicks = home_cornerkicks
        else:
            home_cornerkicks = ''

        home_saves = h_MatchStat.get('Saves', '')
        if home_saves:
            home_saves = home_saves
        else:
            home_saves = ''

        home_yellow_cards = h_MatchStat.get('YellowCard', '')
        if home_yellow_cards:
            home_yellow_cards = home_yellow_cards
        else:
            home_yellow_cards = ''

        home_red_cards = h_MatchStat.get('RedCard', '')
        if home_red_cards:
            home_red_cards = home_red_cards
        else:
            home_red_cards = ''

        home_team_id = h_MatchStat.get('TeamId','')
        if home_team_id:
            home_team_id = str(home_team_id)
        else:
            home_team_id = ''

        #home_goals = h_MatchStat.get('GoalsScored','')
        home_goals = response.meta['home_goals']
        home_fouls = h_MatchStat.get('FoulsCommitted','')
        if home_fouls:
            home_fouls = home_fouls
        else:
            home_fouls = ''

        home_offsides = h_MatchStat.get('Offside','')
        if home_offsides:
            home_offsides= home_offsides
        else:
            home_offsides = ''

        home_possession = h_MatchStat.get('BallPossession', '')
        if home_possession:
            home_possession = home_possession
        else:
            home_possession = ''

        result.setdefault(home_team_id, {}).update({'shots'     : home_shots, 'shots_on_goal'  : home_shots_on_goal,
                       'fouls'     : home_fouls, 'cornerkicks'    : home_cornerkicks,
                       'offsides'  : home_offsides, 'possession'  : home_possession,
                       'yellow_cards'  : home_yellow_cards ,
                       'red_cards'     : home_red_cards, 'saves'  : home_saves})

        result.setdefault(away_team_id, {}).update({'shots'     : away_shots, 'shots_on_goal'  : away_shots_on_goal,
                       'fouls'     : away_fouls, 'cornerkicks'    : away_cornerkicks,
                       'offsides'  : away_offsides, 'possession'  : away_possession,
                       'yellow_cards'  : away_yellow_cards,
                       'red_cards'     : away_red_cards, 'saves'  : away_saves})

        result[home_team_id]['H1'] = response.meta['half_time'].get('home_half', '')
        result[away_team_id]['H1'] = response.meta['half_time'].get('away_half', '')

        total_score = str(home_goals) + " " + "-" + " " + str(away_goals)

        result[home_team_id]['final'] = str(home_goals)
        result[away_team_id]['final'] = str(away_goals)

        is_tie = False

        winner = ''
        if response.meta['status'] == "completed" and "-" in response.meta['final_score']:
            result.setdefault('0', {})['score'] = response.meta['final_score']
            home_goals = int(home_goals)
            away_goals = int(away_goals)

            result[home_team_id]['final'] = home_goals
            result[away_team_id]['final'] = away_goals

            if int(home_goals) > int(away_goals):
                result['0']['winner'] = str(home_team_id)
            elif int(home_goals) < int(away_goals):
                result['0']['winner'] = str(away_team_id)
            elif int(home_goals) == int(away_goals):
                is_tie      = True

        channel     = ''
        location    = ''
        stadium     = ''
        event_name  = ''
        now         = datetime.datetime.now()
        date_now    = now.strftime("%Y-%m-%d %H:%M:%S")

        participants    = {str(home_team_id): (1, ''), str(away_team_id): (0, '')}
        stats   = {}

        rich_data   = {'stats' : stats, 'stadium' : response.meta['stadium'], 'game_note' : response.meta['game_note'],\
                        'location' : response.meta['location']}

        if response.meta['game_date'] <= date_now:
            items   = []
            sportssetupitem['source']           = 'uefa_soccer'
            sportssetupitem['source_key']       = response.meta['game_id']
            sportssetupitem['game_status']      = response.meta['status']
            sportssetupitem['game_datetime']    = response.meta['game_date']
            sportssetupitem['participants']     = participants
            sportssetupitem['result']           = result
            sportssetupitem['reference_url']    = response.url
            sportssetupitem['tournament']       = response.meta['tou_name']
            sportssetupitem['game']             = 'soccer'
            sportssetupitem['event']            = event_name
            sportssetupitem['rich_data']        = rich_data
            sportssetupitem['tz_info']          = response.meta['tz_info']

            items.append(sportssetupitem)
            for item in items:
                yield item
