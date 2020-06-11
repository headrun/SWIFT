import datetime, time
from datetime import timedelta
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, get_nodes, extract_data, extract_list_data, log, get_utc_time, get_tzinfo
from vtvspider_test import get_height, get_weight
import pytz

true = True
false = False
null = ''


STATUS_DICT = {'final': 'completed', 'completed': 'completed', 'game over': \
               'completed', 'in progress': 'ongoing', 'postponed': 'postponed', \
               'suspended': 'cancelled', 'cancelled' : 'cancelled', 'completed early' : 'completed',
               'preview': 'scheduled'}

COLLEGE_TEAMS = ['Giants Futures', 'Tigres', 'Blue Wahoos', 'Braves Futures', \
'Tides', 'Biscuits', 'Bats', 'National', 'American', 'NL Lower Seed', \
'NL Higher Seed', 'AL Champion', 'NL Champion', "TBD"]

STATES_DICT = {'Cleveland' : 'Ohio', 'Toronto' : 'Ontario', 'Detroit' :
'Michigan', 'Flushing' : 'Michigan', 'Cincinnati' : 'Ohio', 'Philadelphia' :
'Pennsylvania', 'Washington' : 'Washington', 'Boston' : 'Massachusetts',
'Milwaukee' : 'Wisconsin', 'Chicago' : 'Illinois', 'Arlington' : 'Texas',
'San Francisco' : 'California', 'Seattle' : 'Washington', 'Los Angeles' :
'California', 'Phoenix' : 'Arizona', 'Bronx' :  'New York City', 'Baltimore' :
'Maryland', 'Atlanta' : 'Georgia', 'Minneapolis' : 'Minnesota', 'St. Louis' :
'Missouri', 'Anaheim' : 'California', 'San Diego' : 'California', 'Miami' :
'Florida', 'Houston' : 'Texas', 'Pittsburgh' : 'Pennsylvania', 'St. Petersburg'
: 'Florida', 'Denver' : 'Colorado', 'Kansas City' : 'Missouri', 'Oakland' :
'California'}


def get_city_state_country(location):
    loc = {}
    if STATES_DICT.has_key(location):
        state = STATES_DICT[location]
        country = 'United States'
    else:
        state = ''
        country = ''

    loc = {'city' : location, 'state' : state, 'country' : country}

    return loc

class MLBSpider(VTVSpider):

    name    = "mlbspider"
    allowed_domains = ['mlb.mlb.com']
    start_urls = []
    game_note_dict = {}

    def get_timezone_info(self, loc, city_info, date):
        if city_info == "Ft. Myers":
            city_info = "Fort Myers"
        if city_info:
            tz_info = get_tzinfo(city = city_info, game_datetime= date)
            if not tz_info:
                tz_info = get_tzinfo(city = city_info, country = loc.get('country', ''), game_datetime= date)
        else:
            tz_info = get_tzinfo(city = loc.get('city', ''))
        return tz_info

    def get_channels(self, channel_home, channel_away):
        if channel_home == {}:
            channel_home = ''
        if channel_away == {}:
            channel_away = ''
        channels = ''
        channel_list = []
        channel_list = [chn.strip() for chn in channel_home.split(',')]
        away_channels = channel_away.split(',')
        for away_chanel in away_channels:
            if away_chanel.strip() not in channel_list:
                channel_list.append(away_chanel.strip())

        channels = '<>'.join(ch_ for ch_ in channel_list if ch_)
        return channels


    def start_requests(self):
        req = []
        next_week_days = []
        date_time = datetime.datetime.now()
        top_url = 'http://mlb.mlb.com/gdcross/components/game/mlb/%s/master_scoreboard.json'
        if  self.spider_type == "schedules":
            for i in range(0, 500):
                next_week_days.append((date_time + datetime.timedelta(days=i)).strftime('year_%Y/month_%m/day_%d'))
        else:
            for i in range(0, 3):
                next_week_days.append((date_time - datetime.timedelta(days=i)).strftime('year_%Y/month_%m/day_%d'))

        for wday in next_week_days:
            r = top_url % wday
            yield Request(r, callback = self.parse)

    @log
    def parse(self, response):
        result = [[[0],[u''],{'winner': '', 'ot': [], 'hits':'', 'errors':'', \
                 'innings_scores':[]}], [[1],[u''],{'winner': '', 'ot': [], \
                 'hits':'','errors':'','innings_scores':[]}]]
        games_data = eval(response.body)

        pk_dict = eval(open('pk_channel_mapping', 'r').read())
        if 'schedules' == self.spider_type:
            if games_data:
                items = self.get_schedules(games_data, pk_dict, result)
                for item in items:
                    yield item

        elif "scores" == self.spider_type  or "clips" == self.spider_type:
            now = datetime.datetime.now()
            data = games_data['data']['games']

            data_games = data.get('game', {})

            if type(data_games) is not list and data_games:
                data_games = [data_games]

            items = []
            for data in data_games:
                sportssetupitem = SportsSetupItem()
                radio = ""
                channels = []
                home_tbd_title = ()
                away_tbd_title = ()
                tbd_tuple = ()
                r = {}
                team_names = []
                home_team_name = data['home_team_name']
                home_name_abbrev = data['home_name_abbrev']

                if home_team_name in COLLEGE_TEAMS:
                    home_name_abbrev = 'tbd1'
                    home_tbd_title = (home_name_abbrev, home_team_name)
                    team_names.append({'callsign': home_name_abbrev, 'name': home_team_name})
                elif home_team_name and home_name_abbrev:
                    team_names.append({'callsign': home_name_abbrev, 'name': home_team_name})
                away_team_name = data['away_team_name']
                away_name_abbrev = data['away_name_abbrev']

                if away_team_name in COLLEGE_TEAMS:
                    away_name_abbrev = 'tbd2'
                    away_tbd_title = (away_name_abbrev, away_team_name)
                    team_names.append({'callsign': away_name_abbrev, 'name': away_team_name})
                elif away_team_name and away_name_abbrev:
                    team_names.append({'callsign': away_name_abbrev, 'name': away_team_name})
                if home_tbd_title and away_tbd_title:
                    tbd_tuple = (home_tbd_title, away_tbd_title)
                elif home_tbd_title and not away_tbd_title:
                    tbd_tuple = (home_tbd_title)
                else:
                    tbd_tuple = (away_tbd_title)

                game_venue = data['venue']
                game_location =  data['home_team_city']
                city_info = data.get('location', '')
                if not city_info:
                    city_info = game_location
                loc = get_city_state_country(game_location)
                game_id = data.get('id', '')
                game_pk = data.get("game_pk", '')
                channel_home = data.get('broadcast', '')
                if channel_home:
                    radio = channel_home.get('home', '').get('radio', '')
                    channel_home = channel_home.get('home', '').get('tv', '')
                    if "," in radio:
                        radio = '<>'.join(radio_.strip() for radio_ in radio.split(','))
                    elif radio == {}:
                        radio = ""
                channel_away = data.get('broadcast', '')
                if channel_away:
                    channel_away = channel_away.get('away', '').get('tv', '')

                if game_pk in pk_dict.keys():
                    channels.append(pk_dict[game_pk])

                time_ampm = data['ampm']
                date = data['time_date'].replace('/', '-').strip()
                date = date + time_ampm
                game_id = game_id.replace('/', '_').replace('-', '_')
                reference_url = 'http://mlb.mlb.com/mlb/gameday/index.jsp?gid=' + game_id
                if time_ampm:
                    date = get_utc_time(date, '%Y-%m-%d %I:%M%p', 'US/Eastern')
                else:
                    date = get_utc_time(date, '%Y-%m-%d %I:%M', 'US/Eastern')
                sportssetupitem['game_datetime'] = str(date)
                tz_info = self.get_timezone_info(loc, city_info, date)

                gamestatus = data['status']['status']

                game_status = self.get_game_status(gamestatus)
                if not game_status:
                    continue
                if not gamestatus:
                    print 'gamestatus not found for %s' %reference_url
                if 'linescore' in  data.keys():
                    result = self.get_result(data)
                else:
                    print "Game status is not Completed"

                event  = ''
                tou_name = ''
                game_type = data['game_type']
                if game_type in ["R", "S"]:
                    division = ''
                else:
                    division = data.get('series', '')
                game_type, tou_name, event = self.get_tou_event(game_type, division=division)

                if game_id == "2015_03_19_texmlb_cinmlb_1":import pdb;pdb.set_trace()
                rich_data  = {'video_links': [], 'update': now, 'stadium': game_venue,
                              'game_status': game_status, 'game_pk': game_pk, 'Radio': str(radio),
                              'reference_url': reference_url, 'game_type': game_type,
                              'event' : event, 'tou_name' : tou_name, 'tbd_title': tbd_tuple}
                game_data = {'participants': team_names, 'game': 'baseball',
                              'participant_type': 'team', 'tournament': 'MLB Baseball',
                              'affiliation': 'mlb', 'source': 'mlb', 'event': event,
                              'game_status': game_status, 'result': result,
                              'rich_data': rich_data, 'source_key': game_id,
                              'tz_info': tz_info}
                channels_info = self.get_channels(channel_home, channel_away)
                if channels:
                    rich_data['channels'] = channels
                else:
                    rich_data['channels'] = str(channels_info)

                if game_status == 'cancelled' or game_status == 'postponed':
                    if data.has_key('series_num'):
                        series_num = data['series_num']
                        reason = data['status']['reason']
                        game_note = "Series Game %s: %s Due to %s" % (series_num, game_status, reason)
                        rich_data['game_note'] = game_note

                for k, v in game_data.iteritems():
                    sportssetupitem[k] = v

                if "completed" in game_status or "ongoing" in game_status:
                    box_url = "http://mlb.mlb.com/mlb/gameday/index.jsp?gid=%s&mode=box" % (game_id)
                    ref_url = "http://mlb.mlb.com/ws/search/MediaSearchService?start=0&site=mlb&hitsPerPage=12&hitsPerSite=10&type=json&c_id=mlb&src=vpp&sort=desc&sort_type=date&game=%s" %game_pk
                    yield Request(box_url, callback=self.parse_completed, meta={'item': sportssetupitem, 'game_pk': game_pk, 'ref_url': ref_url})
                else:
                    print "Game status is not Completed"
                    if game_status == 'cancelled' or game_status == 'postponed':
                        yield sportssetupitem

    def get_schedules(self, data, pk_dict, result):
        channels = []
        sportssetupitem = {}
        items = []
        final_game_note = ''
        probable = ''
        games_data = data['data']['games'].get('game', [])
        if isinstance(games_data, dict):
            games_data = [games_data]
        for game in games_data:
            _tbd_title = ()
            radio = ""
            home_team_name = game['home_team_name']
            away_team_name = game['away_team_name']
            game_location = game.get('home_team_city', '')
            city_info = game.get('location', '')
            if not city_info:
                city_info = game_location
            game_venue = game['venue']
            channel_home = game.get('broadcast', '')
            if channel_home:
                radio = channel_home.get('home', '').get('radio', '')
                channel_home = channel_home.get('home', '').get('tv', '')
                if "," in radio:
                    radio = '<>'.join(radio_.strip() for radio_ in radio.split(','))
                elif radio == {}:
                    radio = ""
            channel_away = game.get('broadcast', '')
            if channel_away:
                channel_away = channel_away.get('away', '').get('tv', '')
            home_name_abbrev = game['home_name_abbrev']
            away_name_abbrev = game['away_name_abbrev']
            if home_team_name == "Hurricanes":
                home_name_abbrev = "HUR"
            if away_team_name == "Hurricanes":
                away_name_abbrev = "HUR"
            team_key1 = (home_name_abbrev, away_name_abbrev)
            team_key2 = (away_name_abbrev, home_name_abbrev)
            home_probable = game['home_probable_pitcher']['name_display_roster']
            away_probable = game['away_probable_pitcher']['name_display_roster']

            division = game.get('description', '')
            final_game_note = ''
            series_num = game.get('series_num', '')
            if series_num:
                final_game_note = 'Series Game %s' %str(series_num)
            if home_probable and away_probable:
                probable = '%s vs %s' %(away_probable, home_probable)
            game_id = game['id'].replace('/', '_').replace('-', '_')
            reference_url = 'http://mlb.mlb.com/mlb/gameday/index.jsp?gid=' + game_id
            status = game['status']['ind']
            if status == 'S':
                game_status = 'scheduled'
            else:
                continue
            game_pk = game['game_pk']
            if game_pk in pk_dict.keys():
                channels.append(pk_dict[game_pk])

            if home_team_name in COLLEGE_TEAMS:
                home_name_abbrev = 'tbd1'
                _tbd_title = (home_name_abbrev, home_team_name)

            if away_team_name in COLLEGE_TEAMS:
                away_name_abbrev = 'tbd2'
                _tbd_title = (away_name_abbrev, away_team_name)

            team_names = []
            if home_team_name:
                team_names.append({'callsign': home_name_abbrev, 'name': home_team_name})
            if away_team_name:
                team_names.append({'callsign': away_name_abbrev, 'name': away_team_name})

            time_ampm = game['ampm']
            date = game['time_date'].replace('/', '-').strip()
            time_unknown = 0
            if "3:33" in date:
                date = date.replace('3:33', '12:00AM')
                time_unknown = 1
            else:
                date = date + time_ampm
            game_id = game_id.replace('/', '_').replace('-', '_')
            reference_url = 'http://mlb.mlb.com/mlb/gameday/index.jsp?gid=' + game_id
            if time_ampm:
                date = get_utc_time(date, '%Y-%m-%d %I:%M%p', 'US/Eastern')
            else:
                date = get_utc_time(date, '%Y-%m-%d %I:%M', 'US/Eastern')

            date = str(date)
            event = ''

            game_type = game['game_type']
            if game_type in ["R", "S"]:
                division = ''
            else:
                division = game.get('series', '')

            game_type, tou_name, event = self.get_tou_event(game_type, division=division)

            loc = get_city_state_country(game_location)

            tz_info = self.get_timezone_info(loc, city_info, date)

            rich_data = {'video_links': [], 'stadium': game_venue, 'Radio': str(radio),
                         'update': datetime.datetime.now(), 'location' : loc,
                         'game_pk': game_pk, 'game_type': game_type, 'tbd_title' : _tbd_title, 'game_note': final_game_note}

            channels_info = self.get_channels(channel_home, channel_away)
            if channels:
                rich_data['channels'] = channels
            else:
                rich_data['channels'] = str(channels_info)


            data_dict = {'participants': team_names, 'game': 'baseball',
                          'source': 'mlb', 'tournament': tou_name,
                          'reference_url': reference_url, 'participant_type': 'team',
                          'affiliation': 'mlb', 'event': event,
                          'game_status': game_status, 'result': result,
                          'source_key': game_id, 'rich_data': rich_data, 'game_datetime': date,
                          'tz_info': tz_info, 'time_unknown': time_unknown}

            sportssetupitem = SportsSetupItem()

            for k, v in data_dict.iteritems():
                sportssetupitem[k] = v

            items.append(sportssetupitem)
        return items


    @log
    def parse_completed(self, response):
        sportssetupitem = response.meta.get('item', '')
        home_run_hitter, my_data, details, teams, tea = {}, {}, {}, {}, {}
        hxs = HtmlXPathSelector(response)
        team_nodes = hxs.select('//div[@class="gameday-linescore"]/div[contains(@class, "logo")]')
        for team in team_nodes:
            team_category = team.select('./@class').extract()[0].split(' ')[1]
            team_url = team.select('./a/img/@src').extract()[0].split('/')[-1].replace('.png', '').upper()
            tea[team_category] = team_url
        teams = tea
        nodes = hxs.select('//div[contains(@id, "pitcher")]//table[@id="pitching-stats"]')

        game_note = hxs.select('//div[@class="gameday-linescore"]/div[@class="logo away"]//div[@class="record"]/text()')
        if game_note:
            series_score = game_note[0].extract().strip('(').strip(')')
            series_score = [int(i) for i in series_score.split('-')]
            participants = sportssetupitem['participants']
            game_type = sportssetupitem['event']
            limit = None
            if ' Division ' in game_type:
                limit = 3
            elif ' Championship ' in game_type or 'World Series' in game_type:
                limit = 4
            elif ' Wild Card' in game_type:
                 if 'AL ' in game_type:
                     game_type = 'American League Wild Card'
                 elif 'NL ' in game_type:
                     game_type = 'National League Wild Card'
                 limit = 1
            away = participants[0]['name']
            home = participants[1]['name']
            if limit:
                if series_score[0] > series_score[1]:
                    score = '%d-%d' %(series_score[0], series_score[1])
                elif series_score[0] < series_score[1]:
                    score = '%d-%d' % (series_score[1], series_score[0])
                else:
                    score = '%d-%d' % (series_score[1], series_score[1])

                short_title = ''.join([g_type[0] for g_type in game_type.split(' ')])
                if ' Division ' in game_type:
                    short_title = 'series'


                if limit in series_score:
                    if limit == series_score[0]:
                        game_note = home + ' wins ' + short_title + ' ' + score
                    elif limit == series_score[1]:
                        game_note = away + ' wins ' + short_title + ' ' + score
                    else:
                        game_note = 'Series Tied ' + score
                else:
                    if series_score[0] > series_score[1]:
                        game_note = home + ' leads '+ short_title + ' ' + score
                    elif series_score[0] < series_score[1]:
                        game_note = away + ' leads '+ short_title + ' ' + score
                    else:
                        game_note = 'Series Tied ' + score


        for node in nodes:
            team_stats = [node.select('./tbody/tr[not(contains(@class, "pitching-totals"))]')[0]]
            stats_node = node.select('./tbody/tr[not(contains(@class, "pitching-totals"))]')
            team_in = node.select('./ancestor::div[@class="team-container"]/@id').extract()[0]
            if "-team-pitcher" in team_in:
                team_name = team_in.replace('-team-pitcher', '')
            for key,val in teams.iteritems():
                if key in team_name:
                    team_sk = val
            team_sk_new = team_sk
            data_new = []
            for stat in stats_node:
                home_hitter = stat.select('./td/a/@href').extract()[0]
                home_hitter_value = stat.select('./td[8]/text()').extract()[0]
                home_hitter = home_hitter.split('=')[-1]
                if int(home_hitter_value) > 0:
                    data_new.append((home_hitter, home_hitter_value))
            details[team_sk_new] = data_new
        if not game_note or not isinstance(game_note, str):
            game_note = hxs.select('//div[@class="gameday-linescore"]//div[@class="results"]//text()').extract()
            game_note = "".join(game_note).split('SV')[0].replace('\n', '').replace('\t', '').replace(' ', '').replace('L:', ' L:').strip()
        sportssetupitem['rich_data']['game_note'] = game_note

        if details:
            sportssetupitem['rich_data']['stats'] = {}
            sportssetupitem['rich_data']['stats']['home_run_hitter_'] = details
        if response.meta['game_pk'] and "clips" in self.spider_type:
             yield Request(response.meta.get('ref_url', ''), callback = self.parse_details, meta = {'item': sportssetupitem})
        elif sportssetupitem['game_status'] == "ongoing":
            reference = "http://mlb.mlb.com/shared/components/gameday/wrapper/v1/release/RC3/common/jsp/subcomponents/plays.jsp?gid=%s" %sportssetupitem['source_key']
            yield Request(reference, callback = self.get_game_note, meta = {'item': sportssetupitem})
        else:
            import pdb;pdb.set_trace()
            yield sportssetupitem

    @log
    def parse_details(self, response):

        hxs = HtmlXPathSelector(response)
        error = False
        try:
            data_clips =  eval(response.body)["mediaContent"]
        except KeyError:
            error = True

        if not error:
            xml_urls = []
            for data in data_clips:
                xml_urls.append(data["url"])

            for xml_url in xml_urls:
                yield Request(xml_url, callback=self.parse_clips, meta={'item' : response.meta.get('item')})

    @log
    def parse_clips(self, response):

        hxs = HtmlXPathSelector(response)
        clip_data = {'sk': '', 'title': '', 'desc': '', 'reference': '', 'img_link': ''}
        sk = "".join(hxs.select('//media[@type="video"]/@id').extract()).strip()
        if sk:
            clip_data['sk'] = sk
            clip_data['reference'] = 'http://mlb.mlb.com/video/play.jsp?content_id=%s' % sk
        clip_data['title'] = "".join(hxs.select('//headline/text()').extract()).strip()
        clip_data['desc'] = "".join(hxs.select('//big-blurb/text()').extract()).strip()
        images = (hxs.select('//thumbnailScenario/text()').extract())
        for image in images[:1]:
            clip_data['img_link'] = image

        media_urls = hxs.select('//url[not(contains(@playback_scenario, "HTTP_CLOUD"))]/text()').extract()
        char_list = map(chr, range(97, 123))
        len_urls = 0
        for media_url in media_urls:
            char_set = char_list[len_urls]
            if media_url.endswith('mp4'):
                clip_data[char_set] = {'url': media_url, 'mimetype': 'video/mp4'}
            len_urls = len_urls + 1
        item = response.meta['item']
        item['rich_data']['video_links'].append(clip_data)
        item['rich_data']['update'] = datetime.datetime.now()
        return r

    @log
    def get_game_note(self, response):
        hxs = HtmlXPathSelector(response)
        pitcher = home = away = ''
        sports_item = response.meta.get('item')

        nodes = hxs.select("//ul[@id='plays']//li[@class='plays-half-inning']")
        if len(nodes) > 2:
            innings = ''.join(nodes[-1].select('./h3').extract()).strip('<h3>\n<span></span>').strip('</h3>')
            innings1 = ''.join(nodes[-2].select('./h3').extract()).strip('<h3>\n<span></span>').strip('</h3>')
            pitcher = nodes[-1].select(".//h5[contains(text(), 'Pitcher')]")
            if pitcher:
                pitcher = ''.join(pitcher[-1].select(".//following-sibling::strong[1]/strong/text()").extract())
            if "Top" in innings and pitcher:
                home = pitcher
            elif "Bottom" in innings and pitcher:
                away = pitcher

            pitcher1 = nodes[-2].select(".//h5[contains(text(), 'Pitcher')]")
            if pitcher1:
                pitcher1 = ''.join(pitcher1[-1].select(".//following-sibling::strong[1]/strong/text()").extract())
            if "Top" in innings1 and pitcher1:
                home = pitcher1
            elif "Bottom" in innings1 and pitcher1:
                away = pitcher1

        game_note = None
        if home and away:
            game_note = "%s vs %s" %(away, home)
        sports_item['rich_data']['game_note'] = game_note
        yield sports_item

    def get_game_status(self, gamestatus):
        game_status = ''
        if gamestatus.lower() in STATUS_DICT:
            game_status = STATUS_DICT[gamestatus.lower()]
        return game_status

    def get_tou_event(self, game_type, division=''):
        event    = ''
        tou_name = 'mlb baseball'
        if "S" in game_type or "E" in game_type:
            event = "Spring Training"
        elif "A" in game_type:
            event = "MLB All-Star Game"
        elif "R" in game_type:
            event = "Regular Season"
        elif "F" in game_type:
            if division == "NL wild card":
                event = "NL Wild Card"
            elif division == "AL Wild Card":
                event = "AL Wild Card"
        elif "L" in game_type:
            if division == "NLCS":
                event = "National League Championship Series"
            elif division == "ALCS":
                event = "American League Championship Series"
        elif "D" in game_type:
            if division == "NLDS":
                event = "National League Division Series"
            elif division == "ALDS":
                event = "American League Division Series"
        elif "W" in game_type:
            event = "World Series"
        else:
            game_type = ''
        return game_type, tou_name, event


    def get_result(self, data):
        result = [[[0],[u''],{'winner': '', 'ot': [], 'hits':'','errors':'','innings_scores':[]}], \
                 [[1],[u''],{'winner': '', 'ot': [], 'hits':'','errors':'','innings_scores':[]}]]
        home_score = []
        away_score = []
        total_score = data['linescore']
        if "inning" in total_score.keys():
            total_scores = total_score['inning']
            for score in total_scores:
                if type(score) is not dict: score={}
                if 'home' in score.keys():
                    home_score.append(score['home'])
                else:
                    home_score.append('X')
                if 'away' in score.keys():
                    away_score.append(score['away'])
                else:
                    away_score.append('0')


            away_total_score = data['linescore']['r']['away']
            home_total_score = data['linescore']['r']['home']

            if len(home_score) > 1 and len(away_score) > 1:
                result[0][1], result[1][1] = [home_total_score], [away_total_score]
                result[0][-1]['innings_scores'] = home_score[:9]
                result[0][-1]['hits']   = data['linescore']['h']['home']
                result[0][-1]['errors'] = data['linescore']['e']['home']
                result[0][-1]['ot']     = home_score[9:]

                result[1][-1]['innings_scores'] = away_score[:9]
                result[1][-1]['hits']   = data['linescore']['h']['away']
                result[1][-1]['errors'] = data['linescore']['e']['away']
                result[1][-1]['ot']     = away_score[9:]

                if int(away_total_score) > int(home_total_score):
                    result[0][-1]['winner'] = 0
                    result[1][-1]['winner'] = 1
                elif int(away_total_score) == int(home_total_score):
                    result[1][-1]['winner'] = ''
                    result[0][-1]['winner'] = ''
                else:
                    result[1][-1]['winner'] = 0
                    result[0][-1]['winner'] = 1
        return result
