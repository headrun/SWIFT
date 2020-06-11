import datetime
import time
import re
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes, get_utc_time
from vtvspider_dev import get_tzinfo
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from urlparse import urlparse

true = True
false = False
null = ''

REPLACE_TOU_SPL_DICT = {' at ' : ' ', ' &amp; ' : ' ', 'GC': 'Golf Club', \
                        'CC': 'Country Club', 'TPC' : 'Tournament Players Club', \
                        'G&amp;CC' : 'Golf and Country Club', \
                        'GCC' : 'Golf and Country Club', 'G&amp;' : 'Golf and '}

REPLACE_STATE_DICT = {'FL' : 'Florida', 'CA' : 'California', 'NV' : 'Nevada', \
                    'AZ' : 'Arizona', 'TX' : 'Texas', 'SC' : 'South Carolina', 'LA' : 'Louisiana', \
                    'NC' : 'North Carolina', 'TN' : 'Tennessee', 'CT' : 'Connecticut', \
                    'RI' : 'Rhode Island', 'WV' : 'West Virginia', 'MD' : 'Maryland', 'IL' : 'Illinois', \
                    'OH' : 'Ohio', 'KY' : 'Kentucky', 'NJ' : 'New Jersey', \
                    'MA' : 'Massachusetts', 'CO' : 'Colorado', 'HI' : 'Hawaii', 'PUR' : 'Puerto Rico', 'GA' : 'Georgia'}

CTRY_ACC_STATE = {'Florida' : 'USA', 'Alabama' : 'USA', 'California' : 'USA', \
                'Nevada' : 'USA', 'Arizona' : 'USA', 'Texas' : 'USA', 'South Carolina' : 'USA', \
                'Gerogia' : 'Gerogia', 'SCO' : 'Scotland', 'Louisiana' : 'USA', \
                'North Carolina' : 'USA', 'Tennessee' : 'USA', 'Connecticut' : 'USA', 'Rhode Island' : 'USA', \
                'West Virginia' : 'USA', 'Maryland' : 'USA', 'Illinois' : 'USA', \
                'Ohio' : 'USA', 'Kentucky' : 'USA', 'New Jersey' : 'USA', 'Massachusetts' : 'USA', \
                'Colorado' : 'USA', 'CAN' : 'Canada', 'Eng' : 'England', 'Georgia' : 'USA', \
                'Puerto Rico' : 'USA', 'Hawaii' : 'USA', 'AUS' : 'Australia', 'Mex' : 'Mexico',\
                'CHN' : 'China', 'MAS' : 'Malaysia', 'BER' : 'Bermuda', \
                'Puerto Rico' : 'USA', 'HI' : 'USA', 'TX' : 'USA'}

REPLACE_TOU_DICT = {'u.s. open golf championship': 'u.s. open', \
                    'alfred dunhill championship': 'alfred dunhill links championship', \
                    'the senior open championship pres. by rolex': 'senior open championship', \
                    'quicken loans national' : 'at&t national', \
                    'world golf championships-cadillac championship' : 'world golf championships-ca championship'}

IGNORE_WORDS = ['presented', 'in partnership', 'pres. by', ' at ', 'partnership', 'powered by', "pres'd by", ' by ']

REPLACE_WORDS = ['waste management', 'omega', 'isps handa', \
                'alstom', 'aberdeen asset management', 'isps handa', 'nature valley', 'vivendi', 'm2m']

REPLACE_WORDS_by = {}


def get_refined_std_name(stadium):
    for key, value in REPLACE_TOU_SPL_DICT.iteritems():
        if key in stadium:
            stadium = stadium.replace(key, value).strip()

    if '(' in stadium:
        stadium_extra = "".join(re.findall(r'\(.*\)', stadium))
        stadium = stadium.replace(stadium_extra, '').strip()
    return stadium

def get_game_location(venue):
    city = state = country = ''
    venue = venue.split(',')
    if len(venue) == 2:
        city  = venue[0].strip()
        state = REPLACE_STATE_DICT.get(venue[-1].strip())
        if not state:
            country = CTRY_ACC_STATE.get(venue[-1].strip())
    elif len(venue) == 3:
        city = venue[0].strip()
        state = venue[1].strip()
        country = venue[-1].strip()
        if country : country = CTRY_ACC_STATE.get(country)
    elif len(venue) == 1:
        country = venue.strip()

    return city, state, country

def get_refined_tou_name(tou_name):
    tou_name = tou_name.lower()

    if REPLACE_TOU_DICT.has_key(tou_name):
        tou_name = REPLACE_TOU_DICT[tou_name]

    for k, v in REPLACE_WORDS_by.iteritems():
        if k in tou_name:
            tou_name = tou_name.replace(k, v).strip()

    for i in IGNORE_WORDS:
        if i in tou_name:
            tou_name = tou_name.split(i)[0].strip()

    for i in REPLACE_WORDS:
        tou_name = tou_name.replace(i, '').strip()
    return tou_name


def get_str_end_dates(tou_date):
    str_date, e_date = [i.strip() for i in tou_date.split('-')]
    if len(e_date.split()) < 2:
        str_month, str_day = str_date.split()
        e_date = '%s %s' % (str_month, e_date)

    return str_date, e_date

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position

    return pos

def get_tou_dates(start_date, end_date, start_date_format, end_date_format):
    end_date = (datetime.datetime(*time.strptime(end_date.strip(), end_date_format)[0:6])).date()
    game_year = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    if game_year.month > end_date.month:
        tou_year = end_date.year - 1
    else:
        tou_year = end_date.year

    if len(start_date.split()) == 2:
        start_date += ', %s' % tou_year
        start_date_format += ', %Y'

    start_date = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    tou_date = start_date.strftime('%b %-d') +' - '+ end_date.strftime('%b %-d')

    return tou_date, start_date, end_date

def get_full_url(exp_url, base_url):
    url_data = urlparse(exp_url)
    if isinstance(url_data, tuple):
        scheme, domain, path = url_data[:3]
    else:
        scheme, domain, path = url_data.scheme, url_data.netloc, url_data.path

    if not path:
        print "Path is mandatory, Given Url: %s" % exp_url
        return exp_url
    else:
        path = path.strip()

    if domain and scheme:
        return exp_url

    if not path.startswith('/'):
        path += '/'

    base_url_data = urlparse(base_url)
    if isinstance(base_url_data, tuple):
        base_scheme, base_domain = base_url_data[:2]
    else:
        base_scheme, base_domain = base_url_data.scheme, base_url_data.netloc

    if not base_scheme and not base_domain:
        print "Provide valid Base Url, Given Url: %s" % base_url
        return exp_url

    return base_scheme + '://' + base_domain + path

def get_game_datetime(tou_date, year):
    game_dates = [i.strip() for i in tou_date.split('-') if i]
    if len(game_dates) == 2:
        start_date, end_date = game_dates
        game_datetime = start_date + ' %s' % year
    elif len(game_dates) >= 1:
        game_datetime = game_dates[0] + ' %s' % year
    else:
        game_datetime = ''

    if game_datetime:
        game_datetime = get_utc_time(game_datetime, '%b %d %Y', 'US/Eastern')

    return game_datetime

def get_golfers_info(to_par, total_score, pl_pos, rounds_scores, tee_time):
    players = {}
    players = {'TO PAR' : to_par, 'final' : total_score, 'position' : pl_pos}
    for i in xrange(len(rounds_scores)):
        rt = "R%s" % str(i + 1)
        rv = rounds_scores[i]
        players.update({rt : rv})

    tee_times = []
    for t in tee_time:
        if t == '':
            break
        else:
            tee_times.append(t)

    for i in xrange(len(tee_times)):
        rt = "R%s_tee_time" % str(i+1)
        rv = tee_times[i]
        players.update({rt : rv})


    return players


class PGAOngoing(VTVSpider):
    name    = "pga_ongoing"
    start_urls = ['http://www.pgatour.com/players.html']
    domain = "http://www.pgatour.com"
    player_id_sk = {}


    def parse(self, response):
        items   = []
        hxs     = HtmlXPathSelector(response)
        player_nodes = get_nodes(hxs, '//div[@class="overview"]/div[@class="directory-item"]/div[@class="item-data"] \
                                        //ul[@class="ul-inline items"]/li/span[@class="name"]')
        for node in player_nodes:
            player_ref = extract_data(node, './a/@href').strip()
            player_det = extract_data(node, './a/text()').strip()

            if player_ref:
                _id = re.findall('player\.(.*?)\.', player_ref)
                if _id and len(_id) == 1:
                    player_id = _id[0].strip()
                else:
                    player_id = 0
            else:
                player_id = 0

            if player_id and not player_id.isdigit():
                player_id = 0

            if ',' in player_det:
                player_det = [i.strip() for i in player_det.split(',') if i]
                if len(player_det) == 2:
                    l_name, f_name = player_det
                elif len(player_det) == 3:
                    l_name, ext, f_name = player_det
                    l_name = l_name +', '+ ext
                elif len(player_det) > 3:
                    l_name, f_name = player_det[0], player_det[-1]
                else:

                    f_name, l_name = player_det, ''
            else:
                f_name, l_name = player_det, ''

            if player_ref and 'http' not in player_ref:
                player_ref = get_full_url(player_ref, "http://www.pgatour.com/tournaments/schedule.html")

            if not f_name or not player_id:
                continue

            player_name = ' '.join(i.strip() for i in [f_name, l_name] if i).lower()
            self.player_id_sk[player_name] = player_id
            start_url = 'http://www.pgatour.com/tournaments/schedule.html'

            yield Request(start_url, callback = self.parse_start_nodes, meta = {})


    def parse_start_nodes(self, response):
        hxs             = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//table[@class="table-styled"]//tbody/tr[contains(@class, "odd") or contains(@class, "even")]')
        for node in nodes:
            st_date     = extract_data(node, './td[@class="num-date"]/span[2]/text()')
            _e_date     = extract_data(node, './td[@class="num-date"]/span[3]/text()')
            main_date   = st_date + '-' + _e_date
            str_date, e_date = get_str_end_dates(main_date)

            tou_venue   = extract_data(node, './td[@class="tournament-name"]/text()')
            if "Purse" in tou_venue:
                _venue  = "".join(re.findall(r'...Purse:.*', tou_venue)) or "".join(re.findall(r'.. Purse:.*', tou_venue))
                tou_venue       = tou_venue.replace(_venue, '').strip()

            tournament_link     = extract_data(node, './td[@class="tournament-name"]/a/@href')
            tou_name            = extract_data(node, './td[@class="tournament-name"]/span/text()').replace('&amp;', '&').strip()

            channels            = extract_data(node, './td[@class="network-name hidden-small"]/text()')
            channels            = channels.replace(' ', '').split(',')[0].replace('\n', '')
            def_data            = ["".join(i).strip() for i in extract_list_data(node, './td[@class="champions-name hidden-small"]/text()') if i]
            if len(def_data) == 2:
                defending_chp, prize_money = def_data
            else:
                defending_chp, prize_money = '', ''

            today_date = datetime.datetime.utcnow().date()

            tou_year = datetime.date.today().year
            if len(str_date.split()) == 2 and len(e_date.split()) == 2:
                e_date = e_date + ', %s' % tou_year
                e_format, s_format = '%b %d, %Y', '%b %d'
            else:
                e_date = e_date
                e_format, s_format =  '%a %b %d %H:%M:%S %Z %Y', '%a %b %d %H:%M:%S %Z %Y'

            tou_date, start_date, end_date = get_tou_dates(str_date, e_date, s_format, e_format)
            tou_year = start_date.year
            if start_date <= today_date and end_date >= today_date:
                status = "ongoing"
                print status
            elif start_date < today_date:
                status = "completed"
            else:
                status = "scheduled"

            previous_date = datetime.timedelta(-1)

            new_date = today_date + previous_date

            game_datetime = get_game_datetime(tou_date, tou_year)

            if tournament_link and 'http' not in tournament_link:
                tournament_link = self.domain + tournament_link

            if not tou_name or not e_date or not str_date or not tournament_link:
                continue

            stadium = tou_venue.split(',')[0].strip()

            stadium = get_refined_std_name(stadium)

            loc = {}
            venue = ",".join(tou_venue.split(',')[1:]).strip()
            _city, state, country = get_game_location(venue)
            if not country:
                country = CTRY_ACC_STATE.get(state)

            if '$' in stadium or 'Purse' in stadium:
                _city = state = country = stadium = ''

            loc = {'city' : _city, 'country' : country, 'state' : state}
            tz_info = get_tzinfo(city = _city)

            details = {'tournament_Link' : tournament_link, 'str_date' : str_date, 'e_date': e_date, \
                        'loc' : loc, 'tou_name' : tou_name, 'tou_date' : tou_date, \
                        'def_chp': defending_chp, 'prize_money': prize_money, 'tz_info' : 'tz_info', \
                        'player_id_sk': self.player_id_sk, 'start_date' : start_date, 'end_date' : end_date, 'tou_year' : tou_year, \
                        'status' : status, 'game_datetime' : game_datetime, 'stadium' : stadium, 'channels': channels}

            if "worldgolfchampionships" in tournament_link and status ==  'completed' and end_date >= new_date:
                yield Request(tournament_link, callback = self.parse_listing, meta = {'details' : details})

            if "World Cup of Golf" in tou_name and end_date == new_date:
                yield Request(tournament_link, callback = self.parse_world, meta = {'details' : details})

            if 'www.pgatour.com/' not in tournament_link:
                continue

            if status == "ongoing":
                import pdb;pdb.set_trace()
                yield Request(tournament_link, callback = self.parse_listing, meta = {'details' : details})

            elif status == "completed" and end_date == new_date:
                yield Request(tournament_link, callback = self.parse_listing, meta= {'details' : details})

            elif status == "completed":
                yield Request(tournament_link, callback = self.parse_listing, meta= {'details' : details})



    def parse_listing(self, response):
        sportssetupitem     = SportsSetupItem()
        hxs                 = HtmlXPathSelector(response)
        details             = response.meta['details']
        leader_board_link   = extract_data(hxs, '//ul[@class="nav nav-tabs slim nav-tabs-drop"]/li/a[contains(@href, "leaderboard")]/@href')


        if leader_board_link and 'http' not in leader_board_link:
            leader_board_link = get_full_url(leader_board_link, response.url)

        result_link     = extract_data(hxs, '//ul[@class="nav nav-tabs slim nav-tabs-drop"]/li/a[contains(@href, "past-results")]/@href')

        if result_link and 'http' not in result_link:
            result_link = get_full_url(result_link, response.url)

        if not details['tou_date'] or not details['tou_name']:
            print "Tou-date: %s --- Tou-Name: %s" % (details['tou_date'], details['tou_name'])
        else:
            modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
            modified_tou_date = '_'.join(i.strip() for i in details['tou_date'].strip().split('-')).replace(' ', '_').strip()
            tou_sk = modified_tou_name + '_' + modified_tou_date

            tour_name = get_refined_tou_name(details['tou_name'])
            details.update({'tou_sk' : tou_sk, 'tour_name' : tour_name, 'tou_link' : response.url, 'leader_board_link' : leader_board_link,
                            'result_link' : result_link})

            rich_data   = {'location' : details['loc'], 'prize_money' : details['prize_money'], \
                            'stadium' : details['stadium'], 'channels': str(details['channels'])}

            sportssetupitem['game_status']      = details['status']
            sportssetupitem['game_datetime']    = details['game_datetime']
            sportssetupitem['source_key']       = tou_sk
            sportssetupitem['tournament']       = details['tour_name']
            sportssetupitem['rich_data']        = rich_data
            sportssetupitem['reference_url']    = response.url
            sportssetupitem['game']             = 'golf'
            sportssetupitem['source']           = 'pga_golf'
            sportssetupitem['participant_type'] = 'player'
            sportssetupitem['affiliation']      = 'pga'
            sportssetupitem['time_unknown']     = 1
            sportssetupitem['tz_info']          = details['tz_info']

            if details['status'] == "ongoing":
                if leader_board_link:
                    yield Request(leader_board_link, callback = self.parse_details, meta = {'details' : details, 'sportssetupitem' : sportssetupitem})
                else:
                    (details['tou_name'], details['status'], response.url)
            elif details['status'] == "completed":
                if result_link:
                    yield Request(result_link, callback = self.parse_results, meta = {'details' : details, 'sportssetupitem' : sportssetupitem})
                elif leader_board_link:
                    yield Request(leader_board_link, callback = self.parse_details, meta = {'details' : details, 'sportssetupitem' : sportssetupitem})

    def parse_details(self, response):
        hxs             = HtmlXPathSelector(response)
        sportssetupitem = response.meta['sportssetupitem']
        domain          = response.url.split('.com/')[0] + '.com'
        json_link       = extract_data(hxs, ('//script[contains(text(), "lbJson")]'))
        if json_link:
            json_link_doc   = re.findall(r'(.*leaderboard-v2.json)', json_link)[0]
            json_new        = json_link_doc.split(":")[-1]
            json_link       = json_new.split("'")[1]
            if 'http:' not in json_link:
                json_link   = domain + json_link

            yield Request(json_link, callback = self.parse_json_details, meta = {'details' : response.meta['details'], 'sportssetupitem' : sportssetupitem})

    def parse_json_details(self, response):
        items           = []
        participants    = {}
        result          = {}
        details         = response.meta['details']
        sportssetupitem = response.meta['sportssetupitem']
        raw_data        = response.body
        data            = eval(raw_data)
        if data:
            l_info      = data.get("debug", '')
            tou_id      = data.get('leaderboard', '').get('tournament_id', '')
            _round      = data.get('leaderboard', '').get('current_round', '')
            rnd_status  = data.get('leaderboard', '').get('round_state', '')
            if details['status'] == 'ongoing': current_round = 'Round' + ' ' + str(_round) + ' - ' + rnd_status
            else: current_round = ''
            info        = data.get('leaderboard', '')
            p_info      = info.get('players','')
            players, player_name_map = {}, {}
            ids_map, player_ids = {}, []
            is_round = 0
            for i in p_info:
                position    = i.get('current_position', '')
                pl_pos      = get_position(position)
                country     = i.get('player_bio', '').get('country', '')
                thru        = i.get('thru', '')
                to_par      = i.get('total', '') or i.get('today', '')
                results     = i.get('rounds', '')
                total_strokes = i.get('total_strokes', '')

                rounds_scores = []
                for result_ in results:
                    tee_time = result_['tee_time'].split('T')[-1]
                    if len(tee_time.split(':')) >= 2:
                        tee_time = ':'.join(tee_time.split(':')[:-1])
                    rounds_scores.append((result_['round_number'], result_['strokes'], tee_time))
                rounds_scores.sort()
                scores = [score for round, score, tee_time in rounds_scores]
                start_time = [tee_time for round, score, tee_time in rounds_scores]

                first_name  = i.get('player_bio', '').get('first_name', '')
                last_name   = i.get('player_bio', '').get('last_name', '')
                player_name = ' '.join(i.strip() for i in [first_name, last_name] if i)
                if '\/' in player_name:
                    player_name = ' '.join(i.strip() for i in player_name.split('\/') if i)


                if pl_pos == '1' or pl_pos == 'T1':
                    winner_name = player_name

                player_id = i.get('player_id', '')
                if player_id:
                    player_name_map[player_name]    = player_id
                    participants[player_id]         = (0, player_name)
                    players                         = get_golfers_info(to_par, total_strokes, pl_pos, scores, start_time)
                    result[player_id]               = players
                    sportssetupitem['participants'] = participants
                    ids_map[player_id] = player_name
                    player_ids.append(player_id)

            if details['status'] == 'completed':
                print details['def_chp']
                winner = player_name_map.get(winner_name, '')
                if not winner:
                    if pl_pos == "1":
                        winner = player_id 

            else:
                winner = ''

            response.meta['sportssetupitem']['rich_data'].update({'game_note' : current_round})
            result.setdefault('0', {}).update({'winner' : winner})
            sportssetupitem['result']           = result
            sportssetupitem['reference_url'] = response.url
            if participants:
                items.append(sportssetupitem)
                for item in items:
                    import pdb;pdb.set_trace()
                    yield item

    def parse_results(self, response):
        items           = []
        participants    = {}
        result          = {}
        sportssetupitem = response.meta['sportssetupitem']
        details         = response.meta['details']
        hxs             = HtmlXPathSelector(response)
        tour_ending     = extract_data(hxs, '//span[@class="header-row"]/b[contains(text(), "Ending")]/text()').split('/')[-1].strip()
        if not tour_ending and 'www.worldgolfchampionships.com' in response.url:
            tour_ending = extract_data(hxs, '//div[contains(@class, "tourTableContent")]/parent::div/text()[preceding::b[1][contains(text(),"Ending")]]')

        target_par_values = extract_list_data(hxs, '//span[@class="header-row"]/span[contains(text(), "PAR")]/text()')

        target_par_values   = ["".join(i.replace('PAR:', '').strip()) for i in target_par_values if i.strip()]
        if not target_par_values and 'www.worldgolfchampionships.com' in response.url:
            target_par_values = extract_list_data(hxs, '//div[contains(@class, "tourTableContent")]/parent::div/text()[preceding::b[1][contains(text(),"Par")]]')
            target_par_values = [i.strip() for i in target_par_values if i.strip()]

        if len(target_par_values) > 1:
            target_par  = target_par_values[-1]
        elif target_par_values:
            target_par  = target_par_values[0]
        else:
            target_par  = 0

        total_rounds    = extract_list_data(hxs, '//li[@class="col-left"]//th[@class="head-data-cell hidden-small"]//tr[2]/td/text()')
        total_rounds    = [int(i) for i in total_rounds if i]

        if not isinstance(target_par, int) and not target_par.isdigit():
            target_par  = 0
        else:
            target_par  = int(target_par)

        if target_par and str(details['end_date'].year) in tour_ending and total_rounds:
            players, player_name_map = {}, {}
            ids_map, player_ids = {}, []
            index_of_data       = 2 + len(total_rounds)
            player_nodes        = get_nodes(hxs, '//li[@class="col-left"]//tr[contains(@class, "") or contains(@class, "odd")]')
            for node in player_nodes:
                player_nodes = get_nodes(node, './/td')
                if len(player_nodes) < index_of_data:
                    continue

                req_player_data = [extract_data(i, './text()') for i in player_nodes[:index_of_data]]
                player_name, position = [i.strip() for i in req_player_data[:2]]
                pl_pos  = get_position(position)
                _rounds = extract_list_data(node, './/td[@class="hidden-small"]/text()')[:len(total_rounds)]
                rounds  = [i for i in _rounds if len(i) == 2]

                if pl_pos == '1' or pl_pos == 'T1':
                    winner_name = player_name

                rounds_scores = []
                is_score_zero = False
                for round in rounds:
                    if round.isdigit():
                        if round == '0':
                            is_score_zero = True

                        rounds_scores.append(int(round))
                    elif '-' in round or '+' in round:
                        try:
                            rounds_scores.append(target_par - int(round))
                        except:
                            rounds_scores.append(0)
                    else:
                        rounds_scores.append(0)


                if not player_name or (len(set(rounds_scores)) == 1 and rounds_scores[0] == 0 and not is_score_zero) or not pl_pos:
                    continue

                player_id = details['player_id_sk'].get(player_name, '')


                to_par_per_round = [score - target_par for score in rounds_scores if score]
                total_score = sum(rounds_scores)
                to_par      = sum(to_par_per_round)
                if to_par == 0:
                    to_par  = 'E'

                if 'W/D' in position or 'CUT' in position:
                    to_par = ''

                if player_id:
                    player_name_map[player_name]    = player_id
                    ids_map[player_id]              = player_name
                    player_ids.append(player_id)
                    participants[player_id]         = (0, player_name)
                    players                         = get_golfers_info(to_par, total_score, pl_pos, rounds, [])
                    result[player_id]               = players
                    sportssetupitem['participants'] = participants

            if details['status'] == 'completed':
                winner = player_name_map.get(winner_name, '')
                if not winner:
                    if pl_pos == "1":
                        winner = player_id 

            else:
                winner = ''


            result.setdefault('0', {}).update({'winner' : winner})
            sportssetupitem['result']            = result
            if participants:
                player_ids = [i for i in player_ids if i]
                items.append(sportssetupitem)
                for item in items:
                    yield item
        else:
            yield Request(details['leader_board_link'], callback = self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})


    def parse_world(self, response):
        hxs             = HtmlXPathSelector(response)
        details         = response.meta['details']
        today_date      = datetime.datetime.utcnow().date()
        tou_year        = details['tou_year']
        sportssetupitem = SportsSetupItem()

        if len(details['str_date'].split()) == 2 and len(details['e_date'].split()) == 2:
            e_date = details['e_date'] + ', %s' % tou_year
            e_format, s_format = '%b %d, %Y', '%b %d'
        elif ',' in details['e_date']:
            e_date = details['e_date']
            e_format, s_format = '%b %d, %Y', '%b %d'
        else:
            e_date = details['e_date']
            e_format, s_format =  '%a %b %d %H:%M:%S %Z %Y', '%a %b %d %H:%M:%S %Z %Y'

        tou_date, start_date, end_date = get_tou_dates(details['str_date'], e_date, s_format, e_format)

        if start_date <= today_date and today_date <= end_date:
            status = "ongoing"
        elif start_date < today_date:
            status = "completed"
        else:
            status = "scheduled"
            sportssetupitem['participants'] = {}
            sportssetupitem['result'] = {}



        leader_board_link = "http://worldcup.pgatour.com/" + extract_data(hxs, '//ul[@id="header_nav"]//a[contains(@href, "leaderboard")]/@href')

        lb_link = "http://worldcup.pgatour.com/LB.aspx"

        tour_name = get_refined_tou_name(details['tou_name'])
        game_datetime = get_game_datetime(tou_date, tou_year)

        details.update({'status' : status, 'leader_board_link' : leader_board_link, 'start_date' : start_date, 'tz_ifo' : details['tz_info'], \
                            'end_date' : end_date, 'tou_date' : tou_date, 'tour_name' : tour_name, 'game_datetime' : game_datetime})

        yield Request(lb_link, callback = self.parse_world_next, meta = {'details' : details})

    def parse_world_next(self, response):
        items       = []
        details_doc = HtmlXPathSelector(response)
        details     = response.meta['details']
        players, player_name_map = {}, {}
        ids_map, player_ids = {}, []
        data, participants, result = {}, {}, {}
        sportssetupitem = SportsSetupItem()
        nodes = get_nodes(details_doc, '//table[@id="TeamStands"]//tr')
        for node in nodes:
            position    = extract_data(node, './/td[1]/text()')
            pl_pos      = get_position(position)
            country     = extract_data(node, './/td[3]/text()')
            player_name = extract_data(node, './/td[2]/a/text()')
            to_par      = extract_data(node, './/td[6]/text()')
            total_score = extract_data(node, './/td[11]/text()')
            thru        = extract_data(node, './/td[5]/text()')
            round_scores = []
            round_r1          = extract_data(node, './/td[7]/text()')
            round_scores.append(round_r1)
            round_r2          = extract_data(node, './/td[8]/text()')
            round_scores.append(round_r2)
            round_r3          = extract_data(node, './/td[9]/text()')
            round_scores.append(round_r3)
            round_r4          = extract_data(node, './/td[10]/text()')
            round_scores.append(round_r4)
            rounds      = round_scores

            player_id = details['player_id_sk'].get(player_name, '')


            if player_id:
                player_name_map[player_name]    = player_id
                ids_map[player_id]              = player_name
                player_ids.append(player_id)
                participants[player_id]         = (0, player_name)
                players                         = get_golfers_info(to_par, total_score, pl_pos, rounds, [])
                result[player_id]               = players
                sportssetupitem['participants'] = participants

        if details['status'] == 'completed':
            winner = player_name_map.get(details['def_chp'], '')
            if not winner:
                    if pl_pos == "1":
                        winner = player_id
        else:
            winner = ''

        result.setdefault('0', {}).update({'winner' : winner})

        modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
        modified_tou_date = '_'.join(i.strip() for i in details['tou_date'].strip().split('-')).replace(' ', '_').strip()
        tou_sk = modified_tou_name + '_' + modified_tou_date


        rich_data   = {'location' : details['loc'], 'stadium': details['stadium'],
                            'prize_money' : details['prize_money']}

        if players:
            sportssetupitem['game_status']       = details['status']
            sportssetupitem['game_datetime']     = details['game_datetime']
            sportssetupitem['source_key']        = tou_sk
            sportssetupitem['tournament']        = details['tour_name']
            sportssetupitem['rich_data']         = rich_data
            sportssetupitem['result']            = result
            sportssetupitem['reference_url']     = response.url
            sportssetupitem['game']              = 'golf'
            sportssetupitem['source']            = 'pga_golf'
            sportssetupitem['participant_type']  = 'player'
            sportssetupitem['affiliation']       = 'pga'
            sportssetupitem['tz_info']           = details['tz_info']
            sportssetupitem['time_unknown']      = 1

            items.append(sportssetupitem)
            for item in items:
                yield item
