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

REPLACE_TOU_SPL_DICT = {' at ' : ' ', ' &amp; ' : ' ', 'GC': 'Golf Club', 'CC': 'Country Club', 'TPC' : 'Tournament Players Club', \
                        'G&amp;CC' : 'Golf and Country Club', 'GCC' : 'Golf and Country Club', 'G&amp;' : 'Golf and '}

REPLACE_STATE_DICT = {'FL' : 'Florida', 'CA' : 'California', 'NV' : 'Nevada', 'AZ' : 'Arizona', \
                        'TX' : 'Texas', 'SC' : 'South Carolina', 'LA' : 'Louisiana', \
                        'NC' : 'North Carolina', 'TN' : 'Tennessee', 'CT' : 'Connecticut', \
                        'RI' : 'Rhode Island', 'WV' : 'West Virginia', 'MD' : 'Maryland', 'IL' : 'Illinois', \
                        'OH' : 'Ohio', 'KY' : 'Kentucky', 'NJ' : 'New Jersey', 'MA' : 'Massachusetts', \
                        'CO' : 'Colorado', 'HI' : 'Hawaii', 'PUR' : 'Puerto Rico', 'GA' : 'Georgia'}

CTRY_ACC_STATE = {'Florida' : 'USA', 'Alabama' : 'USA', 'California' : 'USA', 'Nevada' : 'USA', \
                'Arizona' : 'USA', 'Texas' : 'USA', 'South Carolina' : 'USA', \
                'Gerogia' : 'Gerogia', 'SCO' : 'Scotland', 'Louisiana' : 'USA', \
                'North Carolina' : 'USA', 'Tennessee' : 'USA', 'Connecticut' : 'USA', 'Rhode Island' : 'USA', \
                'West Virginia' : 'USA', 'Maryland' : 'USA', 'Illinois' : 'USA', \
                'Ohio' : 'USA', 'Kentucky' : 'USA', 'New Jersey' : 'USA', 'Massachusetts' : 'USA', \
                'Colorado' : 'USA', 'CAN' : 'Canada', 'Eng' : 'England', 'Georgia' : 'USA', \
                'Puerto Rico' : 'USA', 'Hawaii' : 'USA', 'AUS' : 'Australia', 'Mex' : 'Mexico', \
                'CHN' : 'China', 'MAS' : 'Malaysia', 'BER' : 'Bermuda', 'Puerto Rico' : 'USA', 'HI' : 'USA', 'TX' : 'USA'}

REPLACE_TOU_DICT = {'u.s. open golf championship': 'u.s. open', \
                    'alfred dunhill championship': 'alfred dunhill links championship', \
                    'the senior open championship pres. by rolex': 'senior open championship', \
                    'quicken loans national' : 'at&t national', \
                    'world golf championships-cadillac championship' : 'world golf championships-ca championship'}

IGNORE_WORDS = ['presented', 'in partnership', 'pres. by', ' at ', \
                'partnership', 'powered by', "pres'd by", ' by ']

REPLACE_WORDS = ['waste management', 'omega', 'isps handa', \
                'alstom', 'aberdeen asset management', \
                'isps handa', 'nature valley', 'vivendi', 'm2m']

REPLACE_WORDS_by = {}


def get_stadium_name(stadium):
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
        state = REPLACE_STATE_DICT.get(venue[-1].strip(), '')
        if not state:
            country = CTRY_ACC_STATE.get(venue[-1].strip())
    elif len(venue) == 3:
        city = venue[0].strip()
        state = venue[1].strip()
        country = venue[-1].strip()
        if country : country = CTRY_ACC_STATE.get(country)
    elif len(venue) == 1:
        country = venue[0].strip()

    return city, state, country

def get_refined_tou_name(tou_name):
    tou_name = tou_name.lower()

    if REPLACE_TOU_DICT.has_key(tou_name):
        tou_name = REPLACE_TOU_DICT[tou_name]

    for k, v in REPLACE_WORDS_by.iteritems():
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
        return exp_url

    return base_scheme + '://' + base_domain + path

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position

    return pos

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

class PGATour(VTVSpider):
    name         = "pga_spider"
    start_urls   = ['http://www.pgatour.com/players.html']
    domain       = 'http://www.pgatour.com'
    player_id_sk = {}

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        player_nodes = get_nodes(hxs, '//div[@class="overview"]/div[@class="directory-item"]\
                                       /div[@class="item-data"]//ul[@class="ul-inline items"]/li/span[@class="name"]/a')
        for node in player_nodes:
            player_url = extract_data(node, './@href').strip()
            player_name = extract_data(node, './text()').strip()

            player_id = 0
            if player_url:
                player_sk = re.findall('player\.(.*?)\.', player_url)
                if player_sk and len(player_sk) == 1:
                    player_id = player_sk[0].strip()

            if player_id and not player_id.isdigit():
                player_id = 0

            if ',' in player_name:
                player_name = [i.strip() for i in player_name.split(',') if i]
                if len(player_name) == 2:
                    last_name, first_name = player_name
                elif len(player_name) == 3:
                    last_name, ext, first_name = player_name
                    last_name = last_name +', '+ ext
                elif len(player_name) > 3:
                    last_name, first_name = player_name[0], player_name[-1]
                else:
                    first_name, last_name = player_name, ''
            else:
                first_name, last_name = player_name, ''

            if not first_name or not player_id:
                continue

            player_name = ' '.join(i.strip() for i in [first_name, last_name] if i).lower()
            self.player_id_sk[player_name] = player_id

        start_url = 'http://www.pgatour.com/tournaments/schedule.html'
        yield Request(start_url, callback = self.parse_start_nodes, meta = {})


    def parse_start_nodes(self, response):
        hxs             = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//table[@class="table-styled"]//tbody/tr[contains(@class, "odd") or contains(@class, "even")]')
        for node in nodes:
            str_date     = extract_data(node, './td[@class="num-date"]/span[2]/text()')
            end_date       = extract_data(node, './td[@class="num-date"]/span[3]/text()')
            main_date      = str_date + '-' + end_date
            str_date, end_date = get_str_end_dates(main_date)
            tou_venue      = extract_data(node, './td[@class="tournament-name"]/text()')
            if "Purse" in tou_venue:
                _venue     = "".join(re.findall(r'...Purse:.*', tou_venue)) or "".join(re.findall(r'.. Purse:.*', tou_venue))
                tou_venue  = tou_venue.replace(_venue, '').strip()

            tournament_link = extract_data(node, './td[@class="tournament-name"]/a/@href')
            tou_name       = extract_data(node, './td[@class="tournament-name"]/span/text()').replace('&amp;', '&').strip()

            #channels       = extract_data(node, './td[@class="network-name hidden-small"]/text()').replace(' ', ',').strip()
            def_data       = ["".join(i).strip() for i in extract_list_data(node, './td[@class="champions-name hidden-small"]/text()') if i]

            defending_chp, prize_money = '', ''
            if len(def_data) == 2:
                defending_chp, prize_money = def_data

            if tournament_link and 'http' not in tournament_link:
                tournament_link = self.domain + tournament_link
            '''if 'Ryder Cup' in tou_name:
                continue'''

            if not tou_name or not end_date or not str_date or not tournament_link:
                continue

            stadium = tou_venue.split(',')[0].strip()

            stadium = get_stadium_name(stadium)
            game_note = stadium.replace('&', '').strip()

            loc     = {}
            venue   = ",".join(tou_venue.split(',')[1:]).strip()
            _city, state, country = get_game_location(venue)
            if not country:
                country = CTRY_ACC_STATE.get(state.strip())

            if '$' in tou_venue or 'Purse' in tou_venue:
                _city = state = country = stadium = ''
            loc = {'city' : _city, 'country' : country, 'state' : state}
            tz_info = get_tzinfo(city = _city)

            details = {'str_date'       : str_date,     'e_date'            : end_date,
                        'loc'           : loc,          'tou_name'          : tou_name,
                        'def_chp'       : defending_chp, 'game_note'        : game_note,
                        'prize_money'   : prize_money,    'tz_info'         : tz_info,
                        'player_id_sk'  : self.player_id_sk,'stadium'   : stadium}

            if "worldgolfchampionships.com" in tournament_link \
            or "cadillac-championship" in tournament_link or "hsbc-champions" in tournament_link:
                yield Request(tournament_link, callback = self.parse_listing, meta={'details' : details})
            if "Presidents Cup" in tou_name or "Presidents Cup" in tournament_link:
                yield Request(tournament_link, callback = self.parse_presidents, meta = {'details' : details})

            if "World Cup of Golf" in tou_name or "worldcup.pgatour.com" in tournament_link:
                yield Request(tournament_link, callback = self.parse_world, meta = {'details' : details})

            if 'www.pgatour.com/' not in tournament_link:
                continue

            yield Request(tournament_link, callback= self.parse_listing, meta={'details' : details})

    def parse_listing(self, response):
        items               = []
        url                 = response.url
        sportssetupitem     = SportsSetupItem()
        hxs                 = HtmlXPathSelector(response)
        details             = response.meta['details']
        leader_board_link   = extract_data(hxs, '//ul[@class="nav nav-tabs slim nav-tabs-drop"]/li/a[contains(@href, "leaderboard")]/@href')


        if not leader_board_link and 'www.majorschampionships.com' in response.url:
            leader_board_link = extract_data(hxs, '//a[@title="SCORING" or @title="Scoring"]/@href')
        if not leader_board_link and 'www.worldgolfchampionships.com' in response.url and "hsbc-champions" not in details['tou_name']:
            leader_board_link = extract_data(hxs, '//li[@class="static"]//a[contains(text(), "SCORING") or contains(text(), "scoring") or contains(text(), "Scoring")]')
        if leader_board_link and 'http' not in leader_board_link:
            leader_board_link = get_full_url(leader_board_link, response.url)

        result_link  = extract_data(hxs, '//ul[@class="nav nav-tabs slim nav-tabs-drop"]/li/a[contains(@href, "past-results")]/@href')

        if result_link and 'http' not in result_link:
            result_link = get_full_url(result_link, response.url)

        today_date  = datetime.datetime.utcnow().date()
        t_year      = extract_data(hxs, '//div[@class="info"]//span[@class="dates"]/text()').replace('\n', ' ')

        if t_year and 'UTC' in t_year:
            tou_year = t_year.split('UTC')[-1].replace(' ','').strip()
        elif t_year:
            tou_year = t_year.split(',')[-1].strip()
        elif not t_year and "worldgolfchampionships.com" in response.url:
            tou_year = '2014'
        else:
            tou_year = today_date.year

        if len(details['str_date'].split()) == 2 and len(details['e_date'].split()) == 2:
            e_date = details['e_date'] + ', %s' % tou_year
            e_format, s_format = '%b %d, %Y', '%b %d'
        else:
            e_date = details['e_date']
            e_format, s_format =  '%a %b %d %H:%M:%S %Z %Y', '%a %b %d %H:%M:%S %Z %Y'

        tou_date, start_date, end_date = get_tou_dates(details['str_date'], e_date, s_format, e_format)
        tou_year = start_date.year

        game_datetime = get_game_datetime(tou_date, tou_year)

        if start_date <= today_date and today_date <= end_date:
            status = "ongoing"
        elif start_date < today_date:
            status = "completed"
        else:
            status = "scheduled"
            sportssetupitem['participants'] = {}
            sportssetupitem['result'] = {}


        if not tou_date or not details['tou_name']:
            print "Tou-date: %s --- Tou-Name: %s" % (tou_date, details['tou_name'])
        else:
            modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
            modified_tou_date = '_'.join(i.strip() for i in tou_date.strip().split('-')).replace(' ', '_').strip()
            tou_sk = modified_tou_name + '_' + modified_tou_date

            tour_name = get_refined_tou_name(details['tou_name'])

            details.update({    'tou_date'  : tou_date,     'tou_year'     : tou_year,
                                'tou_sk'    : tou_sk,       'game_status'  : status,
                                'tour_name' : tour_name,
                                'tou_link'  : response.url, 'end_date'     : end_date,
                                'leader_board_link' : leader_board_link, 'start_date' : start_date })

            rich_data   = {     'location'      : details['loc'],
                                'prize_money'   : details['prize_money'],
                                'stadium'       : details['stadium'],
                                'game_note'     : details['game_note']}

            sportssetupitem['game_status']       = status
            sportssetupitem['game_datetime']     = game_datetime
            sportssetupitem['source_key']        = tou_sk
            sportssetupitem['tournament']        = tour_name
            sportssetupitem['rich_data']         = rich_data
            sportssetupitem['reference_url']     = url
            sportssetupitem['game']              = 'golf'
            sportssetupitem['source']            = 'pga_golf'
            sportssetupitem['participant_type']  = 'player'
            sportssetupitem['affiliation']       = 'pga'
            sportssetupitem['time_unknown']      = 1
            sportssetupitem['tz_info']           = details['tz_info']

            if status == "ongoing":
                if leader_board_link:
                    yield Request(leader_board_link, callback = self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})
            elif status == "completed":
                if result_link:
                    yield Request(result_link, callback = self.parse_results, meta={'details' : details, 'sportssetupitem' : sportssetupitem})
                elif leader_board_link:
                    yield Request(leader_board_link, callback= self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})
            else:
                sportssetupitem['participants'] = {}
                sportssetupitem['result'] = {}
                items.append(sportssetupitem)
                for item in items:
                    if 'Ryder Cup' in sportssetupitem['tournament']:
                        import pdb;pdb.set_trace()
                        yield item

    def parse_details(self, response):
        hxs                 = HtmlXPathSelector(response)
        domain              = response.url.split('.com/')[0] + '.com'
        json_link           = extract_data(hxs, ('//script[contains(text(), "lbJson")]'))
        if json_link:
            json_link_doc   = re.findall(r'(.*leaderboard-v2.json)', json_link)[0]
            json_new        = json_link_doc.split(":")[-1]
            json_link       = json_new.split("'")[1]
            if 'http:' not in json_link:
                json_link   = domain + json_link

            yield Request(json_link, callback = self.parse_json_details, meta = {'details' : response.meta['details'], 'sportssetupitem' : response.meta['sportssetupitem']})

    def parse_json_details(self, response):
        participants    = {}
        items           = []
        result          = {}
        details         = response.meta['details']
        raw_data        = response.body
        data            = eval(raw_data)
        sportssetupitem = response.meta['sportssetupitem']
        if data:
            l_info      = data.get("debug", '')
            tou_id      = data.get('leaderboard', '').get('tournament_id', '')
            _round      = data.get('leaderboard', '').get('current_round', '')
            rnd_status  = data.get('leaderboard', '').get('round_state', '')
            if details['game_status'] == 'ongoing': current_round = 'Round' + ' ' + str(_round) + ' - ' + rnd_status
            else: current_round = ''
            info        = data.get('leaderboard', '')
            p_info      = info.get('players','')
            players, player_name_map  = {}, {}
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
                    rounds_scores.append((result_['round_number'], result_['strokes']))

                rounds_scores.sort()
                scores = [score for round, score in rounds_scores]

                first_name  = i.get('player_bio', '').get('first_name', '')
                last_name   = i.get('player_bio', '').get('last_name', '')
                player_name = ' '.join(i.strip() for i in [first_name, last_name] if i)
                if '\/' in player_name:
                    player_name = ' '.join(i.strip() for i in player_name.split('\/') if i)

                player_id = i.get('player_id', '')
                if player_id:
                    player_name_map[player_name]    = player_id
                    participants[player_id]         = (0, player_name)
                    players                         = get_golfers_info(to_par, total_strokes, pl_pos, scores, [])
                    result[player_id]               = players
                    sportssetupitem['participants'] = participants

                    ids_map[player_id] = player_name
                    player_ids.append(player_id)

            if details['game_status'] == 'completed':
                winner = player_name_map.get(details['def_chp'], '')
                if not winner:
                    if pl_pos == "1":
                        winner = player_id 
            else:
                winner = ''

            sportssetupitem['rich_data'].update({'game_note' : current_round})
            result.setdefault('0', {}).update({'winner' : winner})
            sportssetupitem['result']           = result

            if participants:
                items.append(sportssetupitem)
                for item in items:
                    yield item

    def parse_results(self, response):
        items           = []
        participants    = {}
        result          = {}
        details         = response.meta['details']
        hxs             = HtmlXPathSelector(response)
        sportssetupitem = response.meta['sportssetupitem']
        tour_ending     = extract_data(hxs, '//span[@class="header-row"]/b[contains(text(), "Ending")]/text()').split('/')[-1].strip()
        if not tour_ending and 'www.worldgolfchampionships.com' in response.url:
            tour_ending = extract_data(hxs, '//div[contains(@class, "tourTableContent")]/parent::div/text()[preceding::b[1][contains(text(),"Ending")]]')

        target_par_values   = extract_list_data(hxs, '//span[@class="header-row"]/span[contains(text(), "PAR")]/text()')

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
            index_of_data = 2 + len(total_rounds)
            player_nodes = get_nodes(hxs, '//li[@class="col-left"]//tr[contains(@class, "") or contains(@class, "odd")]')
            for node in player_nodes:
                player_nodes = get_nodes(node, './/td')
                if len(player_nodes) < index_of_data:
                    continue

                req_player_data = [extract_data(i, './text()') for i in player_nodes[:index_of_data]]
                player_name, position = [i.strip() for i in req_player_data[:2]]
                pl_pos  = get_position(position)
                _rounds = extract_list_data(node, './/td[@class="hidden-small"]/text()')[:len(total_rounds)]
                rounds  = [i for i in _rounds if len(i) == 2]

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
                    to_par = 'E'

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

            if details['game_status'] == 'completed':
                winner = player_name_map.get(details['def_chp'], '')
                if not winner:
                    if pl_pos == "1":
                        winner = player_id

            else:
                winner = ''

            result.setdefault('0', {}).update({'winner' : winner})
            sportssetupitem['result']           = result

            if participants:
                player_ids = [i for i in player_ids if i]

                items.append(sportssetupitem)
                for item in items:
                    yield item
        else:
            yield Request(details['leader_board_link'], callback = self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})

    def parse_world(self, response):
        hxs = HtmlXPathSelector(response)
        details = response.meta['details']
        today_date = datetime.datetime.utcnow().date()
        tou_year = "2013"
        sportssetupitem = SportsSetupItem()

        if len(details['str_date'].split()) == 2 and len(details['e_date'].split()) == 2:
            e_date = details['e_date'] + ', %s' % tou_year
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

        details.update({'status' : status, 'leader_board_link' : leader_board_link, 'game_datetime' : game_datetime, 'start_date' : start_date, 'end_date' : end_date, 'tou_date' : tou_date, 'tour_name' : tour_name})

        yield Request(lb_link, callback = self.parse_world_next, meta = {'details' : details})

    def parse_world_next(self, response):
        sportssetupitem = SportsSetupItem()
        items           = []
        details_doc     = HtmlXPathSelector(response)
        details         = response.meta['details']
        players, player_name_map, participants = {}, {}, {}
        result = {}
        ids_map, player_ids = {}, []
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
            round_r2         = extract_data(node, './/td[8]/text()')
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


        rich_data   = {'loc' : details['loc'], 'prize_money' : details['prize_money']}

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
    def parse_presidents(self, response):
        hxs = HtmlXPathSelector(response)
        details = response.meta['details']
        leader_board = extract_data(hxs, '//script[contains(text(), "prescupJson")]')
        if leader_board:
            leader_board_link = re.findall(r'(.*/pcup.json)', leader_board).split(":")[1].replace("'", "").strip()
            if "http" not in leader_board_link:
                json_link = "http://www.presidentscup.com" + leader_board_link
                yield Request (json_link, callback = self.parse_presidents_next, meta = {'details': details})
        else:
            leader_board_link = ''

            yield Request (json_link, callback = self.parse_presidents_next, meta = {'details': details})

    def parse_presidents_next (self, response):
        details = response.meta['details']
        sportssetupitem = SportsSetupItem()
        items = []
        result          = {}
        players         = {}
        raw_data        = response.body
        data = eval(raw_data)
        teams = {'United States' : 'usa', 'International' : 'International_Team'}
        if data:
            t_info = data.get("tournament", {})
            r_info = t_info.get("rounds", [])

            for round in r_info:
                cup_teams = round.get('cupTeams', [])
                for t_data in cup_teams:
                    scores = t_data.get('score', '')
                    name = t_data.get('name', '')
                    for k, v in teams.iteritems():
                        if k == name:
                            p_name = v
                            players[p_name] = {'to_par' : scores, 'tscore': scores}
                            result = players[p_name]
            test_dict = players.keys()
            if players[test_dict[0]] > players[test_dict[1]]:
                winner = test_dict[0]
            elif players[test_dict[0]] < players[test_dict[1]]:
                winner = test_dict[1]
            else:
                winner = ''

            result.setdefault('0', {}).update({'winner' : winner})

            modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].split('-')).replace(' ', '_').strip()
            modified_tou_date = '_'.join(i.strip() for i in details['tou_date'].split('-')).replace(' ', '_').strip()
            tou_sk = modified_tou_name +'_'+ modified_tou_date
            rich_data   = {'loc' : details['loc'], 'prize_money' : details['prize_money']}

            sportssetupitem['participants']      = players
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

