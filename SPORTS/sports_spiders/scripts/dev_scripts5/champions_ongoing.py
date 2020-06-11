import datetime
import time
import re
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from vtvspider_dev import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import copy
import pytz
import lxml.html
import urllib2

RECORD = SportsSetupItem()

REPLACE_TOU_SPL_DICT = {' at ': ' ', ' &amp; ': ' ', 'GC': 'Golf Club',
                        'CC': 'Country Club', 'TPC': 'Tournament Players Club',
                        'G&amp;CC': 'Golf and Country Club',
                        'GCC': 'Golf and Country Club',
                        ' G&amp;S': 'Golf and Spa', 'G&amp;': 'Golf and '}

REPLACE_STATE_DICT = {'HI': 'Hawaii', 'CA': 'California', 'FL': 'Florida',
                      'GA': 'Georgia', 'TX': 'Texas', 'AL': 'Alabama',
                      'MI': 'Michigan', 'IL': 'Illinois', 'PA': 'Pennsylvania',
                      'OK': 'Oklahoma', 'MN': 'Minnesota', 'NY': 'New York',
                      'WA': 'Washington', 'AB': 'Alberta',
                      'NC': 'North Carolina', 'AZ': 'Arizona',
                      'MS': 'Mississippi', 'IA': 'Iowa', 'MO': 'Missouri'}

CTRY_ACC_STATE = {'Hawaii': 'USA', 'California': 'USA', 'Florida': 'USA',
                  'Georgia': 'USA', 'Texas': 'USA', 'Alabama': 'USA',
                  'Michigan': 'USA', 'Illinois': 'USA', 'Pennsylvania': 'USA',
                  'Oklahoma': 'USA', 'Minnesota': 'USA', 'New York': 'USA',
                  'Washington': 'USA', 'Alberta': 'USA',
                  'North Carolina': 'USA', 'Arizona': 'USA', 'CAN': 'Canada',
                  'Mississippi': 'USA', 'WAL': 'England',
                  'Iowa': 'USA', 'Missouri': 'USA'}

REPLACE_TOU_DICT = {'u.s. open golf championship': 'u.s. open',
                    'alfred dunhill championship': \
                    'alfred dunhill links championship',
                    'the senior open championship pres. by rolex': \
                    'senior open championship'}

IGNORE_WORDS = ['presented', 'in partnership', 'pres. by', \
                ' at ', 'partnership', 'powered by', "pres'd by", ' by ']

REPLACE_WORDS = ['waste management', 'omega', 'isps handa', 'alstom', \
                 'aberdeen asset management', 'isps handa', \
                 'nature valley', 'vivendi', 'm2m']

REPLACE_WORDS_BY = {"hawai'i": 'hawaii', 'the senior': 'senior', \
                    'hickory kia classic': 'hickory classic'}

SKIP_TOU = ['Big Cedar Lodge Legends of Golf presented by Bass Pro Shops', \
            'PNC Father Son Challenge', \
            'The Senior Open Championship presented by Rolex']

START_END_DATE = """./td[@class="num-date"]/span[%s]/text()"""

TOU_XPATH = """./td[@class="tournament-name"]/%s"""

RESULT_LINK = """//ul[@class="nav nav-tabs slim nav-tabs-drop"]\
                /li/a[contains(@href, "%s")]/@href"""

PGA_LINK = """http://www.pgatour.com"""

CHANNEL_DATA = """./td[@class="%s-name hidden-small"]/text()"""

def create_doc(url):
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 \
            (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,\
           application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'}

    req = urllib2.Request(url, headers=hdr)
    data = urllib2.urlopen(req).read()
    doc = lxml.html.fromstring(data)
    return doc

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position

    return pos

def get_utc_time(date_val, pattern):
    utc          = pytz.utc
    eastern      = pytz.timezone('US/Eastern')
    fmt          = '%Y-%m-%d %H:%M:%S'
    date         = datetime.datetime.strptime(date_val, pattern)
    date_eastern = eastern.localize(date, is_dst=None)
    date_utc     = date_eastern.astimezone(utc)
    utc_date     = date_utc.strftime(fmt)

    return utc_date

def get_modified_tou_details(tou_data):
    tou_ = '_'.join(i.strip() for i in tou_data.strip().\
            split('-')).replace(' ', '_').strip()
    return tou_

def get_golfers_info(to_par, total_score, pl_pos, rounds_scores, tee_time):
    players = {}
    players = {'TO PAR' : to_par, 'final' : total_score, 'position' : pl_pos}
    R1 = R2 = R3 = R4 = ''
    for i in xrange(len(rounds_scores)):
        round_t = "R%s" % str(i + 1)
        round_v = rounds_scores[i]
        players.update({round_t : round_v})
    tee_times = []
    for tee_t in tee_time:
        if tee_t == '':
            break
        else:
            tee_times.append(tee_t)

    for i in xrange(len(tee_times)):
        round_t = "R%s_tee_time" % str(i+1)
        round_v = tee_times[i]
        players.update({round_t : round_v})

    return players

def get_game_datetime(tou_date, year):
    game_dates = [i.strip() for i in tou_date.split('-') if i]
    if len(game_dates) == 2:
        start_date = game_dates[0]
        game_datetime = start_date + ' %s' % year
    elif len(game_dates) >= 1:
        game_datetime = game_dates[0] + ' %s' % year
    else:
        game_datetime = ''

    if game_datetime:
        game_datetime = get_utc_time(game_datetime, '%b %d %Y')

    return game_datetime

def get_str_end_dates(tou_date):
    str_date, e_date = [i.strip() for i in tou_date.split('-')]
    if len(e_date.split()) < 2:
        str_month = str_date.split()[0]
        e_date = '%s %s' % (str_month, e_date)

    return str_date, e_date

def get_refined_tou_name(tou_name):
    tou_name = tou_name.lower()

    if REPLACE_TOU_DICT.has_key(tou_name):
        tou_name = REPLACE_TOU_DICT[tou_name]

    for key, value in REPLACE_WORDS_BY.iteritems():
        if key in tou_name:
            tou_name = tou_name.replace(key, value).strip()

    for i in IGNORE_WORDS:
        if i in tou_name:
            tou_name = tou_name.split(i)[0].strip()

    for i in REPLACE_WORDS:
        tou_name = tou_name.replace(i, '').strip()
    return tou_name

def permutations(iterable, player_r=None):
    pool = tuple(iterable)
    player_n = len(pool)
    if player_r is None:
        player_r = player_n

    if player_r > player_n:
        return

    indices = range(player_n)
    cycles = range(player_n, player_n-player_r, -1)
    yield tuple(pool[i] for i in indices[:player_r])
    while player_n:
        for i in reversed(range(player_r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = player_n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:player_r])
                break
        else:
            return

def get_city_state_ctry(venue):
    venue_ = venue.split(',')
    if len(venue_) == 2:
        country = ''
        city  = venue_[0].strip()
        state = REPLACE_STATE_DICT.get(venue_[-1].strip())
        if not state:
            state = ''
            country = CTRY_ACC_STATE.get(venue_[-1].strip())
    elif len(venue_) == 3:
        city = venue_[0].strip()
        state = venue_[1].strip()
        country = venue_[-1].strip()
        if country:
            country = CTRY_ACC_STATE.get(country)
    elif len(venue_) == 1:
        country = venue.strip()
        city, state = '', ''
    else:
        city = state = country = ''

    return city, state, country

def get_refined_std_name(stadium):
    for key, value in REPLACE_TOU_SPL_DICT.iteritems():
        if key in stadium:
            stadium = stadium.replace(key, value).strip()

    if '(' in stadium:
        stadium_extra = "".join(re.findall(r'\(.*\)', stadium))
        stadium = stadium.replace(stadium_extra, '').strip()

    return stadium

def get_tou_dates(start_date, end_date, start_date_format, end_date_format):
    end_date = (datetime.datetime(*time.strptime(end_date.strip(), \
                end_date_format)[0:6])).date()
    game_year = (datetime.datetime(*time.strptime(start_date.strip(), \
                 start_date_format)[0:6])).date()
    if game_year.month > end_date.month:
        tou_year = end_date.year - 1
    else:
        tou_year = end_date.year
    if len(start_date.split()) == 2:
        start_date += ', %s' % tou_year
        start_date_format += ', %Y'

    start_date = (datetime.datetime(*time.strptime(start_date.strip(), \
                  start_date_format)[0:6])).date()
    tou_date = start_date.strftime('%b %-d') + ' - ' + \
               end_date.strftime('%b %-d')

    return tou_date, start_date, end_date

def get_players_ids(player_link):
    player_id_map = {}
    if not player_link:
        return player_id_map
    try:
        pl_doc = create_doc(player_link)
    except:
        return player_id_map
    player_nodes = pl_doc.xpath('//div[@class="overview"]/\
                              div[@class="directory-item"]/\
                              div[@class="item-data"]//\
                              ul[@class="ul-inline items"]/li/\
                              span[@class="name"]')
    for node in player_nodes:
        player_ref = "".join(node.xpath('./a/@href')).strip()
        player_det = "".join(node.xpath('./a/text()')).strip()

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
            player_ref = PGA_LINK + player_ref

        if not f_name or not player_id:
            continue

        player_name = ' '.join(i.strip() for i in [f_name, l_name] if i).lower()
        player_id_map[player_name] = player_id

    return player_id_map

WEB_PLYRS = "http://www.pgatour.com/webcom/players.html"

PGA_PLYRS = "http://www.pgatour.com/players.html"

CHAMP_PLYRS = "http://www.pgatour.com/champions/players.html"

class ChampionsOngoing(VTVSpider):
    name = 'champions_ongoing'
    start_urls = ['http://www.pgatour.com/champions/tournaments/schedule.html']

    participants = {}
    result       = {}


    def parse(self, response):
        hxs = Selector(response)
        player_link = extract_data(hxs, '//div[@class="nav-container"]\
                                    //li/a[contains(@href, "players")]/@href')
        if player_link and 'http' not in player_link:
            player_link = PGA_LINK + player_link
        elif not player_link:
            player_link = CHAMP_PLYRS

        player_id_map = get_players_ids(player_link)
        all_players_map = copy.deepcopy(player_id_map)
        webcom_players  = get_players_ids(WEB_PLYRS)
        pga_players    = get_players_ids(PGA_PLYRS)

        all_players_map.update(webcom_players)
        all_players_map.update(pga_players)

        nodes = get_nodes(hxs, '//table[@class="table-styled"]//\
                          tbody/tr[contains(@class, "odd") or \
                          contains(@class, "even")]')
        for node in nodes:
            st_date = extract_data(node, START_END_DATE % '2')
            _e_date = extract_data(node, START_END_DATE % '3')
            main_date = st_date + '-' + _e_date
            str_date, e_date = get_str_end_dates(main_date)
            tou_venue = extract_data(node , TOU_XPATH % ('text()'))
            if "Purse" in tou_venue:
                _venue = "".join(re.findall(r'.. Purse:.*', tou_venue))
                tou_venue = tou_venue.replace(_venue, '').strip()

            tournament_link = extract_data(node, TOU_XPATH % ('a/@href'))
            tou_name = extract_data(node, TOU_XPATH % ('span/text()')).\
                                    replace('&amp;', '&').strip()
            if tou_name in SKIP_TOU:
                continue
            stadium = tou_venue.split(',')[0].strip()

            stadium = get_refined_std_name(stadium)

            loc = {}
            venue = ",".join(tou_venue.split(',')[1:]).strip()
            _city, state, country = get_city_state_ctry(venue)
            if not country:
                country = CTRY_ACC_STATE.get(state.strip())

            if '$' in stadium or 'Purse' in stadium:
                _city = state = country = stadium = ''
            loc = {'city': _city, 'country': country, 'state': state}
            tz_info = get_tzinfo(city = _city)
            channels = extract_data(node, CHANNEL_DATA % 'network').\
                        replace(' ', ',')
            def_data = ["".join(i).strip() for i in extract_list_data\
                        (node, CHANNEL_DATA % 'champions') if i]

            if len(def_data) == 2:
                defending_chp, prize_money = def_data
            elif len(def_data) == 3:
                defending_chp, prize_money = def_data[0], def_data[2]
            else:
                defending_chp, prize_money = '', ''

            if defending_chp.endswith(','):
                defending_chp = defending_chp.rstrip(',').strip()

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

            if start_date <= today_date and today_date <= end_date:
                status = "ongoing"
            elif start_date < today_date:
                status = "completed"
            else:
                status = "scheduled"

            previous_date = datetime.timedelta(-1)
            new_date = today_date + previous_date

            game_datetime = get_game_datetime(tou_date, tou_year)

            if tournament_link and 'http' not in tournament_link:
                tournament_link = PGA_LINK + tournament_link

            if not tou_name or not e_date or not str_date or \
               not tournament_link:
                continue

            if ('www.pga.com/' or 'ussenioropen') in tournament_link \
                or 'pgatour.com' not in tournament_link:
                continue

            details = {'tournament_link': tournament_link, \
                       'str_date': str_date, 'e_date' : e_date, 'loc' : loc, \
                       'tou_name': tou_name, 'channels': channels, \
                       'def_chp': defending_chp, 'prize_money': prize_money, \
                       'player_id_map': player_id_map, 'tz_info': tz_info, \
                       'start_date' : start_date, 'end_date' : end_date,
                       'tou_date' : tou_date, 'tou_year' : tou_year,
                       'game_datetime' : game_datetime, 'status' : status,
                       'all_players_map': all_players_map, 'stadium': stadium}

            if status == "ongoing":
                yield Request(tournament_link, callback = self.parse_listing, \
                                meta = {'details' : details})
            elif status == "completed" and end_date == new_date:
                yield Request(tournament_link, callback = self.parse_listing, \
                                meta = {'details' : details})

    def parse_listing(self, response):
        hxs = Selector(response)
        details = response.meta['details']

        leader_board_link = extract_data(hxs, RESULT_LINK % ("leaderboard")) or\
                            extract_data(hxs, '//div[@class="tourPodHeader"]//a[contains(@href,"leaderboard")]/@href') or \
                            extract_data(hxs, '//table[@class="tourHeaderMainNavTable"]//td/div/a[contains(@href,"leaderboard")]/@href')

        if leader_board_link and 'http' not in leader_board_link:
            leader_board_link = PGA_LINK + leader_board_link

        result_link = extract_data(hxs, RESULT_LINK % ("past-results"))
        if result_link and 'http' not in result_link:
            result_link = PGA_LINK + result_link

        if details['tou_date'] or details['tou_name']:
            modified_tou_name = get_modified_tou_details(details['tou_name'])
            modified_tou_date = get_modified_tou_details(details['tou_date'])
            tou_sk = modified_tou_name + '_' + modified_tou_date
            tour_name = get_refined_tou_name(details['tou_name'])

            rich_data = {"channels": details['channels'],
                         "location": details['loc'],
                         "stadium": details['stadium']}

            RECORD['rich_data'] = rich_data
            RECORD['source_key'] = tou_sk
            RECORD['game'] = 'golf'
            RECORD['source'] = 'champions_golf'
            RECORD['participant_type'] = 'player'
            RECORD['affiliation'] = 'champions'
            RECORD['game_datetime'] = details['game_datetime']
            RECORD['tournament'] = tour_name
            RECORD['event'] = ''
            RECORD['season'] = details['tou_year']
            RECORD['location_info'] = details['loc']
            RECORD['tz_info']       = details['tz_info']
            RECORD['time_unknown']  = 1

            details.update({'leader_board_link': leader_board_link,
                            'result_link': result_link,
                            'tou_sk': tou_sk, 'tour_name' : tour_name})

            if details['status'] == "ongoing":
                if leader_board_link:
                    yield Request(leader_board_link, \
                                  callback = self.parse_details, \
                                  meta = {'details' : details})
            elif details['status'] == "completed":
                if result_link:
                    yield Request(result_link, \
                                  callback = self.parse_results, \
                                  meta = {'details' : details})
                elif leader_board_link:
                    yield Request(leader_board_link, \
                                  callback = self.parse_details, \
                                  meta = {'details' : details})
            elif details['status'] == "scheduled":
                RECORD['result'] = {}
                RECORD['participants'] = {}
                RECORD['game_status'] = details['status']
                RECORD['reference_url'] = response.url
                yield RECORD

    def parse_results(self, response):
        hxs             = Selector(response)
        details         = response.meta['details']
        tour_ending     = extract_data(hxs, '//span[@class="header-row"]/b\
                          [contains(text(), "Ending")]/text()').\
                          split('/')[-1].strip()
        target_par_values = extract_list_data(hxs, '//span[@class="header-row"]/span[contains(text(), "PAR")]/text()')
        target_par_values = ["".join(i.replace('PAR:', '').strip()) for i in target_par_values if "".join(i)]
        if len(target_par_values) > 1:
            target_par = target_par_values[-1]
        elif target_par_values:
            target_par = target_par_values[0]
        else:
            target_par = 0

        total_rounds = extract_list_data(hxs, '//li[@class="col-left"]//th[@class="head-data-cell hidden-small"]//tr[2]/td/text()')
        total_rounds = [int(i) for i in total_rounds if i]

        if not isinstance(target_par, int) and not target_par.isdigit():
            target_par = 0
        else:
            target_par = int(target_par)

        if target_par and str(details['end_date'].year) in tour_ending and total_rounds:
            players, player_name_map, missing_players = {}, {}, {}
            missing_player_name_map = {}
            ids_map, player_ids = {}, []
            index_of_data = 2 + len(total_rounds)
            player_nodes = get_nodes(hxs, '//li[@class="col-left"]//tr[contains(@class, "") or contains(@class, "odd")]')
            for node in player_nodes:
                player_nodes = get_nodes(node, './/td')
                if len(player_nodes) < index_of_data:
                    continue

                req_player_data = [extract_data(i, './text()') for i in player_nodes[:index_of_data]]
                player_name, position = [i.strip() for i in req_player_data[:2]]
                pl_pos = get_position(position)
                _rounds = extract_list_data(node, './/td[@class="hidden-small"]/text()')[:len(total_rounds)]
                rounds  = [i for i in _rounds if len(i) == 2]

                rounds_scores = []
                is_score_zero = False
                for round_ in rounds:
                    if round_.isdigit():
                        if round_ == '0':
                            is_score_zero = True

                        rounds_scores.append(int(round_))
                    elif '-' in round_ or '+' in round_:
                        try:
                            rounds_scores.append(target_par - int(round_))
                        except:
                            rounds_scores.append(0)
                    else:
                        rounds_scores.append(0)

                if not player_name or (len(set(rounds_scores)) == 1 and \
                   rounds_scores[0] == 0 and not is_score_zero) or not pl_pos:
                    continue

                player_id = details['player_id_map'].get(player_name, '')
                if not player_id:
                    player_set = [' '.join(i) for i in permutations(player_name.lower().strip().split())]
                    for i in player_set:
                        if details['player_id_map'].has_key(i):
                            player_id = details['player_id_map'][i]

                if not player_id:
                    player_set = [' '.join(i) for i in permutations(player_name.lower().strip().split())]
                    for i in player_set:
                        if details['all_players_map'].has_key(i):
                            player_id = details['all_players_map'][i]

                to_par_per_round = [score - target_par for score in rounds_scores if score]
                total_score = sum(rounds_scores)
                to_par = sum(to_par_per_round)
                if to_par == 0:
                    to_par = 'E'

                if 'W/D' in position or 'CUT' in position:
                    to_par = ''

                if player_id:
                    player_name_map[player_name] = player_id
                    ids_map[player_id] = player_name
                    player_ids.append(player_id)
                    self.participants.update({player_id: (0, player_name)})
                    players                         = get_golfers_info(to_par, total_score, pl_pos, rounds, [])
                    self.result[player_id]               = players
                    RECORD['participants'] = self.participants

            if details['status'] == 'completed':
                winner = player_name_map.get(details['def_chp'], '')
                if not winner:
                    player_set = [' '.join(i) for i in permutations(details['def_chp'].strip().split())]
                    for i in player_set:
                        if player_name_map.has_key(i):
                            winner = player_name_map[i]

                if not winner:
                    player_set = [' '.join(i) for i in permutations(details['def_chp'].strip().split())]
                    for i in player_set:
                        if missing_player_name_map.has_key(i):
                            winner = missing_player_name_map[i]
                            players[winner] = missing_players[winner]
            else:
                winner = ''

            self.result.setdefault('0', {}).update({'winner' : winner,})
            RECORD['result']           = self.result
            RECORD['reference_url'] = response.url
            RECORD['game_status'] = details['status']

            if self.participants:
                yield RECORD
        else:
            yield Request(details['leader_board_link'], callback = self.parse_details, meta = {'details' : details})

    def parse_details(self, response):
        hxs = Selector(response)
        domain = response.url.split('.com/')[0] + '.com'
        json_link = extract_data(hxs, '//script[contains(text(), "lbJson")]')
        if json_link:
            json_link_doc = re.findall(r'(.*leaderboard-v2.json)', json_link)[0]
            json_new = json_link_doc.split(":")[-1]
            json_link = json_new.split("'")[1]
            if 'http:' not in json_link:
                json_link = domain + json_link

            yield Request(json_link, callback = self.parse_json_details, \
                          meta = {'details': response.meta['details']})

    def parse_json_details(self, response):
        true = True
        false = False
        null = ''
        missing_players = {}
        details      = response.meta['details']
        raw_data     = response.body
        data         = eval(raw_data)

        if data:
            _round = data.get('leaderboard', '').get('current_round', '')
            rnd_status = data.get('leaderboard', '').get('round_state', '')
            if details['status'] == 'ongoing':
                curr_round = 'Round' + ' ' + str(_round) + ' - ' + rnd_status
            else:
                curr_round = ''
            info = data.get('leaderboard', '')
            p_info = info.get('players','')
            players, player_name_map = {}, {}
            ids_map, player_ids = {}, []
            for i in p_info:
                position = i.get('current_position', '')
                pl_pos = get_position(position)
                to_par = i.get('today', '')
                results = i.get('rounds', '')
                total_strokes = i.get('total_strokes', '')

                rounds_scores = []
                for self.results in results:
                    rounds_scores.append((self.results['round_number'], self.results['strokes']))

                rounds_scores.sort()
                scores = [score for round_, score in rounds_scores]

                first_name = i.get('player_bio', '').get('first_name', '')
                last_name = i.get('player_bio', '').get('last_name', '')
                player_name = ' '.join(i.strip() for i in [first_name, last_name] if i)
                if '\/' in player_name:
                    player_name = ' '.join(i.strip() for i in player_name.split('\/') if i)

                player_id = i.get('player_id', '')
                if player_id:
                    player_name_map[player_name]    = player_id
                    self.participants.update({player_id: (0, player_name)})
                    players                         = get_golfers_info(to_par, total_strokes, pl_pos, scores, [])
                    self.result[player_id]               = players

                    ids_map[player_id] = player_name
                    player_ids.append(player_id)
                else:
                    player_id = '-'.join(i.lower().strip() for i in player_name.split())
                    missing_players[player_id] = {'to_par' : to_par, 'tscore' : scores, 'name': player_name, 'rounds' : rounds_scores, 'pos' : pl_pos}

                winner = ''
                if details['status'] == 'completed':
                    if len(score) == 3:
                        import pdb;pdb.set_trace()
                        
                    winner = player_name_map.get(details['def_chp'], '')
                    if not winner:
                        player_set = [' '.join(i) for i in permutations(details['def_chp'].strip().split())]
                        for i in player_set:
                            if player_name_map.has_key(i):
                                winner = player_name_map[i]

            RECORD['participants'] = self.participants
            RECORD['rich_data'].update({'game_note' : curr_round})
            self.result.setdefault('0', {}).update({'winner' : winner,})
            RECORD['result']           = self.result
            RECORD['game_status'] = details['status']
            RECORD['reference_url'] = response.url
            yield RECORD
