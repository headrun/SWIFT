import time
import datetime
import re
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_utc_time
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from urlparse import urlparse
import urllib2


REPLACE_STD_SPL_DICT = {' at ' : ' ', ' &amp; ' : ' ', 'GC': 'Golf Club', 'CC': 'Country Club', 'TPC' : 'Tournament Players Club', \
                        'G&amp;CC' : 'Golf and Country Club', 'GCC' : 'Golf and Country Club', 'G&amp;' : 'Golf and ', 'TBC,.' : 'TBC'}

REPLACE_STATE_DICT = {'FL' : 'Florida', 'CA' : 'California', 'Calif.' : 'California', \
                        'MI' : 'Michigan', 'OH' : 'Ohio', 'NY' : 'New York', 'AL' : 'Alabama', \
                        'NC' : 'North Carolina', 'NJ' : 'New Jersey', 'VA' : 'Virginia', 'AR' : 'Arkansas', \
                        'Ark.' : 'Arkansas', 'MD' : 'Maryland', 'Arizona' : 'Arizona', \
                        'Alabama' : 'Alabama', 'Texas' : 'Texas', 'Prattville' : 'Prattville', 'Lancashire': 'Lancashire'}

CTRY_ACC_STATE = {'Florida' : 'USA', 'Alabama' : 'USA', 'New York' : 'USA', 'Maryland' : 'USA', \
                    'Michigan' : 'USA', 'Ohio' : 'USA', 'Arkansas' : 'USA', \
                    'North Carolina' : 'USA', 'New Jersey' : 'USA', 'Virginia' : 'USA', \
                    'Texas' : 'USA', 'California' : 'USA', 'Arizona' : 'USA', 'Prattville' : 'USA', 'Lancashire': 'UK'}

REPLACE_STD_NAME_DICT = {'Pinehurst #2' : 'Pinehurst No. 2', 'Pinehurst Resort' : 'Pinehurst No. 2'}

REPLACE_TOU_DICT = {'the evian championship': 'the evian masters', \
                    'airbus mobile bay lpga classic' : 'mobile bay lPGA classic', \
                    'jtbc founders cup' : 'lpga founders cup', \
                    'fubon 2014 lpga taiwan championship' : 'LPGA Taiwan Championship'}

IGNORE_WORDS = ['presented', 'in partnership', 'pres. by', ' at ', \
                'partnership', 'powered by', "pres'd by", ' by ', 'conducted']

REPLACE_WORDS = ['ricoh']

REPLACE_WORDS_by = {'cn canadian': 'canadian', 'the 2013 solheim': 'solheim'}
REPLACE_CHARS = ['AM', 'AM*', 'PM', 'PM*', '*', '* ', ' *']




def get_game_location(venue):
    city = state = country = ''
    venue = venue.split(',')
    if len(venue) == 2:
        city  = venue[0].strip()
        state = REPLACE_STATE_DICT.get(venue[-1].strip())
        if not state:
            state = ''
            country = venue[-1].strip()
    elif len(venue) == 3:
        city = venue[0].strip()
        state = venue[1].strip()
        country = venue[-1].strip()
    elif len(venue) == 1:
        country = venue
    elif '$' in (city or state or country):
        city = state = country = ''

    return city, state, country

def get_refined_std_name(stadium):
    for key, value in REPLACE_STD_SPL_DICT.iteritems():
        if key in stadium:
            stadium = stadium.replace(key, value).strip()

    if '(' in stadium:
        std_extra = "".join(re.findall(r'\(.*\)', stadium))
        stadium = stadium.replace(std_extra, '').strip()

    if ',' in stadium:
        stadium = stadium.split(',')[0]

    for k, v in REPLACE_STD_NAME_DICT.iteritems():
        if k in stadium:
            stadium = stadium.replace(k, v).strip()

    if '$' in stadium:
        stadium = ''

    return stadium

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

def format_data(data):
    return ' '.join(i.strip() for i in data.split() if i)

def get_sk(player_url, player_name):
    try:
        redir_url = urllib2.urlopen(player_url).url
        sk = redir_url.split('/')[-1].split('.asp')[0].strip()
    except urllib2.HTTPError:
        sk = '-'.join(i.lower().strip() for i in player_name.split() if i)
        old_id = player_url.split('/')[-1].split('.asp')[0].strip()
    except:
        sk = '-'.join(i.lower().strip() for i in player_name.split() if i)
        old_id = player_url.split('/')[-1].split('.asp')[0].strip()

    return sk
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
        players.update({rt : rv.strip()})

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

def get_cur_rnd(rnd_scores, status):
    cur_rnd = ''
    if status == "ongoing":
        count = len(rnd_scores)
        for index, score in enumerate(rnd_scores):
            if index == 0 and score == '':
                cur_rnd = ""
                break
            elif index == 0 and score != '':
                cur_rnd = "Round 1"
            elif index != 0 and score == '':
                count = count - 1
                cur_rnd = "Round" + ' ' + str(count)
            else:
                cur_rnd = "Round" + ' ' + str(len(round_scores))

    elif status == "completed":
        cur_rnd = ""

    return cur_rnd

def modify(data):
    try:
        data = ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
        return data
    except ValueError or UnicodeDecodeError or UnicodeEncodeError:
        try:
            return data.encode('utf8')
        except  ValueError or UnicodeEncodeError or UnicodeDecodeError:
            try:
                return data
            except ValueError or UnicodeEncodeError or UnicodeDecodeError:
                try:
                    return data.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    data = extract_data('NFKD', data.decode('utf-8')).encode('ascii')
                    return data


class LPGAOngoing(VTVSpider):
    name = 'lpga_ongoing'
    start_urls = ['http://www.lpga.com/events/golf-tournaments.aspx']


    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        sportssetupitem = SportsSetupItem()
        lpga_nodes      = get_nodes(hxs, '//div[@id="lpgaEntries"]//div[@class="tournamentEntry"]')
        symetra_nodes   = get_nodes(hxs, '//div[@id="symetraEntries"]//div[@class="tournamentEntry"]')

        tou_nodes = [('lpga', lpga_nodes)]
        today_date = datetime.datetime.utcnow().date()
        for _type, nodes in tou_nodes:
            for node in nodes:
                tou_name = extract_data(node, './/div[@class="entryCenterCol"]/h4/a/text()').strip()
                print tou_name
                venue = extract_data(node, './/div[@class="entryCenterCol"]//p[@class="locale"]/text()').strip()
                if "presented by Kraft" in tou_name:
                    tou_name = "Meijer LPGA Classic"


                year = '2014'
                tou_date = extract_data(node, './/div[@class="entryLeftCol"]/p/text()').strip()
                if tou_date:
                    s_date, e_date = get_str_end_dates(tou_date)
                    e_date += ', %s' % year
                    tou_date, start_date, end_date = get_tou_dates(s_date, e_date, '%b %d', '%b %d, %Y')

                tournament_results_link = extract_data(node, './/div[@class="entryRightCol"]/a[@class="topLink"][contains(text(), "View Results")]/@href').strip()
                if tournament_results_link and 'http:' not in tournament_results_link:
                    tournament_results_link =  get_full_url(tournament_results_link, response.url)

                tou_link = extract_data(node, './/div[@class="entryCenterCol"]/h4/a/@href')
                if tou_link and 'http:' not in tou_link:
                    tou_link = get_full_url(tou_link, response.url)
                if start_date <= today_date and today_date <= end_date:
                    game_status = "ongoing"
                    
                    print game_status
                elif start_date < today_date:
                    game_status = "completed"
                    if "Meijer LPGA Classic" in tou_name:
                        game_status = "ongoing"
                    print game_status
                else:
                    game_status = "scheduled"
                    sportssetupitem['participants'] = {}
                    sportssetupitem['result'] = {}

                previous_date = datetime.timedelta(-1)

                new_date = today_date + previous_date

                game_datetime = get_game_datetime(tou_date, year)

                winner_name = extract_data(node, './/div[@class="entryRightCol"]//p[@class="winnerLabel"][contains(text(),"Winner")] \
                                            /following-sibling::a/text()').strip()
                winner_name = format_data(winner_name)
                winner_link = extract_data(node, './/div[@class="entryRightCol"]//a[@class="winnerLink"]/@href').strip()
                winner = winner_link.split('/')[-1].split('.asp')[0].strip()

                if 'kingsmill championship' in tou_name.lower():
                    tournament_results_link = 'http://www.lpga.com/golf/tournaments/lpga/kingsmill-championship-presented-by-jtbc/full-results.aspx'
                    winner = 'lizette salas'

                details = {'tou_date' : tou_date, 'tournament_results_link' : tournament_results_link, 'tou_link' : tou_link, \
                            'start_date' : start_date, 'game_status' : game_status, 'previous_date' : previous_date, 'new_date' : new_date, \
                            'winner_name' : winner_name, 'winner_link' : winner_link, 'winner' : winner, 's_date' : s_date, 'e_date' : e_date, \
                            'game_datetime' : game_datetime, 'tou_name' : tou_name, 'sportssetupitem': sportssetupitem}

                if game_status == "scheduled" :
                    if not tou_date or not tou_name:
                        continue

                    modified_tou_name = '_'.join(i.strip() for i in tou_name.strip().split('-')).replace(' ', '_').strip()
                    modified_tou_date = '_'.join(i.strip() for i in tou_date.strip().split('-')).replace(' ', '_').strip()
                    tou_sk = modified_tou_name +'_'+ modified_tou_date

                    if tou_link:
                        yield Request(tou_link, callback = self.parse_std_details, meta = {'details' : details})
                    else:
                        tou_link = "http://www.lpga.com/events/golf-tournaments.aspx"
                        yield Request(tou_link, callback = self.parse_std_details, meta = {'details' : details})

                elif game_status == "ongoing":
                    if tou_link:
                        yield Request(tou_link, callback = self.parse_std_details, meta = {'details' : details})
                    else:
                        tou_link = "http://www.lpga.com/events/golf-tournaments.aspx"
                        yield Request(tou_link, callback = self.parse_std_details, meta = {'details' : details})
                elif game_status == "completed"  and details['e_date'] == details['new_date']:
                    if tou_link:
                        yield Request(tou_link, callback = self.parse_std_details, meta = {'details' : details})
                    else:
                        tou_link = "http://www.lpga.com/events/golf-tournaments.aspx"
                elif game_status == "completed":
                    if tou_link:
                        yield Request(tou_link, callback = self.parse_std_details, meta = {'details' : details})
                    else:
                        tou_link = "http://www.lpga.com/events/golf-tournaments.aspx"


    def parse_std_details(self, response):
        tou_doc = HtmlXPathSelector(response)
        details = response.meta['details']
        stadium = extract_data(tou_doc, '//div[@class="courseInfo"]/h4/text()')

        stadium = get_refined_std_name(stadium)

        loc = {}
        venue = extract_data(tou_doc, '//div[@class="courseInfo"]/p[1]/text()')
        city, state, country = get_game_location(venue)
        if not country:
            country = CTRY_ACC_STATE.get(state).strip()

        loc = {'city' : city, 'country' : country, 'state' : state}


        tour_name = get_refined_tou_name(details['tou_name'])

        modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
        modified_tou_date = '_'.join(i.strip() for i in details['tou_date'].strip().split('-')).replace(' ', '_').strip()
        tou_sk = modified_tou_name +'_'+ modified_tou_date

        details.update({'tou_sk' : tou_sk, 'tour_name' : tour_name, 'location' : loc})
        rich_data   = {'location' : loc, 'stadium' : stadium}

        sportssetupitem                      = SportsSetupItem()
        sportssetupitem['game_status']       = details['game_status']
        sportssetupitem['game_datetime']     = details['game_datetime']
        sportssetupitem['source_key']        = tou_sk
        sportssetupitem['tournament']        = tour_name
        sportssetupitem['rich_data']         = rich_data
        sportssetupitem['game']              = 'golf'
        sportssetupitem['source']            = 'lpga_golf'
        sportssetupitem['participant_type']  = 'player'
        sportssetupitem['affiliation']       = 'lpga'
        sportssetupitem['reference_url']     = response.url


        if details['game_status'] == "ongoing" :
            leader_board_link = extract_data(tou_doc, '//div[@class="moduleContentInner"]/a[@href="http://www.lpgascoring.com"]/@href')
            #leader_board_link = extract_data(tou_doc, '//div[@class="moduleContentInner"]/a[contains(@href, "http://www.lpgascoring.com")]/@href')
            print leader_board_link
            if leader_board_link:
                json_link = leader_board_link + "/public/Leaderboard_JSON.aspx"
                yield Request(json_link, callback = self.parse_ongoing, meta = {'details' : details, 'sportssetupitem' : sportssetupitem})

            elif not leader_board_link and "LPGA Final Qualifying Tournament" in details['tou_name'] and details['e_date'] == details['new_date']:
                leader_board_link = "http://rts.symetrascoring.com/"
                yield Request(leader_board_link, callback = self.parse_qualifying, meta = {'details' : details, 'sportssetupitem' : sportssetupitem})


        elif details['game_status'] == "completed" and details['e_date'] == details['new_date']:
            if "LPGA Final Qualifying Tournament" in details['tou_name'] and details['e_date'] == details['new_date']:
                leader_board_link = "http://rts.symetrascoring.com/"
                yield Request(leader_board_link, callback = self.parse_qualifying, meta = {'details' : details})

            if details['tournament_results_link']:
                yield Request(details['tournament_results_link'], callback = self.parse_details, meta = {'details': details, 'sportssetupitem' : sportssetupitem})
        elif details['game_status'] == "completed":
            if details['tournament_results_link']:
                 yield Request(details['tournament_results_link'], callback = self.parse_details, meta = {'details': details, 'sportssetupitem' : sportssetupitem})

    def parse_ongoing(self, response):
        import pdb;pdb.set_trace()
        url = response.url
        print url
        sportssetupitem = response.meta['sportssetupitem']
        details     = response.meta['details']
        items       = []
        id_map      = {}
        players     = {}
        result      = {}
        participants = {}
        #data        = (response.body).replace("\r\n","")
        data        = response.body
        r_data      = eval(data)
        l_info      = r_data.get('leaderboard', '')
        rnd_status  = l_info.get('tournament', '').get('status', '')
        _round      = l_info.get('round', '').get('id', '')

        if details['game_status'] == 'ongoing': current_round = 'Round' + ' ' + _round + ' - ' + rnd_status
        else: current_round = ''

        sportssetupitem['rich_data'].update({'game_note' : current_round})

        lb_info     = l_info.get('lb', '')
        player_info = lb_info.get('players', ' ').get('player', [])
        for player in player_info:
            player_id   = player.get('lpgaId', '')
            pos         = player.get('pos', '')
            round_r1    = player.get('r1', '')
            round_r2    = player.get('r2', '')
            round_r3    = player.get('r3', '')
            round_r4    = player.get('r4', '')
            round_scores = [round_r1, round_r2, round_r3, round_r4]
            total       = player.get('total', '')
            to_par      = player.get('toPar', '')
            country     = player.get('cntry', '')
            player_name = player.get('nFull', '')
            first_name  = player.get('nFirst', '')
            last_name   = player.get('nLast', '')
            tee_time    = player.get('thru', '')
            for i in REPLACE_CHARS:
                if i in tee_time: tee_time = tee_time.replace(i, '').strip()
                else: tee_time = tee_time

            pos = get_position(pos)

            if player_id:
                participants[player_id]         = (0, player_name)
                players                         = get_golfers_info(to_par, total, pos, round_scores, tee_time)
                result[player_id]               = players
                sportssetupitem['participants'] = participants

            sportssetupitem['rich_data'].update({'game_note' : current_round})
            sportssetupitem['result']           = result

        if details['game_status']   == 'completed':
            result.setdefault('0', {}).update({'winner' : details['winner']})
            sportssetupitem['result']           = result
        else:
            result.setdefault('0', {}).update({'winner' : ''})
            sportssetupitem['result']           = result
        items.append(sportssetupitem)
        for item in items:
            import pdb;pdb.set_trace()
            yield item

    def parse_details(self, response):
        url = response.url
        print url
        sportssetupitem = response.meta['sportssetupitem']
        details = response.meta['details']
        items   = []
        hxs     = HtmlXPathSelector(response)
        year    = extract_data(hxs, '//div[@id="cphMain_phCol1_ctl01_ctl00_TournamentInfoDiv"]//p//span[contains(text(),"Date: ")]/../text()').strip(). \
                                            split(',')[-1].strip()
        if not details['tou_date']:
            tou_date = extract_data(hxs, '//div[@id="cphMain_phCol1_ctl01_ctl00_TournamentInfoDiv"]//p//span[contains(text(),"Date: ")]/../text()'). \
                                            strip().split('-')[0].strip()
        if not details['location']:
            loc = extract_data(hxs, '//div[@id="cphMain_phCol1_ctl01_ctl00_TournamentInfoDiv"]//p//span[contains(text(),"Location: ")]/../text()').strip()

        nodes = get_nodes(hxs, '//div[@id="fullResultsTable"][@class="statsTable"]//tbody//tr')
        players     = {}
        player_ids  = []
        ids_map     = {}
        result      = {}
        participants = {}

        for node in nodes:
            player_link = extract_data(node, './/td[2]//a/@href').strip()
            if not player_link.startswith('http:'):
                player_link = 'http://www.lpga.com' + player_link

            player_name = extract_data(node, './/td[2]//a/text()').strip()
            player_name = format_data(player_name)
            player_sk = get_sk(player_link, player_name)

            if player_sk:
                ids_map[player_sk] = player_name
            player_ids.append(player_sk)
            results = [i.strip() for i in extract_list_data(node, './/td/text()') if i ]
            pos, round_scores, total, to_par, prize_money = results
            round_scores = round_scores.strip().split('-')
            pl_pos = get_position(pos)
            if pl_pos == '1' and details['game_status'] == 'ongoing':
                current_round = get_cur_rnd(round_scores, details['game_status'])
            else:
                current_round = ''
            sportssetupitem['rich_data'].update({'game_note' : current_round})
            if total:
                participants[player_sk]    = ('0', player_name)
                players                    = get_golfers_info(to_par, total, pl_pos, round_scores, [])
                result[player_sk]          = players
                sportssetupitem['participants'] = participants
            else:
                players[player_sk] = {'name': player_name, 'ref_url': player_link}

        if not details['tou_date'] or not details['tour_name']:
            print "One of the tou_name, Tou-date is missed"
        else:

            player_ids = [i for i in player_ids if i]

            if details['game_status']   == 'completed':
                result.setdefault('0', {}).update({'winner' : details['winner']})
                sportssetupitem['result']           = result
            else:
                result.setdefault('0', {}).update({'winner' : ''})
                sportssetupitem['result']           = result

            items.append(sportssetupitem)
            for item in items:
                import pdb;pdb.set_trace()
                yield item


    def parse_qualifying(self, response):
        sportssetupitem = response.meta['sportssetupitem']
        details     = response.meta['details']
        items       = []
        year        = '2013'
        hxs         = HtmlXPathSelector(response)
        nodes       = get_nodes(hxs, '//table[@class="shadow"]//tr[@align="center"]')
        players     = {}
        participants = {}
        result      = {}
        player_ids  = []
        ids_map     = {}
        for node in nodes:
            pos     = extract_data(node, './td[1]/text()')
            pl_pos  = get_position(pos)
            player_name = extract_data(node, './td[3]/a/text()')
            if '(' in player_name:
                player_name = player_name.replace('(a)', '').strip()
            if not player_name:
                continue
            player_sk = player_name.lower().replace(' ', '-').strip()
            if player_sk:
                player_sk   = modify(player_sk).strip()
            thru    = extract_data(node, './td[6]/text()').strip()
            to_par  = extract_data(node, './td[7]/text()').strip()
            round_scores = []
            round_r1 = extract_data(node, './td[8]/text()').strip()
            round_scores.append(round_r1)
            round_r2 = extract_data(node, './td[9]/text()').strip()
            round_scores.append(round_r2)
            round_r3 = extract_data(node, './td[10]/text()').strip()
            round_scores.append(round_r3)
            round_r4 = extract_data(node, './td[11]/text()').strip()
            round_scores.append(round_r4)
            round_r5 = extract_data(node, './td[12]/text()').strip()
            round_scores.append(round_r5)
            total = extract_data(node, './td[13]/text()').strip()
            player_ids.append(player_sk)

            if player_sk:
                ids_map[player_sk] = player_name

            if total:
                participants[player_sk]         = ('0', player_name)
                players                         = get_golfers_info(to_par, total, pl_pos, round_scores, [])
                result[player_sk]               = players
                sportssetupitem['participants'] = participants

        if not details['tou_date'] or not details['tou_name']:
            print "One of the tou_name, Tou-date is missed"
        else:
            player_ids = [i for i in player_ids if i]

        if details['game_status']   == 'completed':
            result.setdefault('0', {}).update({'winner' : details['winner']})
            sportssetupitem['result'] = result
        else:
            result.setdefault('0', {}).update({'winner' : ''})
            sportssetupitem['result'] = result

        if participants:
            items.append(sportssetupitem)
            for item in items:
                import pdb;pdb.set_trace()
                yield item
