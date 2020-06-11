import time
import datetime
import re
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import golf_utils as gu
import copy

true = True
false = False
null = ''

REPLACE_TOU_SPL_DICT = {' at ' : ' ', ' &amp; ' : ' ', 'GC': 'Golf Club', 'CC': 'Country Club', 'TPC' : 'Tournament Players Club', \
                        'G&amp;CC' : 'Golf and Country Club', 'GCC' : 'Golf and Country Club', 'G&amp;' : 'Golf and '}

REPLACE_STATE_DICT = {'FL' : 'Florida', 'CA' : 'California', 'NV' : 'Nevada', 'AZ' : 'Arizona', 'TX' : 'Texas', 'SC' : 'South Carolina', 'LA' : 'Louisiana', \
    'NC' : 'North Carolina', 'TN' : 'Tennessee', 'CT' : 'Connecticut', 'RI' : 'Rhode Island', 'WV' : 'West Virginia', 'MD' : 'Maryland', 'IL' : 'Illinois', \
    'OH' : 'Ohio', 'KY' : 'Kentucky', 'NJ' : 'New Jersey', 'MA' : 'Massachusetts', 'CO' : 'Colorado', 'HI' : 'Hawaii', 'PUR' : 'Puerto Rico', 'GA' : 'Georgia'}

CTRY_ACC_STATE = {'Florida' : 'USA', 'Alabama' : 'USA', 'California' : 'USA', 'Nevada' : 'USA', 'Arizona' : 'USA', 'Texas' : 'USA', 'South Carolina' : 'USA', \
    'Gerogia' : 'Gerogia', 'SCO' : 'Scotland', 'Louisiana' : 'USA', 'North Carolina' : 'USA', 'Tennessee' : 'USA', 'Connecticut' : 'USA', 'Rhode Island' : 'USA', \
    'West Virginia' : 'USA', 'Maryland' : 'USA', 'Illinois' : 'USA', 'Ohio' : 'USA', 'Kentucky' : 'USA', 'New Jersey' : 'USA', 'Massachusetts' : 'USA', \
    'Colorado' : 'USA', 'CAN' : 'Canada', 'Eng' : 'England', 'Georgia' : 'USA', 'Puerto Rico' : 'USA', 'Hawaii' : 'USA', 'AUS' : 'Australia', 'Mex' : 'Mexico', \
    'CHN' : 'China', 'MAS' : 'Malaysia', 'BER' : 'Bermuda', 'Puerto Rico' : 'USA', 'HI' : 'USA', 'TX' : 'USA'}

REPLACE_TOU_DICT = {'u.s. open golf championship': 'u.s. open', 'alfred dunhill championship': 'alfred dunhill links championship', \
                    'the senior open championship pres. by rolex': 'senior open championship', 'quicken loans national' : 'at&t national', \
                    'world golf championships-cadillac championship' : 'world golf championships-ca championship'}

IGNORE_WORDS = ['presented', 'in partnership', 'pres. by', ' at ', 'partnership', 'powered by', "pres'd by", ' by ']

REPLACE_WORDS = ['waste management', 'omega', 'isps handa', 'alstom', 'aberdeen asset management', 'isps handa', 'nature valley', 'vivendi', 'm2m']

REPLACE_WORDS_by = {}


def get_stadium_name(stadium):
    for key, value in REPLACE_TOU_SPL_DICT.iteritems():
        if key in stadium:
            stadium = stadium.replace(key, value).strip()

    if '(' in stadium:
        stadium_extra = "".join(re.findall(r'\(.*\)', stadium))
        stadium = stadium.replace(stadium_extra, '').strip()
    return stadium

def get_city_state_ctry(venue):

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


class PGATour(VTVSpider):
    name    = "pga_games"
    start_urls = ['http://www.pgatour.com/tournaments/schedule.html']
    domain     = 'http://www.pgatour.com'

    def parse(self, response):

        hxs = HtmlXPathSelector(response)
        if 'standings' in self.spider_type:
            stats_url = extract_data(hxs, '//div[@class="nav-container"]/ul/li/a[contains(@href, "fedexcup")]/@href')
            if 'http' not in stats_url:
                stats_url = self.domain + stats_url
                yield Request(stats_url, callback = self.parse_standings)

        elif 'scores' in self.spider_type:
            player_link     = extract_data(hxs, '//div[@class="nav-container"]//li/a[contains(@href, "players")]/@href')
            if player_link and 'http' not in player_link:
                player_link = self.domain + player_link
            elif not player_link:
                player_link = "http://www.pgatour.com/players.html"

            yield Request(player_link, callback = self.parse_next, meta = {'count': 0})

    def parse_next(self, response):
        player_id_map   = {}
        champions_players = {}
        webcom_players  = {}
        count           = response.meta['count']
        start_url       = self.start_urls[0]
        hxs             = HtmlXPathSelector(response)
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

            if player_url and 'http' not in player_url:
                player_url = self.domain + player_url

            player_name = ' '.join(i.strip() for i in [first_name, last_name] if i).lower()

            if count == 1:
                player_id_map[player_name] = player_id
            if count == 2:
                webcom_players[player_name] = player_id
            if count == 3:
                champions_players[player_name] = player_id

        if count == 1:
            all_players_map     = copy.deepcopy(player_id_map)
            yield Request('http://www.pgatour.com/webcom/players.html', callback = self.parse_next, meta = {'all_players_map' : all_players_map, 'count' : 1, 'start_url' : start_url, 'player_id_map' : player_id_map})

        if count == 2:
            response.meta['all_players_map'].update(webcom_players)
            yield Request('http://www.pgatour.com/champions/players.html', callback = self.parse_next, meta = {'all_players_map' : response.meta['all_players_map'], 'count' : 2, 'start_url' : start_url, 'player_id_map' : response.meta['player_id_map']})

        if count == 3:
            response.meta['all_players_map'].update(champions_players)

            yield Request(start_url, callback = self.parse_start_nodes, meta = {'all_players_map' : response.meta['all_players_map'], 'player_id_map' : response.meta['player_id_map']})

    def parse_start_nodes(self, response):
        hxs             = HtmlXPathSelector(response)
        all_players_map = response.meta['all_players_map']
        player_id_map   = response.meta['player_id_map']
        nodes = get_nodes(hxs, '//table[@class="table-styled"]//tbody/tr[contains(@class, "odd") or contains(@class, "even")]')
        for node in nodes:
            st_date         = extract_data(node, './td[@class="num-date"]/span[2]/text()')
            _e_date         = extract_data(node, './td[@class="num-date"]/span[3]/text()')
            main_date       = st_date + '-' + _e_date
            str_date, e_date = gu.get_str_end_dates(main_date)

            tou_venue       = extract_data(node, './td[@class="tournament-name"]/text()')
            if "Purse" in tou_venue:
                _venue      = "".join(re.findall(r'...Purse:.*', tou_venue)) or "".join(re.findall(r'.. Purse:.*', tou_venue))
                tou_venue   = tou_venue.replace(_venue, '').strip()

            tournament_link = extract_data(node, './td[@class="tournament-name"]/a/@href')
            tou_name        = extract_data(node, './td[@class="tournament-name"]/span/text()').replace('&amp;', '&').strip()

            channels        = extract_data(node, './td[@class="network-name hidden-small"]/text()').replace(' ', ',').strip()
            def_data        = ["".join(i).strip() for i in extract_list_data(node, './td[@class="champions-name hidden-small"]/text()') if i]
            if len(def_data) == 2:
                defending_chp, prize_money = def_data
            else:
                defending_chp, prize_money = '', ''

            if tournament_link and 'http' not in tournament_link:
                tournament_link = self.domain + tournament_link


            if not tou_name or not e_date or not str_date or not tournament_link:
                continue

            stadium = tou_venue.split(',')[0].strip()

            stadium = get_stadium_name(stadium)

            loc     = {}
            venue   = ",".join(tou_venue.split(',')[1:]).strip()
            city, state, country = get_city_state_ctry(venue)
            if not country:
                country = CTRY_ACC_STATE.get(state.strip())

            if '$' in tou_venue or 'Purse' in tou_venue:
                city = state = country = stadium = ''
            loc = {'city' : city, 'country' : country, 'state' : state}

            details = {'str_date'       : str_date,     'e_date'            : e_date,
                        'loc'           : loc,          'tou_name'          : tou_name,
                        'channels'      : channels,     'def_chp'           : defending_chp,
                        'prize_money'   : prize_money,  'all_players_map'   : all_players_map,
                        'player_id_map' : player_id_map,'stadium'           : stadium}

            if "worldgolfchampionships.com" in tournament_link:
                yield Request(tournament_link, callback = self.parse_listing, meta={'details' : details})
            if "Presidents Cup" in tou_name or "Presidents Cup" in tournament_link:
                import pdb; pdb.set_trace()
                tou_image = 'http://upload.wikimedia.org/wikipedia/en/5/57/Presidents-cup-tnfeat.jpg'
                yield Request(tournament_link, callback = self.parse_presidents, meta = {'details' : details})
            if "hsbc-champions" in tournament_link:
                yield Request(tournament_link, callback = self.parse_listing, meta = {'details' : details})

            if "World Cup of Golf" in tou_name or "worldcup.pgatour.com" in tournament_link:
                tou_image = 'http://www.golfindustrycentral.com.au/images/editorimages/http:/cmaa/logo.jpg'
                details.update({'tou_image' : tou_image} )
                yield Request(tournament_link, callback = self.parse_world, meta = {'details' : details})
            if "cadillac-championship" in tournament_link:
                yield Request(tournament_link, callback = self.parse_listing, meta = {'details' : details})

            if 'www.pgatour.com/' not in tournament_link:
                continue

            yield Request(tournament_link, callback= self.parse_listing, meta={'details' : details})

    def parse_listing(self, response):
        items               = []
        sportssetupitem     = SportsSetupItem()
        hxs                 = HtmlXPathSelector(response)
        details             = response.meta['details']
        leader_board_link   = extract_data(hxs, '//ul[@class="nav nav-tabs slim nav-tabs-drop"]/li/a[contains(@href, "leaderboard")]/@href')

        tou_image = extract_data(hxs, '//div[@class="banner"]//img/@src').replace('\n', '')
        if tou_image and 'http' not in tou_image:
            tou_image = gu.get_full_url(tou_image, response.url)
        elif not tou_image:
            if detalis['tou_name'].lower() == 'bmw championship':
                tou_image = 'http://www.pgatour.com/content/dam/pgatour/tournament_images/r028/tourn_logo.gif'
            else:
                tou_image = 'http://www.pgatour.com/etc/designs/pgatour/element/img/5.0/global/nav/tour_logo.png'

        if not leader_board_link and 'www.majorschampionships.com' in response.url:
            leader_board_link = extract_data(hxs, '//a[@title="SCORING" or @title="Scoring"]/@href')
        if not leader_board_link and 'www.worldgolfchampionships.com' in response.url and "hsbc-champions" not in details['tou_name']:
            leader_board_link = extract_data(hxs, '//li[@class="static"]//a[contains(text(), "SCORING") or contains(text(), "scoring") or contains(text(), "Scoring")]')
        if leader_board_link and 'http' not in leader_board_link:
            leader_board_link = gu.get_full_url(leader_board_link, response.url)

        result_link  = extract_data(hxs, '//ul[@class="nav nav-tabs slim nav-tabs-drop"]/li/a[contains(@href, "past-results")]/@href')

        if result_link and 'http' not in result_link:
            result_link = gu.get_full_url(result_link, response.url)

        today_date  = datetime.datetime.utcnow().date()
        t_year      = extract_data(hxs, '//div[@class="info"]//span[@class="dates"]/text()').replace('\n', ' ')

        if t_year and 'UTC' in t_year:
            tou_year = t_year.split('UTC')[-1].replace(' ','').strip()
        elif t_year:
            tou_year = t_year.split(',')[-1].strip()
        elif not t_year and "worldgolfchampionships.com" in response.url:
            tou_year = '2014'
        elif not t_year:
            tou_year = gu.get_tou_year(details['tou_name'])
        else:
            tou_year = today_date.year

        if len(details['str_date'].split()) == 2 and len(details['e_date'].split()) == 2:
            e_date = details['e_date'] + ', %s' % tou_year
            e_format, s_format = '%b %d, %Y', '%b %d'
        else:
            e_date = details['e_date']
            e_format, s_format =  '%a %b %d %H:%M:%S %Z %Y', '%a %b %d %H:%M:%S %Z %Y'

        tou_date, start_date, end_date = gu.get_tou_dates(details['str_date'], e_date, s_format, e_format)
        tou_year = start_date.year

        game_datetime = gu.get_game_datetime(tou_date, tou_year)

        if start_date <= today_date and today_date <= end_date:
            status = "ongoing"
        elif start_date < today_date:
            status = "completed"
        else:
            status = "scheduled"

        if not tou_date or not details['tou_name']:
            print "Tou-date: %s --- Tou-Name: %s" % (tou_date, details['tou_name'])
        else:
            modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
            modified_tou_date = '_'.join(i.strip() for i in tou_date.strip().split('-')).replace(' ', '_').strip()
            tou_sk = modified_tou_name + '_' + modified_tou_date

            tour_name = get_refined_tou_name(details['tou_name'])

            details.update({    'tou_date'  : tou_date,     'tou_year'     : tou_year,
                                'tou_sk'    : tou_sk,       'game_status'  : status,
                                'tour_name' : tour_name,    'tou_image'    : tou_image,
                                'tou_link'  : response.url, 'end_date'     : end_date,
                                'leader_board_link' : leader_board_link, 'start_date' : start_date })

            rich_data   = {     'channels'      : details['channels'],      'location'  : details['loc'],
                                'prize_money'   : details['prize_money'],   'game_id'   : tou_sk,
                                'image_link'    : tou_image,
                                'stadium'       : details['stadium']}

            sportssetupitem['game_status']       = status
            sportssetupitem['game_datetime']     = game_datetime
            sportssetupitem['source_key']        = tou_sk
            sportssetupitem['tournament']        = tour_name
            sportssetupitem['rich_data']         = rich_data
            sportssetupitem['reference_url']     = response.url
            sportssetupitem['game']              = 'golf'
            sportssetupitem['source']            = 'pga_golf'
            sportssetupitem['participant_type']  = 'player'
            sportssetupitem['affiliation']       = 'pga'

            if status == "ongoing":
                if leader_board_link:
                    yield Request(leader_board_link, callback = self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})
            elif status == "completed":
                if result_link:
                    yield Request(result_link, callback = self.parse_results, meta={'details' : details, 'sportssetupitem' : sportssetupitem})
                elif leader_board_link:
                    yield Request(leader_board_link, callback= self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})
            else:
                sportssetupitem['reference_url']    = response.url
                sportssetupitem['participants']     = ''
                sportssetupitem['result']           = ''
                items.append(sportssetupitem)
                for item in items:
                    import pdb; pdb.set_trace()
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
            players, player_name_map, missing_players = {}, {}, {}
            ids_map, player_ids = {}, []
            is_round = 0
            for i in p_info:
                position    = i.get('current_position', '')
                pl_pos      = gu.get_position(position)
                country     = i.get('player_bio', '').get('country', '')
                thru        = i.get('thru', '')
                to_par      = i.get('total', '') or i.get('today', '')
                results     = i.get('rounds', '')
                total_strokes = i.get('total_strokes', '')

                rounds_scores = []
                for result in results:
                    rounds_scores.append((result['round_number'], result['strokes']))

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
                    players                         = gu.get_golfers_info(to_par, total_strokes, pl_pos, scores, [])
                    result[player_id]               = players
                    sportssetupitem['participants'] = participants

                    ids_map[player_id] = player_name
                    player_ids.append(player_id)

            if details['game_status'] == 'completed':
                winner = player_name_map.get(details['def_chp'], '')
                if not winner:
                    winner = ''
                    player_set = [' '.join(i) for i in gu.permutations(details['def_chp'].strip().split())]
                    for i in player_set:
                        if player_name_map.has_key(i):
                            winner = player_name_map[i]

                winner_name = details['def_chp'].strip()
            else:
                winner, winner_name = '', ''

            sportssetupitem['rich_data'].update({'game_note' : current_round})
            result.setdefault('0', {}).update({'winner' : winner,})
            sportssetupitem['result']           = result

            if participants:
                items.append(sportssetupitem)
                for item in items:
                    import pdb; pdb.set_trace()
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
                pl_pos  = gu.get_position(position)
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

                #player_id = '-'.join(i.lower().strip() for i in player_name.split())
                player_id = details['player_id_map'].get(player_name, '')
                if not player_id:
                    player_set = [' '.join(i) for i in gu.permutations(player_name.lower().strip().split())]
                    for i in player_set:
                        if details['player_id_map'].has_key(i):
                            player_id = details['player_id_map'][i]

                if not player_id:
                    player_set = [' '.join(i) for i in gu.permutations(player_name.lower().strip().split())]
                    for i in player_set:
                        if details['all_players_map'].has_key(i):
                            player_id = details['all_players_map'][i]

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
                    players                         = gu.get_golfers_info(to_par, total_score, pl_pos, rounds, [])
                    result[player_id]               = players
                    sportssetupitem['participants'] = participants

            if details['game_status'] == 'completed':
                winner = player_name_map.get(details['def_chp'], '')
                if not winner:
                    winner = ''
                    player_set = [' '.join(i) for i in gu.permutations(details['def_chp'].strip().split())]
                    for i in player_set:
                        if player_name_map.has_key(i):
                            winner = player_name_map[i]

                if not winner:
                    player_set = [' '.join(i) for i in gu.permutations(details['def_chp'].strip().split())]
                    for i in player_set:
                        if missing_player_name_map.has_key(i):
                            winner = missing_player_name_map[i]
                            players[winner] = missing_players[winner]

                winner_name = details['def_chp'].strip()
            else:
                winner, winner_name = '', ''

            result.setdefault('0', {}).update({'winner' : winner,})
            sportssetupitem['result']           = result

            if participants:
                player_ids = [i for i in player_ids if i]

                items.append(sportssetupitem)
                for item in items:
                    import pdb; pdb.set_trace()
                    yield item
        else:
            yield Request(details['leader_board_link'], callback = self.parse_details, meta={'details' : details, 'sportssetupitem' : sportssetupitem})

    def parse_world(self, response):
        hxs = HtmlXPathSelector(response)
        details = response.meta['details']
        today_date = datetime.datetime.utcnow().date()
        tou_year = '2013'

        if len(details['str_date'].split()) == 2 and len(details['e_date'].split()) == 2:
            e_date = details['e_date'] + ', %s' % tou_year
            e_format, s_format = '%b %d, %Y', '%b %d'
        else:
            e_date = details['e_date']
            e_format, s_format =  '%a %b %d %H:%M:%S %Z %Y', '%a %b %d %H:%M:%S %Z %Y'

        tou_date, start_date, end_date = gu.get_tou_dates(details['str_date'], e_date, s_format, e_format)

        if start_date <= today_date and today_date <= end_date:
            status = "ongoing"
        elif start_date < today_date:
            status = "completed"
        else:
            status = "scheduled"

        leader_board_link = "http://worldcup.pgatour.com/" + extract_data(hxs, '//ul[@id="header_nav"]//a[contains(@href, "leaderboard")]/@href')

        lb_link = "http://worldcup.pgatour.com/LB.aspx"

        tour_name = get_refined_tou_name(details['tou_name'])
        game_datetime = gu.get_game_datetime(tou_date, tou_year)

        details.update({'status' : status, 'leader_board_link' : leader_board_link, 'game_datetime' : game_datetime, 'start_date' : start_date, 'end_date' : end_date, 'tou_date' : tou_date, 'tour_name' : tour_name})

        yield Request(lb_link, callback = self.parse_world_next, meta = {'details' : details})

    def parse_world_next(self, response):
        sportssetupitem = SportsSetupItem()
        items           = []
        details_doc     = HtmlXPathSelector(response)
        details         = response.meta['details']
        players, player_name_map, missing_players, participants = {}, {}, {}, {}
        missing_player_name_map, result = {}, {}
        ids_map, player_ids = {}, []
        nodes = get_nodes(details_doc, '//table[@id="TeamStands"]//tr')
        for node in nodes:
            position    = extract_data(node, './/td[1]/text()')
            pl_pos      = gu.get_position(position)
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

            player_id = details['player_id_map'].get(player_name, '')
            if not player_id:
                player_set = [' '.join(i) for i in gu.permutations(player_name.lower().strip().split())]
                for i in player_set:
                    if details['player_id_map'].has_key(i):
                        player_id = details['player_id_map'][i]

            if not player_id:
                player_set = [' '.join(i) for i in gu.permutations(player_name.lower().strip().split())]
                for i in player_set:
                    if details['all_players_map'].has_key(i):
                        player_id = details['all_players_map'][i]

            if player_id:
                player_name_map[player_name]    = player_id
                ids_map[player_id]              = player_name
                player_ids.append(player_id)
                participants[player_id]         = (0, player_name)
                players                         = gu.get_golfers_info(to_par, total_score, pl_pos, rounds, [])
                result[player_id]               = players
                sportssetupitem['participants'] = participants

        if details['status'] == 'completed':
            winner = player_name_map.get(details['def_chp'], '')
            if not winner:
                winner = ''
                player_set = [' '.join(i) for i in gu.permutations(details['def_chp'].strip().split())]
                for i in player_set:
                    if player_name_map.has_key(i):
                        winner = player_name_map[i]

            if not winner:
                player_set = [' '.join(i) for i in gu.permutations(details['def_chp'].strip().split())]
                for i in player_set:
                    if missing_player_name_map.has_key(i):
                        winner = missing_player_name_map[i]
                        players[winner] = missing_players[winner]

            winner_name = details['def_chp'].strip()
        else:
            winner, winner_name = '', ''

        result.setdefault('0', {}).update({'winner' : winner})

        modified_tou_name = '_'.join(i.strip() for i in details['tou_name'].strip().split('-')).replace(' ', '_').strip()
        modified_tou_date = '_'.join(i.strip() for i in details['tou_date'].strip().split('-')).replace(' ', '_').strip()
        tou_sk = modified_tou_name + '_' + modified_tou_date

        tou_image = "http://www.golfindustrycentral.com.au/images/editorimages/http:/cmaa/logo.jpg"

        rich_data   = {'loc' : details['loc'], 'channels' : details['channels'], 'prize_money' : details['prize_money'], 'image_link' : tou_image}

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

            items.append(sportssetupitem)
            for item in items:
                import pdb; pdb.set_trace()
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

    def parse_presidents_next (self, response):
        hxs = HtmlXPathSelector(response)
        details = response.meta['details']
        sportssetupitem = SportsSetupItem()
        items = []
        result          = {}
        participants    = {}
        players         = {}
        raw_data        = response.body
        data = eval(raw_data)
        teams = {'United States' : 'usa', 'International' : 'International_Team'}
        if data:
            t_info = data.get("tournament", {})
            r_info = t_info.get("rounds", [])

            for round in r_info[:1]:
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
            rich_data   = {'loc' : details['loc'], 'channels' : details['channels'], 'prize_money' : details['prize_money'], 'image_link' :details['tou_image']}

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

            items.append(sportssetupitem)
            for item in items:
                yield item

    def parse_standings(self, response):
        hxs = HtmlXPathSelector(response)
        standings_url = extract_data(hxs, '//div[@class="fedexcup-top5"]/div[@class="visible-large"]/a/@href')
        yield Request(standings_url, callback = self.parse_standings_details, meta={'stat_url' : response.url})

    def parse_standings_details(self, response):
        sportssetupitem = SportsSetupItem()
        player_ids  = []
        items       = []
        players     = {}
        ids_map     = {}
        details_doc = HtmlXPathSelector(response)
        tou_name    = extract_data(details_doc, '//div[@class="header"]/p/strong/text()')
        _name       = tou_name.split(',')[0]
        year        = tou_name.split(',')[-1].strip()
        nodes       = get_nodes(details_doc, '//div[@class="details-table-wrap"]//tbody/tr')
        for node in nodes:
            rank_this_week  = extract_data(node, './td[1]/text()')
            player_name     = extract_data(node, './td[@class="player-name"]/a/text()')
            player_url      = extract_data(node, './td[@class="player-name"]/a/@href')
            player_id       = "".join(re.findall(r'\d+', player_url))
            events          = extract_data(node, './td[4]/text()')
            points          = extract_data(node, './td[5]/text()')
            wins            = extract_data(node, './td[6]/text()')
            points_behind   = extract_data(node, './td[8]/text()')

            if player_id:
                players[player_id] = {'rank' : rank_this_week, 'points' : points, 'wins' : wins, 'points_behind_lead' : points_behind, 'events' : events}
                sportssetupitem['result'] = players
                ids_map[player_id] = player_name
                player_ids.append(player_id)

            sportssetupitem['participant_type']     = 'player'
            sportssetupitem['result_type']          = 'tournament_standings'
            sportssetupitem['source']               = 'pga_golf'
            sportssetupitem['season']               = year
            sportssetupitem['tournament']           = _name
            items.append(sportssetupitem)
            for item in items:
                yield item
