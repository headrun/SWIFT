import datetime
import time
import re
from vtvspider_new import VTVSpider, get_nodes, extract_data, get_utc_time
from vtvspider_new import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

REPLACE_TOU_SPL_DICT = {'at ' : ' ', ' &amp; ' : ' ', 'GC': 'Golf Club',
                        'CC': 'Country Club', 'TPC': 'Tournament Players Club',
                        'G&amp;CC': 'Golf and Country Club',
                        'GCC': 'Golf and Country Club', 'G&amp;': 'Golf and '}

REPLACE_STATE_DICT = {'FL': 'Florida', 'CA': 'California',
                      'Calif.': 'California', 'MI': 'Michigan',
                      'OH': 'Ohio', 'NY': 'New York', 'AL': 'Alabama',
                      'NC': 'North Carolina', 'NJ': 'New Jersey',
                      'VA': 'Virginia', 'AR': 'Arkansas', 'Ark.': 'Arkansas',
                      'MD': 'Maryland', 'Arizona': 'Arizona',
                      'Alabama': 'Alabama', 'Texas': 'Texas',
                      'Prattville': 'Prattville'}

CTRY_ACC_STATE = {'Florida': 'USA', 'Alabama': 'USA',
                  'New York': 'USA', 'Maryland': 'USA',
                  'Michigan' : 'USA', 'Ohio' : 'USA', 'Arkansas' : 'USA',
                  'North Carolina': 'USA', 'New Jersey': 'USA',
                  'Virginia' : 'USA', 'Texas' : 'USA', 'California': 'USA',
                  'Arizona' : 'USA', 'Prattville' : 'USA'}

REPLACE_STD_NAME_DICT = {'Pinehurst #2': 'Pinehurst No. 2',
                         'Pinehurst Resort' : 'Pinehurst No. 2'}

REPLACE_TOU_DICT = {'the evian championship': 'The Evian Championship',
                    'china - tbd': 'Reignwood LPGA Classic',
                    'lpga volvik championship': 'LPGA Volvik Championship',
                    'volunteers of america texas shootout' : 'Volunteers of America North Texas Shootout',
                    'lpga keb hanabank': 'lpga hana bank',
                    'lotte championship' : 'LPGA Lotte Championship',
                    'pure silk': 'pure silk bahamas lpga classic',
                    'jtbc founder cup': 'lpga founders cup' ,
                    'yokohama tire lpga classic': 'Yokohama Tire LPGA Classic',
                    'meijer lpga classic': 'Meijer LPGA Classic',
                    'shoprite lpga classic' : 'ShopRite LPGA Classic',
                    "kpmg women's pga championship" : "Women's PGA Championship",
                    'lpga taiwan championship': \
                    'LPGA Taiwan Championship',
                    "japan classic": "Mizuno Classic",
                    'ul international crown': 'International Crown',
                    'manulife lpga classic': 'Manulife LPGA Classic',
                    'fubon lpga taiwan championship': 'Fubon LPGA Taiwan Championship',
                    "hanabank championship": "LPGA Hana Bank Championship",
                    "reignwood": "Reignwood LPGA Classic",
                    'swinging skirts lpga classic': 'Swinging Skirts LPGA Classic',
                    "canadian pacific women's open": "Canadian Women's Open",
                    'toto japan classic': 'Toto Japan Classic',
                    "portland classic": "Portland Classic",
                    "u.s. women's open": "U.S. Women's Open Golf Championship",
                    "women's pga championship": "LPGA Championship",
                    "north texas shootout": "North Texas LPGA Shootout",
                    "women's australian open": "Women's Australian Open"}

IGNORE_WORDS = ['presented', 'in partnership', 'pres. by', \
                ' at ', 'partnership', \
                'powered by', "pres'd by", ' by ', 'conducted', \
                'presented by', 'hershey', 'kraft', 'acer', 'ctbc']

REPLACE_WORDS = ['ricoh']
REPLACE_WORDS_BY = {'cn canadian': 'canadian', 'the 2013 solheim': 'solheim',
                    'lpga keb hanabank': 'lpga hana bank'}



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

def get_refined_tou_name(tou_name):
    tou_name = tou_name.lower()

    for k, v in REPLACE_WORDS_BY.iteritems():
        if k in tou_name:
            tou_name = tou_name.replace(k, v).strip()

    for i in IGNORE_WORDS:
        if i in tou_name:
            tou_name = tou_name.split(i)[0].strip()

    for i in REPLACE_WORDS:
        tou_name = tou_name.replace(i, '').strip()
    for key, value in REPLACE_TOU_DICT.iteritems():
        if key in tou_name:
            tou_name = value

    return tou_name

def get_refined_std_name(stadium):
    for key, value in REPLACE_TOU_SPL_DICT.iteritems():
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

def get_game_location(venue):
    city = state = country = ''
    venue = venue.split(',')
    if len(venue) == 2:
        city  = venue[0].strip()
        state = REPLACE_STATE_DICT.get(venue[-1].strip())
        if not state:
            country = venue[-1].strip()
            state = ''
    elif len(venue) == 3:
        city = venue[0].strip()
        state = venue[1].strip() 
        country = venue[-1].strip()
    elif len(venue) == 1:
        city = venue[0].strip()
    elif '$' in (city or state or country):
        city = state = country = ''
    if country.lower() == 'pa' or state.lower() == 'pa':
        state = 'Pennsylvania'
        country = 'usa'

    return city, state, country

def format_data(data):
    return ' '.join(i.strip() for i in data.split() if i)

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position

    return pos

def get_golfers_info(to_par, total_score, pl_pos, rounds_scores, tee_time):
    players = {}
    players = {'TO PAR' : to_par, 'final' : total_score, 'position' : pl_pos}
    for i in xrange(len(rounds_scores)):
        rt = "R%s" % str(i+1)
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

class LPGAGames(VTVSpider):
    name = 'lpga_games'
    start_urls = ['http://www.lpga.com/tournaments']
    domain_url = "http://www.lpga.com"

    def parse(self, response):
        sel = Selector(response)
        domain_url = "http://www.lpga.com"
        year = '2016'
        upcoming_nodes = sel.xpath('//div[@class="tournament-schedule"]/div[@class="table-wrapper"][header[h2[contains(text(),"Upcoming")]]]/div[@class="table-scroll"]/table/tbody/tr[not(contains(@class,"network"))]')

        for node in upcoming_nodes:

            tou_month = extract_data(node, './/div[@class="month"]/text()')
            tou_days = extract_data(node, './/div[@class="day"]/text()')
            tou_link = extract_data(node, './/a[@data-omniture-channelname="tournaments"]/@href')
            fin_lin = domain_url + tou_link
            yield Request(fin_lin, callback = self.parse_tournaments, meta = {'tou_month': tou_month, 'tou_days': tou_days, 'year': year})

        completed_links = sel.xpath('//div[@class="tournament-schedule"]/div[@class="table-wrapper"][header[h2[div[contains(text(),"Completed")]]]]/div[@class="table-scroll"]/table/tbody/tr[not(contains(@class,"network"))]//a[@data-omniture-channelname="tournaments"]/@href').extract()
        for link in completed_links:
            fi_lin = domain_url + link
            #yield Request(fi_lin, callback = self.parse_tournaments, meta = {'year': year})

    def parse_tournaments(self, response):
        sel = Selector(response)
        year = response.meta['year']
        sportssetupitem = SportsSetupItem()
        today_date = datetime.datetime.utcnow().date()
        tou_name = extract_data(sel, '//div[@class="tournament-banner-title"]/text()')
        tou_date = extract_data(sel, '//div[@class="tournament-banner-date"]/text()')
        venue = extract_data(sel, '//div[@class="tournament-banner-location"]/text()')
        channels = extract_data(sel, '//div[@class="tournament-tvtimes-network single"]//@src').split('/')[-1].split('.png')[0]
        if tou_date:
            s_date, e_date = get_str_end_dates(tou_date)
            e_date += ', %s' % year
            tou_date, start_date, end_date = get_tou_dates(s_date, e_date, '%b %d', '%b %d, %Y')

        if start_date <= today_date and today_date <= end_date:
            game_status = "ongoing"
        elif start_date < today_date:
            game_status = "completed"
        else:
            game_status = "scheduled"
            sportssetupitem['participants'] = {}
            sportssetupitem['result'] = {}

        tour_name = get_refined_tou_name(tou_name)
        if 'Walmart NW Arkansas Championship' in tour_name:
            tour_name = 'Walmart NW Arkansas Championship'
        if 'Marathon Classic' in tour_name:
            tour_name = 'Marathon Classic'
        if 'U.S. Women' in tour_name:
            tour_name = "United States Women\'s Open Championship"
        if 'winging' in tour_name:
            tour_name = 'Swinging Skirts LPGA Classic'
        if 'anulife' in tour_name:
            tour_name = 'Manulife LPGA Classic'
        game_datetime   = get_game_datetime(tou_date, year)
        result, participants = self.get_results(sel, game_status, response.url)

        stadium = extract_data(sel, '//div[@class="heading-main"]/text()')
        stadium = get_refined_std_name(stadium)
        details = {'tour_name': tour_name, 'year': year, 'tou_date': tou_date}
        loc, tou_sk, tz_info = self.parse_std_details(venue, details, game_datetime)
        rich_data = {'location': loc, 'stadium': stadium, 'channels': str(channels)}

        sportssetupitem['tournament']       = tour_name
        sportssetupitem['game_datetime']    = game_datetime
        sportssetupitem['game_status']      = game_status
        sportssetupitem['affiliation']      = 'lpga'
        sportssetupitem['source']           = 'lpga_golf'
        sportssetupitem['game']             = 'golf'
        sportssetupitem['participant_type'] = 'player'
        sportssetupitem['time_unknown']      = 1
        sportssetupitem['season'] = year
        sportssetupitem['reference_url'] = response.url
        sportssetupitem['rich_data']    = rich_data
        sportssetupitem['source_key']   = tou_sk
        sportssetupitem['tz_info']      = tz_info
        yield sportssetupitem

    def parse_std_details(self, venue, details, game_datetime):
        loc = {}
        _city, state, country = get_game_location(venue)
        if not country:
            country = CTRY_ACC_STATE.get(state) or ''

        loc = {'city' : _city, 'country' : country, 'state' : state}

        modified_tou_name = '_'.join(i.strip() for i in details['tour_name'].strip().split('-')).replace(' ', '_').strip()
        modified_tou_date = '_'.join(i.strip() for i in details['tou_date'].strip().split('-')).replace(' ', '_').strip()
        tou_sk = modified_tou_name + "_" + details['year'] + '_' + modified_tou_date

        tz_info = get_tzinfo(city = _city, game_datetime= game_datetime)
        if not tz_info:
            tz_info = get_tzinfo(country = country, game_datetime= game_datetime)

        return loc, tou_sk, tz_info

    def get_results(self, res, game_status, res_link):
        players     = {}
        player_ids  = []
        ids_map     = {}
        participants = {}
        result      = {}
        root_nodes = get_nodes(res, '//table[@class="table leaderboard"]/tbody[@class="full-leaderboard"]')
        for root_node in root_nodes:
            nodes = get_nodes(root_node, './/tr[@class="body player-score-row"]') or get_nodes(root_node, './/tr[@class="body"]')

            for node in nodes:
                player_name = extract_data(node, './/strong[@class="player-name"]/text()').strip()
                player_name = format_data(player_name)
                player_sk   = extract_data(node, './@data-player-id').strip()

                if player_sk:
                    ids_map[player_sk] = player_name
                player_ids.append(player_sk)
                position = extract_data(node, './td[@class="table-content l-p-position"]/text()') or \
                            extract_data(node, './td[@class="table-content"][1]/text()')
                scores = extract_data(node, './td[@class="table-content scores"]/text()')
                if scores:
                    round_scores = scores.split(' - ')
                else:
                    round_1 = extract_data(node, './/td[@class="table-content l-p-round-1-score"]/text()')
                    round_2 = extract_data(node, './/td[@class="table-content l-p-round-2-score"]/text()')
                    round_3 = extract_data(node, './/td[@class="table-content l-p-round-3-score"]/text()')
                    round_4 = extract_data(node, './/td[@class="table-content l-p-round-4-score"]/text()')
                    round_scores = [round_1, round_2, round_3, round_4]
                to_par = extract_data(node, './/td[@class="table-content l-p-to-par"]/text()') or \
                        extract_data(node, './/td[@class="table-content"][3]/text()')
                total = extract_data(node, './/td[@class="table-content l-p-total-score"]/text()') or \
                        extract_data(node, './/td[@class="table-content"][2]/text()')
                pl_pos = get_position(position)
                if pl_pos == "DQ":
                    continue

                if total:
                    participants[player_sk]    = ('0', player_name)
                    players                    = get_golfers_info(to_par, total, pl_pos, round_scores, [])
                    result[player_sk]          = players

                if (game_status == 'completed') and (pl_pos == "1") and "round4" in res_link:
                    result.setdefault('0', {}).update({'winner' : player_sk})
        return result, participants
