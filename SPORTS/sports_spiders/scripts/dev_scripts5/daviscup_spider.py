from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
        get_nodes, extract_list_data, get_utc_time, get_tzinfo
import time
import datetime
import re

EVENT_DICT = {'group-i/americas': 'Davis Cup Americas Zone Group 1', \
              'group-i/asia-oceania': 'Davis Cup Asia/Oceania Zone Group 1', \
              'group-i/europe-africa': 'Davis Cup Europe/Africa Zone Group 1', \
              'group-ii/americas': 'Davis Cup Americas Zone Group 2', \
              'group-ii/asia-oceania': 'Davis Cup Asia/Oceania Zone Group 2', \
              'group-ii/europe-africa': 'Davis Cup Europe/Africa Zone Group 2', \
              'group-iii/americas': 'Davis Cup Americas Zone Group 3', \
              'group-iii/asia-oceania': 'Davis Cup Asia/Oceania Zone Group 3', \
              'group-iii/europe': 'Davis Cup Europe Zone Group 3', \
              'group-iii/africa': 'Davis Cup Africa Zone Group 3', \
              'group-iv/asia-oceania': 'Davis Cup Asia/Oceania Zone Group 4'
              }

RECORD  = SportsSetupItem()
def get_tou_dates(start_date, end_date, start_date_format, end_date_format):
    mid_date = ''
    e_date    = (datetime.datetime(*time.strptime(end_date.strip(), end_date_format)[0:6])).date()
    str_date   = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    if str_date.month == e_date.month:
        _date = e_date.day - 1
        end_month = "".join(end_date.split(' ')[1]).strip()
        mid_date = str(_date) + ' ' + end_month + ' ' + str(e_date.year)
    elif str_date.month in [1, 3, 5, 7, 8, 10, 12]:
        _date = e_date.day - 1
        end_month = "".join(end_date.split(' ')[1]).strip()
        mid_date = str(_date) + ' ' + end_month + ' ' + str(e_date.year)
    else:
        _date = str_date.day + 1
        start_month = "".join(start_date.split(' ')[1]).strip()
        mid_date = str(_date) + ' ' + start_month + ' ' + str(str_date.year)
    e_date = datetime.datetime(*time.strptime(end_date.strip(), end_date_format)[0:6])
    str_date = datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])
    return mid_date, start_date, end_date



def get_std_loc(venue):
    loc             = {}
    if len(venue.split(',')) == 6:
        _details    = venue.split(',')[1:-2]
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[-1].strip()
        state       = ''
    elif len(venue.split(',')) ==5 :
        _details    = venue.split(',')[1:-2]
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[-1].strip()
        state       = ''
    elif len(venue.split(' ')) == 1:
        country     = venue
        city = state = stadium = ''
    elif len(venue.split(',')) == 3:
        country = venue.split(',')[-1].strip()
        stadium = venue.split(',')[0].strip()
        city    = venue.split(',')[1].strip()
        state   = ''
    else:
        stadium = city = country = state = ''

    loc = {'city' : city, 'state' : state, \
                 'country' : country, 'stadium' : stadium}

    return loc

class DavisCupSpider(VTVSpider):
    name    = "tennis_davis"
    allowed_domains = ['daviscup.com']
    start_urls = ['http://www.daviscup.com/en/draws-results/davis-cup-structure/diagram.aspx']


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="strWrap"]//a')

        for node in nodes:
            team_link       = extract_data(node, './@href')

            if 'http' not in team_link:
                team_link   = "http://www.daviscup.com" + team_link

            if '.aspx' in team_link:
                yield Request(team_link, callback = self.parse_games, \
                meta = {})

    def parse_games(self, response):
        hxs             = Selector(response)
        game = {}
        grp_num_year    = extract_data(hxs, '//div[@id="titleBar"]/h1/text()')
        year = "".join(re.findall('\d+', grp_num_year))
        nodes     = get_nodes(hxs, '//ul[@id="event"]/li[@id="liEventRound"]//ul[@id="roundTie"]//li[@class="tie clfx"]')
        for node in nodes:
            event_name = extract_data(node, '././/../li//../../ul/preceding-sibling::div[@id="divEventRoundDesc"]/h2[@id="h2Highlight"]/text()').strip()
            if 'World Group' in grp_num_year:
                game_date_ = extract_data(node, '././/../li//../../ul/preceding-sibling::div[@id="divEventRoundDesc"]/p[@id="pHighlight"]/text()')
                if "/" not in game_date_:

                    game_date = game_date_.split(' - ')[0]+" "+game_date_.split(' - ')[-1].split(' ')[-1]
                    _date = datetime.datetime.strptime(game_date, "%d %b %Y")
                    game_datetime = _date.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    game_date = game_date_.split(' / ')[0]
                    game_date = game_date.split('-')[0]+" " + game_date.split('-')[-1].split( )[-1] + " " + year
                game_end = game_date_.split(' - ')[-1].strip()
                game_end = datetime.datetime.strptime(game_end, "%d %b %Y")
                game_end = game_end.strftime("%Y-%m-%d")
                _date = datetime.datetime.strptime(game_date, "%d %b %Y")
                _date_format = ("%d %b %Y")
                game_datetime  = get_utc_time(game_date, _date_format, 'US/Eastern')
                _game_sk = _date.strftime("%Y-%m-%d")
                now = datetime.datetime.now()
                now = now.strftime("%Y-%m-%d")
                if _game_sk <= now:
                    status = "cancelled"
                else:
                    status = "scheduled"
                game['game_status'] = status
                game['game_datetime'] = game_datetime
                if event_name in ["First Round", "1st Round"]:
                    event_name = "Davis cup First Round"
                elif event_name in ["Second Round", "2nd Round"]:
                    event_name = "Davis cup Second Round"
                elif event_name in ["3rd Round", "Third Round"]:
                    event_name = "Davis cup Third Round"
                elif event_name in ["1st Round Play-off", "2nd Round Play-off", \
                "Play-offs", "Play-off Round"]:
                    event_name = "Davis cup Playoffs"
                elif event_name in ["Quarterfinals"]:
                    event_name = "Davis cup Quarterfinals"
                elif event_name in ["Semifinals"]:
                    event_name = "Davis cup Semifinals"
                elif event_name in ["Final"]:
                    event_name = "Davis cup Finals"
                game['event_name'] = event_name
                game_note  = ''
            else:
                group_name = response.url.replace('http://www.daviscup.com/en/draws-results/', '') \
                .replace('/2015', '').replace('.aspx', '').strip()
                game_note = event_name
                event_name = EVENT_DICT[group_name]
                game['event_name'] = event_name
                game_datetime = ''
                game_end = ''

            game['tou_name'] = "Davis cup"
            game['source'] = "daviscup_tennis"
            team_nodes = get_nodes(node, './/div[@class="panel-r"]//table[@class="dsTable"]//tr/td[@class="team"]/span/a')
            _team = extract_list_data(team_nodes, './@href')
            game_st = extract_data(node, './/h3/span//a/text()')
            if game_st in ['Results', 'Preview']:
                if len(_team) == 2:
                    hm_tm = _team[0].rsplit('id=')[-1]
                    aw_tm = _team[-1].rsplit('id=')[-1]
                    game['home_team_id'] = hm_tm
                    game['away_team_id'] = aw_tm
                    game_link   = extract_data(node, './/h3/span//a/@href')
                    game_id     = game_link.split('Id=')[-1]
                    game['game_sk'] = game_id
                    game_link = "http://www.daviscup.com" + game_link
                    yield Request(game_link, callback= self.parse_games_details, \
                    meta={'game_id' : game_id, \
                    'game': game, 'game_datetime': game_datetime, 'hm_tm': hm_tm, \
                    'aw_tm': aw_tm, 'event_name': event_name, 'game_end': game_end, \
                    'game_note': game_note})

        group_nodes = get_nodes(hxs, '//div[@id="divPOEvent"]//div[@class="innerBox"]')
        for group_node in group_nodes:
            game_link = get_nodes(group_node, './/ul//li')
            game_note = extract_data(group_node, './/preceding-sibling::h2//text()')
            for game_links in game_link:
                game_link = extract_data(game_links, './/a//@href')
                group_name = response.url.replace('http://www.daviscup.com/en/draws-results/', '') \
                    .replace('/2015', '').replace('.aspx', '').strip()
                event_name = EVENT_DICT[group_name]
                game['event_name'] = event_name
                game_datetime = ''
                game_end = ''
                game_id     = game_link.split('Id=')[-1]
                game['game_sk'] = game_id
                game_link = "http://www.daviscup.com" + game_link

                yield Request(game_link, callback= self.parse_games_details, \
                    meta={'game_id' : game_id, \
                    'game_datetime': game_datetime, 'event_name': event_name, \
                    'game_end': game_end, 'game_note': game_note})

    def parse_games_details(self, response):
        hxs         = Selector(response)
        source = "daviscup_tennis"
        game_datetime = response.meta['game_datetime']
        event_name = response.meta['event_name']
        game_end = response.meta['game_end']
        game_note = response.meta['game_note']
        if not game_datetime:
            game_date_ = extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Date:")]/strong//text()')
            if not game_date_:
                game_date_ = extract_data(hxs, '//div[@class="groupVenue"]//p//text()').split(',')[0].strip()
            if "-" in game_date_:
                game_date = game_date_.split(' - ')[0]+" "+game_date_.split(' - ')[-1].split(' ')[-1]
            else:
                game_date = game_date_
            _date = datetime.datetime.strptime(game_date, "%d %b %Y")
            game_datetime = _date.strftime("%Y-%m-%d %H:%M:%S")
            if "-" in game_date_:
                game_end = game_date_.split(' - ')[-1].strip()
                game_end = datetime.datetime.strptime(game_end, "%d %b %Y")
                game_end = game_end.strftime("%Y-%m-%d")
            else:
                game_end = ''
            _date_format = ("%d %b %Y")
            game_datetime  = get_utc_time(game_date, _date_format, 'US/Eastern')


        tou_name = "Davis Cup"
        game_id     = response.url.split('Id=')[-1]
        nodes       = get_nodes(hxs, '//div[@id="webletHome"]')
        now = datetime.datetime.now()
        now = now.strftime("%Y-%m-%d")
        start_date = game_datetime.split(' ')[0].strip()
        if start_date <= now and game_end >= now:
            status = "ongoing"
        elif start_date < now:
            status = "completed"
        else:
            status = "scheduled"
        for node in nodes:
            home_team_link  = extract_data(node, './/div[@class="header"]/div[@class="lft"]//a/@href')
            home_team_id    = home_team_link.split('id=')[-1].strip()

            if 'http' not in home_team_link:
                home_team_link = 'http://www.daviscup.com' + home_team_link

            away_team_link  = extract_data(node, './/div[@class="header"]/div[@class="rht"]//a/@href')
            away_team_id    = away_team_link.split('id=')[-1].strip()

            if 'http' not in away_team_link:
                away_team_link = 'http://www.daviscup.com' + away_team_link


            total_score         = extract_data(node, './/div[@class="vs"]//text()')
            home_team_score, away_team_score = '', ''

            if '0 : 0' not in total_score and 'V' not in total_score:
                venue_details   = extract_data(node, './/div[@class="groupVenue"]/p/text()')
                if not venue_details:
                    venue_details = extract_data(node, './/ul[@class="tiedetails clfx"]//li[contains(text(), "Venue")]/strong//text()')
                loc             = get_std_loc(venue_details)
                home_team_score = total_score.split(':')[0].strip()
                away_team_score = total_score.split(':')[-1].strip()
                final_score     = home_team_score+" - "+away_team_score
                if int(home_team_score)>int(away_team_score):
                    home_win = 1
                    away_win = 0
                elif int(home_team_score)<int(away_team_score):
                    home_win = 0
                    away_win = 1
                elif int(home_team_score)==int(away_team_score):
                    home_win = 1
                    away_win = 1
                if home_win and away_win:
                    team_winner = ''
                elif home_win:
                    team_winner = home_team_id
                elif away_win:
                    team_winner = away_team_id

                game_status     = status
                RECORD['game'] = "tennis"
                RECORD['source'] = source
                RECORD['tournament'] = tou_name
                RECORD['source_key'] = game_id
                RECORD['affiliation'] = "atp"
                RECORD['game_status'] = game_status
                RECORD['game_datetime'] = game_datetime
                RECORD['event'] = event_name
                RECORD['reference_url'] = response.url
                RECORD['rich_data'] = {'location': loc, 'game_note': game_note}
                RECORD['time_unknown'] = 1
                RECORD['tz_info'] = get_tzinfo(city = loc['city'], \
                game_datetime = game_datetime)
                if not RECORD['tz_info']:
                    RECORD['tz_info'] = get_tzinfo(country = loc['country'], \
                                        game_datetime = game_datetime)
                RECORD['participants'] = { home_team_id: (0, ''), away_team_id: (0, '')}
                RECORD['result'] = {home_team_id: { 'final': home_team_score}, \
                        away_team_id: { 'final': away_team_score}}
                RECORD['result'].setdefault('0', {})['score'] = final_score
                if game_status == "completed":
                    RECORD['result'].setdefault('0', {})['winner'] = team_winner
                yield RECORD

            elif 'V' in total_score:
                game_status     = 'scheduled'
            elif '0 : 0' in total_score:
                game_status     = 'scheduled'
            if '0 : 0' not in total_score and 'V' not in total_score:
                game_status     = 'completed'
                venue_details   = extract_data(node, './/div[@class="groupVenue"]/p/text()')
                loc             = get_std_loc(venue_details)
                if not venue_details:
                    venue_details   = extract_data(node, './/ul[@class="tiedetails clfx"]//li[contains(text(), "Date")]/strong//text()')
                    venue_details_ = extract_data(node, './/ul[@class="tiedetails clfx"]//li[contains(text(), "Venue")]/strong//text()')
                    tou_year = venue_details.split('-')[1].split(' ')[-1]
                    str_date = venue_details.split('-')[0].strip()
                    str_date = str_date + ' ' + tou_year
                    end_date = venue_details.split('-')[1].strip()
                    e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                    mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
                    start_date    = get_utc_time(start_date, s_format, 'US/Eastern')
                    end_date  = get_utc_time(end_date, e_format, 'US/Eastern')
                    mid_date = get_utc_time(mid_date, m_format, 'US/Eastern')
                    loc             = get_std_loc(venue_details_)
                tou_year        = " ".join(venue_details.split(',')[0].split(' ')[-1:]).strip()
                if "-" in venue_details:
                    str_date        = venue_details.split(',')[0].split('-')[0].strip() + ' ' + tou_year.strip()
                    end_date        = venue_details.split(',')[0].split('-')[-1].strip()
                else:
                    str_date        = venue_details.split(',')[0].strip()
                    end_date        = venue_details.split(',')[0].strip()
                e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)

                start_date    = get_utc_time(start_date, s_format, 'US/Eastern')
                end_date  = get_utc_time(end_date, e_format, 'US/Eastern')
                mid_date = get_utc_time(mid_date, m_format, 'US/Eastern')


                rounds          = get_nodes(node, './/ul[@class="tieList"]/li[contains(@id, "liRubbers")]')
                for round_ in rounds:
                    r_score                 = extract_data(round_, './/div[@class="listScore"]/text()').strip()
                    _score                  = r_score.split(' ')
                    if "retired" in r_score.lower():
                        pl_status = "retired"
                    elif "not"  in r_score.lower() or "played" in r_score.lower() or  "not played" in r_score.lower():
                        pl_status = "cancelled"
                    elif r_score == "v":
                        pl_status = "scheduled"
                    elif "walkover" in r_score.lower():
                        pl_status = "walkover"
                    elif r_score == "In Progress":
                        pl_status = "ongoing"

                    else:
                        pl_status = "completed"

                    round_num = extract_data(round_, './h3/text()')
                    if "R1" in round_num or "R2" in round_num:
                        pl_game_datetime = start_date
                    elif "R3"  in round_num:
                        pl_game_datetime = mid_date
                    elif "R4" in round_num or "R5" in round_num:
                        pl_game_datetime = end_date

                    round_sk = round_num + '_' + game_id
                    winner_nodes = get_nodes(round_, './/div[contains(@class, "winningSide")]/a')
                    winner_id = [extract_data(winner, './@href').split('playerid=')[-1].strip() for winner in winner_nodes]

                    home_rounds             = get_nodes(round_, './/div[contains(@class, "divSide1")]/a')
                    home_player_id = [extract_data(home_player, './/@href').split('playerid=')[-1].strip() for home_player in home_rounds]

                    away_rounds             = get_nodes(round_, './/div[contains(@class, "divSide2")]/a')
                    away_player_id      = [extract_data(away_player, './/@href').split('playerid=')[-1].strip() for away_player in away_rounds]
                    if away_player_id and  home_player_id:
                        self.generate_scores(_score, home_player_id, away_player_id)
                        RECORD['game'] = "tennis"
                        RECORD['affiliation'] = "atp"
                        RECORD['source_key'] = round_sk
                        RECORD['game_datetime'] = pl_game_datetime
                        RECORD['game_status'] = pl_status
                        RECORD['event'] = event_name
                        RECORD['reference_url'] = response.url
                        RECORD['source'] = source
                        if len(home_player_id) == 1:
                            RECORD['participants'] = { home_player_id[0]: (0, ''), \
                                                    away_player_id[0]: (0, '')}
                        else:
                            if len(away_player_id) == 1:
                                away_2 = 'tbd1'
                                away_1 = away_player_id[0]
                            else:
                                away_1 = away_player_id[0]
                                away_2 = away_player_id[1]
                            RECORD['participants'] = {home_player_id[0]: (1, ''), \
                                            home_player_id[1]:(1, ''), \
                                            away_1: (2, ''), \
                                            away_2 : (2, '')}

                        RECORD['tournament'] = tou_name
                        RECORD['rich_data'] = {'location': loc, 'game_note': game_note}
                        RECORD['time_unknown'] = 1
                        RECORD['tz_info'] = get_tzinfo(city = loc['city'], \
                                            game_datetime = pl_game_datetime)
                        RECORD['result'].setdefault('0', {})['winner'] = winner_id
                        if not RECORD['tz_info']:
                            RECORD['tz_info'] = get_tzinfo(country = loc['country'], \
                            game_datetime = pl_game_datetime)

                        yield RECORD

            elif '0 : 0' in total_score:
                tou_date = extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Date")]/strong/text()').strip()
                tou_year = tou_date.split('-')[-1].split(' ')[-1]
                str_date = tou_date.split('-')[0] + tou_year.strip()
                end_date = tou_date.split('-')[-1].strip()
                e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
                start_date    = get_utc_time(start_date, s_format, 'US/Eastern')
                end_date  = get_utc_time(end_date, e_format, 'US/Eastern')
                mid_date = get_utc_time(mid_date, m_format, 'US/Eastern')
                venue = extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Venue:")]//text()').replace('Venue:', '')
                loc   = get_std_loc(venue)
                _time = ",", extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Start times")]//strong[contains(text(), "Day")]//text()').strip()
                rounds = get_nodes(node, './/ul[@class="tieList"]/li[contains(@id, "liRubbers")]')

                for round_ in rounds:
                    r_score  = extract_data(round_, './/div[@class="listScore"]/text()').strip()
                    _score   = r_score.split(' ')
                    if "retired" in r_score.lower():
                        pl_status = "retired"
                    elif r_score == "v":
                        pl_status =  "scheduled"
                    elif "not"  in r_score.lower() or "played" in r_score.lower() or "not played" in r_score.lower():
                        pl_status = "cancelled"
                    elif "walkover" in r_score.lower():
                        pl_status = "walkover"
                    elif r_score == "":
                        pl_status = "scheduled"
                    elif r_score == "In Progress":
                        pl_status = "ongoing"
                    else:
                        pl_status = "completed"

                    round_num  = extract_data(round_, './h3/text()')
                    if "R1" in round_num or "R2" in round_num:
                        pl_game_datetime = start_date
                    elif "R3"  in round_num:
                        pl_game_datetime  = mid_date
                    elif "R4" in round_num or "R5" in round_num:
                        pl_game_datetime = end_date
                    round_sk  = round_num + '_' + game_id
                    home_rounds = get_nodes(round_, './/div[contains(@class, "divSide1")]/a')
                    home_player_id = [extract_data(home_player, './/@href').split('playerid=')[-1].strip() for home_player in home_rounds]

                    away_rounds             = get_nodes(round_, './/div[contains(@class, "divSide2")]/a')
                    away_player_id      = [extract_data(away_player, './/@href').split('playerid=')[-1].strip() for away_player in away_rounds]
                    if away_player_id and  home_player_id:
                        RECORD['game'] = "tennis"
                        RECORD['affiliation'] = "atp"
                        RECORD['source_key'] = round_sk
                        RECORD['game_datetime'] = pl_game_datetime
                        RECORD['game_status'] = pl_status
                        RECORD['event'] = event_name
                        RECORD['reference_url'] = response.url
                        RECORD['source'] = source
                        if len(home_player_id) == 1:
                            RECORD['participants'] = { home_player_id[0]: (0, ''), \
                                                    away_player_id[0]: (0, '')}
                        else:
                            if len(away_player_id) == 1:
                                away_2 = 'tbd1'
                                away_1 = away_player_id[0]
                            else:
                                away_1 = away_player_id[0]
                                away_2 = away_player_id[1]
                            RECORD['participants'] = {home_player_id[0]: (1, ''), \
                                            home_player_id[1]:(1, ''), \
                                            away_1: (2, ''), \
                                            away_2 : (2, '')}
 

                        RECORD['tournament'] = tou_name
                        RECORD['rich_data'] = {'location': loc, 'game_note': game_note}
                        RECORD['time_unknown'] = 1
                        RECORD['tz_info'] = get_tzinfo(city = loc['city'], \
                                    game_datetime = pl_game_datetime)
                        if not RECORD['tz_info']:
                            RECORD['tz_info'] = get_tzinfo(country = loc['country'], \
                            game_datetime = pl_game_datetime)

                        RECORD['result'] = {}
                        yield RECORD


            elif "V" in total_score:
                venue_details_ = extract_data(node, './/ul[@class="tiedetails clfx"]//li[contains(text(), "Venue")]/strong//text()')
                loc = get_std_loc(venue_details_)
                RECORD['game'] = "tennis"
                RECORD['affiliation'] = "atp"
                RECORD['source_key'] = game_id
                RECORD['game_datetime'] = game_datetime
                RECORD['game_status'] = game_status
                RECORD['event'] = event_name
                RECORD['source'] = source
                RECORD['reference_url'] = response.url
                RECORD['tournament'] = tou_name
                RECORD['participants'] = {home_team_id: (0, ''), away_team_id: (0, '')}
                RECORD['rich_data'] = {'location': loc, 'game_note': game_note}
                RECORD['time_unknown'] = 1
                RECORD['tz_info'] = get_tzinfo(city = loc['city'], \
                                game_datetime = game_datetime)
                if not RECORD['tz_info']:
                    RECORD['tz_info'] = get_tzinfo(country = loc['country'], \
                    game_datetime = game_datetime)

                RECORD['result'] = {}
                yield RECORD

    def generate_scores(self, _score, home_player_id, away_player_id):
        RECORD['result'] = {}
        result = RECORD['result']
        counter = home_final = away_final = 0
        for score in _score:
            counter += 1
            if '-' not in score:
                continue

            score = score.split('-')
            home_score = score[0]
            away_score = score[1]
            extras = ''
            if '(' in away_score:
                extras = away_score.split('(')[-1].strip(')')
                away_score = away_score.split('(')[0]

                if home_score == "7":
                    extras1 = "7"
                    extras2 = extras
                else:
                    extras2 = "7"
                    extras1 = extras

            if home_score > away_score:
                home_final += 1
            elif home_score < away_score:
                away_final += 1
            if len(home_player_id)==1:
                result.setdefault(home_player_id[0], {})['S%s' % (counter)] = home_score
                result.setdefault(away_player_id[0], {})['S%s' % (counter)] = away_score
                if extras:
                    result.setdefault(home_player_id[0], {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id[0], {})['T%s' % (counter)] = extras2
                score = ' - '.join([str(home_final), str(away_final)])
                result.setdefault('0', {})['score'] = score
                result.setdefault(home_player_id[0], {}).update({'final': str(home_final)})
                result.setdefault(away_player_id[0], {}).update({'final': str(away_final)})
            else:
                result.setdefault(home_player_id[0], {})['S%s' % (counter)] = home_score
                result.setdefault(home_player_id[1], {})['S%s' % (counter)] = home_score
                result.setdefault(away_player_id[0], {})['S%s' % (counter)] = away_score
                result.setdefault(away_player_id[1], {})['S%s' % (counter)] = away_score
                if extras:
                    result.setdefault(home_player_id[0], {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id[1], {})['T%s' % (counter)] = extras2
                    result.setdefault(home_player_id[1], {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id[0], {})['T%s' % (counter)] = extras2
                score = ' - '.join([str(home_final), str(away_final)])
                result.setdefault('0', {})['score'] = score
                result.setdefault(home_player_id[0], {}).update({'final': str(home_final)})
                result.setdefault(away_player_id[0], {}).update({'final': str(away_final)})
                result.setdefault(away_player_id[1], {}).update({'final': str(away_final)})
                result.setdefault(home_player_id[1], {}).update({'final': str(home_final)})

