import datetime
import time
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, \
extract_data, get_nodes, get_utc_time, get_tzinfo, extract_list_data
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re


EVENT_DICT = {'group-i/americas': 'Fed Cup Americas Zone Group 1', \
              'group-i/asia-oceania': 'Fed Cup Asia/Oceania Zone Group 1', \
              'group-i/europe-africa': 'Fed Cup Europe/Africa Zone Group 1', \
              'group-ii/americas': 'Fed Cup Americas Zone Group 2', \
              'group-ii/asia-oceania': 'Fed Cup Asia/Oceania Zone Group 2', \
              'group-ii/europe-africa': 'Fed Cup Europe/Africa Zone Group 2', \
              'group-iii/europe-africa': 'Fed Cup Europe/Africa Zone Group 3'
              }

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
    if len(venue.split(',')) == 6 or len(venue.split(',')) == 7:
        _details    = venue.split(',')[1:-2]
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[2].strip()
        state       = ''
    elif len(venue.split(',')) ==5 :
        _details    = venue.split(',')[1:-2]
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[-1].strip()
        if country == city:
            country = ''
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

record = SportsSetupItem()

class TennisFedCup(VTVSpider):
    name    = "tennis_fedcup"
    allowed_domains = ['fedcup.com']
    start_urls = ['http://www.fedcup.com/en/draws-results/fed-cup-structure/diagram.aspx']
    fed_list = open('fedplayer.txt', 'a+')

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="strWrap"]//a')
        for node in nodes:
            team_link       = extract_data(node, './/@href')
            if 'http' not in team_link:
                team_link   = "http://www.fedcup.com" + team_link
            if ".aspx" not in team_link:
                team_link = team_link + ".aspx"
            if '.aspx' in team_link:
                yield Request(team_link, callback = self.parse_games, \
                        meta = {})


    def parse_games(self, response):
        hxs             = Selector(response)
        grp_num_year    = extract_data(hxs, '//div[@id="titleBar"]/h1/text()').strip()
        #year = grp_num_year.split('World Group')[-1].replace('Play-offs ', '').strip()
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
                event_name = extract_data(node, '././/../li//../../ul/preceding-sibling::div[@id="divEventRoundDesc"]/h2[@id="h2Highlight"]/text()').strip()
                if event_name in ["First Round","1st Round"]:
                    event_name = "Fed cup First Round"
                elif event_name in ["Second Round", "2nd Round"]:
                    event_name = "Fed cup Second Round"
                elif event_name in ["3rd Round" ,"Third Round"]:
                    event_name = "Fed cup Third Round"
                elif event_name in ["1st Round Play-off", "2nd Round Play-off", \
                                "Play-off Round"]:
                    event_name = "Fed cup Playoffs"
                elif event_name in ["Quarterfinals"]:
                    event_name = "Fed cup Quarterfinals"
                elif event_name in ["Semifinals"]:
                    event_name = "Fed cup Semifinals"
                elif event_name in ["Final"]:
                    event_name = "Fed cup Finals"
                if grp_num_year.replace(year, '').strip() == "World Group II":
                    event_name =  "Fed Cup World Group 2"
                if grp_num_year.replace(year, '').strip() ==  "World Group II Play-offs":
                    event_name = "Fed Cup World Group 2 Playoffs"
                game_note  = ''
            else:
                group_name = response.url.replace('http://www.fedcup.com/en/draws-results/', '') \
                .replace('/2015', '').replace('.aspx', '').strip()
                game_note = event_name
                event_name = EVENT_DICT[group_name]
                game_datetime = ''
                game_end = ''

            team_nodes = get_nodes(node, './/div[@class="panel-r"]//table[@class="dsTable"]//tr/td[@class="team"]/span/a')
            _team = extract_list_data(team_nodes, './@href')
            game_st = extract_data(node, './/h3/span//a/text()')
            if game_st in ['Results', 'Preview']:
                if len(_team) == 2:
                    game_link   = extract_data(node, './/h3/span//a/@href')
                    game_id     = game_link.split('Id=')[-1]
                    if game_link:
                        game_link = "http://www.fedcup.com" + game_link
                        yield Request(game_link, callback= self.parse_games_details, \
                            meta={
                            'game_id' : game_id, 'game_datetime': game_datetime, \
                            'event_name': event_name, \
                            'game_end': game_end, 'game_note': game_note})

        group_nodes = get_nodes(hxs, '//div[@id="divPOEvent"]//div[@class="innerBox"]')
        for group_node in group_nodes:
            game_link = get_nodes(group_node, './/ul//li')
            game_note = extract_data(group_node, './/preceding-sibling::h2//text()')
            for game_links in game_link:
                game_link = extract_data(game_links, './/a//@href')
                group_name = response.url.replace('http://www.fedcup.com/en/draws-results/', '') \
                    .replace('/2015', '').replace('.aspx', '').strip()
                event_name = EVENT_DICT[group_name]
                game_datetime = ''
                game_end = ''
                game_id     = game_link.split('Id=')[-1]
                if game_link:
                    game_link = "http://www.fedcup.com" + game_link

                    yield Request(game_link, callback= self.parse_games_details, \
                        meta={'game_id' : game_id, \
                        'game_datetime': game_datetime, 'event_name': event_name, \
                    'game_end': game_end, 'game_note': game_note})




    def parse_games_details(self, response):
        hxs         = Selector(response)
        source = "fedcup_tennis"
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


        tou_name = "Fed Cup"
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

            if 'http' not in home_team_link: home_team_link = 'http://www.fedcup.com' + home_team_link

            away_team_link  = extract_data(node, './/div[@class="header"]/div[@class="rht"]//a/@href')
            away_team_id    = away_team_link.split('id=')[-1].strip()

            if 'http' not in away_team_link: away_team_link = 'http://www.fedcup.com' + away_team_link

            total_score         = extract_data(node, './/div[@class="vs"]/text()')
            home_team_score, away_team_score = '', ''
            if '0 : 0' not in total_score and 'V' not in total_score and "scheduled" not in status:
                record['result'] = {}
                venue_details   = extract_data(node, './/div[@class="groupVenue"]//p//text()')
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
                if home_win and away_win: team_winner = ''
                elif home_win: team_winner = home_team_id
                elif away_win: team_winner = away_team_id

                game_status     = status
                record['game'] = "tennis"
                record['source'] = source
                record['tournament'] = tou_name
                record['source_key'] = game_id
                record['affiliation'] = "wta"
                record['game_status'] = game_status
                record['game_datetime'] = game_datetime
                record['event'] = event_name
                record['reference_url'] = response.url
                record['rich_data'] = {'location': loc, 'game_note': game_note}
                record['time_unknown'] = 1
                record['tz_info'] = get_tzinfo(city = loc['city'], game_datetime = game_datetime)
                if not record['tz_info']:
                    record['tz_info'] = get_tzinfo(country = loc['country'], game_datetime = game_datetime)
                record['participants'] = { home_team_id: (0, ''), away_team_id: (0, '')}
                record['result'] = {home_team_id: { 'final': home_team_score}, \
                        away_team_id:{ 'final': away_team_score}}
                record['result'].setdefault('0', {})['score'] = final_score
                if status == "completed":
                    record['result'].setdefault('0', {})['winner'] = team_winner
                yield record


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


                loc             = get_std_loc(venue_details)

                rounds = get_nodes(node, './/ul[@class="tieList"]/li[contains(@id, "liRubbers")]')
                for r in rounds:
                    r_score                 = extract_data(r, './/div[@class="listScore"]/text()').strip()
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
                    home_pl_score           = [i.split('-')[0].strip() for i in _score if i.lower() not in ["retired", "not", "played", "walkover"]]
                    away_pl_score           = [i.split('-')[-1].strip() for i in _score if i.lower() not in ["retired", "not", "played", "walkover"]]

                    round_num               = extract_data(r, './h3/text()')
                    if "R1" in round_num or "R2" in round_num:
                        pl_game_datetime = start_date
                    elif "R3"  in round_num:
                        pl_game_datetime = mid_date
                    elif "R4" in round_num or "R5" in round_num:
                        pl_game_datetime = end_date

                    round_sk                = round_num + '_' + game_id
                    if "R4_100021923" in round_sk:
                        pl_status = "Hole"


                    winner_nodes = r.select('.//div[contains(@class, "winningSide")]/a')
                    winner_id = [extract_data(winner, './@href').split('playerid=')[-1].strip() for winner in winner_nodes]

                    home_rounds             = get_nodes(r, './/div[contains(@class, "divSide1")]/a')
                    home_player_link = [extract_data(home_player, './@href') for home_player in home_rounds]
                    home_player_id = [extract_data(home_player, './@href').split('playerid=')[-1].strip() for home_player in home_rounds]

                    away_rounds             = get_nodes(r, './/div[contains(@class, "divSide2")]/a')
                    away_player_id      = [extract_data(away_player, './@href').split('playerid=')[-1].strip() for away_player in away_rounds]
                    away_player_link = [extract_data(away_player, './@href') for away_player in away_rounds]
                    if len(home_player_id) == 1:
                        self.fed_list.write("%s\n%s\n" %(home_player_link[0], away_player_link[0]))
                    else:
                        self.fed_list.write("%s\n%s\n%s\n%s\n" %(home_player_link[0], home_player_link[1], away_player_link[0], away_player_link[1]))
                    if away_player_id and  home_player_id:
                        self.generate_scores(_score, home_player_id, away_player_id)
                        record['game'] = "tennis"
                        record['source'] = source
                        record['tournament'] = tou_name
                        record['source_key'] = round_sk
                        record['affiliation'] = "wta"
                        record['game_status'] = pl_status
                        record['game_datetime'] = pl_game_datetime
                        if len(home_player_id) == 1:
                            record['participants'] = { home_player_id[0]: (0, ''), \
                                        away_player_id[0]: (0, '')}
                        else:
                            record['participants'] = {home_player_id[0]: (1, ''), \
                                home_player_id[1]:(1, ''), \
                                away_player_id[0] :(2, ''), away_player_id[1] : (2, '')}
                        record['event'] = event_name
                        record['reference_url'] = response.url
                        record['rich_data'] = {'location': loc, 'game_note': game_note}
                        record['result'].setdefault('0', {})['winner'] =winner_id
                        record['time_unknown'] = 1
                        record['tz_info'] = get_tzinfo(city = loc['city'], game_datetime = pl_game_datetime)
                        if not record['tz_info']:
                            record['tz_info'] = get_tzinfo(country = loc['country'], game_datetime = pl_game_datetime)
                        yield record

            elif '0 : 0' in total_score:
                tou_date = extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Date")]/strong/text()').strip()
                tou_year = tou_date.split('-')[-1].split(' ')[-1]
                str_date = tou_date.split('-')[0] + tou_year
                end_date = tou_date.split('-')[-1].strip()
                e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
                start_date    = get_utc_time(start_date, s_format, 'US/Eastern')
                end_date  = get_utc_time(end_date, e_format, 'US/Eastern')
                mid_date = get_utc_time(mid_date, m_format, 'US/Eastern')
                venue = extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Venue:")]/strong/text()').strip()
                loc             = get_std_loc(venue)
                _time = extract_data(hxs, '//ul[@class="tiedetails clfx"]//li[contains(text(), "Start times")]//strong[contains(text(), "Day")]//text()').strip()
                rounds          = get_nodes(node, './/ul[@class="tieList"]/li[contains(@id, "liRubbers")]')

                for r in rounds:
                    r_score                 = extract_data(r, './/div[@class="listScore"]/text()').strip()
                    _score                  = r_score.split(' ')
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

                    round_num               = extract_data(r, './h3/text()')
                    if "R1" in round_num or "R2" in round_num:
                        pl_game_datetime = start_date
                    elif "R3"  in round_num:
                        pl_game_datetime  = mid_date
                    elif "R4" in round_num or "R5" in round_num:
                        pl_game_datetime = end_date

                    round_sk                = round_num + '_' + game_id



                    home_rounds             = get_nodes(r, './/div[contains(@class, "divSide1")]/a')
                    home_player_id = [extract_data(home_player, './@href').split('playerid=')[-1].strip() for home_player in home_rounds]
                    home_player_link = [extract_data(home_player, './@href') for home_player in home_rounds]

                    away_rounds             = get_nodes(r, './/div[contains(@class, "divSide2")]/a')
                    away_player_id      = [extract_data(away_player, './@href').split('playerid=')[-1].strip() for away_player in away_rounds]
                    away_player_link =  [extract_data(away_player, './@href') or away_player in away_rounds]
                    if len(home_player_id) == 1:
                        self.fed_list.write("%s\n%s\n" %(home_player_link[0], away_player_link[0]))
                    else:
                        self.fed_list.write("%s\n%s\n%s\n%s\n" %(home_player_link[0], home_player_link[1], away_player_link[0], away_player_link[1]))
                    if away_player_id and  home_player_id:
                        record['game'] = "tennis"
                        record['affiliation'] = "wta"
                        record['source_key'] = round_sk
                        record['game_datetime'] = pl_game_datetime
                        record['game_status'] = pl_status
                        record['event'] = event_name
                        record['reference_url'] = response.url
                        record['source'] = source
                        if len(home_player_id) == 1:
                            record['participants'] = { home_player_id[0]: (0, ''), \
                                                    away_player_id[0]: (0, '')}
                        else:
                            record['participants'] = {home_player_id[0]: (1, ''), \
                                            home_player_id[1]:(1, ''), \
                                            away_player_id[0] :(2, ''), \
                                            away_player_id[1] : (2, '')}

                        record['tournament'] = tou_name
                        record['rich_data'] = {'location': loc, 'game_note': game_note}
                        record['time_unknown'] = 1
                        record['tz_info'] = get_tzinfo(city = loc['city'], game_datetime = pl_game_datetime)
                        if not record['tz_info']:
                            record['tz_info'] = get_tzinfo(country = loc['country'], game_datetime = pl_game_datetime)
                        yield record


            else:

                venue = extract_data(node, './/div[@class="summary"]//td[@class="rht"]/ul/li[2]/strong/text()')

                loc  = get_std_loc(venue)
                record['game'] = "tennis"
                record['affiliation'] = "wta"
                record['source_key'] = game_id
                record['game_datetime'] = game_datetime
                record['game_status'] = game_status
                record['event'] = event_name
                record['source'] = source
                record['reference_url'] = response.url
                record['tournament'] = tou_name
                record['participants'] = {home_team_id: (0, ''), away_team_id: (0, '')}
                record['rich_data'] = {'location': loc, 'game_note': game_note}
                record['time_unknown'] = 1
                record['tz_info'] = get_tzinfo(city = loc['city'], game_datetime = game_datetime)
                if not record['tz_info']:
                    record['tz_info'] = get_tzinfo(country =loc['country'], game_datetime = game_datetime)

                record['result'] = {}
                yield record
    def generate_scores(self, _score, home_player_id, away_player_id):
        record['result'] = {}
        result = record['result']
        counter = home_final = away_final = 0
        for score in _score:
            counter += 1
            if '-' not in score:
                continue

            score = score.split('-')
            home_score = score[0].replace('[', '').replace(']', '').strip()
            away_score = score[1].replace('[', '').replace(']', '').strip()
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
