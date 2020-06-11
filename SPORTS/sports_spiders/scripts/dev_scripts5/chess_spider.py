# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data, get_utc_time, get_tzinfo
import datetime
import time
import re
from scrapy_spiders_dev.items import SportsSetupItem

player_dict = {'Anand Viswanathan' : '5000017', 'Kramnik Vladimir': '4101588', \
               'Andreikin Dmitry' : '4158814', 'Topalov Veselin' : '2900084', \
               'Mamedyarov Shakhriyar' : '13401319' , \
               'Aronian Levon' : '13300474' , 'Karjakin Sergey' : '14109603', \
               'Svidler Peter' : '4102142'}

final_result_dict = {'5000017' : 0, '4101588' : 0, '4158814' : 0, \
                     '2900084' : 0, '13401319' : 0, '13300474' : 0, \
                     '14109603' : 0, '4102142' : 0}

class ChessSpider(VTVSpider):
    name = "chess_candidates"
    start_urls = ['http://www.fide.com/calendar.html']
    tou_players_dict = {}

    def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        record['event'] = ''
        record['source'] = "fide_chess"
        record['game'] = "chess"
        record['affiliation'] = 'fide'
        record['result'] = {}
        record['participant_type'] = 'player'
        record['tournament'] = "Women's World Chess Championship"

        events = get_nodes(hxs, '//table/tr/td/a[contains(@href, "fide.com")]')
        for event in events:
            tou_link = extract_data(event, './@href').strip()
            tou_sk = tou_link.split('aid=')[-1]
            tou_name = extract_data(event, './text()')
            if tou_link and tou_sk == "1234":
                yield Request(tou_link, callback = self.parse_event, meta = {'tou_name': tou_name, 'record': record})

    def parse_event(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        tou_name = extract_data(hxs, '//table/tr[1]/td/text()')
        tou_place = extract_data(hxs, '//table/tr/td[contains(text(), "Place")]/following-sibling::td/text()')
        tou_start = extract_data(hxs, '//table/tr/td[contains(text(), "Start")]/following-sibling::td/text()')
        tou_year = tou_start.split('-')[2]
        tou_start = (datetime.datetime(*time.strptime(tou_start.strip(), '%d-%b-%Y')[0:6])).date()
        tou_end = extract_data(hxs, '//table/tr/td[contains(text(), "End")]/following-sibling::td/text()')
        tou_end = (datetime.datetime(*time.strptime(tou_end.strip(), '%d-%b-%Y')[0:6])).date()
        main_link = extract_list_data(hxs, '//table/tr/td/a[contains(@href, "fide.com")]/@href')
        city_info = tou_place.split(',')
        city = city_info[0].strip()
        country = city_info[-1].strip()
        if not main_link:
            main_link = extract_list_data(hxs,'//table/tr/td/a[contains(text(), "Official website")]/@href')
        main_link = main_link[0]
        today_date = datetime.datetime.now().date()
        if tou_end <= today_date:
            if tou_end == today_date:
                status = "ongoing"
            elif tou_end < today_date:
                status = "completed"
        else:
            status = "scheduled"
        event_record = {'tou_name': tou_name, 'tou_place': tou_place, \
                        'tou_start': tou_start, 'tou_end': tou_end, \
                        'status': status, 'year': tou_year}
        tz_info = get_tzinfo(city= city)
        rich_data = {'game_note': '', 'location': {'city': city, 'country': country}}
        record['season'] = tou_year
        record['location_info'] = {'location': {'city': city, 'country': country}}
        record['rich_data'] = rich_data
        record['tz_info'] = tz_info


        if tou_name == "Candidates Tournament":
            if status == "scheduled":
                main_link = main_link + "program/"
            elif status == "completed":
                main_link = main_link + "pairings-and-results/"

            yield Request(main_link, callback = self.parse_cantou, meta = {'event_record': event_record})
        elif "Women's World Championship" in tou_name:
            if main_link:
                yield Request(main_link, callback = self.parse_game_players, meta = {'event_record': event_record, 'record': record})

    def parse_game_players(self, response):
        import pdb;pdb.set_trace()
        event_record = response.meta['event_record']
        hxs = Selector(response)
        schedule_link = extract_data(hxs, '//table//tr/td/a[contains(text(), "Schedule")]/@href')
        game_players = extract_data(hxs, '//table//tr/td/a[contains(text(), "Players")]/@href')
        player_link = extract_data(hxs, '//table[@class="contentpaneopen"]//td/span//a//@href')
        print player_link
        event_record.update({'schedule_link': schedule_link, 'player_link': player_link})
        if game_players:
            player_link = "http://sochi2015.fide.com/" + game_players
            yield Request(player_link, self.parse_players_data, meta = {'event_record': event_record, 'record': response.meta['record']})

    def parse_players_data(self, response):
        import pdb;pdb.set_trace()
        hxs = Selector(response)
        event_record = response.meta['event_record']
        nodes = get_nodes(hxs, '//table[@table]/tbody/tr')
        for node in nodes:
            pl_sk = extract_data(node, './td[2]/text()')
            if not pl_sk:
                continue
            pl_name = extract_data(node, './td/a/text()')
            if pl_sk and pl_name:
                player = pl_name.replace(',', '').replace('.', '').strip()
                self.tou_players_dict[player] = (pl_sk, player)
        player_link = "http://sochi2015.fide.com/" + event_record['player_link']
        yield Request(player_link, self.parse_participants, meta = {'event_record': event_record, 'record': response.meta['record']})

    def parse_participants(self, response):
        import pdb;pdb.set_trace()
        hxs = Selector(response)
        game_players = []
        event_record = response.meta['event_record']
        schedule_link = "http://sochi2015.fide.com/" + event_record['schedule_link']
        script_data = extract_data(hxs, '//script//text()')
        script_data = script_data.replace('\n', '').replace('\r', '').strip()
        #game_data = eval((re.findall('var customData = (.*}])', script_data)[0] + "]}").replace('name', '"name"').replace('flag', '"flag"').replace('teams', '"teams"'))
        game_data = {}
        for key, value in game_data.iteritems():
            for val in value:
                players = self.get_game_players(val)
                game_players.append(players)
        yield Request(schedule_link, self.parse_next, meta = {'event_record': event_record, 'game_players': game_players, 'record': response.meta['record']})


    def get_game_players(self, pl_data):
        players = {}
        for ind, data in enumerate(pl_data):
            if ind == 0:
                is_home = 1
            else:
                is_home = 0
            pl_name = data.get('name', '').replace(',', '').replace('.', '').strip()
            if pl_name == "Mootaz A":
                pl_name = "Moaataz A"
            if pl_name == "Gaponeno I":
                pl_name = "Gaponenko I"
            if pl_name == "Yua Y":
                pl_name = "Yuan Y"
            for key, value in self.tou_players_dict.iteritems():
                if pl_name.lower() in key.lower():
                    pl_sk, player_name = value
                    players[pl_sk] = (is_home, player_name)
            if len(players) < 1:
                print "player name not matched", pl_name
                continue

        return players

    def parse_cantou(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        event_record = response.meta['event_record']
        status = "completed"
        result = {}
        players = {}
        record['event'] = ''
        record['reference_url'] = response.url
        record['source'] = "fide_chess"
        record['game'] = "chess"
        record['affiliation'] = 'fide'
        record['participant_type'] = 'player'
        record['tournament'] = "Candidates chess Tournament"
        record['season'] = event_record['year']
        record['location_info'] = ''

        if (status == "ongoing") or (status == "completed"):
            game_nodes = get_nodes(hxs, '//table//tbody/tr')
            for game_node in game_nodes:
                round_num = extract_data(game_node, './td[contains(@style,"border")]//strong//text()')
                if "Round" in round_num:
                    game_note = round_num
                    Round_sk = round_num.replace(' ', '_')
                round_details = extract_list_data(game_node, './td/text()')
                if len(round_details) == 9:
                    Sno1, GM1, pl_1, fed1, final_res, GM2, pl_2, fed2, Sno2 = round_details
                    game_sk = Sno1 + "_" + Round_sk + "_" + Sno2
                    for k, v in player_dict.iteritems():
                        if k in pl_1:
                            home_pl_sk = v
                        elif k in pl_2:
                            away_pl_sk = v

                    record['source_key'] = game_sk
                    record['game_status'] = status
                    record['game_datetime'] = event_record['tou_start']
                    record['rich_data'] =  {"channels": '', \
                                            "game_note": game_note, \
                                            "location": '', "stadium": ''}

                    if (u'\xbd\xa0-\xa0\xbd' in final_res) or (u'\xbd \u2013 \xbd' in final_res):
                        final_res = "1/2-1/2"
                    elif (u'1\xa0-\xa00' in final_res) or (u'1 \u2013 0' in final_res):
                        final_res = "1-0"
                    elif u'0 \u2013 1' in final_res:
                        final_res = "0-1"
                    else:
                        final_res = final_res

                    pl_1_result = final_res.split('-')[0]
                    pl_2_result = final_res.split('-')[1]

                    if '1/2-1/2' in final_res:
                        final_res = "0.5-0.5"

                    for k, v in final_result_dict.iteritems():
                        if home_pl_sk in k:
                            home_final_res = v + float(final_res.split('-')[0])
                        elif away_pl_sk in k:
                            away_final_res = v + float(final_res.split('-')[1])
                    final_result_dict.update({home_pl_sk: home_final_res, \
                                              away_pl_sk: away_final_res})

                    if '1/2' in pl_1_result or '1/2' in pl_2_result:
                        home_win = "0.5"
                        away_win = "0.5"
                    else:
                        if int(pl_1_result) > int(pl_2_result):
                            home_win = 1
                            away_win = 0
                            winner = home_pl_sk
                        elif int(pl_1_result) < int(pl_2_result):
                            home_win = 0
                            away_win = 1
                            winner = away_pl_sk
                        elif int(pl_1_result) == int(pl_2_result):
                            home_win = 1
                            away_win = 1
                            winner = ''

                    players = {home_pl_sk: (0, pl_1), away_pl_sk: (0, pl_2)}
                    result = {'0': {'score': final_res, 'winner': winner}, \
                              home_pl_sk: {'final': home_final_res, \
                              'game_score': home_win, 'rating': ''}, \
                              away_pl_sk: {'final': away_final_res, \
                              'game_score': away_win, 'rating': ''}}
                    record['result'] = result
                    record['participants'] = players
                    yield record

        elif status == "scheduled":
            hxs = Selector(response)
            event_record = response.meta['event_record']
            game_nodes = get_nodes(hxs,'//table/tbody/tr[td/p[contains(text(), "Round")]]')
            for game_node in game_nodes:
                round_details = extract_list_data(game_node, './/text()')
                Round = []
                for item in round_details:
                    if not ('\n'in item) and not ('on' in item):
                        if ' ' in item:
                            if not "Round" in item:
                                continue
                        Round.append(item)

                date, week, time_R, Round_num = Round
                game_note = Round_num
                date_sk = date.split('.')
                date_sk = date_sk[::-1]
                date_sk = date_sk[0]+"_"+date_sk[1]+"_"+date_sk[2]
                round_date = "".join(Round)
                dt = ()
                dt = round_date.split()
                dt_time = dt[0].replace('Round', '').replace(week, ' ')
                game_date = datetime.datetime(*time.strptime(dt_time.strip(), '%d.%m.%Y %H:%M')[0:6])
                game_sk = Round_num.replace(' ', '_') + "_" + date_sk
                record['source_key'] = game_sk
                record['rich_data'] =  {"channels" : '',"game_note" : game_note,"location" : '',"stadium" : ''}
                record['game_status'] = status
                record['result'] = {}
                record['participants'] = {}
                record['game_datetime'] = game_date
                yield record


    def parse_next(self, response):
        players = {}
        hxs = Selector(response)
        record = response.meta['record']
        event_record = response.meta['event_record']
        record['reference_url'] = response.url

        nodes = get_nodes(hxs, '//table[@align="center"]/tbody/tr[td[@bgcolor="#FFFFFF"]]')

        for node in nodes:
            data = extract_data(node, './td[2]//text()')
            if "Game" not in data:
                continue
            round_ = extract_list_data(node, './/preceding-sibling::tr/td/strong/text()')[-1]
            if round_:
                round_num = round_.split('-')[0].strip()
            round_details = extract_list_data(node, './td//text()')
            dt =  round_details[0] + " " + event_record['year'] + " " + round_details[-1].split('local')[0].strip().replace('.', '')
            game_datetime = get_utc_time(dt, '%B, %d %Y %I:%M %p', 'Europe/Moscow')
            game_sk = round_num.replace(' ', '_') + "_" + game_datetime.split(' ')[0].replace('-', '_')
            record['game_datetime'] = game_datetime
            compare_date = game_datetime.split(' ')[0]
            today_date = str(datetime.datetime.now().date())
            tou_end_date = str(event_record['tou_end'])
            if today_date <= tou_end_date:
                if compare_date == today_date:
                    status = "ongoing"
                elif compare_date < today_date:
                    status = "completed"
                else:
                    status = "scheduled"
            game_note = round_.replace('-', '') + " - " + data
            record['rich_data']['game_note'] = game_note
            if status == "scheduled":
                if "64 players" in round_.lower():
                    for players in response.meta['game_players']:
                        game_pl_sk = '_'.join(players.keys())
                        source_key = game_sk + "_" + game_pl_sk
                        record['source_key'] = source_key
                        record['participants'] = players
                        record['game_status'] = status
                        import pdb;pdb.set_trace()
                        yield record
                else:
                    players = {'tbd1': (0, 'TBD'), 'tbd2': (1, 'TBD')}
                    game_pl_sk = '_'.join(['TBD' ,'TBD'])
                    source_key = game_sk + "_" + game_pl_sk
                    record['source_key'] = source_key
                    record['participants'] = players
                    record['game_status'] = status
                    import pdb;pdb.set_trace()
                    yield record

            else:
                continue
                pl_1 = extract_data(node, './/following-sibling::tr[3]/td[3]/text()').replace(',', '')
                pl_1_rating = extract_data(node, './/following-sibling::tr[3]/td[4]/text()')
                pl_2 = extract_data(node, './/following-sibling::tr[3]/td[7]/text()').replace(',', '')
                pl_2_rating = extract_data(node, './/following-sibling::tr[3]/td[8]/text()')
                final_result = extract_data(node, './/following-sibling::tr[3]/td[5]/text()')
                home_pl_sk = players_sk.get(pl_1)
                away_pl_sk = players_sk.get(pl_2)
                if not final_result:
                    continue
                pl_1_result = final_result.split('-')[0]
                pl_2_result = final_result.split('-')[1]

                if "1/2" in final_result:
                    final_result = final_result.replace('1/2', '0.5')
                else:
                    final_result = final_result

                for k, v in players_dict.iteritems():
                    if home_pl_sk in k:
                        home_final_res = v + float(final_result.split('-')[0])
                    elif away_pl_sk in k:
                        away_final_res = v + float(final_result.split('-')[1])
                players_dict.update({home_pl_sk: home_final_res, \
                                     away_pl_sk: away_final_res})

                if "1/2" in pl_1_result or "1/2" in pl_2_result:
                    home_win = "0.5"
                    away_win = "0.5"
                    winner = ''
                else:
                    if int(pl_1_result) > int(pl_2_result):
                        home_win = 1
                        away_win = 0
                        winner = home_pl_sk
                    elif int(pl_1_result) < int(pl_2_result):
                        home_win = 0
                        away_win = 1
                        winner = away_pl_sk
                    elif int(pl_1_result) == int(pl_2_result):
                        home_win = 1
                        away_win = 1
                        winner = ''

                players = {home_pl_sk: (0, pl_1), away_pl_sk: (0, pl_2)}
                result = {'0': {'score': final_result, 'winner': winner}, \
                          home_pl_sk: {'chess_pieces': 'white', \
                          'final': home_final_res, 'game_score': home_win, \
                          'rating': pl_1_rating}, \
                          away_pl_sk: {'chess_pieces': 'black', \
                          'final': away_final_res, 'game_score': away_win, \
                          'rating': pl_2_rating}}
                record['result'] =  result
                record['participants'] = players

                yield record
