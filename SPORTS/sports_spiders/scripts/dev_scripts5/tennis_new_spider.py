import datetime
from scrapy.selector import HtmlXPathSelector
from vtvspider_new import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


STATUS_DICT = {'final': 'completed', 'completed': 'completed', 'Game Over': \
               'completed', 'in progress': 'ongoing', 'postponed': 'postponed', \
               'suspended': 'cancelled'}
ACCEPT_TITLES = ["women's", "men's", "mixed doubles"]

IGNORE_WORDS = ['presented', 'partnership', 'pres. by', \
                ' by ', ' at ', "men's ", "women's ", \
                "Umag", "Singles First Round"]
REPLACE_WORDS = ['waste management', 'omega', 'atp ', \
                'wta ', "mutua madrilena ", "tour ", "kdb ", \
                "grc bank", "2013", "skistar", "bet-at-home open - ", \
                'match schedule', 'nurnberger', 'vegeta', ', kuala lumpur',\
                'rakuten ', ' basel', 'barclays ', 'aircel ', '2014', '2015']

TOU_NAME  = {"350" : "German Open Tennis Championships", "192" : "Budapest Grand Prix", \
            "18" : "Hall of Fame Tennis Championships", "268" : "Swedish Open", \
            "7" : "Swiss Open (Tennis)", "121" : "Atlanta Tennis Championships", \
            "304" : "Austrian Open Kitzbuhel", "47" : "Cincinnati Masters", \
            "266" : "Cincinnati Masters", "114" : "Open de Moselle" ,\
            "148" : "Stockholm Open", "23" : "Swiss Indoors", "41": "Madrid Open (Tennis)", \
            "276" : "WTA Tour Championships", "170" : "Fed Cup", \
            "164": "Davis Cup", "4": "Rotterdam Open", \
            "367" : "Shenzhen Open", "205" : "Sydney International", \
            "384" : "Shenzhen Open", '215': 'Auckland Open', \
            "283" : "ATP Apia International Sydney", "328" : "Miami Masters", \
            "145" : "US Mens Clay Court Championships", "251": "Indian Wells Masters", \
            "45"  : "Indian Wells Masters", "341": "Monterrey Open", "30": "ATP Auckland Open", \
            "378" : "Copa Colsanitas", "22": "Croatia Open", \
            "353" : "Katowice Open", "383" : "Malaysian Open (Tennis)", \
            "231" : "Grand Prix de SAR La Princesse Lalla Meryem", \
            "338" : "Barcelona Open (Tennis)", "343" : "Madrid Open (Tennis)", \
            "290" : "Rogers Cup", "252": "New Haven Open", \
            "363" : "ATP-Winston Salem Open", "351" : "Malaysian Open (Tennis)",  \
            "232" : "Guangzhou International Women's Open",  \
            "264" : "Toray Pan Pacific Open Tennis Tournament", \
            "233" : "Hansol Korea Open", "381": "Wuhan Open", \
            "223" : "China Open (Tennis)", "29": "Open Sud de France", \
            "21"  : "China Open (Tennis)", "346" : "Garanti Koza WTA Tournament of Champions", \
            "315" : "Shanghai Masters (Tennis)", "281": "PBZ Zagreb Indoors", \
            "197" : "Luxembourg Open", "13" : "Paris Masters", \
            "345" : "HP Open", "385": "Tianjin Open", \
            "360" : "WTA Tour Championships", "115" : "Valencia Open 500",
            "382" : "WTA Tour Championships", "20": "Chennai Open", \
            "119" : "Qatar Open", "336": "Brisbane International", \
            "406" : "Brisbane International", \
            "274" : "U.S. National Indoor Tennis Championships", \
            "296" : "Delray Beach International Tennis Championships", \
            "376" : "Rio Open", "6": "Mexican Open (Tennis)", \
            "375" : "Rio Open", "398": "Bell Challenge", \
            "25"  : "Dubai Tennis Championships", "299": "ATP Buenos Aires", \
            "327" : "Miami Masters", "28": "Grand Prix Hassan II", \
            "42"  : "Monte-Carlo Masters", "11": "BRD Nastase Tiriac Trophy", \
            "12"  : "BMW Open", "26": "Portugal Open", \
            "303" : "Internazionali BNL d'Italia", "1" : "ATP Power Horse World Team Cup", \
            "356" : "ATP Open de Nice Cote d' Azur", "49" : "Mercedes Cup", \
            "117" : "TOPSHELF Open", "27" : "Gerry Weber Open", \
            "129" : "ATP AEGON Championships", "390" : "Nottingham Open", \
            "306" : "ATP Swedish Open", "370": "Claro Open Colombia", \
            "39"  : "Citi Open", "46" : "Rogers Cup", \
            "5"   : "Japan Open Tennis Championships", "10": "Vienna Open", \
            "37"  : "Kremlin Cup", "389" : "Garanti Koza Istanbul Open", \
            "388" : "Quito Ecuador Open", "340" : "Brisbane International", \
            "316" : "ASB Classic", "245" : "Hobart International", \
            "348" : "Thailand Open (WTA)", "393": "Proximus Diamond Games", \
            "208" : "Dubai Tennis Championships", "216" : "Mexican Open (Tennis)", \
            "359" : "Qatar Total Open", "228": "Family Circle Cup", \
            "254" : "Porsche Tennis Grand Prix", "319" : "Internazionali BNL d'Italia", \
            "237" : "Internationaux de Strasbourg", "250" : "TOPSHELF Open", \
            "394" : "Nottingham Open", "227" : "AEGON Classic", \
            "247" : "WTA Swedish Open", "386" : "BRD Bucharest Open", \
            "331" : "Gastein Ladies", "379" : "Istanbul Cup", \
            "361" : "Baku Cup", "377" : "Brasil Tennis Cup", \
            "219" : "Bank of the West Classic", "362" : "Citi Open", \
            "261" : "Tashkent Open",  "199" : "Generali Ladies Linz", \
            "243" : "Kremlin Cup", "395" : "Garanti Koza WTA Tournament of Champions", \
            "400": "Estoril Open (tennis)", "396": "Geneva Open", \
            "369": "Nurnberger Versicherungscup", \
            "397" : "Eastbourne International", \
            "380": "Hong Kong Tennis Open", "339": "ATP World Tour Finals", \
            }




def get_refined_tou_name(tou_name):
    tou_name = tou_name.lower()
    for i in IGNORE_WORDS:
        if i in tou_name:
            tou_name = tou_name.split(i)[0].strip()

    tou_name = tou_name.replace('skistar ', '').replace('collector ', '')
    if tou_name == 'atp swedish open' or tou_name == 'wta swedish open':
        return tou_name

    for i in REPLACE_WORDS:
        tou_name = tou_name.replace(i, '').strip()
    return tou_name


class TennisSpider(VTVSpider):

    name    = "tennis_spider"
    allowed_domains = ['sports.espn.go.com', 'espn.go.com']
    start_urls = []
    record = SportsSetupItem()
    def start_requests(self):
        next_week_days = []
        urls = 'http://sports.espn.go.com/sports/tennis/dailyResults?date=%s'

        if self.spider_type == "scores":
            for i in range(0, 10):
                next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))

        elif self.spider_type == "schedules":
            for i in range(0, 20):
                next_week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        for wday in next_week_days:
            t_url = urls % (wday)
            yield Request(t_url, callback = self.parse, meta = {})
        else:
            top_url = ['http://espn.go.com/tennis/schedule/_/type/women', \
                        'http://sports.espn.go.com/sports/tennis/schedule']

        for t_url in top_url:
            yield Request(t_url, callback = self.parse_preschedules, meta = {})

    def parse_preschedules(self, response):
        hxs   = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//td[contains(text(), "Current Tournaments")]/../../tr[contains(@class, "row")]')
        for node in nodes:
            tournament = extract_data(node, './td[2]//a/text()')
            location = extract_data(node, './td[3]//text()')
            link = extract_data(node, './td[2]//a/@href')
            yield Request(link, callback = self.parse_predetails, \
            meta = {'tou_name': tournament, 'location': location})


    def parse_predetails(self, response):
        hxs   = HtmlXPathSelector(response)
        tournament_name = response.meta['tou_name']
        location = response.meta['location']
        if len(location.split(','))> 2:
            city = location.split(',')[0].strip()
            country = location.split(',')[-1].strip()
            state = location.split(',')[1].strip().replace('D.C', 'Washington').strip()
            if state == "DC":
                state = "Washington"
        elif len(location.split(',')) == 1:
            city = ''
            state = ''
            country = location
        else:
            city = location.split(',')[-2].strip()
            country = location.split(',')[-1].strip()
            state = ''

        url = response.url
        nodes = get_nodes(hxs, '//div[@class="span-4"]/div[@class="matchTitle"][1]/following-sibling::div[contains(@class, "span-2")]/div[@class="matchContainer"][div[@class="matchInfo"]//table//tr[td[contains(text(), "TBD") or  contains(text(), "ET")]]]')
        for node in nodes:
            game_id = extract_data(node, './@id').strip().split('-')[0].strip()
            game_type = extract_list_data(node, './../preceding-sibling::div[@class="matchTitle"]//text()')[-1]
            game_type = game_type.strip().lower()
            game_note = game_type.split(':')[0].replace('-', '').strip().title()
            event_name = game_type.split(':')[-1].replace('-', '').strip()  + " "+ game_type.split(':')[0].replace('-', ' ').strip()
            game_note_ = extract_data(node, './/div[@class="matchCourt"]//text()')
            if "Rogers" in tournament_name:
                event_name = "Rogers Cup " + event_name.title()
                game_note = game_note_
            else:
                event_name = ''
                game_note = game_note
            game_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = 'scheduled'
            game_time = extract_data(node, './/div[@class="matchInfo"]//tr/td[@style="color:#FFF;"]//text()')
            if "ET" not in game_time:
                game_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                game_datetime = game_datetime.split(' ')[0] + " " + "12:00 AM ET"
                game_datetime = get_utc_time(game_datetime, '%Y-%m-%d %I:%M %p ET', 'US/Eastern')
            else:
                game_datetime = game_datetime.split(' ')[0] + " " + game_time
                game_datetime = get_utc_time(game_datetime, '%Y-%m-%d %I:%M %p ET', 'US/Eastern')

            if 'single' in game_type:
                player1 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]//a/@href') \
                                        .strip().split('/_/id/')[-1].split('/')[0].strip()
                player2 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]//a/@href') \
                                        .strip().split('/_/id/')[-1].split('/')[0].strip()
                team1 = team0 = ''
                if not player1:
                    player1 = 'tbd1'
                if not player2:
                    player2 = 'tbd2'
                if "458" in player1 and "458" in player2:
                    player1 = '458'
                    player2 = 'tbd1'
                self.record['participants'] = { player1: (0, ''), player2: (0, '')}
            elif 'double' in game_type:
                player1 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]//a/@href').strip() \
                                            .split('/_/id/')[-1].split('/')[0].strip()
                player3 = extract_list_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]/../following-sibling::tr \
                                        /td[not(@class)]//a/@href')[0].strip().split('/_/id/')[-1].split('/')[0].strip()
                player2 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]//a/@href').strip() \
                                        .split('/_/id/')[-1].split('/')[0].strip()

                player4 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]/../following-sibling::tr/ \
                                        td[not(@class)]//a/@href').strip().split('/_/id/')[-1].split('/')[0].strip()
                team0 = [player1, player3]
                team1 = [player2, player4]
                self.record['participants'] = { player1: (1, ''), player3 :(1, ''), player2 :(2, ''), player4 : (2, '')}
            players = [[player1], [player2]]
            if team0:
                players = [team0, team1]
            if "57095" in game_id or '57105' in game_id:
                continue
            if "57098" in game_id and "scheduled" in status:
                continue
            self.record['source_key'] = game_id
            self.record['game_datetime'] = game_datetime
            self.record['game_status'] = status
            self.record['reference_url'] = response.url
            self.record['source'] = "espn_tennis"
            self.record['game'] = "tennis"
            self.record['event'] = event_name
            self.record['result'] = {}
            if "Davis Cup" in tournament_name or "Fed Cup" in tournament_name  \
                or  "Australian Open" in tournament_name or  \
                "US Open" in tournament_name or "French Open" in tournament_name \
                or "Hopman Cup" in tournament_name:
                continue
            if 'french open' in tournament_name.lower():
                continue
            if 'wimbledon' in tournament_name.lower():
                continue
            self.record['tournament'] = tournament_name
            if "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "Davis Cup" in tournament_name or \
                "Fed Cup" in tournament_name or "French Open" in tournament_name or \
                "hopman cup" in tournament_name:
                game_note = ''
            else:
                game_note  = game_note
            if  game_note == "Team Cup":
                continue
            if "WTA" in tournament_name:
                self.record['affiliation'] = "wta"
            elif "ATP" in tournament_name:
                self.record['affiliation'] = "atp"
            elif "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "French Open" in tournament_name:
                self.record['affiliation'] = "grand slam"
            elif "Mixed" in tournament_name:
                self.record['affiliation'] = "atp_wta"

            self.record['participant_type'] = "player"
            self.record['rich_data'] =  {'channels': '',
                                        'location': {'city': city, 'state' : state,
                                        'country': country
                                        }}
            self.record['rich_data'] ['game_note'] = game_note
            tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
            self.record['tz_info'] = tz_info
            if not tz_info:
                tz_info = get_tzinfo(city = city, country = country, game_datetime = game_datetime)
                self.record['tz_info'] = tz_info
            tournament_name = get_refined_tou_name(tournament_name)
            ref_links = url.split('&year')[0].split('=')[-1]
            if ref_links in TOU_NAME:
                tournament_name = TOU_NAME.get(ref_links)
            else:
                tournament_name = tournament_name
            self.record['tournament'] = tournament_name
            self.record['time_unknown'] = '1'
            if "australian open" in tournament_name:
                continue
            if status == "scheduled":
                yield self.record


    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        if self.spider_type == "schedules" or self.spider_type == "scores":
            nodes = extract_list_data(hxs, '//div[@class="scoreHeadline"]/a/@href')
            for node in nodes:
                link = 'http://sports.espn.go.com/sports/tennis/'+ node
                yield Request(link, callback = self.parse_details, meta ={})

    def parse_details(self, response):
        hxs = HtmlXPathSelector(response)
        url = response.url
        game_date = extract_data(hxs, '//div[@class="key-dates key-dates_sc"]//h2//text()')\
                                    .strip().strip('Results for').strip()
        tournament_name = extract_data(hxs, '//div[@class="scoreHeadline"]//text()').strip()
        tournament_name = tournament_name.replace('Results', '').replace('2014', ''). \
                            replace('Match Schedule', '').strip()
        if "WTA" in tournament_name:
            self.record['affiliation'] = "wta"
        elif "ATP" in tournament_name:
            self.record['affiliation'] = "atp"
        elif "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "French Open" in tournament_name:
            self.record['affiliation'] = "grand slam"
        elif "Mixed" in tournament_name:
            self.record['affiliation'] = "atp_wta"
        tournament_name = get_refined_tou_name(tournament_name)
        ref_links = url.rsplit('=', 1)[-1]
        if ref_links in TOU_NAME:
            tournament_name = TOU_NAME.get(ref_links)
        else:
            tournament_name = tournament_name
        self.record['rich_data'] = {}
        singles_nodes = [('single', i)for i in get_nodes(hxs, '//div[@class="matchContainer"]')]
        doubles_nodes = [('double', i)for i in get_nodes(hxs, '//div[@class="matchContainerDoubles"]')]
        game_nodes = []
        game_nodes.extend(singles_nodes)
        game_nodes.extend(doubles_nodes)
        for node_type, node in game_nodes:
            game_id = extract_data(node, './@id').strip().split('-')[0].strip()
            game_time = extract_list_data(node, './../preceding-sibling::div[@class="matchTitle"]//text()')
            if game_time:
                game_time = game_time[-1].strip()
            game_note_ = game_time.split('-')[1].strip()
            stadium = game_time.split('\n')[-1].split('- Group')[0].strip('-').strip()
            if stadium == "Hisense Arena" or stadium == "Margaret Court Arena" or stadium == "Rod Laver Arena":
                stadium = stadium
            else: stadium = ''
            game_time = game_time.split('Start Time:')[-1].strip('-').strip()
            game_datetime = game_date+'T'+game_time
            game_datetime = get_utc_time(game_datetime, '%B %d, %YT%I:%M %p ET', 'US/Eastern')
            game_data = extract_data(node, './/div[@class="matchCourt"]//text()').strip()
            game_note = game_data.split(':')[0].strip()
            if "Rogers" in tournament_name:
                event_name = "Rogers Cup" + game_data.split(':')[-1]+ " " +game_note.strip().title()
                game_note = game_note_
            else:
                event_name = ''
                game_note = game_note
            if game_note == "Team Cup":
                continue

            self.record['rich_data']['game_note'] = game_note
            status = extract_data(node, './/div[@class="matchInfo"]//tr[1]//text()').strip()

            if "ET" in status:
                game_status = "scheduled"
            elif "Final" in status:
                game_status = "completed"
            elif "Set" in status:
                game_status = "ongoing"
            elif "TBD" in status:
                game_status = "scheduled"
            elif "retired"  in status.lower():
                game_status = "retired"
            elif "walkover" in status.lower():
                game_status = "walkover"

            if 'single' in node_type:
                player1 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]//a/@href') \
                                        .strip().split('/_/id/')[-1].split('/')[0].strip()
                player2 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]//a/@href') \
                                        .strip().split('/_/id/')[-1].split('/')[0].strip()
                team1 = team0 = ''

                self.record['participants'] = { player1: (0, ''), player2: (0, '')}
            elif 'double' in node_type:
                player1 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]//a/@href').strip() \
                                            .split('/_/id/')[-1].split('/')[0].strip()
                player3 = extract_list_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]/../following-sibling::tr \
                                        /td[not(@class)]//a/@href')[0].strip().split('/_/id/')[-1].split('/')[0].strip()
                player2 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]//a/@href').strip() \
                                        .split('/_/id/')[-1].split('/')[0].strip()

                player4 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]/../following-sibling::tr/ \
                                        td[not(@class)]//a/@href').strip().split('/_/id/')[-1].split('/')[0].strip()
                team0 = [player1, player3]
                team1 = [player2, player4]
                self.record['participants'] = { player1: (1, ''), player3 :(1, ''), player2 :(2, ''), player4 : (2, '')}
            players = [[player1], [player2]]
            if team0:
                players = [team0, team1]
            if "57095" in game_id or '57105' in game_id or '59672' in game_id or "63730" in game_id:
                continue
            if "57098" in game_id and "scheduled" in status:
                continue
            if "59020" in game_id and "ongoing" in game_status:
                game_status = "completed"


            self.record['source_key'] = game_id
            self.record['game_datetime'] = game_datetime
            self.record['game_status'] = game_status
            self.record['reference_url'] = url
            self.record['source'] = "espn_tennis"
            self.record['game'] = "tennis"
            self.record['result'] = {}
            self.record['event'] = event_name
            if "Davis Cup" in tournament_name or "Fed Cup" in tournament_name:
                continue
            if "australian open" in tournament_name:
                continue
            if 'french open' in tournament_name.lower():
                continue
            if 'wimbledon' in tournament_name.lower():
                continue
            if 'us open' in tournament_name.lower():
                continue
            self.record['tournament'] = tournament_name
            if "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "Davis Cup" in tournament_name or \
                "Fed Cup" in tournament_name or "French Open" in tournament_name or \
                "hopman cup" in tournament_name:
                game_note = ''
            else:
                game_note  = game_note

            if "WTA" in tournament_name:
                self.record['affiliation'] = "wta"
            elif "ATP" in tournament_name:
                self.record['affiliation'] = "atp"
            elif "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "French Open" in tournament_name:
                self.record['affiliation'] = "grand slam"
            elif "Mixed" in tournament_name:
                self.record['affiliation'] = "atp_wta"

            self.record['participant_type'] = "player"
            self.record['rich_data'] ['game_note'] = game_note

            if self.spider_type == "schedules" and game_status == "scheduled":
                yield self.record

            elif self.spider_type == "scores" and (game_status != "scheduled"):

                game_scores = get_nodes(node, './/div[@class="linescore"]//tr')[1:]
                full_scores = []
                for game_score in game_scores:
                    scores = []
                    nodes = get_nodes(game_score, './/td')
                    for node in nodes:
                        score = extract_list_data(node, './text()')[0]
                        break_score = extract_list_data(node, './sup/text()')
                        if break_score:
                            scores.append([score, break_score[0]])
                        else:
                            scores.append(score)
                    full_scores.append(scores)
                self.generate_scores(full_scores, node_type, players)

                zip_data = zip(full_scores[0], full_scores[1])
                ps1 = ps2 = 0
                for i in zip_data:
                    if isinstance(i[0], list):
                        if 'ongoing' in game_status:
                            if int(i[0][1]) > 5 or int(i[1][1]) > 5:
                                if int(i[0][1]) > int(i[1][1]):
                                    ps1 = ps1+1
                            else:
                                ps2 = ps2+1
                        else:
                            if int(i[0][1]) > int(i[1][1]):
                                ps1 = ps1+1
                            else:
                                ps2 = ps2+1
                    else:
                        if 'ongoing' in game_status:
                            if int(i[0]) > 5 or int(i[1]) > 5:
                                if int(i[0]) > int(i[1]):
                                    ps1 = ps1+1
                                else:
                                    ps2 = ps2+1
                        else:
                            if int(i[0]) > int(i[1]):
                                ps1 = ps1+1
                            elif int(i[0]) < int(i[1]):
                                ps2 = ps2+1

                if team0 and team1:
                    if ps1 > ps2:
                        winner = team0
                        winner1 = player1
                        winner2 = player3
                    elif ps1 < ps2:
                        winner = team1
                        winner1 = player2
                        winner2 = player4
                    else:
                        tie = []
                        tie.extend(team0)
                        tie.extend(team1)
                        winner1 = ''
                        winner2 = ''
                else:
                    if int(ps1) > int(ps2):
                        winner = player1
                    elif int(ps1) < int(ps2):
                        winner = player2
                    elif int(ps1)==int(ps2):
                        winner = ''

                if game_status == "walkover" or game_status == "retired":
                    winner = extract_data(node, './/td[contains(@class, "teamLine")][not(contains(@style, "font-weight:normal;"))] \
                                        /a/@href').strip().split('/_/id/')[-1].split('/')[0].strip()

                self.record['rich_data'] ['game_note'] = game_note
                final = str(ps1)+' - '+str(ps2)

                if game_status == "completed" and team0 and team1:
                    self.record['result'].setdefault('0', {}) ['winner'] = [winner1]
                    self.record['result'].setdefault('0', {})['winner'].append(winner2)
                elif game_status == "completed" and not team0:
                    self.record['result'].setdefault('0', {}) ['winner'] = [winner]
                else:
                    self.record['result'].setdefault('0', {}) ['winner'] = ''
                self.record['result'].setdefault('0', {}).update({'score': final})
                self.record['result'].setdefault(player1, {}).update({'final': str(ps1)})
                self.record['result'].setdefault(player2, {}).update({'final': str(ps2)})
                if team0 and team1:
                    self.record['result'].setdefault(player3, {}).update({'final': str(ps1)})
                    self.record['result'].setdefault(player4, {}).update({'final': str(ps2)})
                    self.record['result'].setdefault(player1, {}).update({'final': str(ps1)})
                    self.record['result'].setdefault(player2, {}).update({'final': str(ps2)})

                yield self.record

    def generate_scores(self, full_scores, node_type, players):
        result = self.record['result']
        team = 0
        for scores in full_scores:
            team += 1
            index = 0
            player = players[0]
            if team > 1:
                player = players[1]

            for score in scores:
                index += 1
                if isinstance(score, list):
                    result.setdefault(player[0], {})['S%s' %(index)] = score[0]
                    result.setdefault(player[0], {})['T%s' %(index)] = score[1]
                else:
                    result.setdefault(player[0], {})['S%s' %(index)] = score

                if 'double' in node_type:
                    if isinstance(score, list):
                        result.setdefault(player[1], {})['S%s' %(index)] = score[0]
                        result.setdefault(player[1], {})['T%s' %(index)] = score[1]
                    else:
                        result.setdefault(player[1], {})['S%s' %(index)] = score

