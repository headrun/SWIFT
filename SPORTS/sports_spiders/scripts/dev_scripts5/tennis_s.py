import time
import datetime
from datetime import timedelta
from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes, get_utc_time, get_tzinfo
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
                'rakuten ', ' basel', 'barclays ', 'aircel ', '2014']

TOU_NAME  = {"350" : "German Open Tennis Championships", "192" : "Budapest Grand Prix", \
            "18" : "Hall of Fame Tennis Championships", "268" : "Swedish Open", \
            "7" : "Swiss Open (Tennis)", "121" : "Atlanta Tennis Championships", \
            "304" : "Austrian Open Kitzbuhel", "47" : "Cincinnati Masters", \
            "266" : "Cincinnati Masters", "114" : "Open de Moselle" ,\
            "148" : "Stockholm Open", "23" : "Swiss Indoors", "41": "Madrid Open (Tennis)", \
            "276" : "WTA Tour Championships", "170" : "Fed Cup", \
            "339" : "ATP World Tour Finals", "164": "Davis Cup", \
            "367" : "Shenzhen Open", "205" : "Sydney International", \
            "283" : "ATP Apia International Sydney", "348" : "Miami Masters", \
            "145" : "US Mens Clay Court Championships", "251": "Indian Wells Masters", \
            "45"  : "Indian Wells Masters", "341": "Heineken Open", "30": "Heineken Open", \
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
            "315" : "Shanghai Masters (Tennis)", \
            "197" : "BGL Luxembourg Open", "13" : "Paris Masters", \
            "345" : "HP Open", "385": "Tianjin Open", \
            "360" : "WTA Tour Championships", "115" : "Valencia Open 500",
            "382" : "WTA Tour Championships", "20": "Chennai Open", \
            "119" : "Qatar ExxonMobil Open", "336": "Brisbane International"}

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


class TennisSpiders(VTVSpider):

    name    = "tennis_spiders"
    allowed_domains = ['sports.espn.go.com', 'espn.go.com']
    start_urls = []
    record = SportsSetupItem()
    def start_requests(self):
        next_week_days = ['20141112']
        urls = 'http://sports.espn.go.com/sports/tennis/dailyResults?date=%s'
        
        if self.spider_type == "scores":
            for i in range(0, 10):
                next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))

        elif self.spider_type == "schedules":
            for i in range(0, 0):
                next_week_days.append((datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))
        for wday in next_week_days:
            t_url = urls % (wday)
            t_url = "http://sports.espn.go.com/sports/tennis/dailyResults"
            yield Request(t_url, callback = self.parse, meta = {})
        else:
            top_url = ['http://espn.go.com/tennis/schedule/_/type/women', \
                        'http://sports.espn.go.com/sports/tennis/schedule']

        for t_url in top_url:
            yield Request(t_url, callback = self.parse_preschedules, meta = {})

    def parse_preschedules(self, response):
        import pdb;pdb.set_trace()
        hxs   = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//td[contains(text(), "Current Tournaments")]/../../tr[contains(@class, "row")]')
        #tournament = extract_data(hxs, '//td[contains(text(), "Current Tournaments")]/../../tr[contains(@class, "row")]/td[2]//a/text()')
        #location = extract_data(hxs, '//td[contains(text(), "Current Tournaments")]/../../tr[contains(@class, "row")]/td[3]//text()')
        for node in nodes:
            tournament = extract_data(node, './td[2]//a/text()')
            location = extract_data(node, './td[3]//text()')
            link = extract_data(node, './td[2]//a/@href')
            yield Request(link, callback = self.parse_predetails, meta = {'tou_name': tournament, 'location': location})

    def parse_predetails(self, response):
        hxs   = HtmlXPathSelector(response)
        tournament_name = response.meta['tou_name']
        location = response.meta['location']
        if len(location)> 2:
            city = location.split(',')[-2].strip()
            country = location.split(',')[-1].strip()
            state = ''
        else:
            city = location.split(',')[0].strip()
            state = location.split(',')[1].strip()
            country = location.split(',')[-1].strip()

        url = response.url
        tz_info =get_tzinfo(city = city)
        if not tz_info:
            tz_info =get_tzinfo(country = country)
        self.record['tz_info'] = tz_info
        nodes = get_nodes(hxs, '//div[@class="span-4"]/div[@class="matchTitle"][1]/following-sibling::div[contains(@class, "span-2")]/div[@class="matchContainer"][div[@class="matchInfo"]//table//tr[td[contains(text(), "TBD") or  contains(text(), "ET")]]]')
        for node in nodes:
            game_id = extract_data(node, './@id').strip().split('-')[0].strip()
            game_type = extract_list_data(node, './../preceding-sibling::div[@class="matchTitle"]//text()')[-1]
            game_type = game_type.strip().lower()
            game_note = game_type.split(':')[0].replace('-', '').strip().title()
            game_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = 'scheduled'
            if 'single' in game_type:
                player1 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine"]//a/@href') \
                                        .strip().split('/_/id/')[-1].split('/')[0].strip()
                player2 = extract_data(node, './/div[@class="matchInfo"]//tr/td[@class="teamLine2"]//a/@href') \
                                        .strip().split('/_/id/')[-1].split('/')[0].strip()
                team1 = team0 = ''
                if not player1:
                    player1 = 'tbd1'
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
            self.record['tz_info'] = tz_info

            self.record['result'] = {}
            if "Davis Cup" in tournament_name or "Fed Cup" in tournament_name or "Australian Open" in tournament_name or "US Open" in tournament_name or "Wimbledon" in tournament_name or "French Open" in tournament_name:
                continue
            self.record['tournament'] = tournament_name
            if "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "Davis Cup" in tournament_name or \
                "Fed Cup" in tournament_name or "French Open" in tournament_name:
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
            self.record['rich_data'] =  {'channels': '',
                                        'location': {'city': city, 'state' :state,
                                        'country': country
                                        }}
            self.record['rich_data'] ['game_note'] = game_note
            tournament_name = get_refined_tou_name(tournament_name)
            ref_links = url.split('&year')[0].split('=')[-1]
            if ref_links in TOU_NAME:
                tournament_name = TOU_NAME.get(ref_links)
            else:
                tournament_name = tournament_name
            self.record['tournament'] = tournament_name
            self.record['time_unknown'] = '1'

            if status == "scheduled":
                import pdb;pdb.set_trace()
                yield self.record


    def parse(self, response):
        print response.url
        hxs = HtmlXPathSelector(response)
        if self.spider_type == "schedules" or self.spider_type == "scores":
            nodes = extract_list_data(hxs, '//div[@class="scoreHeadline"]/a/@href')
            for node in nodes:
                link = 'http://sports.espn.go.com/sports/tennis/'+ node
                link = "http://sports.espn.go.com/sports/tennis/matchSchedule?date=20141116&tournamentId=339"
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
            game_time = extract_data(node, './../preceding-sibling::div[@class="matchTitle"]//text()').strip()
            stadium = game_time.split('\n')[-1].split('- Group')[0].strip('-').strip()
            if stadium == "Hisense Arena" or stadium == "Margaret Court Arena" or stadium == "Rod Laver Arena":
                stadium = stadium
            else: stadium = ''
            game_time = game_time.split('Start Time:')[-1].strip('-').strip()
            game_datetime = game_date+'T'+game_time
            game_datetime = get_utc_time(game_datetime, '%B %d, %YT%I:%M %p ET', 'US/Eastern')
            game_data = extract_data(node, './/div[@class="matchCourt"]//text()').strip()
            game_note = game_data.split(':')[0].strip()
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
            if "57095" in game_id or '57105' in game_id:
                continue
            if "57098" in game_id and "scheduled" in status:
                continue

            self.record['source_key'] = game_id
            self.record['game_datetime'] = game_datetime
            self.record['game_status'] = game_status
            self.record['reference_url'] = url
            self.record['source'] = "espn_tennis"
            self.record['game'] = "tennis"
            self.record['result'] = {}
            if "Davis Cup" in tournament_name or "Fed Cup" in tournament_name:
                continue
            self.record['tournament'] = tournament_name
            if "Australian Open" in tournament_name or "US Open" in tournament_name or \
                "Wimbledon" in tournament_name or "Davis Cup" in tournament_name or \
                "Fed Cup" in tournament_name or "French Open" in tournament_name:
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
            self.record['rich_data'] =  {'channels': '',
                                        'location': {'city': '',
                                        'country': '',
                                        'stadium': stadium}}
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
                    elif ps1 < ps2:
                        winner = team1
                    else:
                        tie = []
                        tie.extend(team0)
                        tie.extend(team1)
                        winner = tie
                else:
                    if int(ps1) > int(ps2):
                        winner = player1
                    elif int(ps1) < int(ps2):
                        winner = player2
                    elif int(ps1)==int(ps2):
                        winner = player1+'<>'+player2

                if game_status == "walkover" or game_status == "retired":
                    winner = extract_data(node, './/td[contains(@class, "teamLine")][not(contains(@style, "font-weight:normal;"))] \
                                        /a/@href').strip().split('/_/id/')[-1].split('/')[0].strip()

                self.record['rich_data'] ['game_note'] = game_note
                final = str(ps1)+' - '+str(ps2)

                self.record['result'].setdefault('0', {}).update({'score': final})
                self.record['result'].setdefault('0', {}) ['winner'] = [winner]
                self.record['result'].setdefault(player1, {}).update({'final': str(ps1)})
                self.record['result'].setdefault(player2, {}).update({'final': str(ps2)})
                if team0 and team1:
                    self.record['result'].setdefault('0', {}) ['winner'] = winner
                    self.record['result'].setdefault(player3, {}).update({'final': str(ps1)})
                    self.record['result'].setdefault(player4, {}).update({'final': str(ps2)})
                    self.record['result'].setdefault(player1, {}).update({'final': str(ps1)})
                    self.record['result'].setdefault(player2, {}).update({'final': str(ps2)})
                import pdb;pdb.set_trace()
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

