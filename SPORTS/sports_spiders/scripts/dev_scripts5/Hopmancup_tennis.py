from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, get_nodes, extract_data, extract_list_data, log, get_utc_time, get_tzinfo
import time
import datetime

def X(data):
   try:
       return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
   except:
       return data.encode('utf8')

record = SportsSetupItem()
class HopmanCup(VTVSpider):

    name = "hopmancup_tennis"
    #allowd_domains = ['hopmancup.com']
    start_urls = []


    def start_requests(self):
        if self.spider_type == "schedules":
            top_url = "http://hopmancup.com/schedules-draws/order-of-play"
            yield Request(top_url, callback = self.parse_schedules, meta = {})

        elif self.spider_type == "scores":
            top_url = "http://hopmancup.com/results/completed-matches"
            yield Request(top_url, callback = self.parse_scores, meta = {})

    def parse_schedules(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        tou_name = extract_data(hxs, '//h2[contains(text(), "schedule")]//text()').split('2016')[0].strip()
        tou_year = extract_data(hxs, '//h2[contains(text(), "schedule")]//text()'). \
            split(' ')[2].replace('provisional schedule', '').strip()
        nodes = get_nodes(hxs, '//table/tbody//tr')
        date = ''
        for node in nodes[1:]:
            game_date = extract_data(node, './/td[1]//text()')
            if game_date:
                date = game_date
            else:
                game_date = date

            game_time = extract_data(node, './/td[2]//text()')
            game_details = extract_data(node, './/td[3]//text()')
            home_sk = game_details.split(' v ')[0]
            away_sk = game_details.split(' v ')[1]
            if "Group A" in home_sk:
                home_sk ="TBD1"
            if "Group B" in away_sk:
                away_sk = "TBD2"
            game_sk = home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
            game_datetime = game_date + " " + tou_year + " " +game_time
            if "." in game_time:
                pattern = "%A %d %B %Y %I.%M%p"
            else:
                pattern = "%A %d %B %Y %I%p"
            game_datetime = get_utc_time(game_datetime, pattern, "Australia/Perth")
            game_sk = game_datetime.split(' ')[0].replace('-', '_')+ "_" +game_sk
            today_date = datetime.datetime.now()
            if str(today_date) <= str(game_datetime):
                status = "scheduled"
            else:
                status = "completed"
            record['game'] = "tennis"
            record['source_key'] = game_sk
            record['participant_type'] = "team"
            record['tournament'] = "Hopman Cup"
            record['game_status'] = status
            record['source'] = "hopmancup_tennis"
            record['time_unknown'] = 0
            record['game_datetime'] = game_datetime
            record['reference_url'] = response.url
            record['rich_data'] = {'location': {'city': 'Perth', 'state': 'Western Australia', \
                        'country': "Australia", 'stadium': 'Perth Arena'}}

            record['tz_info'] = get_tzinfo(city = "Perth", country = "Australia")
            record['participants'] = {home_sk : ('0', ''), away_sk : ('0', '')}
            record['result'] = {}
            if status == "scheduled":
                yield record

    def parse_scores(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//div[@class="text"]//p')
        season = extract_data(hxs, '//div[@class="cup-date mob-hide"]//h4/text()').split(' ')[-1]
        for node in nodes:
            data = extract_list_data(node, './/b//text()')
            if not data:
                data = extract_list_data(node, './/strong//text()')
            pl_data = extract_list_data(node, './/text()')
            if not data or "v" in data[0]:
                continue
            if "-" not in data[0]:
                continue
            if "Day" in data[0]:
                game_time = "10am"
            elif "Night" in data[0]:
                game_time = "5.30pm"
            game_date = extract_list_data(node, './preceding-sibling::h4/text()')[-1]. \
            encode("utf8").replace('\xc2\xa0', ' ')
            game_date = game_date + " " + season + " " +game_time
            if "." in game_time:
                pattern = "%A %d %B %Y %I.%M%p"
            else:
                pattern = "%A %d %B %Y %I%p"
            game_datetime = get_utc_time(game_date, pattern, "Australia/Perth")
            if len(data) > 1:
                away_id  = data[0].replace('Day: ', ''). \
                replace(' d', '').replace('Night', '').strip()
                away_id  = X(away_id)
                home_id = data[1].replace('d ', '').strip()
                scores = data[-1]
                away_score = scores.split('-')[0]
                home_score = scores.split('-')[1]
            else:
                details = data[0].replace('*', '').split(' d ')
                if len(details) == 5:
                    away_id  = details[1].replace('Night:', '').replace('Day: ', '').strip()
                    away_id  = X(away_id)
                    if "Great" in away_id:
                        away_id ="Great Britain"

                    home_id = details[3].split(' ')[0].strip()
                    if "France" in home_id:
                        home_id = "France"
                else:
                    if len(details) == 1:
                        away_id = details[0].split('d ')[-1].encode('utf-8').replace('\xc2', '').replace('\xa0', ' ').split(' ')[0]
                        home_id = details[0].split('d ')[0].replace('Night:', '').replace('Day:', '').strip()
                        scores = details[0].split('d ')[-1].encode('utf-8').replace('\xc2', '').replace('\xa0', ' ').split(' ')[-1]
                    else:
                        away_id  = details[0].replace('Night:', '').replace('Day:', '').strip()
                        home_id = "".join(details[1].split(' ')[:1])
                        scores = details[-1].split(' ')[-1].strip()
                    if "Italy" in home_id:
                        home_id = "Italy"
                    if "Czech" in away_id:
                        away_id  =  "Czech Republic"
                    if len(details) !=1:
                        if "Australia Gold" in details[1] and "Australia" in home_id:
                            home_id = "Australia Gold"
                        if "Australia Green" in details[1] and "Australia" in home_id:
                            home_id = "Australia Green"
                home_score = scores.split('-')[1]
                away_score = scores.split('-')[0]
            game_id = home_id.replace(' ', '_') + "_" + away_id.replace(' ', '_')
            game_sk = game_datetime.split(' ')[0].replace('-', '_')+ "_" +game_id

            if "2016_01_03_Germany_Australia_Green" in game_sk:
                game_sk = "2016_01_03_Australia_Green_Germany"
            if "2016_01_03_Czech_Republic_Australia_Gold" in game_sk:
                game_sk = "2016_01_03_Australia_Gold_Czech_Republic"
            if "2016_01_04_USA_Ukraine" in game_sk:
                game_sk = "2016_01_04_Ukraine_USA"
            record['game'] = "tennis"
            record['source_key'] = game_sk
            record['tournament'] = "Hopman Cup"
            record['game_status'] = "completed"
            record['source'] = "hopmancup_tennis"
            record['time_unknown'] = 0
            record['affiliation'] = "atp_wta"
            record['game_datetime'] = game_datetime
            record['reference_url'] = response.url
            record['rich_data'] = {'location': {'city': 'Perth', 'state': 'Western Australia', \
                        'country': "Australia", 'stadium': 'Perth Arena'}}
            record['tz_info'] = get_tzinfo(city = "Perth", country = "Australia")
            record['result'] = {}
            record['participants'] = {home_id : ('0', ''), away_id : ('0', '')}
            if int(home_score) > int(away_score):
                winner = home_id
            elif int(home_score) < int(away_score):
                winner = away_id
            else:
                winner = ''
            record['result'] = {home_id : {'final': home_score}, away_id :{'final': away_score}}
            record['result'].setdefault('0', {}) ['winner'] = winner
            record['result'].setdefault('0', {}) ['score'] = away_score + "-" + home_score
            player_data = extract_data(node, '//text()')
            yield record

            if pl_data:
                    for pl_dat in pl_data:
                        if "Day:" in pl_dat or "Night:" in pl_dat or "-" not in pl_dat:
                            continue
                        pl_details = pl_dat.replace('*', '').split(' d')
                        home_id = pl_details[0].split('(')[0].strip()
                        if "(AUS)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(AUS)')[0].strip()
                            scores = pl_details[-1].split('(AUS)')[-1].strip()
                        if "(GER)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(GER)')[0].strip()
                            scores = pl_details[-1].split('(GER)')[-1].strip()
                        if "(CZE)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(CZE)')[0].strip()
                            scores = pl_details[-1].split('(CZE)')[-1].strip()

                        if "(UKR)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(UKR)')[0].strip()
                            scores = pl_details[-1].split('(UKR)')[-1].strip()

                        if "(USA)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(USA)')[0].strip()
                            scores = pl_details[-1].split('(USA)')[-1].strip()

                        if "(FRA)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(FRA)')[0].strip()
                            scores = pl_details[-1].split('(FRA)')[-1].strip()

                        if "(UKR)" in pl_details[-1]:
                            away_id = pl_details[-1].split('(UKR)')[0].strip()
                            scores = pl_details[-1].split('(UKR)')[-1].strip()



                        pl_game_sk = game_datetime.split(' ')[0] + "_"  + home_id + "_" + away_id
                        if "/" in pl_details[0]:
                            home_player_id  = pl_details[0].split('/')
                            away_player_id  = pl_details[-1].strip().split(' ')[0].split('/')
                            if len(away_player_id):
                                continue
                            _score = pl_details[-1].strip().replace(' (Match TB)', '').split(' ')[1:]
                            home_id1 = home_player_id[0].replace('(CZE)', ''). \
                            replace('(AUS)', '').replace('(FRA)', ''). \
                            replace('(GBR)', '').replace('(USA)', ''). \
                            replace('(UKR)', '').replace('').strip()
                            away_id1 = away_player_id[0].replace('(CZE)', ''). \
                            replace('(AUS)', '').replace('(FRA)', ''). \
                            replace('(GBR)', '').replace('(USA)', ''). \
                            replace('(UKR)', '').replace('').strip()
                            home_id2 = home_player_id[1].replace('(CZE)', ''). \
                            replace('(AUS)', '').replace('(FRA)', ''). \
                            replace('(GBR)', '').replace('(USA)', ''). \
                            replace('(UKR)', '').replace('').strip()
                            awat_id2 = away_player_id[1].replace('(CZE)', ''). \
                            replace('(AUS)', '').replace('(FRA)', ''). \
                            replace('(GBR)', '').replace('(USA)', ''). \
                            replace('(UKR)', '').replace('').strip()
                            pl_game_sk = game_datetime.split(' ')[0] + "_"  + home_id1.strip() + "_" + away_id1
                        else:
                            home_player_id = [home_id]
                            away_player_id = [away_id]
                            _score = scores.split(' ')
                        result = self.generate_scores(_score, home_player_id, away_player_id)
                        record['result'] = result
                        if "/" not in pl_details[0]:
                            record['participants'] = { home_id: (0, ''), \
                                                    away_id: (0, '')}
                            record['result'].setdefault('0', {})['winner'] = home_id
                        else:
                            record['participants'] = {home_id1.strip(): (1, ''), \
                                            home_id2:(1, ''), \
                                            away_id1: (2, ''), \
                                            away_id2 : (2, '')}
                            record['result'].setdefault('0', {})['winner'] = [home_id1, away_id2]
                        record['source_key'] = pl_game_sk
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
                result.setdefault(home_player_id[0].strip(), {})['S%s' % (counter)] = home_score
                result.setdefault(away_player_id[0].strip(), {})['S%s' % (counter)] = away_score
                if extras:
                    result.setdefault(home_player_id[0].strip(), {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id[0].strip(), {})['T%s' % (counter)] = extras2
                score = ' - '.join([str(home_final), str(away_final)])
                result.setdefault('0', {})['score'] = score
                result.setdefault(home_player_id[0].strip(), {}).update({'final': str(home_final)})
                result.setdefault(away_player_id[0].strip(), {}).update({'final': str(away_final)})
            else:
                result.setdefault(home_player_id[0].strip(), {})['S%s' % (counter)] = home_score
                result.setdefault(home_player_id[1].strip(), {})['S%s' % (counter)] = home_score
                if len(away_player_id) == 1:
                    continue
                result.setdefault(away_player_id[0].strip(), {})['S%s' % (counter)] = away_score
                result.setdefault(away_player_id[1].strip(), {})['S%s' % (counter)] = away_score
                if extras:
                    result.setdefault(home_player_id[0].strip(), {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id[1].strip(), {})['T%s' % (counter)] = extras2
                    result.setdefault(home_player_id[1].strip(), {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id[0].strip(), {})['T%s' % (counter)] = extras2
                score = ' - '.join([str(home_final), str(away_final)])
                result.setdefault('0', {})['score'] = score
                result.setdefault(home_player_id[0].strip(), {}).update({'final': str(home_final)})
                result.setdefault(away_player_id[0].strip(), {}).update({'final': str(away_final)})
                result.setdefault(away_player_id[1].strip(), {}).update({'final': str(away_final)})
                result.setdefault(home_player_id[1].strip(), {}).update({'final': str(home_final)})
        return result 
