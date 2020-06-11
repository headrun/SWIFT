from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
        get_nodes, extract_list_data, get_utc_time, get_tzinfo
import datetime

def X(data):
   try:
       return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
   except:
       return data.encode('utf8')

record  = SportsSetupItem()

class HopmanCupSpider(VTVSpider):

    name = "hopmancups_tennis"
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
        #tou_name = extract_data(hxs, '//h2[contains(text(), "schedule")]//text()').split('2015')[0].strip()
        #tou_year = extract_data(hxs, '//h2[contains(text(), "schedule")]//text()'). \
            #split('2015')[1].replace('provisional schedule', '').strip()
        tou_year = extract_data(hxs, '//div[@class="cup-date mob-hide"]//h4//text()').split(' ')[-1].strip()
        stadium = extract_data(hxs, '//div[@class="cup-date mob-hide"]//h3//text()')
        nodes = get_nodes(hxs, '//table[@class="matchSchedule"]/tbody//tr')
        date = ''
        for node in nodes:
            game_date = extract_data(node, './/td[1]//text()')
            if game_date:
                date = game_date
            else:
                game_date = date

            game_time_ = extract_list_data(node, './/td[2]//text()')
            game_detailss = extract_list_data(node, './/td[3]//text()')
            count = 0
            for game_details in game_detailss:

                if " v " not in game_details:
                    continue
                game_details = game_details
                count += 1

                if count == 1 and len(game_time_) != 1:
                    game_time = game_time_[1]
                elif "Group A" in game_details:
                    game_time = game_time_[0]
                else:
                    game_time = game_time_[-2]

                game_details = game_details
                home_sk = game_details.split(' v ')[0]
                away_sk = game_details.split(' v ')[1]

                if "Group A" in home_sk:
                    home_sk = "TBD1"
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
                record['rich_data'] = {'location': {'city': 'Perth', 'state': 'Western Australia',  \
                'country': "Australia", 'stadium':stadium}}
                record['tz_info'] = get_tzinfo(country = "Australia")
                record['participants'] = {home_sk : ('0', ''), away_sk : ('0', '')}
                record['result'] = {}
                if status == "scheduled":
                    yield record

    def parse_scores(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="text"]//p')
        for node in nodes:
            data = extract_list_data(node, './/strong//text()')
            if not data or "v" in data[0]:
                continue
            if "(" in data[0]:
                home_player_id = data[0].split(' d ')[0].split('(')[0].strip()
                if "/" in home_player_id:
                    home_player_id1 = home_player_id.split(' / ')[0]
                    home_player_id2 = home_player_id.split(' / ')[1]
                if "6(" in data[0]:
                    away_player_id = data[0].split(' d ')[1].split(' (')[0].strip()
                    score_ = data[0].split(' d ')[1].split(' (')[1].split(') ')[-1].strip()
                else:
                    away_player_id = data[0].split(' d ')[1].split('(')[0].strip()
                    score_ = data[0].split(' d ')[1].split(')')[-1].strip().replace('Matt Ebden ', '')
                if "Matt Ebden" in away_player_id:
                    away_player_id = "Matt Ebden"
                if "/" in away_player_id:
                    away_player_id1 = away_player_id.split(' / ')[0]
                    away_player_id2 = away_player_id.split(' / ')[1] 
                _score = score_.split(' ')
                game_date = extract_list_data(node, './preceding-sibling::h4/text()')[-1].encode("utf8").replace('\xc2\xa0', ' ')
                game_date = game_date + " " + "2015"+ " " + "5.30pm"
                game_datietime_ = get_utc_time(game_date, "%A %d %B %Y %I.%M%p", "Australia/Perth")
                game_sk = home_player_id.replace(' ', '_') + "_" + away_player_id.replace(' ', '_')
                #game_datietime_  = ''
                record['game'] = "tennis"
                record['source_key'] = game_sk
                record['participant_type'] = "team"
                record['tournament'] = "Hopman Cup"
                record['game_status'] = "completed"
                record['source'] = "hopmancup_tennis"
                record['time_unknown'] = 0 
                record['game_datetime'] = game_datietime_
                record['reference_url'] = response.url
                record['rich_data'] = {'location': {'city': '', 'state': 'Perth', 'country': "Australia"}}
                record['tz_info'] = get_tzinfo(country = "Australia")
                record['participants'] = {home_player_id: ('0', ''), away_player_id: ('0', '')}
                if "/" in away_player_id:
                    record['participants'] = {home_player_id1 : (1, ''), \
                                            home_player_id2 :(1, ''), \
                                            away_player_id1 :(2, ''), \
                                            away_player_id2 : (2, '')}

                self.generate_scores(_score, home_player_id, away_player_id)
                yield record
            else:
                if "Day" in data[0]:
                    game_time = "10am"
                elif "Night" in data[0]:
                    game_time = "5.30pm"
                game_date = extract_list_data(node, './preceding-sibling::h4/text()')[-1].encode("utf8").replace('\xc2\xa0', ' ')
                game_date = game_date + " " + "2015" + " " +game_time
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
                    details = data[0].split(' ')
                    if len(details) == 5:
                        away_id  = details[1].replace('Night:', '').replace('Day: ', '').strip()
                        if "Great" in away_id:
                            away_id  = "Great Britain"
                        home_id = details[3].split(' ')[0].strip()
                        if "France" in home_id:
                            home_id = "France"
                        if "Czech" in home_id:
                            home_id = "Czech Republic"
                    else:
                        away_id  = details[1].replace('Night:', '').replace('Day: ', '').strip()
                        home_id = details[2]
                        if "Italy" in home_id:
                            home_id = "Italy"
                        if "France" in away_id:
                            away_id = "France"
                        if "Czech" in away_id:
                            away_id  =  "Czech Republic"
                    scores = details[-1].strip()
                    home_score = scores.split('-')[1]
                    away_score = scores.split('-')[0]
                game_id = home_id.replace(' ', '_') + "_" + away_id.replace(' ', '_')
                game_sk = game_datetime.split(' ')[0].replace('-', '_')+ "_" +game_id
                if "2015_01_05_France_Great_Britain" in game_sk:
                    game_sk = "2015_01_05_Great_Britain_France"
                if "2015_01_06_Italy_Czech_Republic":
                    game_sk = "2015_01_06_Czech_Republic_Italy"
                record['game'] = "tennis"
                record['source_key'] = game_sk
                record['participant_type'] = "team"
                record['tournament'] = "Hopman Cup"
                record['game_status'] = "completed"
                record['source'] = "hopmancup_tennis"
                record['time_unknown'] = 0
                record['game_datetime'] = game_datetime
                record['reference_url'] = response.url
                record['rich_data'] = {'location': {'city': '', 'state': 'Perth', 'country': "Australia"}}
                record['tz_info'] = get_tzinfo(country = "Australia")
                record['result'] = {}
                record['participants'] = {home_id : ('0', ''), away_id : ('0', '')}
                if int(home_score) > int(away_score):
                    winner = home_id
                elif int(home_score) < int(away_score):
                    winner = away_id
                else:
                    winner = ''
                record['result'] = {home_id : {'final': home_score}, away_id :{'final': away_score}}
                record['result'].setdefault('0', {}) ['winner'] = [winner]
                record['result'].setdefault('0', {}) ['score'] = away_score + "-" + home_score
                player_data = extract_data(node, '//text()')
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
            if '/' not in home_player_id:
                result.setdefault(home_player_id, {})['S%s' % (counter)] = home_score
                result.setdefault(away_player_id, {})['S%s' % (counter)] = away_score
                if extras:
                    result.setdefault(home_player_id, {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id, {})['T%s' % (counter)] = extras2
                score = ' - '.join([str(home_final), str(away_final)])
                result.setdefault('0', {})['score'] = score
                result.setdefault(home_player_id, {}).update({'final': str(home_final)})
                result.setdefault(away_player_id, {}).update({'final': str(away_final)})
            else:
                home_player_id1 = home_player_id.split(' / ')[0]
                home_player_id2 = home_player_id.split(' / ')[1]
                away_player_id1 = away_player_id.split(' / ')[0]
                away_player_id2 = away_player_id.split(' / ')[1]
                result.setdefault(home_player_id1, {})['S%s' % (counter)] = home_score
                result.setdefault(home_player_id2, {})['S%s' % (counter)] = home_score
                result.setdefault(away_player_id1, {})['S%s' % (counter)] = away_score
                result.setdefault(away_player_id2, {})['S%s' % (counter)] = away_score
                if extras:
                    result.setdefault(home_player_id1, {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id1, {})['T%s' % (counter)] = extras2
                    result.setdefault(home_player_id2, {})['T%s' % (counter)] = extras1
                    result.setdefault(away_player_id2, {})['T%s' % (counter)] = extras2
                score = ' - '.join([str(home_final), str(away_final)])
                result.setdefault('0', {})['score'] = score
                result.setdefault(home_player_id1, {}).update({'final': str(home_final)})
                result.setdefault(away_player_id1, {}).update({'final': str(away_final)})
                result.setdefault(away_player_id2, {}).update({'final': str(away_final)})
                result.setdefault(home_player_id2, {}).update({'final': str(home_final)})

