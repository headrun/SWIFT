# -*- coding: utf-8 -*-
from vtvspider_new import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest
from scrapy_spiders_dev.items import SportsSetupItem


STADIUM_CITY_DICT = { 'Pau': ['Stade du Hameau', '', 'France', '', 'Pau'], \
                      'Agen': ['Stade Armandie', '', 'France', '', 'Agen'], \
                      'Albi': ["Stadium Municipal d'Albi", '', 'France', '', 'Albi'], \
                      'Aurillac': ['Stade Jean Alric', '', 'France', '', 'Aurillac'], \
                      'Bourgoin': ['Stade Pierre Rajon', '', 'France', '', 'Bourgoin-Jallieu'], \
                      'Carcassonne': ['Stade Albert Domec ', '', 'France', '', 'Carcassonne'], \
                      'Colomiers': ['Stade Michel Bendichou', '', 'France', '', 'Colomiers'], \
                      'Dax': ['Stade Maurice Boyau', '', 'France', '', 'Dax'], \
                      'Massy': ["Stade Jules Ladoumegue", '', 'France', '', 'Massy'], \
                      'Mont-de-Marsan': ['Stade Guy Boniface', '', 'France', '', 'Mont-de-Marsan'], \
                      'Montauban': ['Stade Sapiac', '', 'France', '', 'Montauban'], \
                      'Narbonne': ["Parc des Sports Et de l'Amitie", '', 'France', '', 'Narbonne'], \
                      'Perpignan': ['Stade Aime Giral', '', 'France', '', 'Perpignan'], \
                      'Tarbes': ['Stade Maurice Trelut', '', 'France', '', 'Tarbes'], \
                      'Biarritz': ['Parc des Sports Aguilera', '', 'France', '', 'Biarritz'], \
                      'Bziers': ['Stade de la Mediterranee', '', 'France', '', 'Bziers']}


class RugbyProDSpiders(VTVSpider):
    name = "ugbyprod_spiders"
    #allowed_domains = ["www.lnr.fr"]
    start_urls = ['http://www.lnr.fr/calendrier-resultats-pro-d2.html']
    record = SportsSetupItem()

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//select[@class="sel-seljournee"]//option')
        import pdb;pdb.set_trace()
        for node in nodes:
            value = extract_data(node, './/@value')
            yield FormRequest(url="http://www.lnr.fr/spip.php?page=calend-resultats-filtre.inc", method = "post",
                                formdata={'competition': 'pro-d2', 'id_competition': '108', \
                                'id_journee': value, 'id_latest_journee': '2618', \
                                'id_premiere_journee': '2554', 'id_rubrique': '3', \
                                'var_mode': 'recalcul', 'page': 'calend-resultats-filtre.inc'},
                            callback=self.parse_details, meta={"ref_url": response.url})


    def parse_details(self, response):
        hxs   = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(hxs, '//ul//li[@class="clearfix"]')
        for node in nodes:
            url_ = extract_data(node, './/a[@class="renc-feuille fancy-match-feuille"]//@href')
            details = extract_list_data(node, './/h5[@class="date"]//span//text()')
            date = details[0].encode('utf-8').replace('\xc3\xa0', ''). \
                                replace('\xc3\xa9', '').replace('\t', '').replace('\r', ''). \
                                replace('\n', '').replace('\xbb', '').replace('\xc3', '').strip()
            date = date.replace('aot', 'august').replace('janvier', 'january'). \
                    replace('bre', 'ber').replace('\xa9', '').replace('fvrier', 'february'). \
                    replace('dcember', 'december').replace('  ', ''). \
                    replace('mars', 'march')
            date = date.replace('avril', 'april').replace('mai', 'may')
            if "17 may" not in date:
                continue
            print date
            teams = extract_list_data(node, './/h3//text()')
            print teams
            home_sk = teams[0].encode('utf-8').replace('\t', '').replace('\r', ''). \
                            replace('\n', '').replace('\xc3\xa9', '').strip()
            away_sk = teams[-1].encode('utf-8').replace('\xc3\xa9', '').strip()
            record['affiliation'] = "irb"
            record['event'] = ''
            record['game'] = "rugby union"
            record['participant_type'] = "team"
            record['source'] = "lnr_rugby"
            record['time_unknown'] = '0'
            record['tournament'] = "Rugby Pro D2"

            schedule_page = extract_data(node, './/a[@class="lien-faceaface"]//@href')

            scores = extract_data(node, './/a[@title="Derouler le match"]//span//text()')
            if url_ and self.spider_type == "scores":
                url = "http://www.lnr.fr" +url_
                if "http://www.lnr.fr/fdm,12080.html" in url:
                    continue
                yield Request(url, callback = self.parse_scores,  \
            meta = {'date': date, 'scores': scores, 'stadium': ""})
            elif schedule_page and self.spider_type == "schedules":
                schedule_page = "http://www.lnr.fr" +schedule_page
                yield Request(schedule_page, callback = self.parse_schedules, \
                        meta = {'record': record, 'home_sk':home_sk, \
                        'away_sk':away_sk, 'date':date})

    def parse_schedules(self, response):
        hxs     = Selector(response)
        record  = response.meta['record']
        home_sk = response.meta['home_sk']
        away_sk = response.meta['away_sk']
        date = response.meta['date']
        game_date = extract_data(hxs, '//div[@class="header"]//span//text()').split(' ')[2]
        game_date  = date.split(' ')[1]+ date.split(' ')[2] + date.split(' ')[3] + " " + game_date
        pattern = "%d%B%Hh%M %Y"
        game_datetime = get_utc_time(game_date, pattern, 'Europe/Paris')
        game_sk = game_datetime.split(' ')[0].replace('-', '_') + "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
        record['game_datetime'] = game_datetime
        record['source_key'] = game_sk
        record['game_status'] = "scheduled"
        record['reference_url'] = response.url
        final_stadium = STADIUM_CITY_DICT.get(home_sk, [])
        if final_stadium:
            stadium  = final_stadium[0]
            continent = final_stadium[1]
            country   = final_stadium[2]
            state     = final_stadium[3]
            city      = final_stadium[4]
        else:
            stadium   = ''
            continent = ''
            country   = ''
            state     = ''
            city      =  ''
        record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
        record['rich_data'] = {'location': {'city': city, 'country': country, \
                                           'continent': continent, 'state': state, \
                                           'stadium': stadium}}
        tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
        record['tz_info'] = tz_info
        if not tz_info:
            record['tz_info'] = get_tzinfo(country = country, \
                        game_datetime = game_datetime)
        yield record

    def parse_scores(self, response):
        import pdb;pdb.set_trace()
        hxs  = Selector(response)
        record = SportsSetupItem()
        scores = response.meta['scores']
        date = response.meta['date']
        date_time = date.split(' ')[-1].replace('h', ':')
        stadium = response.meta['stadium']
        home_sk = extract_list_data(hxs, '//p[@class="noms-clubs"]/a/@title')[-1]. \
                        split(' ')[0].strip().encode('utf-8').replace('\xc3\xa9', '')
        away_sk = extract_list_data(hxs, '//p[@class="noms-clubs"]/a/@title')[-1]. \
                        split(' ')[2].strip().encode('utf-8').replace('\xc3\xa9', '')
        home_half_time_goals = extract_list_data(hxs, '//span[@class="score-mt"]//text()')[0]
        away_half_time_goals = extract_list_data(hxs, '//span[@class="score-mt"]//text()')[1]
        home_final = scores.split(' ')[0]
        away_final = scores.split(' ')[1].split(')')[-1]
        det = extract_data(hxs, '//p/text()')
        date_ = det.split(',')[-2].split('-')[-1].encode('utf-8') \
                    .replace('\xc3', '').replace('\xbb', '').strip() \
                    .replace('aot', 'august').replace('janvier', 'january'). \
                    replace('bre', 'ber').replace('\xa9', '').replace('fvrier', 'february'). \
                    replace('dcember', 'december').replace('mars', 'march'). \
                    replace('avril', 'april').replace('mai', 'may')
        stadium = det.split(',')[-1].encode('utf-8').replace('\xc3\xa0', ''). \
                                replace('\xc3\xa9', '').strip()
        game_datetime = date_ + " " +date_time
        pattern = "%d %B %Y %H:%M"
        game_datetime  = get_utc_time (game_datetime, pattern, 'Europe/Paris')

        final_stadium = STADIUM_CITY_DICT.get(home_sk, [])
        if final_stadium:
            stadium  = final_stadium[0]
            continent = final_stadium[1]
            country   = final_stadium[2]
            state     = final_stadium[3]
            city      = final_stadium[4]
        else:
            stadium   = stadium
            continent = ''
            country   = ''
            state     = ''
            city      =  ''

        tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
        record['tz_info'] = tz_info
        if not tz_info:
            record['tz_info'] = get_tzinfo(country = country, \
                game_datetime = game_datetime)

        if int(home_final) > int(away_final):
            winner = home_sk
        elif int(home_final) < int(away_final):
            winner = away_sk
        else:
            winner = ''

        totla_score = home_final + "-" + away_final + " FT"

        game_sk = game_datetime.split(' ')[0].replace('-', '_') + "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
        record['affiliation'] = "irb"
        record['event'] = ''
        record['game'] = "rugby union"
        record['game_datetime'] = game_datetime
        record['game_status'] = "completed"
        record['participant_type'] = "team"
        record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
        record['reference_url'] = response.url
        record['source'] = "lnr_rugby"
        record['source_key'] = game_sk
        record['time_unknown'] = '0'
        record['tournament'] = "Rugby Pro D2"
        record['rich_data'] = {'location': {'city': city, 'country': country, \
                                       'continent': continent, 'state': state, \
                                       'stadium': stadium}}
        record['result'] = {'0': {'score': totla_score, 'winner': winner}, \
                                home_sk: {'H1': home_half_time_goals, \
                                'final': home_final},
                                away_sk: {'H1': away_half_time_goals, \
                               'final': away_final}}

        yield record

