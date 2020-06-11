from vtvspider import VTVSpider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import extract_data, \
get_nodes, get_utc_time,extract_list_data, get_tzinfo

DOMAIN = 'http://www.fih.ch'

class FIHHockey(VTVSpider):
    name = 'fih_hockey'
    start_urls =['http://fih.ch/events/hockey-world-league/']

    def parse(self, response):
        sel = Selector(response)
        events = extract_list_data(sel,'//div[@id="round_tabs"]/div[@class="content women"]/div[@class="table_container"]/table/tr/td/a/@href')
        for event in events[:1]:
            url = DOMAIN + event + "/pools-matches"
            print url
            yield Request(url, callback = self.gamelinks)

    def gamelinks(self, response):
        sel = Selector(response)
        nodes = sel.xpath('//div[@class="match"]/table/tr')
        for node in nodes:
            game_link1 = extract_data(node,'./td[@class="col_1_5"]/a/@href')
            game_link = DOMAIN + game_link1
            game_status = extract_data(node,'./td[@class="col_1_5"]/text()')
            yield Request(game_link, callback = self.details, meta = {'game_status' : game_status})

    def details(self,response):
        sel =Selector(response)
        record = SportsSetupItem()
        game_id = response.url.split('=')[-1]
        game_status = response.meta['game_status']
        home_sk = extract_data(sel,'//div[@class="grid_2 prefix_1 suffix_1 home_team"]/span/text()').strip().lower() + '_women'
        away_sk = extract_data(sel,'//div[@class="grid_2 prefix_1 suffix_1 away_team"]/span/text()').strip().lower()+ '_women'
        match = extract_data(sel,'//ul[@class="match_info"]/li[span[contains(text(),"Match")]]/text()')
        date = extract_data(sel,'//ul[@class="match_info"]/li[span[contains(text(),"Date")]]/text()').split(',')[-1].strip()
        time = extract_data(sel,'//ul[@class="match_info"]/li[span[contains(text(),"Time")]]/text()')
        date_time = date + " " + time
        game_datetime = get_utc_time(date_time, "%d %b %Y %H:%M", 'America/Argentina/Cordoba')
        game_note = extract_data(sel,'//ul[@class="match_info"]/li[span[contains(text(),"Pool")]]/text()')
        scores = extract_data(sel,'//div[@class="grid_2 prefix_1 suffix_1 score"]/text()').replace('\n','').replace('\t', '')
        venue = extract_data(sel,'//ul[@class="match_info"]/li[span[contains(text(),"Pool")]]/text()')
        report = extract_data(sel,'//div[@class="body_text"]//p/text()')

        if "Official" in game_status:
            game_status = 'completed'

        else:
            game_status = 'scheduled'

        if "hockey-world-league-final-rosario-2015" in response.url:
            event = "Women's FIH Hockey World League Final"
        elif "hockey-world-league-semi-final" in response.url:
            event = "Women's FIH Hockey World League Semifinals"
        elif "hockey-world-league-round-2" in response.url:
            event = "Women's FIH Hockey World League Round2"
        elif "hockey-world-league-round-1" in response.url:
            event = "Women's FIH Hockey World League Round1"

        participants = {home_sk: (1, home_sk), away_sk: (0, away_sk)}
        record['source_key'] = game_id
        record['game'] = 'field hockey'
        record['source'] = 'fih_hockey'
        record['participant_type'] = 'team'
        record['rich_data'] = {'location': {'city': 'Rosario', 'state': 'Santa Fe',  \
        'country': "Argentina", 'stadium': 'Estadio Mundialista de Hockey'}}
        record['rich_data']['game_note'] = game_note
        record['participants'] = participants
        record['participant_type'] = 'team'
        record['event'] = event
        record['tournament'] = "Women's FIH Hockey World League"
        record['affiliation'] = 'fih'
        record['reference_url'] = response.url
        record['game_status'] = game_status
        record['tz_info'] = get_tzinfo(city = 'Rosario', game_datetime = game_datetime)
        record['game_datetime'] = game_datetime
        record['time_unknown'] = 0

        if self.spider_type =='schedules' and game_status == "scheduled":
            record['result'] = {}
            yield record

        elif self.spider_type == 'scores' and  game_status == "completed":
            if "(" not in scores:
                scores = scores.split('-')
                home_score = scores[0].strip()
                away_score = scores[1].strip()
                results = {'0': {'score' : home_score + "-"  + away_score}, \
                         home_sk : {'final': home_score , 'H1' : '0'},
                         away_sk : {'final': away_score , 'H1' : '0'}}

                if game_status == 'completed':
                    if int(home_score) > int(away_score):
                        results['0']['winner'] = home_sk
                    elif int(away_score) < int(home_score):
                        results['0']['winner'] = away_sk

            if "(" in scores:
                scores = scores.split('(')
                final_scores = scores[0].split('-')
                shoot_out = scores[1].split(' ')
                shoot_out_scores = shoot_out[0].split('-')
                home_score = final_scores[0].strip()
                away_score = final_scores[1].strip()
                shoot_out_home_score = shoot_out_scores[0].strip()
                shoot_out_away_score = shoot_out_scores[1].strip(')')
                results = {'0': {'score' : scores[0].strip().replace(' - ', '-') + (SO)},\
                            home_sk : {'final' : home_score, 'H1' : '0', 'SO' : shoot_out_home_score},
                            away_sk : {'final' : away_score, 'H1' : '0', 'SO' : shoot_out_away_score}
                                  }
                if game_status == 'completed':
                    if int(shoot_out_home_score) > int(shoot_out_away_score):
                        results['0']['winner'] = home_sk
                    elif int(shoot_out_away_score) < int(shoot_out_home_score):
                        results['0']['winner'] = away_sk


            record['result'] = results
            yield record
