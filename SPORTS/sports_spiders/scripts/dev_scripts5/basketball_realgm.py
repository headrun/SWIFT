from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, get_nodes, \
extract_data, extract_list_data, get_tzinfo, get_utc_time
from scrapy.http import Request
from scrapy.selector import Selector
import re
import datetime

DOMAIN_LINK = "http://basketball.realgm.com"

TOU_DICT = {'Icelandic Dominos League': 'Premier League (Iceland)', \
            'Norwegian BLNO': 'BLNO', 'Swedish Basketligan': 'Basketligan', \
            'Danish Basketligaen': 'Basketligaen'}

LEAGUE_LIST = ['Icelandic Dominos League', 'Norwegian BLNO', \
               'Swedish Basketligan', 'Danish Basketligaen']

class BasketballRealgm(VTVSpider):
    name = 'basketball_realgm'
    start_urls = ['http://basketball.realgm.com/international/leagues']

    def parse(self, response):
        sel = Selector(response)
        league_nodes = get_nodes(sel, '//div[@class="content linklist"]//a')

        for league_node in league_nodes:
            league_link = extract_data(league_node, './/@href')
            if "http" not in league_link:
                league_link = DOMAIN_LINK + league_link
            league_name = extract_data(league_node, './/text()').strip()
            if league_name in LEAGUE_LIST:
                yield Request(league_link, callback = self.parse_next, \
                meta = {'league_name': league_name})


    def parse_next(self, response):
        sel = Selector(response)
        now = datetime.datetime.now()
        schedules_list = []
        scores_list = []
        league_name = response.meta['league_name']
        if self.spider_type == "schedules":

            for i in range(0, 30):
                schedules_list.append((now + datetime.timedelta(days=i)).strftime('%Y-%m-%d'))
            for schedules in schedules_list:
                link = response.url +  "/schedules/" + schedules
                print link
                yield Request(link, callback= self.parse_schedules, meta = {'league_name': league_name})

        elif self.spider_type == "scores":
            for i in range(0, 50):
                scores_list.append((now - datetime.timedelta(days=i)).strftime('%Y-%m-%d'))
            for scores in scores_list:
                link = response.url +  "/schedules/" + scores
                yield Request(link, callback= self.parse_schedules, meta = {'league_name': league_name})



    def parse_schedules(self, response):
        sel = Selector(response)
        sc_nodes = get_nodes(sel, '//table[@class="basketball compact"]//tr')
        record = SportsSetupItem()
        league_name = response.meta['league_name']
        for sc_node in sc_nodes:
            game_date = extract_data(sc_node, './/td[@data-th="Date"]//text()')
            away_team = extract_data(sc_node, './/td[@data-th="Away Team"]/a/@href')
            home_team = extract_data(sc_node, './/td[@data-th="Home Team"]/a/@href')
            status = extract_data(sc_node, './/td[@data-th="Result"]/a/text()')
            stadium = extract_data(sc_node, './/td[@data-th="Venue"]/a/text()')
            venue = extract_list_data(sc_node, './/td[@data-th="Venue"]/text()')
            ref_url = extract_data(sc_node, './/td[@data-th="Result"]/a/@href')

            if not ref_url:
                continue

            if "http" not in ref_url:
                ref_url = DOMAIN_LINK + ref_url

            away_sk = away_team.split('/team/')[-1].split('/')[0].strip()
            home_sk = home_team.split('/team/')[-1].split('/')[0].strip()
            game_date = extract_list_data(sc_node, './/td[@data-th="Date"]//text()')
            game_datetime = game_date[0]  + " " + game_date[1]
            game_datetime = get_utc_time(game_datetime, '%b %d, %Y %I:%M %p ET', 'US/Eastern')
            city = country = ''
            if venue:
                venue = venue[0]
                city = venue.split(',')[0].strip()
                country = venue.split('(')[-1].replace(')', '').strip()
            if  city in ['Danish Basketligaen', 'Icelandic Dominos League', \
                'Norwegian BLNO', 'Swedish Basketligan']:
                import pdb;pdb.set_trace()
                city = ''
                country = ''

            if status == "Preview":
                game_status = "scheduled"
                source_key = ref_url.split('preview')[-1].strip()
            else:
                game_status = "completed"
                source_key = ref_url.split('boxscore')[-1].strip()
            record['affiliation'] = 'euro-nba'
            record['event'] = ''
            record['game'] = 'basketball'
            record['game_datetime'] = game_datetime
            record['game_status'] = game_status
            record['participant_type'] = "team"
            record['participants'] = {away_sk : (0, ''), \
                                     home_sk : (1, '')}
            record['tournament'] = TOU_DICT[league_name]
            record['source'] = "basketball_realgm"
            record['time_unknown'] = 0
            record['rich_data'] = {'location': {'city': city, 'country': country, 'stadium': stadium}}
            record['tz_info'] = get_tzinfo(city = city, country = country, game_datetime = game_datetime)

            if not record['tz_info'] :
                record['tz_info'] = get_tzinfo(country = country, game_datetime = game_datetime)
            record['source_key'] = source_key
            record['reference_url'] = response.url

            if game_status == "scheduled":
                record['result'] = {}
                yield record

            if game_status == "completed":
                yield Request(ref_url, callback = self.parse_scores_next, meta = {'record': record})

    def parse_scores_next(self, response):
        sel = Selector(response)
        record =response.meta['record']
        team_details = extract_list_data(sel, '//div[@class="boxscore-gamedetails"]//a[contains(@href, "team")]//@href')
        home_link = team_details[1]
        away_link = team_details[0]
        home_sk = home_link.split('/team/')[-1].split('/')[0].strip()
        away_sk = away_link.split('/team/')[-1].split('/')[0].strip()
        away_scores = extract_list_data(sel, '//table[@class="basketball force-table"]//tr[1]//td//text()')
        home_scores = extract_list_data(sel, '//table[@class="basketball force-table"]//tr[2]//td//text()')
        game_status = "completed"
        source_key = response.url.split('boxscore')[-1].strip()
        if away_scores:
            away_q1 = away_scores[1]
            away_q2 = away_scores[2]
            away_q3 = away_scores[3]
            away_q4 = away_scores[4]
            away_final_score = away_scores[5]
        if home_scores:
            home_q1 = home_scores[1]
            home_q2 = home_scores[2]
            home_q3 = home_scores[3]
            home_q4 = home_scores[4]
            home_final_score = home_scores[5]


        if game_status == "completed":
            if (int(away_final_score) > int(home_final_score)):
                winner = away_sk
            elif int(away_final_score) < int(home_final_score):
                winner = home_sk
            else:
                winner = ''

        game_score = home_final_score + '-' + away_final_score


        record['result'] = {'0': {'winner': winner, 'score': game_score}, \
                                home_sk: {'Q1': home_q1, 'Q2': home_q2, \
                                'Q3': home_q3, 'Q4': home_q4, \
                                'final': home_final_score}, \
                                away_sk: { 'Q1': away_q1, 'Q2': away_q2, \
                                'Q3': away_q3, 'Q4': away_q4, \
                                'final': away_final_score}}
        record['reference_url'] = response.url
        record['source_key'] = source_key
        yield record



