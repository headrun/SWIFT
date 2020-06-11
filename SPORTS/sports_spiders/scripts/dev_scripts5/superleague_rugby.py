from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
import re
from scrapy.http import FormRequest

STADIUM_DICT = {'castleford_tigers': ['The Jungle (Wheldon Road)', 'Castleford', 'England'], \
                    'catalans_dragons': ['Stade Gilbert Brutus', 'Perpignan', 'France'], \
                    'huddersfield_giants': ["John Smith's Stadium", 'Huddersfield', 'England'], \
                    'hull_fc': ['KC Stadium', 'Kingston upon Hull', 'England'], \
                    'hull_kingston_rovers': ['Craven Park', 'Kingston upon Hull', 'England'], \
                    'leeds_rhinos': ['Headingley Stadium', 'Leeds', 'England'], \
                    'salford_red_devils': ['AJ Bell Stadium', 'Salford', 'England'], \
                    'wakefield_wildcats': ['Belle Vue (Wakefield)', 'Wakefield', 'England'], \
                    'widnes_vikings': ['Naughton Park', 'Widnes', 'England'], \
                    'warrington_wolves': ['Halliwell Jones Stadium', 'Warrington', 'England'], \
                    'wigan_warriors': ['DW Stadium', 'Wigan', 'England'], \
                    'st_helens': ['Langtree Park', 'St Helens', 'England'] }

DOMAIN_LINK = 'http://www.rugby-league.com'

class SuperLeagueRugby(VTVSpider):
    name ="supreleague_rugby"
    #allowed_domains= ['www.superleague.co.uk']
    start_urls = []

    def start_requests(self):
        if self.spider_type == "schedules":
            top_url = "http://www.rugby-league.com/superleague/fixtures"
            yield Request(top_url, callback = self.parse_schedues_next, meta = {})
        elif self.spider_type == "scores":
            top_url = "http://www.rugby-league.com/superleague/results"
            yield Request(top_url, callback = self.parse_score_next, meta = {})

    def parse_schedues_next(self, response):
        sel = Selector(response)
        values =  ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
        for value in values:
            #url = 'http://www.rugby-league.com/_inc/fixtures_ajax.php?season=undefined&teamID=&month=%s&compID=undefined' %(value)
            url = "http://www.rugby-league.com/_plugin/local/match_centre/logic.php?ajax_request=match_centre&load_type=fixture&start=10&qty=400&cms_params%5Bfix%5D=Yes&cms_params%5Bres%5D=No&cms_params%5Btable%5D=No&cms_params%5Bcomps%5D=71%2C1%2C6&cms_params%5Breport_page%5D=%5B%5BREPORT_PAGE%5D%5D&cms_params%5Bpreview_page%5D=%2Fsuperleague%2Fmatch_preview&compID=All+Competitions&divID=All+Divisions&teamID=All+Teams"
            formdata = {'Host': 'www.rugby-league.com',
            'Referer': 'http://www.rugby-league.com/superleague/fixtures',
            'teamID': 'All Teams',
            'Request URL': url}
            yield FormRequest(url, callback = self.parse_schedues, \
        formdata = formdata, method = 'GET', dont_filter = True)


    def parse_schedues(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        st_nodes = get_nodes(sel, '//section[@class="competition font"]//ul//li')
        for node in st_nodes:
            url = extract_data(node, './/a//@href')
            if "http" not in url:
                url = DOMAIN_LINK + url
                yield Request(url, callback=self.parse_schedules_details)

    def parse_schedules_details(self, response):
        sel =Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(sel, '//div[@class="matchpreview"]//div[contains(@class, "col-sm-4 text-xs-center hidden-sm-down")]')
        for node in nodes:
            game_det = extract_data(node, './/text()')
            team_info = extract_data(node, './/h2//text()')
            tou_name = extract_data(node, './/h4[1]//text()').strip()
            if "Super League" not in tou_name:
                continue

            game_date = extract_data(node, './/h4[3]//text()').strip() \
            .replace('th ', ' ').replace('nd ', ' ').replace('st ', ' ').replace('rd ', ' ')
            if not game_date:
                continue
            game_time = extract_data(node, './/h4[4]//text()').strip()
            if "Local" in game_time:
                game_time = game_time.split('Local Time')[-1].strip()
            if "KO:" in game_time or "GMT" in game_time:
                game_time = game_time.replace('KO: ', '').replace('GMT', '').strip()
                time_pattern = "GMT"
            if "CET" in game_time:
                game_time = game_time.split('CET')[0].strip()
                time_pattern = "CET"
            game_datetime = game_date +  " " + game_time
            game_datetime = get_utc_time(game_datetime, '%a %d %B %Y %H:%M', time_pattern)
            channel_info = extract_data(node, './/img[@alt="TV Channel"]//@src')
            if "skyHD" in channel_info:
                channel = "Sky Sports"
            else:
                channel = ""

            round_info = tou_name.split(' - ')[-1].strip().replace('RD:', 'Round:').strip()
            home_team  = team_info.split(' V ')[0].lower().replace(' ', '_').strip()
            away_team  = team_info.split(' V ')[-1].lower().replace(' ', '_').strip()

            #game_id = game_datetime.split(' ')[0].replace('-', '_') + "_" + home_team + "_" + away_team
            game_id = "".join(re.findall(r'\d+', response.url))

            final_stadium = STADIUM_DICT.get(home_team, [])
            if final_stadium:
                stadium  = final_stadium[0]
                country   = final_stadium[2]
                city      = final_stadium[1]
                state     = ''
            else:
                stadium = country = state = city = ''


            record['affiliation'] = 'irb'
            record['game_datetime'] = game_datetime
            record['game'] = 'rugby league'
            record['source'] = 'superleague_rugby'
            record['game_status'] = 'scheduled'
            record['tournament'] = "Super League"
            record['event'] = ''
            record['participant_type'] = "team"
            record['source_key'] = game_id
            record['time_unknown'] = '0'
            record['reference_url'] = response.url
            record['participants'] = { home_team: ('1',''), away_team: ('0','')}
            record['rich_data'] = {'game_note': round_info, \
                                    'channels': channel, 'location': {'city': city, \
                                    'state': state, 'country': country, 'stadium': stadium}}
            tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
            record['tz_info'] = tz_info
            if not tz_info:
                if country == "England":
                    tz_info = get_tzinfo(country = "United Kingdom", \
                            game_datetime = game_datetime)
                    record['tz_info'] = tz_info
                else:
                    tz_info = get_tzinfo(country = country, \
                            game_datetime = game_datetime)
                    record['tz_info'] = tz_info


            record['result'] = {}
            yield record
    def parse_score_next(self, response):
        sel = Selector(response)
        values =  ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
        for value in values:
            #url = 'http://www.superleague.co.uk/_inc/results_ajax.php?season=%s&teamID=&month=%s&comp=undefined' %(season, value)
            url = "http://www.rugby-league.com/superleague/results"
            formdata = {'Host': 'www.rugby-league.com',
            'Referer': 'http://www.rugby-league.com/superleague/results',
            'teamID': '',
            'Request URL': url}
            yield FormRequest(url, callback = self.parse_scores, \
            formdata = formdata, method = 'GET', dont_filter = True)


    def parse_scores(self, response):
        sel = Selector(response)
        st_nodes = get_nodes(sel, '//section[@class="competition font"]//ul//li')
        for node in st_nodes:
            home_team = extract_data(node, './/span[contains(@class, "home")]//text()').lower().replace(' ', '_')
            away_team = extract_data(node, './/span[contains(@class, "away")]//text()').lower().replace(' ', '_')
            score_link = extract_data(node, './/a/@href')
            if not score_link:
                continue
            score_link = DOMAIN_LINK + score_link
            yield Request(score_link, callback = self.parse_scores_details, \
                            meta = {'home_team': home_team, 'away_team': away_team})

    def parse_scores_details(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        home_sk = response.meta['home_team']
        away_sk = response.meta['away_team']
        game_date = extract_list_data(sel, '//div[@class="match-report-page"]//h2[1]/text()')[0].strip()
        game_date = game_date.replace('th ', ' ').replace('nd ', ' ').replace('st ', ' ').replace('rd ', ' ')
        round_info = game_date.split(' - ')[-1].title()
        game_datetime = game_date.split(' - ')[0].strip()
        final_scores = extract_list_data(sel, '//div[@class="match-report-page"]//h3[contains(text(), "FT")]//text()')[0].strip()
        half_time_scores = extract_list_data(sel, '//div[@class="match-report-page"]//h4[contains(text(), " HT")]//text()')[0].strip()
        half_time_scores = half_time_scores.replace('(', '').replace(')', '').replace('HT', '').strip()
        home_final = final_scores.split('-')[0].strip()
        away_final = final_scores.split('-')[-1].replace(' FT', '').strip()
        home_half_scores = half_time_scores.split('-')[0].strip()
        away_half_scores = half_time_scores.split('-')[-1].strip()
        if "catalans_dragons" in home_sk:
            time_pattern = "CET"
        else:
            time_pattern = "GMT"
        try:
            game_datetime = get_utc_time(game_datetime, '%A %d %b %Y %H:%M', time_pattern)
        except:
            game_datetime = get_utc_time(game_datetime, '%A %d %B %Y %H:%M', time_pattern)

        if int(home_final) > int(away_final):
            winner = home_sk
        elif int(home_final) < int(away_final):
            winner = away_sk
        else:
            winner = ''

        totla_score = home_final + "-" + away_final + " FT"
        game_sk = "".join(re.findall(r'\d+', response.url))

        record['affiliation'] = "irb"
        record['event'] = ''
        record['game'] = "rugby league"
        record['game_datetime'] = game_datetime
        record['game_status'] = "completed"
        record['participant_type'] = "team"
        record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
        record['reference_url'] = response.url
        record['source'] = "superleague_rugby"
        record['tournament'] = "Super League"
        record['source_key'] = game_sk
        record['time_unknown'] = '0'
        final_stadium = STADIUM_DICT.get(home_sk, [])
        if final_stadium:
            stadium  = final_stadium[0]
            country   = final_stadium[2]
            city      = final_stadium[1]
            state     = ''
        else:
            stadium = country = state = city = ''
        record['rich_data'] = {'game_note': round_info, \
                                'location': {'city': city, \
                        'state': state, 'country': country, 'stadium': stadium}}

        tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
        record['tz_info'] = tz_info
        if not tz_info:
            if country == "England":
                tz_info = get_tzinfo(country = "United Kingdom", \
                                game_datetime = game_datetime)
                record['tz_info'] = tz_info
            else:
                tz_info = get_tzinfo(country = country, \
                        game_datetime = game_datetime)
                record['tz_info'] = tz_info

        record['result'] = {'0': {'score': final_scores, 'winner': winner}, \
                                home_sk: {'H1': home_half_scores, \
                                'final': home_final},
                                away_sk: {'H1': away_half_scores, \
                               'final': away_final}}
        yield record


