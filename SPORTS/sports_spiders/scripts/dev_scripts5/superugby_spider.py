from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
import re


class SuperRugbySpider(VTVSpider):
    name = "superrugby_spider"
    allowed_domains = []
    start_urls = []
    record = SportsSetupItem()

    def start_requests(self):
        fixtures = []
        top_url = 'http://en.espn.co.uk/super-rugby-2015/rugby/series/242041.html?noredir=1;template=%s'
        if self.spider_type == "schedules":
            fixtures.append('fixtures')
        else:
            fixtures.append('results')
        for fixer in fixtures:
            top_urls = top_url % fixer
            yield Request(top_urls, callback = self.parse)

    def parse(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        if self.spider_type == "schedules":
            nodes = get_nodes(hxs, '//div[contains(@id, "scrumArticlesBox")]//table[@id="tableSearch"]//tr')
            for node in nodes:
                date = extract_data(node, './/td[@class="fixtureTableDate"]//text()').strip()
                if not date:
                    continue
                yr  = extract_list_data(node, './/preceding-sibling::tr/td[@class="fixtureTblDateColHdr"]/a//text()')[-1]
                _date = date + " " + yr
                teams_info = extract_data(node, './/td[@class="fixtureTblContent"]//text()')
                teams_link = extract_data(node, './/td[@class="fixtureTblContent"]//a//@href')
                if teams_link:
                    continue

                team_sk = teams_info.split(',')[0].split(' v ')
                home_sk = team_sk[0].lower().replace('final:', '').strip().replace(' ', '_')
                away_sk = team_sk[1].lower().strip().replace(' ', '_')
                if "TBC" in team_sk:
                    continue

                if "final" in team_sk[0].lower():
                    game_note  = team_sk[0].strip().split(':')[0].strip()
                else:
                    game_note  = ""
                if "GMT" in teams_info:
                    _time = "".join(re.findall('local, (.*)GMT', teams_info)).strip()
                    date_time = _date + " " + _time
                    pattern = '%a %d %B %Y %H:%M'
                    game_datetime = get_utc_time(date_time, pattern, 'GMT')
                    game_sk = game_datetime.split(' ')[0].replace('-', '_') + "_" \
                    + home_sk.replace(' ', '_') + "_" +     away_sk.replace(' ', '_')
                else:
                    date_time = _date.strip()
                    pattern = '%a %d %B %Y'
                    game_datetime = get_utc_time(date_time, pattern, 'GMT')
                    game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                    "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')

                if len(teams_info.split(',')) > 1:
                    stadium = teams_info.split(',')[1].strip()
                    if "local" in stadium:
                        stadium = stadium.replace('local', '')
                        if ":" in stadium:
                            stadium = "".join(re.findall('(.*)\d+\d+:\d+\d+', stadium))
                            stadium = stadium
                loc_info = extract_list_data(node, './/td[@class="fixtureTblContent"]//text()')
                if len(loc_info[1].split(',')) == 3:
                    stadium = loc_info[1].split(',')[1].strip()
                    city    = loc_info[1].split(',')[-1].strip()
                else:
                    city    = ''
                if city in ["Hamilton", "Dunedin"]:
                    country = "New Zealand"
                else:
                    country = ""
                tz_info = get_tzinfo(city = city, \
                            game_datetime = game_datetime)
                record['tz_info'] = tz_info
                if not tz_info:
                    tz_info = get_tzinfo(city = city, country = country, \
                                game_datetime = game_datetime)

                record['affiliation'] = 'irb'
                record['game_datetime'] = game_datetime
                record['game'] = 'rugby union'
                record['source'] = 'espn_rugby'
                record['game_status'] = 'scheduled'
                record['event'] = ''
                record['tournament'] = "Super Rugby"
                record['participant_type'] = "team"
                record['source_key'] = game_sk
                record['time_unknown'] = '0'
                record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
                record['rich_data'] = {'location': {'city': city, 'country': country,
                                       'stadium': stadium}, 'game_note': game_note}
                record['reference_url'] = response.url
                record['result'] = {}
                import pdb;pdb.set_trace()
                yield record
        else:
            nodes = get_nodes(hxs, '//div[@id="scrumArticlesBoxContent"]/table//tr')
            date = ''
            for node in nodes:
                game_date = extract_data(node, './td[@class="fixtureTableDate"]/text()')
                if game_date:
                    date = game_date
                tou_name = extract_data(node, './td[2]/text()')
                link = extract_data(node, './td[@class="fixtureTblContent"]/a/@href')
                score_link = "http://www.espnscrum.com" + link.replace('match', 'current/match') + "?view=matchscore"
                yield Request(score_link, callback = self.parse_scores, \
                        meta = {'tou_name': tou_name})

    def parse_scores(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        node = extract_data(hxs, '//td[@class="liveSubNavText"]/text()')
        _city = node.split(',')[0].strip().split('- ')[1].strip()
        date = node.split(',')[1].strip()
        time_ = node.split(',')[-1].strip()
        if time_:
            date_time = date + " " + time_
            pattern = '%d %B %Y %H:%M GMT'
        else:
            date_time = date
            pattern = '%d %B %Y'
        _date = get_utc_time(date_time, pattern, 'GMT')
        scores = extract_data(hxs, '//td[@class="liveSubNavText1"]//text()')
        hm_scores =  scores.split(' - ')[0]
        aw_scores = scores.split(' - ')[1]
        home_sk = re.findall('(.*) \(\d+\) \d+', hm_scores)[0].replace(' ', '_').lower()
        home_half_time_goals = re.findall('.*(\(\d+)\)', hm_scores)[0].replace('(', '')
        home_final  = re.findall('.* (\d+)', hm_scores)[0]
        away_sk  = re.findall('.*\) (.* \(FT)\)', aw_scores)[0] \
        .split('(FT')[0].strip().lower().replace(' ', '_')
        away_half_time_goals = re.findall('\d+ (\(\d+)\).*', aw_scores)[0] \
                                .replace('(', '')
        away_final =  re.findall('(\d+) .*', aw_scores)[0]
        game_sk = _date.split(' ')[0].replace('-', '_') + "_" + \
        home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
        tou_name = extract_data(hxs, '//td[@class="liveSubNavText"]//text()').split('-')[0].strip()

        if int(home_final) > int(away_final):
            winner = home_sk
        elif int(home_final) < int(away_final):
            winner = away_sk
        else:
            winner = ''

        totla_score = home_final + "-" + away_final + " FT"
        if _city in ["Hamilton", "Dunedin"]:
            country = "New Zealand"
        else:
            country = ""
        tz_info = get_tzinfo(city = _city, game_datetime = _date)
        record['tz_info'] = tz_info
        if not tz_info:
            tz_info = get_tzinfo(city = _city, country = country, \
                            game_datetime = _date)
        record['tz_info'] = tz_info
        record['affiliation'] = "irb"
        record['event'] = ''
        record['game'] = "rugby union"
        record['game_datetime'] = _date
        record['game_status'] = "completed"
        record['participant_type'] = "team"
        record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
        record['reference_url'] = response.url
        record['source'] = "espn_rugby"
        record['source_key'] = game_sk
        record['time_unknown'] = '0'
        record['tournament']   = tou_name
        record['rich_data'] = { 'location': {'city': _city, 'country': country}}
        record['result'] = {'0': {'score': totla_score, 'winner': winner}, \
                            home_sk: {'H1': home_half_time_goals, 'final': home_final},
                            away_sk: {'H1': away_half_time_goals,'final': away_final}}
        yield record
