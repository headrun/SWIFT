import re
from vtvspider_dev import VTVSpider, extract_data
from vtvspider_dev import get_nodes
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

class SoccerwayStandings(VTVSpider):
    name        = "soccerway_standings"
    start_urls  = ['http://int.soccerway.com/competitions/club-domestic/?ICID=TN_03_01']
    dont_filter = True
    domain_url  = "http://www.soccerway.com"

    leagues     = {'China PR': 'national/china-pr/csl/2015',
                    'Norway': '/eliteserien/',
                    'Sweden': '/allsvenskan/',
                    'Finland': '/veikkausliiga/',
                    'Iceland': '/urvalsdeild/',
                    'Denmark': '/denmark/superliga/',
                    'Wales': '/premier-league/',
                    'Scotland': '/premier-league/',
                    'Northern Ireland': '/ifa-premiership/',
                    'England': '/premier-league/',
                    'Spain': '/national/spain/primera-division/',
                    'Germany': '/national/germany/bundesliga/',
                    'Italy': 'national/italy/serie-a/',
                    'France': '/national/france/ligue-1/'}

    tou_dict    = {'China PR': 'Chinese Super League',
                    'Norway': 'Tippeligaen',
                    'Sweden': 'Allsvenskan',
                    'Finland': 'Veikkausliiga',
                    'Iceland': 'Urvalsdeild',
                    'Denmark': 'Danish Superliga',
                    'Scotland': 'Scottish Premier League',
                    'Wales': 'Welsh Premier League',
                    'Northern Ireland': 'NIFL Premiership',
                    'England': 'Premier League',
                    'Spain': 'La Liga',
                    'Germany': 'Bundesliga',
                    'Italy': 'Serie A',
                    'France': 'Ligue 1'}


    source      = 'soccerway_soccer'
    standings   = 'tournament_standings'
    team        = 'team'

    def parse(self, response):
        hdoc = Selector(response)
        leagues = get_nodes(hdoc, '//li[@class="expandable "]/div[@class="row"]/a')

        for league in leagues:
            league_name = extract_data(league, './text()')

            if league_name in self.leagues.keys():
                league_link = extract_data(league, './@href')

                if league_link:
                    league_link = self.domain_url + league_link
                    yield Request(league_link, callback = self.parse_next, \
                            meta={'l_name': league_name})

    def parse_next(self, response):
        tou_name = self.tou_dict[response.meta['l_name']]
        hdoc = Selector(response)
        link = extract_data(hdoc, '//div[@class="clearfix"]//ul/li/a[contains(text(), "Tables")]/@href')
        if link:
            req_link = self.domain_url + link
            yield Request(req_link, self.parse_standings, \
                    meta={'tou_name': tou_name})

    def parse_standings(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        details = re.findall('\d+', response.url)
        round_ = re.findall('r(\d+)', response.url)
        season = response.url.split('/regular')[0].split('/')[-1]

        if "-phase/" in response.url:
            season = response.url.split('-phase/')[0]. \
            replace('/1st', '').replace('/2nd', '').split('/')[-1]

        season = season.replace('20152016', '2015-16'). \
        replace('20142015', '2014-15').replace('20162017', '2016-17')

        if '1st-phase' in response.url:
            nodes = get_nodes(hxs, '//table[@data-round_id="%s"]/tbody/tr' % (round_[0]))
        else:
            nodes = get_nodes(hxs, '//table[@data-round_id="%s"]/tbody/tr' % (round_[0]))

        for node in nodes:
            rank = extract_data(node, './td[contains(@class, "rank")]//text()')
            team_sk = extract_data(node, './@data-team_id')
            played = extract_data(node, './td[contains(@class, "total mp")]//text()')
            total_won = extract_data(node, './td[contains(@class,"total won")]//text()')
            total_draw = extract_data(node, './td[contains(@class, "total_drawn")]//text()')
            totla_loss = extract_data(node, './td[contains(@class, "total_lost")]//text()')
            goals_for = extract_data(node, './td[contains(@class, "total_gf")]//text()')
            against = extract_data(node, './td[contains(@class, "total_ga")]//text()')
            goal_diff = extract_data(node, './td[@class="number gd"]//text()')
            points = extract_data(node, './td[contains(@class, "points")]//text()')

            if not against:
                continue

            if response.meta['tou_name'] == "Chinese Super League":

                record['result'] = {team_sk: {'played': played,
                                  'total_won': total_won,
                                  'total_draw': total_draw,
                                  'total_loss': totla_loss,
                                  'goals_for': goals_for,
                                  'against': against,
                                  'goal_diff': goal_diff,
                                  'points': points,
                                  'rank': rank}}
            else:
                record['result'] = {team_sk: {'played': played,
                                    'total_wins': total_won,
                                    'total_lost': totla_loss,
                                    'total_draws': total_draw,
                                    'goal_difference': goal_diff,
                                    'for': goals_for,
                                    'against': against,
                                    'total_points': points,
                                    'rank': rank}}

            record['tournament'] = response.meta['tou_name']
            record['result_type'] = self.standings
            record['source'] = self.source
            record['participant_type'] = self.team
            record['season'] = season
            yield record
