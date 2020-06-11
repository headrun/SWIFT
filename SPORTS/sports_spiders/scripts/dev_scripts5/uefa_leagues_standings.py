# coding=utf8
# -*- coding: utf8 -*-
# vim: set fileencoding=utf8 :
import re
import time
import datetime
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider
from vtvspider_dev import extract_data, get_nodes, extract_list_data, log, get_utc_time, get_tzinfo

class UEFALeaguesStandings(VTVSpider):
    name = 'uefa_leagues_standings'
    domain_url = 'http://www.uefa.com/'
    start_urls = []

    def start_requests(self):
        #Add the name of the tournament name (in website as key) and tournament(in db as value)
        league_link = "http://www.uefa.com/memberassociations/association=%s/domesticleague/index.html"
        league_list = ['aut','eng','den','por','sco','ger','gre','esp','sui',
                        'tur','rus','fra','swe','ned','ita', 'rou', 'ukr', 'pol',
                        'alb', 'and', 'arm', 'aze', 'blr', 'cyp', 'cze', 'est', 'fro',
                        'fin', 'mkd', 'geo', 'gib', 'hun', 'bih', 'isl', 'isr', 'kaz',
                        'lva', 'ltu', 'lux', 'mlt', 'mda', 'mne' ,'nir', 'nor', 'irl',
                        'srb', 'svk', 'svn', 'wal', 'bul', 'cro', 'bel']
        league_list = ['ger']

        for league in league_list:
            url = league_link % league
            yield Request(url, callback=self.parse, meta = {})

    def parse(self, response):
        sel = Selector(response)
        full_standings_link = extract_data(sel, '//div[@class="row more innerText"]/a[contains(text(), "Full standings")]/@href')
        if full_standings_link and "http" not in full_standings_link:
            full_standings_link = self.domain_url + full_standings_link

        yield Request(full_standings_link, callback=self.parse_standings, meta = {})

    def parse_standings(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        home_scores  = {}
        away_scores  = {}
        total_scores = {}

        tou           = extract_data(sel, '//div/h3[contains(@class, "bigTitle")]/text()')
        tou_name      = tou.split(' 20')[0]
        if "Scottish" in tou_name:
            tou_name = 'Scottish Premier League'

        season_text = extract_data(sel, '//h3[contains(@class, "vis_title")]//text()').replace('/', '-')
        season = re.findall('\w+-\d+', season_text)
        if season:
            season = season[0]
        else:
            season = re.findall('\d+', season_text)[0]

        team_nodes = get_nodes(sel, '//tr/td[contains(@class,"club l nosort ")]')# | //tr/td[@class="rank nosort innerText"]')
        for node in team_nodes:
            team_rank = extract_data(node, './../td[@class="rank nosort innerText"]/text()')
            team_link    = extract_data(node, './a/@href')
            team_sk      = team_link.split('club=')[-1].split('/')[0]
            played_games = extract_data(node, './following-sibling::td[@class="nosort"]//text()')
            import pdb;pdb.set_trace()
            #home info
            home_wins    = extract_data(node, './following-sibling::td[@class="dCol w30"][1]//text()')
            home_draws   = extract_data(node, './following-sibling::td[@class="w30"][1]//text()')
            home_lost    = extract_data(node, './following-sibling::td[@class="w30"][2]//text()')
            home_scores['home_wins'] = home_wins
            home_scores['home_draws']= home_draws
            home_scores['home_lost'] = home_lost

            #away info
            away_wins    = extract_data(node, './following-sibling::td[@class="dCol w30"][2]//text()')
            away_draws   = extract_data(node, './following-sibling::td[@class="w30"][3]//text()')
            away_lost    = extract_data(node, './following-sibling::td[@class="w30"][4]//text()')
            away_scores['away_wins'] = away_wins
            away_scores['away_draws']= away_draws
            away_scores['away_lost'] = away_lost

            #total info
            team_wins   = extract_data(node, './following-sibling::td[@class="dCol w30"][3]//text()')
            team_draws  = extract_data(node, './following-sibling::td[@class="w30"][5]//text()')
            team_lost   = extract_data(node, './following-sibling::td[@class="w30"][6]//text()')
            team_for    = extract_data(node, './following-sibling::td[@class="w30"][7]//text()')
            team_again  = extract_data(node, './following-sibling::td[@class="w30"][8]//text()')
            goal_diff   = extract_data(node, './following-sibling::td[@class="w30"][9]//text()')
            team_points = extract_data(node, './following-sibling::td[@class="dCol w30 b"]//text()')

            total_scores['total_wins']   = team_wins
            total_scores['total_draws'] = team_draws
            total_scores['total_lost']  = team_lost
            total_scores['for']   = team_for
            total_scores['against'] = team_again
            total_scores['goal_difference']  = goal_diff
            total_scores['total_points']= team_points
            total_scores['played'] = played_games
            total_scores['rank'] = team_rank

            total_scores.update(home_scores)
            total_scores.update(away_scores)

            record['source'] = 'uefa_soccer'
            record['season'] = season
            record['result_type'] = 'tournament_standings'
            record['tournament'] = tou_name
            record['result'] = {team_sk : total_scores}
            import pdb;pdb.set_trace()
            yield record

