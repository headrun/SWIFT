import datetime, time
from vtvspider_dev import VTVSpider, get_nodes, \
extract_data, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re

class CBAStandings(VTVSpider):
    name        = 'cba_standings'
    start_urls  = ['http://www.sportstats.com/basketball/china/cba/standings/?blockcontent=pos_84&table=table&table_sub=overall&ts=4YfLHqEL&dcheck=0']

    def parse(self, response):
        record          = SportsSetupItem()
        hxs             = Selector(response)
        riders          = {}
        year            = "2014-15"
        tou_name        = 'Chinese Basketball Association'
        nodes           = get_nodes(hxs, '//div[@class="stats-table-container"]//tr[contains(@class, "glib-participant")]')
        for node in nodes:
            rank         = extract_data(node, './td[contains(@class, "rank col_rank")]/text()').replace('.', '')
            team_sk      = extract_data(node, './/td//a//span/text()').lower()
            match_played = extract_data(node, './/td[@class="matches col_matches"]//text()')
            wins         = extract_data(node, './/td[@class="wins col_wins"]//text()')
            wins_ot      = extract_data(node, './/td[@class="wins_ot col_wins_ot"]//text()')
            losses_ot    = extract_data(node, './/td[@class="losses_ot col_losses_ot"]//text()')
            losses       = extract_data(node, './/td[@class="losses col_losses"]//text()')
            goals        = extract_data(node, './/td[@class="goals col_goals"]//text()')
            pct          = extract_data(node, './/td[contains(@class, "winning_percentage")]//text()')

            result = {team_sk: {'rank': rank, 'match_pl': match_played, 'wins': wins, \
                                        'wins_ot': wins_ot, 'losses_ot': losses_ot, \
                                        'losses': losses, 'points': goals, \
                                        'pct': pct}}
            record['result']           = result
            record['season']           = year
            record['source']           = 'sportstats_basketbal'
            record['tournament']       = tou_name
            record['participant_type'] = 'team'
            record['result_type']      = 'tournament_standings'
            yield record

