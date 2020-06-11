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

class ScandbasketballStandings(VTVSpider):
    name = 'scand_standings'
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
                league_link = league_link +"/standings"
                yield Request(league_link, callback = self.parse_next, \
                meta = {'league_name': league_name})


    def parse_next(self, response):
        sel = Selector(response)
        league_name = response.meta['league_name']
        record = SportsSetupItem()
        season = extract_list_data(sel, '//div[@class="main wrapper clearfix container"]//h2//text()')[0]
        season = season.split(' ')[0].replace('2015-2016', '2015-16').strip()
        st_nodes  = get_nodes(sel, '//table//tr')

        for st_node in st_nodes:
            rank = extract_data(st_node, './/td[1]//text()')
            team_link = extract_data(st_node, './/td[2]//a//@href') 

            if not team_link:
                continue

            team_sk = team_link.split('/team/')[-1].split('/')[0].strip()
            wins = extract_data(st_node, './/td[3]//text()')
            losses = extract_data(st_node, './/td[4]//text()')
            pct    = extract_data(st_node, './/td[5]//text()')
            gb_    = extract_data(st_node, './/td[6]//text()')
            l10    = extract_data(st_node, './/td[7]//text()')
            strk   = extract_data(st_node, './/td[8]//text()')
            ppg    = extract_data(st_node, './/td[9]//text()')
            oppg   = extract_data(st_node, './/td[10]//text()')
            pts_diff = extract_data(st_node, './/td[11]//text()')
            home   = extract_data(st_node, './/td[12]//text()')
            away  = extract_data(st_node, './/td[13]//text()')

            record['tournament'] = TOU_DICT[league_name]
            record['season'] = season
            record['source'] = "basketball_realgm"
            record['participant_type'] = "team"
            record['result_type'] = "tournament_standings"
            record['affiliation'] = 'euro-nba'
            record['result']= { team_sk: {'win': wins, 'loss': losses, 'pct': pct, 'gb': gb_,'home': home, \
                                       'away': away, 'ppg': ppg, 'oppg': oppg,'diff': pts_diff, \
                                       'strk': strk, 'l10': l10, 'rank': rank}}
            yield record



