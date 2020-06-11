import re
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, get_nodes, extract_list_data, extract_data


class EspnStandings(VTVSpider):
    name = 'espn_standings'
    start_urls = []

    def start_requests(self):
        top_urls = {'Ecuadorian Serie A' : 'http://www.espnfc.com/tables/_/league/ecu.1/primera-a-de-ecuador?cc=4716',
                  'Paraguan Primera Division' : 'http://www.espnfc.com/tables/_/league/par.1/primera-division-de-paraguay?cc=4716',
                  'Campeonato Brasileiro Serie A' : 'http://www.espnfc.com/tables/_/league/bra.1/futebol-brasileiro?cc=4716',
                  'Liga de Futbol Profesional Boliviano' : 'http://www.espnfc.com/tables/_/league/bol.1/liga-profesional-boliviana?cc=4716',
                  'Argentine Primera Division' : 'http://www.espnfc.com/tables/_/league/arg.1/primera-division-de-argentina?cc=4716',
                  'Chilean Primera Division' : 'http://www.espnfc.com/tables/_/league/chi.1/primera-division-de-chile?cc=4716',
                  'Uruguan Primera Division' : 'http://www.espnfc.com/tables/_/league/uru.1/primera-division-de-uruguay?cc=4716',
                  'Venezuelan Primera Division' : 'http://www.espnfc.com/tables/_/league/ven.1/primera-division-de-venezuela?cc=4716',
                  'Peruvian Primera Division': 'http://www.espnfc.com/tables/_/league/per.1/primera-profesional-de-peru?cc=4716',
                  'Liga MX': 'http://www.espnfc.com/mexican-liga-mx/22/table',
                  "English Conference": "http://www.espnfc.com/english-conference/27/table",
                  'Australian A-League': "http://www.espnfc.us/australian-a-league/1308/table",
                  "Ligue 2": "http://www.espnfc.com/french-ligue-2/96/table",
                  "English League Championship": "http://www.espnfc.com/english-league-championship/24/table",
                  "Serie B" : "http://www.espnfc.com/italian-serie-b/99/table",
                  "Eerste Divisie" : "http://www.espnfc.com/dutch-eerste-divisie/105/table",
                  "Honduras Primera Division" : "http://www.espnfc.com/dutch-eerste-divisie/105/table",
                  "German Bundesliga 2": "http://www.espnfc.com/german-2-bundesliga/97/table"}

        for tou, url in top_urls.iteritems():
            yield Request(url, callback=self.parse, meta = {'tou' : tou})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        season = extract_data(hxs, '//div[@id="tables-season-dropdown"]//p//span//text()')
        if season:
            season = season.replace(' / 20', '-')

        nodes = get_nodes(hxs, '//table[@data-fixed-columns="2"]/tbody/tr[not(contains(@class, "columns"))]')
        for node in nodes:
            items = SportsSetupItem()
            pos = extract_data(node, './td[@class="pos"]/text()')
            team = extract_data(node, './td[@class="team"]/text()')
            team_id = "".join(re.findall(r'/(\d+)/index', extract_data(node, './td[@class="team"]/a/@href')))
            if not team_id:
                team_id = team.replace(' ', '-').lower()
            team_score = extract_list_data(node, './td[@class="groupA"]/text()')
            if len(team_score) == 6:
                totp, totw, totd, totl, totf, tota = team_score
            else:
                totp = extract_data(node, './td[4]/text()')
                totw = extract_data(node, './td[5]/text()')
                totd = extract_data(node, './td[6]/text()')
                totl = extract_data(node, './td[7]/text()')
                totf = extract_data(node, './td[8]/text()')
                tota = extract_data(node, './td[9]/text()')
            gd = extract_data(node, './td[@class="gd"]/text()')
            pts = extract_data(node, './td[@class="pts"]/text()')

            items['tournament'] = response.meta['tou']
            items['season'] = season
            items['source'] = 'espn_soccer'
            items['game'] = 'soccer'
            items['participant_type'] = 'team'
            items['result_type'] = 'tournament_standings'
            items['result'] = {team_id : {'rank' : pos, 'played' : totp, 'total_wins' : totw, \
                               'total_draws' : totd, 'total_lost' : totl, 'for' : totf, 'against' : tota,\
                               'goal_difference' : gd, 'total_points' : pts}}
            yield items

