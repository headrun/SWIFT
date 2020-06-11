from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data 
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

STANDINGS_URLS = {'division': 'http://espn.go.com/nhl/standings/_/group/3', \
                  'conference': 'http://espn.go.com/nhl/standings/_/group/2', \
                  'league': 'http://espn.go.com/nhl/standings/_/group/1'}

class ESPNNHLStandings(VTVSpider):
    '''Declaring class variables'''
    name = "espnnhl_standings"
    allowed_domains = []
    start_urls = ['http://espn.go.com/nhl/standings']

    def parse(self, response):
        sel = Selector(response)
        season = extract_data(sel, '//div[@class="mod-content"]/h2/text()')
        if season:
            for standings_type, url in STANDINGS_URLS.iteritems():
                req_url = url
                yield Request(req_url, callback = self.parse_standings, meta = {'standings_type': standings_type})


    def parse_standings(self, response):
        sel = Selector(response)
        season = extract_data(sel, '//div[@class="mod-content"]/h2/text()')
        season = season.replace('NHL Standings - ', '').strip()
        record = SportsSetupItem()
        if response.meta['standings_type'] == 'division':
            game_nodes = get_nodes(sel, '//div[@class="mod-content"]')
            for game_node in game_nodes:
                nodes = get_nodes(game_node, '//table[@class="tablehead"]//tr[contains(@class, "team")]')
                count = 0
                for node in nodes:
                    division     = "NHL " + extract_list_data(node, './/../preceding-sibling::tr[@class="colhead"]/td/text()')[-1]
                    count += 1
                    rank = count
                    if count > 8:
                        rank = count - 8
                    if 'CENTRAL' in division and rank > 8:
                        rank = count - 16
                    if 'PACIFIC' in division and rank > 8:
                        rank = count - 23
                    team        = extract_data(node, './td[1]/a[1]/@href')
                    team = team.split('/')[-2].upper()
                    game_played = extract_data(node, './td[2]/text()')
                    wins        = extract_data(node, './td[3]/text()')
                    losses      = extract_data(node, './td[4]/text()')
                    ot_losses   = extract_data(node, './td[5]/text()')
                    pct         = extract_data(node, './td[6]/text()')
                    row         = extract_data(node, './td[7]/text()')
                    sow         = extract_data(node, './td[8]/text()')
                    sol         = extract_data(node, './td[9]/text()')
                    home        = extract_data(node, './td[10]//text()')
                    away        = extract_data(node, './td[11]//text()')
                    goal_for    = extract_data(node, './td[12]/text()')
                    goal_against = extract_data(node, './td[13]/text()')
                    diff        = extract_data(node, './td[14]//text()')
                    last_10     = extract_data(node, './td[15]/text()')
                    streak      = extract_data(node, './td[16]/text()')

                    record['participant_type']  =  'team'
                    record['result_type']  = 'group_standings'
                    record['source'] =  'espn_nhl'
                    record['season'] = season
                    record['tournament'] = division
                    record['result'] = {team : {'w':wins, 'l': losses,'row' : row, 'rank' : rank, \
                                         'ot' : ot_losses, 'pct' : pct, 'gp': game_played, \
                                         'gf': goal_for, 'ga': goal_against, 'diff': diff, 'home': home, 'away': away, \
                                         'so' : sow + "-" + sol, 'l10' : last_10, 'streak' : streak}}
                    yield record
        elif response.meta['standings_type'] == 'conference':
            game_nodes = get_nodes(sel, '//div[@class="mod-content"]//table[@class="tablehead"]')
            for game_node in game_nodes:
                nodes = get_nodes(game_node, '//tr[contains(@class, "team")]')
                count = 0
                for node in nodes:
                    count  = count +1
                    rank = count
                    if count >16:
                        rank = count - 16
                    division     = "NHL " + extract_list_data(node, './/../preceding-sibling::tr[@class="colhead"]/td/text()')[-1].strip() + " conference"
                    team        = extract_data(node, './td[1]/a[1]/@href')
                    team = team.split('/')[-2].upper()
                    game_played = extract_data(node, './td[2]/text()')
                    wins        = extract_data(node, './td[3]/text()')
                    losses      = extract_data(node, './td[4]/text()')
                    ot_losses   = extract_data(node, './td[5]/text()')
                    pct         = extract_data(node, './td[6]/text()')
                    row         = extract_data(node, './td[7]/text()')
                    sow         = extract_data(node, './td[8]/text()')
                    sol         = extract_data(node, './td[9]/text()')
                    home        = extract_data(node, './td[10]//text()')
                    away        = extract_data(node, './td[11]//text()')
                    goal_for    = extract_data(node, './td[12]/text()')
                    goal_against = extract_data(node, './td[13]/text()')
                    diff        = extract_data(node, './td[14]//text()')
                    last_10     = extract_data(node, './td[15]/text()')
                    streak      = extract_data(node, './td[16]/text()')

                    record['participant_type']  =  'team'
                    record['result_type']  = 'group_standings'
                    record['source'] =  'espn_nhl'
                    record['season'] = season
                    record['tournament'] = division
                    record['result'] = {team : {'w':wins, 'l': losses,'row' : row, 'rank' : rank, \
                                         'ot' : ot_losses, 'pct' : pct, 'gp': game_played, \
                                         'gf': goal_for, 'ga': goal_against, 'diff': diff, 'home': home, 'away': away, \
                                         'so' : sow + "-" + sol, 'l10' : last_10, 'streak' : streak}}
                    yield record
        elif response.meta['standings_type'] == 'league':
            game_nodes = get_nodes(sel, '//div[@class="mod-content"]')
            for game_node in game_nodes:
                nodes = get_nodes(game_node, '//table[@class="tablehead"]//tr[contains(@class, "team")]')
                count = 0
                for node in nodes:
                    count += 1
                    team        = extract_data(node, './td[1]/a[1]/@href')
                    team = team.split('/')[-2].upper()
                    game_played = extract_data(node, './td[2]/text()')
                    wins        = extract_data(node, './td[3]/text()')
                    losses      = extract_data(node, './td[4]/text()')
                    ot_losses   = extract_data(node, './td[5]/text()')
                    pct         = extract_data(node, './td[6]/text()')
                    row         = extract_data(node, './td[7]/text()')
                    sow         = extract_data(node, './td[8]/text()')
                    sol         = extract_data(node, './td[9]/text()')
                    home        = extract_data(node, './td[10]//text()')
                    away        = extract_data(node, './td[11]//text()')
                    goal_for    = extract_data(node, './td[12]/text()')
                    goal_against = extract_data(node, './td[13]/text()')
                    diff        = extract_data(node, './td[14]//text()')
                    last_10     = extract_data(node, './td[15]/text()')
                    streak      = extract_data(node, './td[16]/text()')

                    record['participant_type']  =  'team'
                    record['result_type']  = 'tournament_standings'
                    record['source'] =  'espn_nhl'
                    record['season'] = season
                    record['tournament'] = "National Hockey League"
                    record['result'] = {team : {'w':wins, 'l': losses,'row' : row, 'rank' : count, \
                                         'ot' : ot_losses, 'pct' : pct, 'gp': game_played, \
                                         'gf': goal_for, 'ga': goal_against, 'diff': diff, 'home': home, 'away': away, \
                                         'so' : sow + "-" + sol, 'l10' : last_10, 'streak' : streak}}
                    yield record

