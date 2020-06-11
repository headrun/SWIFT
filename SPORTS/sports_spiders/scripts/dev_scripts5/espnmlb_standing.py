from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes

DIVISIONS = {'American LeagueNational League EAST' : 'NL East', \
             'American LeagueNational League CENTRAL' : 'NL Central', \
             'American LeagueNational League WEST' : 'NL West', \
             'American League EAST' : 'AL East', \
             'American League WEST' : 'AL West', \
             'American League CENTRAL' : 'AL Central'}

class MLBStandings(VTVSpider):
    name = "mlbstandings"
    start_urls = []

    def start_requests(self):
        top_urls = {'division' : 'http://espn.go.com/mlb/standings', \
                    'conference' : 'http://espn.go.com/mlb/standings/_/group/5', \
                    'league' : 'http://espn.go.com/mlb/standings/_/group/9'}
        for standings_type, url in top_urls.iteritems():
            yield Request(url, callback = self.parse, meta = {'standings_type': standings_type})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        season = extract_data(hxs, '//div[@class="mod-content"]//h1//text()')
        season = season.split('-')[1].strip()
        if response.meta['standings_type'] == 'division':
            nodes = get_nodes(hxs, '//div[@class="mod-content"]//table[@class="tablehead"]//tr[@class="colhead"]')
            for node in nodes:
                leagues = extract_data(node, './preceding-sibling::tr[@class="stathead"]//td[contains(text(), "League")]/text()')
                sub_league = extract_data(node, './td/text()')
                sub_league = leagues + " " + sub_league
                division = DIVISIONS.get(sub_league, '')
                sc_nodes = get_nodes(node, './/following-sibling::tr[contains(@class, "team")]')
                count = 0
                for sc_n in sc_nodes[:5]:
                    count = count + 1
                    team_sk = extract_data(sc_n, './td/a/@href').split('/')[-2]
                    team_sk = team_sk.upper()
                    wins = extract_data(sc_n, './td[2]/text()')
                    lows = extract_data(sc_n, './td[3]/text()')
                    pct = extract_data(sc_n, './td[4]/text()')
                    gb_s = extract_data(sc_n, './td[5]/text()')
                    home = extract_data(sc_n, './td[6]/text()')
                    road = extract_data(sc_n, './td[7]/text()')
                    l10  = extract_data(sc_n, './td[12]/text()')
                    strk = extract_data(sc_n, './td[11]/text()')
                    if "Lost" in strk:
                        strk = strk.replace('Lost  ', 'L')
                    elif "Won" in strk:
                        strk = strk.replace('Won  ', 'W')
                    record  =  SportsSetupItem()
                    record['participant_type']  = 'team'
                    record['result_type']      = 'group_standings'
                    record['source'] =  'MLB'
                    record['affiliation'] = 'mlb'
                    record['season'] = season
                    record['tournament'] = division
                    record['result'] = {team_sk : {'w': wins, 'l': lows, 'pct': pct, 'gb': gb_s, 'home': home, \
                                            'road': road, 'l10': l10, 'strk': strk, 'rank' : count}}
                    yield record
        elif response.meta['standings_type'] == 'conference':
            season = extract_data(hxs, '//div[@class="mod-content"]//h1//text()')
            season = season.split('-')[1].strip()

            nodes = get_nodes(hxs, '//div[@class="mod-content"]//table[@class="tablehead"]//tr[@class="colhead"]')
            for node in nodes:
                leagues = extract_data(node, './/td[@align="left"]/text()')
                details = get_nodes(node, './/following-sibling::tr[contains(@class, "team")]')
                if "AMERICAN" in leagues:
                    conference_ = "AL"
                elif "NATIONAL" in leagues:
                    conference_ = "NL"
                count = 0
                for det in details[:15]:
                    count = count + 1
                    team_sk = extract_data(det, './td/a/@href').split('/')[-2]
                    team_sk = team_sk.upper()
                    wins = extract_data(det, './td[2]/text()')
                    lows = extract_data(det, './td[3]/text()')
                    pct = extract_data(det, './td[4]/text()')
                    gb_s = extract_data(det, './td[5]/text()')
                    home = extract_data(det, './td[6]/text()')
                    road = extract_data(det, './td[7]/text()')
                    l10  = extract_data(det, './td[12]/text()')
                    strk = extract_data(det, './td[11]/text()')
                    if "Lost" in strk:
                        strk = strk.replace('Lost  ', 'L')
                    elif "Won" in strk:
                        strk = strk.replace('Won  ', 'W')
                    record  =  SportsSetupItem()
                    record['participant_type']  = 'team'
                    record['result_type']      = 'group_standings'
                    record['source'] =  'MLB'
                    record['affiliation'] = 'mlb'
                    record['season'] = season
                    record['tournament'] = conference_
                    record['result'] = {team_sk : {'w': wins, 'l': lows, 'pct': pct, 'gb': gb_s, 'home': home, \
                                            'road': road, 'l10': l10, 'strk': strk, 'rank' : count}}
                    yield record
        elif response.meta['standings_type'] == 'league':
            season = extract_data(hxs, '//div[@class="mod-content"]//h1//text()')
            season = season.split('-')[1].strip()
            nodes = get_nodes(hxs, '//div[@class="mod-content"]//table[@class="tablehead"]//tr[@class="colhead"]')
            for node in nodes:
                details = get_nodes(node, './/following-sibling::tr[contains(@class, "team")]')
                count = 0
                for det in details:
                    count = count + 1
                    team_sk = extract_data(det, './td/a/@href').split('/')[-2]
                    team_sk = team_sk.upper()
                    wins = extract_data(det, './td[2]/text()')
                    lows = extract_data(det, './td[3]/text()')
                    pct = extract_data(det, './td[4]/text()')
                    gb_s = extract_data(det, './td[5]/text()')
                    home = extract_data(det, './td[6]/text()')
                    road = extract_data(det, './td[7]/text()')
                    l10  = extract_data(det, './td[12]/text()')
                    strk = extract_data(det, './td[11]/text()').strip()
                    if "Lost" in strk:
                        strk = strk.replace('Lost  ', 'L')
                    elif "Won" in strk:
                        strk = strk.replace('Won  ', 'W')
                    record  =  SportsSetupItem()
                    record['participant_type']  = 'team'
                    record['result_type']      = 'tournament_standings'
                    record['source'] =  'MLB'
                    record['affiliation'] = 'mlb'
                    record['season'] = season
                    record['tournament'] = "MLB Baseball"
                    record['result'] = {team_sk : {'w': wins, 'l': lows, 'pct': pct, 'gb': gb_s, 'home': home, \
                                            'road': road, 'l10': l10, 'strk': strk, 'rank' : count}}
                    yield record

