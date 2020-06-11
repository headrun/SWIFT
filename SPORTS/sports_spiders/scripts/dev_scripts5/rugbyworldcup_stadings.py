from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider

true = True
false = False
null = ''

class RugbyWCupStandings(VTVSpider):
    name = "rugby_worldcup_stadings"
    allowd_domains = []
    start_urls = []

    def start_requests(self):
        top_url = "http://cmsapi.pulselive.com/rugby/event/1238/standings.json"
        yield Request(top_url, callback  = self.parse, meta = {})

    def parse(self, response):
        raw_data = response.body
        data = eval(raw_data)
        record = SportsSetupItem()
        season = data.get('event', '').get('label', '')
        season = season.split(' ')[-1]
        nodes = data.get('tables')
        for node in nodes:
            group_name = node.get('label', '')
            group_name = "Rugby World Cup " + group_name
            data_ = node.get('entries')
            count = 0
            for details in data_:
                count = count + 1
                team_sk = details.get('team', '').get('name', '')
                played  = details.get('played', '')
                won     = details.get('won', '')
                lost    = details.get('lost', '')
                drawn   = details.get('drawn', '')
                pts_for = details.get('pointsFor', '')
                pts_ag  = details.get('pointsAgainst', '')
                tries_for = details.get('triesFor', '')
                tries_ag = details.get('triesAgainst', '')
                bonus_pt = details.get('bonusPoints', '')
                points   = details.get('points', '')
                record['participant_type']  = 'team'
                record['result_type']      = 'group_standings'
                record['source'] =  'espn_rugby'
                record['affiliation'] = 'irb'
                record['season'] = season
                record['tournament'] = group_name
                record['result'] = {team_sk : {'w': won, 'l': lost, \
                                    'd': drawn, 'p': played, 'pf': pts_for,
                                    'pa': pts_ag, 'tf': tries_for, \
                                    'ta': tries_ag, 'rank' : count,
                                    'pts': points, 'bpts': bonus_pt}}
                yield record

