from vtvspider_dev import VTVSpider, extract_data, get_nodes
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


class SuperRugbyRoster(VTVSpider):
    name = "superrugby_roster"
    allowed_domains = []
    start_urls = ['http://www.sanzarrugby.com/superrugby/teams/']
    participants = {}


    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//div[@id="teamlist"]//div[@class="teamlist-summary"]//ul//li//a')

        for tm_node in tm_nodes:
            tema_url = extract_data(tm_node, './/@href')
            season = tema_url.split('=')[2].split('&')[0].strip()
            url = "http://omo.akamai.opta.net/competition.php?feed_type=ru4&competition=205&season_id=%s&user=owv2&psw=wacRUs5U&jsoncallback=RU4_205_%s_t73" % (season, season)

            yield Request(url, callback = self.parse_next, \
                        meta = {'season': season})

    def parse_next(self, response):
        record      = SportsSetupItem()
        raw_data    = response.body.replace('RU4_205_2015_t73(', '').replace(')', '')
        data        = eval(raw_data)
        season      = response.meta['season']

        if data:
            tm_data = data.get('seasonstats', '').get('teams', '').get('team', '')

            for tm_dat in tm_data:
                tm_id = tm_dat.get('@attributes', '').get('team_id', '')
                pl_data = tm_dat.get('players', '').get('player', '')

                for pl_dat in pl_data:
                    pl_sk = pl_dat.get('@attributes', '').get('player_id', '')
                    pl_pos   = pl_dat.get('@attributes', '').get('regular_position', '')
                    pl_pos = pl_pos.replace('No.', 'Number').strip()
                    pl_no   = pl_dat.get('@attributes', '').get('position_id', '')

                    players = {pl_sk: {"player_role": pl_pos,
                        "player_number": pl_no,
                        "season": season, "status": "active", 'language': "ENG", \
                        "entity_type": "participant", "field_type": "description"}}

                    self.participants.setdefault(tm_id, {}).update(players)

                record['source'] = 'sanzar_rugby'
                record['season'] = season
                record['result_type'] = 'roster'
                record['participants'] = self.participants
                yield record
