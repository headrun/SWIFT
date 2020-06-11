import re
from datetime import datetime
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider import extract_data, get_nodes

ALLOWED_TOURNMANETS = [ 'UEFA Champions League', 'UEFA Europa League']

def clean_text(data):
    data = data.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').strip()
    data = data.rstrip().lstrip().strip()
    return data

class UEFAMajorLeaguesRoster(VTVSpider):
    name = 'uefa_major_league_roster'
    allowed_domains = ['www.uefa.com']
    domain_url = "http://www.uefa.com"
    year = datetime.now().year + 1
    start_urls = []
    participants = {}

    def start_requests(self):
        top_urls = ['http://www.uefa.com/uefaeuropaleague/season=%s/clubs/index.html',
                  'http://www.uefa.com/uefachampionsleague/season=%s/clubs/index.html']
        for turl in top_urls:
            url = turl % str(self.year)
            yield Request(url, callback = self.parse, meta = {})

    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        team_links = get_nodes(sel, '//ul[@class="clubList clearfix"]//li//a[contains(@href, "club")]')
        for tlink in team_links:
            team_url       = extract_data(tlink, './@href')
            team_name = extract_data(tlink, './/text()')

            if team_url and "http" not in team_url:
                team_url = self.domain_url + team_url
                if "http://www.uefa.com/uefaeuropaleague/season=2015/clubs/club=64272/index.html" in team_url:
                    continue
                yield Request(team_url, callback=self.parse_team_details, \
                            meta = {'team_name' : team_name, 'record': record})

    def parse_team_details(self, response):
        sel = Selector(response)
        record = response.meta['record']

        team_id   = ''
        if "club" in response.url:
            team_id = re.findall('/club=(\d+)/', response.url)[0]
        else:
            team_id = re.findall('/team=(\d+)/', response.url)[0]

        players_links  = get_nodes(sel, '//div[@id="SquadList"]//tr[contains(@class, "player")]')
        last_node = players_links[-1]
        for plink in players_links[::1]:
            terminal_crawl = False
            if plink == last_node:
                terminal_crawl = True
            player_url       = extract_data(plink, './/td//a[contains(@href, "player")]/@href')
            if player_url and "http" not in player_url:
                player_url = self.domain_url + player_url
                yield Request(player_url, callback = self.parse_details, \
                meta = {'team_id' : team_id, 'terminal_crawl': terminal_crawl, 'record': record})

    def parse_details(self, response):
        sel = Selector(response)
        record = response.meta['record']
        meta = response.meta
        player_sk = re.findall('player=(\d+)', response.url)[0]
        player_sk = 'PL' + player_sk
        player_number = extract_data(sel, '//ul[@class="innerText"]//li[span[contains(text(), "Squad")]]/text()')

        main_role = extract_data(sel, '//ul//li[contains(text(),"Position")]//text()')
        main_role = re.findall('Position :(.*)', main_role)
        role = ''
        if main_role:
           role  = main_role[0].strip()
        players = {player_sk : {'player_role' : role, 'season' : "2014-15", \
        'status' : 'active', 'player_number': player_number}}

        self.participants.setdefault(meta['team_id'], {}).update(players)
        record['result_type'] = 'roster'
        record['source'] = 'uefa_soccer'
        record['season'] = '2014-15'
        record['participants'] = self.participants

        if response.meta['terminal_crawl']:
            yield record

