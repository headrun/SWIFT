import re
import time
import MySQLdb
import requests
from datetime import datetime
from vtvspider_dev import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_dev import log, get_utc_time
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import extract_data, get_nodes, extract_list_data

ALLOWED_TOURNMANETS = [ 'UEFA Champions League', 'UEFA Europa League']

LEAGUES             = [ 'aut','eng','den','por','sco','ger','gre','esp','sui','tur',
                        'rus','fra','bel','swe','ned','ita', 'rou', 'ukr', 'pol']


LEAGUES = ['swe']

ROLES_DICT = {'Midfield': 'Midfielder',
              'Right Back': 'Right back',
              'Goalkeepers': 'Goalkeeper',
              'Defenders': 'Defender',
              'Midfielders': 'Midfielder',
              'Forwards': 'Forward'}

def clean_text(data):
    data = data.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').strip()
    data = data.rstrip().lstrip().strip()
    return data

class UEFALeaguesRoster(VTVSpider):
    name = 'uefa_leagues_roster'
    allowed_domains = ['www.uefa.com']
    domain_url = "http://www.uefa.com/"
    start_urls = []
    roster_details = {}

    def start_requests(self):
        top_url = 'http://www.uefa.com/memberassociations/association=%s/index.html'
        for league in LEAGUES:
            url = top_url % league
            yield Request(url, callback=self.parse, meta = {})

    @log
    def parse(self, response):
        sel = Selector(response)
        season = extract_data(sel, '//div[@class="t_standings"]//h3[contains(@class, "bigTitle")]/text()')
        if season:
            ssn = "".join(re.findall(r' (\d+/\d+)', season)) or \
                  "".join(re.findall(r' (\d+-\d+)', season))
            if '/' in season or '-' in ssn:
                season = ssn.replace('/', '-')
            else:
                season = "".join(re.findall(r' (\d+)', season))

        team_links = get_nodes(sel, '//div[@class="t_standings"]//a[contains(@href, "teams")]')
        for link in team_links:
            team_url = extract_data(link, './@href')
            if not "http" in team_url and team_url:
                team_url = self.domain_url + team_url
            yield Request(team_url, callback=self.parse_team_details, meta = {'season' : season})

    @log
    def parse_team_details(self, response):
        sel = Selector(response)
        team_id   = ''
        if "club" in response.url:
            team_id = re.findall('/club=(\d+)/', response.url)[0]
        else:
            team_id = re.findall('/team=(\d+)/', response.url)[0]

        team_name = extract_data(sel, '//h1[@class="bigTitle"]//text()')
        pl_links = get_nodes(sel, '//table//tr[@class="player"]')
        root_nodes = get_nodes(sel, '//div[h2[@class="medTitle"]]')
        for root_node in root_nodes:
            pl_links = get_nodes(root_node, './/table//tr[@class="player"]')
            pl_role = extract_data(root_node, './h2/text()')
            for node in pl_links[::1]:
                pl_link = extract_data(node, './td/a/@href')
                pl_name = extract_data(node, './td[@class="playername l"]//text()')
                pl_number = extract_list_data(node, './td[@class="number"]//text()')[0]
                if not pl_link:
                    pl_sk = pl_name.lower().replace(' ', '-').encode('utf-8')
                else:
                    pl_sk = "PL" + re.findall('player=(\d+)', pl_link)[0]
                    if "http" not in pl_link:
                        pl_link = self.domain_url + pl_link
                    data = requests.get(pl_link).text
                    position = re.findall(r"Position : [\w]*", data)
                    if position:
                        pl_role = re.findall('Position :(.*)', position[0])[0].strip()
                if ROLES_DICT.get(pl_role, ''):
                    pl_role = ROLES_DICT.get(pl_role)

                player = {pl_sk: {'player_number': pl_number,
                                  'season': response.meta['season'],
                                  'status': 'active', 'player_role': pl_role}}
                self.roster_details.setdefault(team_id, {}).update(player)

        record = SportsSetupItem()
        record['result_type'] = 'roster'
        record['source'] = 'uefa_soccer'
        record['season'] = response.meta['season']
        record['participants'] = self.roster_details
        import pdb;pdb.set_trace()
        yield record

