import re
import time
import MySQLdb
import urllib
from datetime import datetime
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider import log, get_utc_time
from sports_spiders.items import SportsSetupItem
from vtvspider import extract_data, get_nodes, extract_list_data

class CWPlayers(VTVSpider):
    name = 'cwplayers'
    player_domain_url = 'http://results.glasgow2014.com/'
    tournament = 'Commonwealth - %s'
    start_urls = []

    #conn = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    con = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
    cursor = conn.cursor()

    PLAYER_SUBMISSION_URL = """http://10.4.18.34/cgi-bin/add_players_bak.py?name=%s&aka=%s&game=%s
                            &participant_type=%s&img=%s&bp=%s&ref=%s&loc=%s&debut=%s&main_role=%s
                            &roles=%s&gender=%s&age=%s&height=%s&weight=%s&birth_date=%s&birth_place=%s
                            &salary_pop=%s&rating_pop=%s&weight_class=%s&marital_status=%s
                            &participant_since=%s&competitor_since=%s&src=%s&sk=%s&tou=%s&season=%s
                            &status=%s&st_remarks=%s&standing=%s&seed=%s&action=submit"""

    def check_player(self, player):
        query = 'select entity_id from sports_source_keys where entity_type = %s and source = %s and source_key = %s'
        values = ('participant', 'cw_glasgow', player)
        self.cursor.execute(query, values)
        player_id = self.cursor.fetchone()
        if player_id:
            player_id = player_id[0]
        return player_id

    def start_requests(self):
        sports_call_signs = {#'Cycling' : 'CT', 'Gymnastics' : 'GR',\
                             #'Judo' : 'JU', 'Swimming' : 'SW', \
                             #'Triathlon' : 'TR', 'Weightlifting': 'WL', \
                             #'Badminton' : 'BD', 'Boxing' : 'BX', 'Diving' : 'DV', \
                             #'Gymnastics' : 'GA', 'Lawn Bowls' : 'LB', \
                             #'Netball' : 'NB', 'Shooting' : 'SH', \
                             #'Squash' : 'SQ', 'Table Tennis' : 'TT', \
                             #'Wrestling' : 'WR', 'Cycling' : 'CR', 'Cycling' : 'CM'}
                             'Athletics' : 'AT'}
        top_url = 'http://widgets.glasgow2014.com/WSEARCHPART/WSEARCHPART_%s.html'
        for sport, call_sign in sports_call_signs.iteritems():
            tournament = self.tournament % sport
            url = top_url % call_sign
            yield Request(url, callback=self.parse, meta = {'tournament' : tournament, 'sport' : sport})

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//table[@id="athletes_table"]//tr[@id="rowAthlete"]')
        for node in nodes:
            player_url = extract_data(node, './td/a/@href')
            if "http" not in player_url:
                player_url = self.player_domain_url + player_url
            yield Request(player_url, callback = self.parse_player_details, \
                          meta = {'tournament' : response.meta['tournament'], 'sport' : response.meta['sport']})

    def parse_player_details(self, response):
        sel = Selector(response)
        name = extract_data(sel, '//h1[@class="head_title"]/text()')
        image = extract_data(sel, '//figure/img/@src')
        if not "http" in image:
            image = self.player_domain_url + image
        location = extract_data(sel, '//article[@class="athnation"]//a/text()')
        gender = extract_data(sel, '//article[@class="athsecdata"]/dl/dt[contains(text(), "Gender")]/following-sibling::dd/text()')
        age = extract_data(sel, '//article[@class="athsecdata"]/dl/dt[contains(text(), "Age")]/following-sibling::dd[1]/text()')
        dob = extract_data(sel, '//article[@class="athsecdata"]/dl/dt[contains(text(), "Date of Birth")]/following-sibling::dd[1]/text()')
        sk = re.findall(r'PIC/(.*).jpg', image)[0]
        if response.meta['sport'] == 'Swimming':
            response.meta['tournament'] = 'Commonwealth - Swimming'
            response.meta['sport'] = 'aquatics'

        if response.meta['sport'] == 'Diving':
            response.meta['tournament'] = 'Commonwealth - Diving'
            response.meta['sport'] = 'aquatics'

        aka = loc = dbt = mrl = role = height = weight = bplace = \
        sal_pop = rpop = wclass = mstatus = psince = csince = status = \
        status_remarks = standing = seed = ''
        player_values = (name, aka, response.meta['sport'], 'player', image, 200, \
                         response.url, loc, dbt, mrl, role, gender, age, height, \
                         weight, dob, bplace, sal_pop, rpop, wclass, mstatus, \
                         psince, csince, 'cw_glasgow', sk, response.meta['tournament'], \
                         '2014', status, status_remarks, standing, seed)
        player_id = self.check_player(sk)
        if not player_id:
            submission_url = (self.PLAYER_SUBMISSION_URL % player_values).replace('\n', '')
            urllib.urlopen(submission_url)
