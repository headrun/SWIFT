import datetime
import time
import re
import MySQLdb
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

DOMAIN_LINK = "http://www.rugby-league.com"


ROLES_DICT = {'Back Row': 'Back row', 'Half Back': 'Half-back',  \
              'Outside Back': 'Outside back', \
              'Fly Half': 'Fly-half', 'Winger': 'Wing'}

def create_cursor():
    #con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
    cursor = con.cursor()
    return con, cursor


def check_player(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    con.close()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id


def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'superleague_rugby', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, display_title, short_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="superleague_rugby" and source_key= "%s"'

GENDER = "male"
GAME = 'rugby league'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''

class SuperLeaguePlayers(VTVSpider):
    name = "superleague_players"
    allowed_domains = []
    start_urls = ['http://www.rugby-league.com/superleague/stats/club_stats']


    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//table[contains(@class, "fullstattable")]//tr//td//a[contains(@href, "/club")]')
        for team_node in team_nodes:
            team_url = extract_data(team_node, './/@href')
            if "http" not in team_url:
                team_url = DOMAIN_LINK + team_url
            yield Request(team_url, callback=self.parse_next, meta = {})

    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//div[@class="container"]//div[@class="card player"]//a')
        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/@href')
            if "http" not in pl_link:
                pl_link = DOMAIN_LINK +pl_link
            yield Request(pl_link, callback = self.parse_players, meta = {})


    def parse_players(self, response):
        sel = Selector(response)
        player_title = extract_data(sel, '//div[@class="player-info"]//h1//text()').strip()
        pl_role   = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "Position:")]//following-sibling::text()').title()
        player_image = extract_data(sel, '//div[@class="col-md-6 col-lg-4 m-b-3"]//img//@src')
        player_sk = "".join(re.findall(r'\d+', response.url))
        pl_height = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "Height:")]//following-sibling::text()')
        pl_dob = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "DOB:")]//following-sibling::text()')
        pl_weight = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "Weight:")]//following-sibling::text()')
        pl_age = extract_data(sel, '//div[@class="player-info"]//h2//span[contains(text(), "Age:")]//following-sibling::text()')

        pl_role = pl_role.strip().title()

        if ROLES_DICT.get(pl_role, ''):
            pl_role = ROLES_DICT.get(pl_role, '')

        if "Unknown" in pl_age:
            pl_age = ''
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(player_sk)
        if pl_exists == False:
            pts_id = check_title(player_title)
            if pts_id:
                print "source key", player_title
                add_source_key(str(pts_id[0]), player_sk)
            else:
                print "add player", player_title
                age = pl_age.strip()
                height = pl_height.strip()
                if height != '0':
                    height = height + " cm"
                else:
                    height = ''
                weight = pl_weight.strip()
                if weight !='0':
                    weight = weight + " kg"
                else:
                    weight = ''
                dob = pl_dob.strip()
                try:
                    dob = datetime.datetime.strptime(dob, '%d/%m/%y').strftime('%Y-%m-%d %H:%M:%S')
                except:
                    dob = ''

                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)
                values = (next_id, next_gid, player_title, AKA, GAME, PAR_TYPE, player_image, \
                      BASE_POP, response.url, LOC)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, pl_role, ROLES, GENDER, \
                      age, height, weight, dob, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, player_sk)
                con.close()

