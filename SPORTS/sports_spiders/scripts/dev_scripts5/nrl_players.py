import datetime
import time
import re
import MySQLdb
from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


def create_cursor():
    #con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
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
        values = (entity_id, 'participant', 'nrl_rugby', _id)

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
            participant_since, competitor_since, created_at, modified_at) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="nrl_rugby" and source_key= "%s"'


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

DOMAIN = 'http://www.nrl.com'

class NRLPlayers(VTVSpider):
    name ="nrl_players"
    allowed_domains = []
    start_urls = ['http://www.nrl.com/DrawResults/Statistics/PlayerStatistics/tabid/10877/Default.aspx']


    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//div[@class="nwTeam"]//a')
        for team_node in team_nodes:
            team_url = extract_data(team_node, './/@href')
            if team_url:
                team_url = DOMAIN + team_url
                yield Request(team_url, callback = self.parse_players)

    def parse_players(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//table[@class="clubPlayerStats__statsTable"]//tr//th')
        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/a//@href')
            if pl_link:
                pl_link = DOMAIN + pl_link
                yield Request(pl_link, callback = self.parse_players_details)


    def parse_players_details(self, response):
        sel = Selector(response)
        player_title = extract_data(sel, '//div[@class="playerProfile__container"]//h1[@class="pageTitle"]//text()')
        player_image = extract_data(sel, '//div[@class="playerProfile__headshot"]//img//@src')
        if player_image:
            player_image = DOMAIN + player_image
        pl_dob = extract_data(sel, '//ul//li//strong[contains(text(), "DOB")]//following-sibling::em//text()')
        pl_height = extract_data(sel, '//ul//li//strong[contains(text(), "Height")]//following-sibling::em//text()')
        pl_weight = extract_data(sel, '//ul//li//strong[contains(text(), "Weight")]//following-sibling::em//text()')
        pl_position = extract_data(sel, '//ul//li//strong[contains(text(), "Position")]//following-sibling::em//text()')
        player_sk = "".join(re.findall('playerid/(.*)/seasonid', response.url))
        if "Unknown" in pl_position:
            pl_position = ''
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(player_sk)
        if pl_exists == False:
            pts_id = check_title(player_title)
            if pts_id:
                print "source key", player_title
                add_source_key(str(pts_id[0]), player_sk)
            else:
                print "add player", player_title
                age = ''
                height = pl_height.strip()
                if height == '-':
                    height = ''
                weight = pl_weight.strip()
                if weight == '-':
                    weight = ''
                dob = pl_dob.strip()
                try:
                    dob = datetime.datetime.strptime(pl_dob, '%d/%m/%Y').strftime('%Y-%m-%d %H:%M:%S')
                except:
                    dob = ''
                pos = pl_position.strip().replace(' 1', '').replace(' 2', '')
                if pos == '-':
                    pos = ''
                if pos == "Interchange":
                    pos = "Prop"
                if "2nd Row" in pos:
                    pos = "Second-row"
                if "Halfback" in pos:
                    pos = "Half back"

                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)
                values = (next_id, next_gid, player_title, AKA, GAME, PAR_TYPE, player_image, \
                      BASE_POP, response.url, LOC)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, pos, ROLES, GENDER, \
                      age, height, weight, dob, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, player_sk)
                con.close()

