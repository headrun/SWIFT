import datetime
import time
import re
import MySQLdb
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = con.cursor()
    return con, cursor


def check_player(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

def add_source_key(entity_id, _id):
    con, cursor = create_cursor()
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'espn_ncaa-ncf', _id)

        cursor.execute(query, values)


def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
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
entity_type="participant" and source="espn_ncaa-ncf" and source_key= "%s"'


GAME = 'football'
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
GENDER = "male"


class HeismanPlayers(VTVSpider):
    name            = "heisman_players"
    allowed_domains = []
    start_urls      = ['http://espn.go.com/college-football/awards/_/id/9']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//table[@class="tablehead"]//tr[contains(@class, "row")]')
        for node in nodes:
            year  = extract_data(node, './/td[1]//text()')
            player_title = extract_data(node, './/td[2]//text()')
            player_sk = extract_data(node, './/td[2]//a//@href')
            if player_sk:
                player_sk = player_sk.split('/')[-2]
            if not player_sk:
                player_sk = extract_data(node, './/td[2]//text()')
            con, cursor = create_cursor()
            pl_exists, pl_id = check_player(player_sk)
            if pl_exists == False:
                pts_id = check_title(player_title)
                if pts_id:
                    add_source_key(str(pts_id[0]), player_sk)
                    print "player_title"
                else:
                    age = ''
                    height = ''
                    weight = ''
                    dob = ''
                    pos = ''
                    player_image = ''
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
