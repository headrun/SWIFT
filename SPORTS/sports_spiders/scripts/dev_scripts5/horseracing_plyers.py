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
    #con = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP")
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
        values = (entity_id, 'participant', 'horseracing_nation', _id)

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
entity_type="participant" and source="horseracing_nation" and source_key= "%s"'


GAME = 'horse racing'
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


class HorseRacingPlayers(VTVSpider):
    name            = "horseracing_players"
    allowed_domains = ["horseracingnation.com"]
    #start_urls      = ['http://www.horseracingnation.com/race/2014_Breeders_Cup_Classic']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Kentucky_Derby', \
                       'http://www.horseracingnation.com/race/2015_Preakness_Stakes', \
                       'http://www.horseracingnation.com/race/2015_Florida_Derby']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Stewards_Cup', \
                        'http://www.horseracingnation.com/race/2015_Hong_Kong_Gold_Cup', \
                        'http://www.horseracingnation.com/race/2015_Randwick_Guineas', \
                        'http://www.horseracingnation.com/race/2015_Rosehill_Guineas']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Kings_Stand_Stakes',
                       'http://www.horseracingnation.com/race/2015_Queen_Anne_Stakes',
                        'http://www.horseracingnation.com/race/2015_St_Jamess_Palace_Stakes',
                        'http://www.horseracingnation.com/race/2015_Ascot_Gold_Cup',
                        'http://www.horseracingnation.com/race/2015_Prince_Of_Waless_Stakes',
                        'http://www.horseracingnation.com/race/2015_Diamond_Jubilee_Stakes',
                        'http://www.horseracingnation.com/race/2015_Coronation_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Coventry_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Queen_Mary_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Duke_of_Cambridge_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Ribblesdale_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Norfolk_Stakes' \
                        'http://www.horseracingnation.com/race/2015_King_Edward_VII_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Hardwicke_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Jersey_Stakes', \
                         'http://www.horseracingnation.com/race/2015_Tercentenary_Stakes', \
                         'http://www.horseracingnation.com/race/2015_Queens_Vase', \
                         'http://www.horseracingnation.com/race/2015_Albany_Stakes']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Norfolk_Stakes']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Albany_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Queens_Vase', \
                        'http://www.horseracingnation.com/race/2015_Coronation_Stakes', \
                        'http://www.horseracingnation.com/race/2015_King_Edward_VII_Stakes']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Diamond_Jubilee_Stakes', \
                        'http://www.horseracingnation.com/race/2015_Hardwicke_Stakes']

    start_urls      = ['http://www.horseracingnation.com/race/2015_English_St_Leger']
    start_urls      = ['http://www.horseracingnation.com/race/2015_Breeders_Cup_Distaff', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Classic', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Juvenile', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Turf', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Sprint', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Dirt_Mile', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Turf_Sprint', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Filly_and_Mare_Sprint', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Juvenile_Turf', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Juvenile_Fillies', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Juvenile_Fillies_Turf', \
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Mile',
                    'http://www.horseracingnation.com/race/2015_Breeders_Cup_Filly_and_Mare_Turf']
    #start_urls      = ['http://www.horseracingnation.com/race/2015_English_St_Leger']

    '''def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="adnav"]/a')
        for node in nodes:
            link = extract_data(node, './/@href')
            if "superscreener.com" in link:
                continue
            else:
                yield Request(link , callback=self.parse_player_details, meta = {})'''

    def parse(self, response):
        hxs = Selector(response)
        GENDER = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Age/Sex:")]\
                               /following-sibling::div[@class="value"]/text()').replace('\r\n', '')
        if "M" in GENDER:
            GENDER = "male"
        elif "F" in GENDER:
            GENDER = "female"
        player_nodes = get_nodes(hxs, '//table/tr//td//span[@class="horse-name-link"]/a')
        for node in player_nodes:
            player_link = extract_data(node, './@href')
            player_sk   = player_link.split('/')[-1]
            player_title = extract_data(node, './text()')
            yield Request(player_link, callback=self.parse_player, meta = {'player_sk': player_sk, 'player_title': player_title, 'GENDER': GENDER})

    def parse_player(self, response):
        hxs = Selector(response)
        player_title = response.meta['player_title']
        player_sk   = response.meta['player_sk']
        GENDER = response.meta['GENDER']
        player_image  = extract_data(hxs, '//div[@class="bio-silks"]//img//@src')
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
                height = ''
                weight = ''
                dob = ''
                pos = ''
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
