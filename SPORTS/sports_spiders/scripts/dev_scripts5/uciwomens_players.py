import datetime
import time
import re
import MySQLdb
from vtvspider import VTVSpider, \
extract_data, get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector

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
        values = (entity_id, 'participant', 'uciworldtour_cycling', _id)

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
entity_type="participant" and source="uciworldtour_cycling" and source_key= "%s"'

GAME = 'cycling'
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
GENDER = "female"

DOMAIN = "http://www.procyclingstats.com/"
class UCIWomensPlayers(VTVSpider):
    name = "uci_players"
    allowed_domains = []
    start_urls = ['http://www.procyclingstats.com/teams.php']
    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//div//a[@class="BlackToRed"]')
        for node in team_nodes[39:77]:
            team_url = extract_data(node, './/@href')
            team_name = extract_data(node, './/text()')
            team_url = DOMAIN+team_url
            yield Request(team_url, callback=self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//div//a[contains(@href, "rider")]')
        for node in pl_nodes:
            player_link = extract_data(node, './/@href')
            pl_link     = DOMAIN +player_link
            yield Request(pl_link, callback=self.parse_players)


    def parse_players(self, response):
        sel = Selector(response)
        #pl_name = extract_data(sel, '//div[@class="race_header"]//h1/text()')
        #pl_name = pl_name.encode("utf8").replace('\xc2\xa0\xc2\xbb', '').strip()
        pl_name   = response.url.split('/')[-1].replace('_', ' ')
        player_sk = response.url.split('/')[-1].lower()
        if len(player_sk.split('_'))== 2:
            player_sk = player_sk.split('_')[-1] + "_" + player_sk.split('_')[0]
        elif len(player_sk.split('_'))== 3:
            player_sk = player_sk.split('_')[1] + "_" + player_sk.split('_')[-1] + "_" + player_sk.split('_')[0]
        pl_img  = extract_data(sel, '//a[contains(@href, "rider")]//img//@src')
        pl_img  = DOMAIN + pl_img
        age     = extract_list_data(sel,  '//span//text()')[4].strip()
        age     = age.split('(')[-1].replace(')', '')
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(player_sk)
        if pl_exists == False:
            pts_id = check_title(pl_name)
            if pts_id:
                print "source key", pl_name
                add_source_key(str(pts_id[0]), player_sk)
            else:
                print "add player", pl_name
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

                values = (next_id, next_gid, pl_name, AKA, GAME, PAR_TYPE, pl_img, \
                      BASE_POP, response.url, LOC)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, pos, ROLES, GENDER, \
                      age, height, weight, dob, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, player_sk)
                con.close()

