from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, get_nodes
import re
import datetime
import urllib2
import MySQLdb

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = con.cursor()
    return con, cursor


def add_source_key(entity_id, _id):
    con, cursor = create_cursor()
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now())"
        values = (entity_id, 'participant', 'MLB', _id)
        cursor.execute(query, values)


def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    return pl_id

def check_player(sourcekey):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY% (sourcekey))
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
    else:
        pl_exists = False

    return pl_exists
    con.close()

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="MLB" and source_key= "%s"'

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

GAME = 'baseball'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
GENDER = 'male'
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''


class MVPSpider(VTVSpider):
    name = "ml_players"
    start_urls = []

    def start_requests(self):
        urls = ['http://m.mlb.com/awards/history-winners/?award_id=NLMVP', 'http://m.mlb.com/awards/history-winners/?award_id=ALMVP' \
                'http://m.mlb.com/awards/history-winners/?award_id=NLCY', 'http://m.mlb.com/awards/history-winners/?award_id=ALCY']
        for url in urls:
            yield Request(url, callback=self.parse)

    def parse(self, response):
        hxs = Selector(response)
        pl_nodes = get_nodes(hxs, './/table[contains(@class, "data awards-list")]/tbody/tr')

        for pl_node in pl_nodes:
            year = extract_data(pl_node, './td[1]/text()')
            player_url = extract_data(pl_node, './td/a/@href')
            player_sk = player_url.split('/')[-1]
            pl_link = "http://m.mlb.com" + player_url
            player_name = extract_data(pl_node, './td/a/text()')
            if player_name in ['Larry Walker', 'Jeff Bagwell']:
                continue
            yield Request(pl_link, callback=self.parse_next, meta = {'player_sk': player_sk, 'player_name': player_name})

    def parse_next(self, response):
        hxs = Selector(response)
        height = extract_data(hxs, '//div[@class="player-attributes"]//ul//li[@class="detail-height"]//text()').strip()
        weight = extract_data(hxs, '//div[@class="player-attributes"]//ul//li[@class="detail-weight"]/text()').strip()
        player_name = response.meta['player_name']
        sourcekey = response.meta['player_sk']
        player_link = response.url
        dob = ''
        b_place = ''
        main_role = ''
        age = ''
        img = extract_data(hxs, '//div[@class="player-profile-details"]//img//@src')
        if not img:
            img = 'http://mlb.mlb.com/images/players/mugshot/ph_%s.jpg' %(sourcekey)
        con, cursor = create_cursor()
        pl_exists = check_player(sourcekey)
        if pl_exists == False:
            pts_id = check_title(player_name)
            if pts_id:
                add_source_key(str(pts_id[0]), sourcekey)
                print "source_layer_title"

            else:
                print "add_players"
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, player_name, AKA, GAME, PAR_TYPE, img, \
                      BASE_POP, player_link, LOC)
                cursor.execute(PAR_QUERY, values)

                values = (next_id, DEBUT, main_role, ROLES, GENDER, \
                      age, height, weight, dob, b_place, SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)

                add_source_key(next_id, sourcekey)
                con.close()

