import datetime
import re
import MySQLdb
from vtvspider import VTVSpider, \
extract_data, get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector

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
        values = (entity_id, 'participant', 'women_cycling', _id)

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
entity_type="participant" and source="women_cycling" and source_key= "%s"'

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

class UCIPlayerss(VTVSpider):
    name = "uci_playerss"
    allowed_domains = []
    start_urls = ['http://women.cyclingfever.com/riders.html']

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//div[@class="blokje"]//a[contains(@href, "riders.html")]')
        for node in nodes:
            team_link = extract_data(node, './/@href')
            team_link = "http://women.cyclingfever.com/" + team_link
            yield Request(team_link, callback=self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//table[@class="tab100"]//tr//td//a')
        for node in pl_nodes:
            pl_link = extract_data(node, './/@href')
            pl_link = "http://women.cyclingfever.com/" + pl_link
            yield Request(pl_link, callback=self.parse_players)

    def parse_players(self, response):
        sel = Selector(response)
        pl_img = extract_data(sel, '//tr[@valign="top"]//td//center//img//@src')
        pl_name = extract_list_data(sel, '//table[@class="tab100"]//tr//td[3]//text()')
        pl_name = pl_name[0].strip().replace('  ', ' ')
        #player_sk = "".join(re.findall('\d+', response.url))
        player_sk   = pl_name.replace(' ', '_').lower()
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
                age = ''
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


